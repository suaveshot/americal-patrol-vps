#!/bin/bash

mkdir -p /app/data /app/data/call_intelligence/recordings

# ── Persist state files to Docker volume ────────────────────────────
# /app/ is rebuilt from git on every deploy. /app/data/ is a Docker volume
# that survives rebuilds. For each state file/dir we:
#   1. Create the target dir in the volume
#   2. Seed from the image copy on first deploy (if volume copy doesn't exist)
#   3. Remove the image copy and symlink to the volume copy
# This way Python code writes to the same paths but data lands in the volume.

persist_file() {
    local src="$1"   # path in image, e.g. /app/sales_pipeline/pipeline_state.json
    local dst="$2"   # path in volume, e.g. /app/data/sales_pipeline/pipeline_state.json
    mkdir -p "$(dirname "$dst")"
    # Seed: if volume copy doesn't exist but image has one, copy it over
    if [ ! -f "$dst" ] && [ -f "$src" ] && [ ! -L "$src" ]; then
        cp "$src" "$dst"
        echo "  Seeded $dst from image"
    fi
    # Symlink: remove image copy (or stale symlink) and point to volume
    rm -f "$src"
    ln -sf "$dst" "$src"
}

persist_dir() {
    local src="$1"   # dir in image, e.g. /app/sales_pipeline/learning
    local dst="$2"   # dir in volume, e.g. /app/data/sales_pipeline/learning
    mkdir -p "$dst"
    # Seed: if volume dir is empty but image has files, copy them over
    if [ -d "$src" ] && [ ! -L "$src" ] && [ "$(ls -A "$src" 2>/dev/null)" ]; then
        cp -rn "$src"/* "$dst"/ 2>/dev/null
        echo "  Seeded $dst from image"
    fi
    # Symlink: remove image dir and point to volume
    rm -rf "$src"
    ln -sf "$dst" "$src"
}

echo "Persisting state files to volume..."

# Sales Pipeline
persist_file /app/sales_pipeline/pipeline_state.json    /app/data/sales_pipeline/pipeline_state.json
persist_file /app/sales_pipeline/pipeline_drafts.json   /app/data/sales_pipeline/pipeline_drafts.json
persist_file /app/sales_pipeline/call_transcripts.json  /app/data/sales_pipeline/call_transcripts.json
persist_file /app/sales_pipeline/automation.log          /app/data/sales_pipeline/automation.log
persist_dir  /app/sales_pipeline/learning               /app/data/sales_pipeline/learning

# Watchdog
persist_file /app/watchdog/health_status.json  /app/data/watchdog/health_status.json
persist_file /app/watchdog/digest_state.json   /app/data/watchdog/digest_state.json

# Email Assistant learning (main state already in /app/data/)
persist_dir /app/email_assistant/learning  /app/data/email_assistant/learning

# Pipeline events (cross-pipeline event bus)
persist_dir /app/pipeline_events  /app/data/pipeline_events

# Call Intelligence config (rarely written, but save_config() exists)
persist_file /app/call_intelligence/config.json  /app/data/call_intelligence/config.json

# Usage Tracker (token usage logs + aggregator state)
persist_dir  /app/usage_logs                       /app/data/usage_logs
persist_file /app/usage_tracker/alert_state.json   /app/data/usage_tracker/alert_state.json

# Payment Guard (state, config, audit trail)
persist_file /app/payment_guard/guard_state.json   /app/data/payment_guard/guard_state.json
persist_file /app/payment_guard/config.json        /app/data/payment_guard/config.json
persist_file /app/payment_guard/audit_log.jsonl    /app/data/payment_guard/audit_log.jsonl

# Tenant config (active flag written by payment guard)
persist_file /app/tenant_config.json               /app/data/tenant_config.json

echo "State persistence ready."

# Decode OAuth tokens from environment variables (first run only)
if [ ! -f /app/patrol_automation/token.json ] && [ -n "$GOOGLE_TOKEN_B64" ]; then
    echo "$GOOGLE_TOKEN_B64" | base64 -d > /app/patrol_automation/token.json
    echo "Decoded token.json"
fi
if [ ! -f /app/patrol_automation/credentials.json ] && [ -n "$GOOGLE_CREDS_B64" ]; then
    echo "$GOOGLE_CREDS_B64" | base64 -d > /app/patrol_automation/credentials.json
    echo "Decoded credentials.json"
fi

# Persist container env vars to a file cron jobs can source.
# Without this, cron runs in a minimal environment and pipelines see
# empty ANTHROPIC_API_KEY / GHL_* / etc., failing validate_config().
python3 -c "
import os, shlex
skip = {'PATH','HOME','PWD','SHLVL','HOSTNAME','_','TERM','OLDPWD','LANG','LC_ALL'}
for k, v in os.environ.items():
    if k in skip or not k.replace('_','').isalnum():
        continue
    print(f'export {k}={shlex.quote(v)}')
" > /etc/container_env.sh
chmod 600 /etc/container_env.sh
echo "Wrote $(wc -l < /etc/container_env.sh) env vars to /etc/container_env.sh"

# Set up cron (UTC times -- 8 AM Pacific = 15:00 UTC)
# Each cron line sources /etc/container_env.sh so pipelines get the container env,
# then tees output to BOTH the on-disk log AND PID-1 stdout so
# Docker logs (VPS_getProjectLogsV1) captures every run. The canary line
# confirms cron itself is alive minute-by-minute.
cat > /etc/cron.d/ap << 'CRONEOF'
PATH=/usr/local/bin:/usr/bin:/bin
* * * * * root echo "[$(date -u +\%FT\%TZ)] cron alive" > /proc/1/fd/1
0 * * * * root . /etc/container_env.sh && cd /app && python3 -m sales_pipeline.run_pipeline --hourly 2>&1 | tee -a /var/log/ap-sales.log > /proc/1/fd/1
0 15 * * 1-5 root . /etc/container_env.sh && cd /app && python3 -m sales_pipeline.run_pipeline --daily 2>&1 | tee -a /var/log/ap-sales.log > /proc/1/fd/1
*/15 * * * * root . /etc/container_env.sh && cd /app && python3 -m sales_pipeline.transcribe_calls 2>&1 | tee -a /var/log/ap-transcribe.log > /proc/1/fd/1
*/5 * * * * root . /etc/container_env.sh && cd /app && python3 email_assistant/email_monitor.py 2>&1 | tee -a /var/log/ap-email.log > /proc/1/fd/1
15 * * * * root . /etc/container_env.sh && cd /app && python3 watchdog/watchdog.py 2>&1 | tee -a /var/log/ap-watchdog.log > /proc/1/fd/1
# Call Intelligence
5 * * * * root . /etc/container_env.sh && cd /app && python3 -m call_intelligence.run_ingestion 2>&1 | tee -a /var/log/ap-call-intel.log > /proc/1/fd/1
0 13 * * * root . /etc/container_env.sh && cd /app && python3 -m call_intelligence.sync_deals 2>&1 | tee -a /var/log/ap-call-intel.log > /proc/1/fd/1
# Payment Guard (9:05 AM Pacific = 16:05 UTC, after n8n QBO sync)
5 16 * * * root . /etc/container_env.sh && cd /app && python3 -m payment_guard.run_payment_guard 2>&1 | tee -a /var/log/ap-payment-guard.log > /proc/1/fd/1
# Usage Aggregator (11:00 PM Pacific = 06:00 UTC next day)
0 6 * * * root . /etc/container_env.sh && cd /app && python3 -m usage_tracker.aggregate_usage 2>&1 | tee -a /var/log/ap-usage.log > /proc/1/fd/1
CRONEOF
chmod 0644 /etc/cron.d/ap
cron

echo "AP automations running. Cron started."

# Keep alive with a simple sleep loop
while true; do sleep 3600; done
