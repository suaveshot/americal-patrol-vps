#!/bin/bash

mkdir -p /app/data /app/pipeline_events /app/data/call_intelligence/recordings

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
*/15 * * * * root . /etc/container_env.sh && cd /app && python3 email_assistant/email_monitor.py 2>&1 | tee -a /var/log/ap-email.log > /proc/1/fd/1
15 * * * * root . /etc/container_env.sh && cd /app && python3 watchdog/watchdog.py 2>&1 | tee -a /var/log/ap-watchdog.log > /proc/1/fd/1
# Call Intelligence
5 * * * * root . /etc/container_env.sh && cd /app && python3 -m call_intelligence.run_ingestion 2>&1 | tee -a /var/log/ap-call-intel.log > /proc/1/fd/1
0 13 * * * root . /etc/container_env.sh && cd /app && python3 -m call_intelligence.sync_deals 2>&1 | tee -a /var/log/ap-call-intel.log > /proc/1/fd/1
CRONEOF
chmod 0644 /etc/cron.d/ap
cron

echo "AP automations running. Cron started."

# Keep alive with a simple sleep loop
while true; do sleep 3600; done
