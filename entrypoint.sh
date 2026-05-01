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

# Win-Back
persist_file /app/win_back/winback_state.json  /app/data/win_back/winback_state.json
persist_file /app/win_back/automation.log       /app/data/win_back/automation.log

# Review Engine
persist_file /app/review_engine/review_state.json      /app/data/review_engine/review_state.json
persist_file /app/review_engine/competitor_data.json    /app/data/review_engine/competitor_data.json
persist_file /app/review_engine/automation.log          /app/data/review_engine/automation.log

# Social Media (state is per-platform rotation indexes + post history; image_library
# is the catalog of generated/uploaded images. Persist so a redeploy doesn't reset
# rotation back to slot 0 and re-post recent topics.)
persist_file /app/social_media_automation/social_state.json         /app/data/social_media_automation/social_state.json
persist_file /app/social_media_automation/image_library.json        /app/data/social_media_automation/image_library.json
persist_file /app/social_media_automation/link_tracker.json         /app/data/social_media_automation/link_tracker.json
persist_file /app/social_media_automation/hashtag_performance.json  /app/data/social_media_automation/hashtag_performance.json
persist_file /app/social_media_automation/automation.log            /app/data/social_media_automation/automation.log
persist_dir  /app/social_media_automation/media                     /app/data/social_media_automation/media

# GBP rotation index (used by social pipeline's GBP publisher)
persist_file /app/gbp_automation/gbp_state.json  /app/data/gbp_automation/gbp_state.json

# SEO state (alert dedup, last competitor scan, etc.)
persist_file /app/seo_automation/seo_state.json  /app/data/seo_automation/seo_state.json
persist_file /app/seo_automation/automation.log  /app/data/seo_automation/automation.log

# Blog state (topic rotation index, last_run, posts_published)
persist_file /app/blog_post_automation/blog_state.json   /app/data/blog_post_automation/blog_state.json
persist_file /app/blog_post_automation/automation.log    /app/data/blog_post_automation/automation.log
# blog_config.json is mutated by seo_automation/topic_updater.py (writes
# priority_topics + keyword_intelligence). Persist so SEO updates survive
# redeploys instead of resetting to the in-image (frozen) topics.
persist_file /app/blog_post_automation/blog_config.json  /app/data/blog_post_automation/blog_config.json

# Weekly Update (week-over-week deltas + last digest send)
persist_file /app/weekly_update/weekly_state.json  /app/data/weekly_update/weekly_state.json
persist_file /app/weekly_update/automation.log    /app/data/weekly_update/automation.log

# Guard Compliance (officer state + DCA BSIS CSV cache — refreshed monthly)
persist_file /app/guard_compliance/compliance_state.json  /app/data/guard_compliance/compliance_state.json
persist_file /app/guard_compliance/automation.log         /app/data/guard_compliance/automation.log
persist_dir  /app/guard_compliance/bsis_data              /app/data/guard_compliance/bsis_data

# QBR Generator (per-quarter generated PDFs + state)
persist_file /app/qbr_generator/qbr_state.json   /app/data/qbr_generator/qbr_state.json
persist_file /app/qbr_generator/automation.log   /app/data/qbr_generator/automation.log
persist_dir  /app/qbr_generator/output           /app/data/qbr_generator/output

# Patrol Automation (Morning Reports) — generated PDFs land in MORNING_DIR which
# both patrol_automation/main.py writes and Harbor Lights/hl_update.py reads.
# Persist as a dir so PDFs survive redeploys (Harbor Lights tracks 'processed_pdfs.json'
# to avoid re-processing).
mkdir -p "/app/Americal Patrol Morning Reports"
persist_dir  "/app/Americal Patrol Morning Reports"  /app/data/morning_reports
persist_file /app/patrol_automation/automation.log   /app/data/patrol_automation/automation.log

# Harbor Lights — Excel tracker + processed PDF log. Excel is the canonical
# data store the parking_audit reads to draft violation letters.
persist_file "/app/Harbor Lights/Harbor Lights Guest Parking UPDATED.xlsx"  "/app/data/harbor_lights/Harbor Lights Guest Parking UPDATED.xlsx"
persist_file "/app/Harbor Lights/processed_pdfs.json"  /app/data/harbor_lights/processed_pdfs.json
persist_file "/app/Harbor Lights/harbor_lights.log"    /app/data/harbor_lights/harbor_lights.log
persist_file "/app/Harbor Lights/parking_audit.log"    /app/data/harbor_lights/parking_audit.log

echo "State persistence ready."

# Decode OAuth tokens from environment variables (first run only)
if [ ! -f /app/patrol_automation/token.json ] && [ -n "$GOOGLE_TOKEN_B64" ]; then
    echo "$GOOGLE_TOKEN_B64" | base64 -d > /app/patrol_automation/token.json
    echo "Decoded token.json"
fi
# GBP token (same credentials, separate token for GBP API scopes)
mkdir -p /app/gbp_automation
if [ ! -f /app/gbp_automation/gbp_token.json ] && [ -n "$GBP_TOKEN_B64" ]; then
    echo "$GBP_TOKEN_B64" | base64 -d > /app/gbp_automation/gbp_token.json
    echo "Decoded gbp_token.json"
fi
# Symlink credentials.json to gbp_automation (reuses same OAuth app)
if [ -f /app/patrol_automation/credentials.json ] && [ ! -f /app/gbp_automation/credentials.json ]; then
    ln -sf /app/patrol_automation/credentials.json /app/gbp_automation/credentials.json
fi
if [ ! -f /app/patrol_automation/credentials.json ] && [ -n "$GOOGLE_CREDS_B64" ]; then
    echo "$GOOGLE_CREDS_B64" | base64 -d > /app/patrol_automation/credentials.json
    echo "Decoded credentials.json"
fi
# Social Media — Drive token (separate scope: drive.file for image upload)
if [ ! -f /app/social_media_automation/social_drive_token.json ] && [ -n "$SOCIAL_DRIVE_TOKEN_B64" ]; then
    echo "$SOCIAL_DRIVE_TOKEN_B64" | base64 -d > /app/social_media_automation/social_drive_token.json
    echo "Decoded social_drive_token.json"
fi
# SEO — GA4 + GSC token (separate scopes: analytics.readonly + webmasters.readonly)
if [ ! -f /app/seo_automation/seo_token.json ] && [ -n "$SEO_TOKEN_B64" ]; then
    echo "$SEO_TOKEN_B64" | base64 -d > /app/seo_automation/seo_token.json
    echo "Decoded seo_token.json"
fi
# SEO — Indexing API service account (separate auth path from user OAuth)
if [ ! -f /app/seo_automation/indexing_service_account.json ] && [ -n "$SEO_INDEXING_SA_B64" ]; then
    echo "$SEO_INDEXING_SA_B64" | base64 -d > /app/seo_automation/indexing_service_account.json
    echo "Decoded indexing_service_account.json"
fi
# Harbor Lights — Google Sheets OAuth token (spreadsheets scope).
# Replaces the local Excel-on-OneDrive write path. Mint via:
#   cd "Harbor Lights" && python auth_setup.py    # on Sam's PC
# then base64 the resulting sheets_token.json into SHEETS_TOKEN_B64.
if [ ! -f "/app/Harbor Lights/sheets_token.json" ] && [ -n "$SHEETS_TOKEN_B64" ]; then
    mkdir -p "/app/Harbor Lights"
    echo "$SHEETS_TOKEN_B64" | base64 -d > "/app/Harbor Lights/sheets_token.json"
    echo "Decoded sheets_token.json"
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
# Win-Back (weekly Monday 10 AM Pacific = 17:00 UTC)
0 17 * * 1 root . /etc/container_env.sh && cd /app && python3 -m win_back.run_winback 2>&1 | tee -a /var/log/ap-winback.log > /proc/1/fd/1
# Review Engine - Respond to reviews (daily 9 AM Pacific = 16:00 UTC)
0 16 * * * root . /etc/container_env.sh && cd /app && python3 review_engine/run_reviews.py --respond 2>&1 | tee -a /var/log/ap-reviews.log > /proc/1/fd/1
# Review Engine - Competitor monitoring (1st of month 8 AM Pacific = 15:00 UTC)
0 15 1 * * root . /etc/container_env.sh && cd /app && python3 review_engine/run_reviews.py --competitors 2>&1 | tee -a /var/log/ap-reviews.log > /proc/1/fd/1
# Social Media (Tue/Thu/Sat 10 AM Pacific = 17:00 UTC; GBP weekly-gated in code)
0 17 * * 2,4,6 root . /etc/container_env.sh && cd /app/social_media_automation && python3 run_social.py 2>&1 | tee -a /var/log/ap-social.log > /proc/1/fd/1
# Social Media calendar preview (Sun 8 PM Pacific = 03:00 UTC Mon)
0 3 * * 1 root . /etc/container_env.sh && cd /app/social_media_automation && python3 calendar_preview.py 2>&1 | tee -a /var/log/ap-social.log > /proc/1/fd/1
# Social Media engagement tracker (daily 10 AM Pacific = 17:00 UTC; pulls 48h-old post stats)
0 17 * * * root . /etc/container_env.sh && cd /app/social_media_automation && python3 engagement_tracker.py 2>&1 | tee -a /var/log/ap-social.log > /proc/1/fd/1
# SEO weekly analysis (Mon 7 AM Pacific = 14:00 UTC — must run BEFORE blog at 16:00 UTC)
0 14 * * 1 root . /etc/container_env.sh && cd /app/seo_automation && python3 run_seo.py 2>&1 | tee -a /var/log/ap-seo.log > /proc/1/fd/1
# SEO daily ranking alert (10 AM Pacific = 17:00 UTC)
0 17 * * * root . /etc/container_env.sh && cd /app/seo_automation && python3 alert_checker.py 2>&1 | tee -a /var/log/ap-seo.log > /proc/1/fd/1
# Blog Post (every other Mon 9 AM Pacific = 16:00 UTC, gated to even ISO weeks
# to match the original "every 2 weeks" Task Scheduler cadence — last run was
# week 16, 2026-04-20). Toggle to weekly by removing the [ -eq 0 ] guard.
0 16 * * 1 root . /etc/container_env.sh && [ $(($(date +\%V | sed 's/^0*//') % 2)) -eq 0 ] && cd /app/blog_post_automation && python3 run_blog.py 2>&1 | tee -a /var/log/ap-blog.log > /proc/1/fd/1
# Weekly Update digest (Fri 12 PM Pacific = 19:00 UTC; client-facing summary)
0 19 * * 5 root . /etc/container_env.sh && cd /app && python3 -m weekly_update.run_weekly_update 2>&1 | tee -a /var/log/ap-weekly.log > /proc/1/fd/1
# Guard Compliance (daily 6 AM Pacific = 13:00 UTC — guard card/cert expiry tracker)
0 13 * * * root . /etc/container_env.sh && cd /app && python3 -m guard_compliance.run_compliance 2>&1 | tee -a /var/log/ap-guard.log > /proc/1/fd/1
# QBR Generator (1st Mon of Jan/Apr/Jul/Oct, 9 AM Pacific = 16:00 UTC).
# Cron OR-s dom and dow when both are non-*, so we use dow=Mon + month limit
# and shell-guard the day-of-month to 1-7 to get "first Monday only".
0 16 * 1,4,7,10 1 root . /etc/container_env.sh && [ $(date +\%d | sed 's/^0*//') -le 7 ] && cd /app/qbr_generator && python3 run_qbr.py 2>&1 | tee -a /var/log/ap-qbr.log > /proc/1/fd/1
# ─── Morning Reports + Harbor Lights — DISABLED PENDING SAM VERIFICATION ───
# These send client-facing PDFs and parking violation drafts. Code + state are
# now in the container, but the cron lines stay commented until Sam manually runs:
#   docker exec americal-patrol-automations-1 python3 -m patrol_automation.main --check
#   docker exec americal-patrol-automations-1 sh -c 'cd /app && python3 "Harbor Lights/hl_update.py" --dry-run'
# ...and confirms the output matches the Windows TS run, then disables the
# Windows AmericalPatrolMorningReports task and uncomments below.
#
# Patrol — daily 7 AM Pacific = 14:00 UTC
#0 14 * * * root . /etc/container_env.sh && cd /app && python3 -m patrol_automation.main 2>&1 | tee -a /var/log/ap-patrol.log > /proc/1/fd/1
# Harbor Lights — daily 7:30 AM Pacific = 14:30 UTC (after morning reports finish)
#30 14 * * * root . /etc/container_env.sh && cd "/app/Harbor Lights" && python3 hl_update.py 2>&1 | tee -a /var/log/ap-hl.log > /proc/1/fd/1
# Harbor Lights parking audit — daily 8 AM Pacific = 15:00 UTC (drafts violation letters)
#0 15 * * * root . /etc/container_env.sh && cd "/app/Harbor Lights" && python3 parking_audit.py 2>&1 | tee -a /var/log/ap-hl.log > /proc/1/fd/1
# WCAS Dashboard heartbeat (every 30 min — decoupled from per-pipeline cadences,
# so the dashboard rings reflect current state from disk every cycle even if
# a pipeline is idle. Mirror of the Windows-side AmericalPatrolHeartbeatPush,
# but reads container truth instead of stale OneDrive state.)
*/30 * * * * root . /etc/container_env.sh && cd /app && python3 -m shared_utils.push_heartbeat 2>&1 | tee -a /var/log/ap-heartbeat.log > /proc/1/fd/1
CRONEOF
chmod 0644 /etc/cron.d/ap
cron

echo "AP automations running. Cron started."

# Keep alive with a simple sleep loop
while true; do sleep 3600; done
