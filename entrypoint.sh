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

# Set up cron (UTC times -- 8 AM Pacific = 15:00 UTC)
cat > /etc/cron.d/ap << 'CRONEOF'
0 * * * * root cd /app && python3 -m sales_pipeline.run_pipeline --hourly >> /var/log/ap-sales.log 2>&1
0 15 * * 1-5 root cd /app && python3 -m sales_pipeline.run_pipeline --daily >> /var/log/ap-sales.log 2>&1
*/15 * * * * root cd /app && python3 -m sales_pipeline.transcribe_calls >> /var/log/ap-transcribe.log 2>&1
30 * * * * root cd /app && python3 email_assistant/email_monitor.py >> /var/log/ap-email.log 2>&1
15 * * * * root cd /app && python3 watchdog/watchdog.py >> /var/log/ap-watchdog.log 2>&1
# Call Intelligence
5 * * * * root cd /app && python3 -m call_intelligence.run_ingestion >> /var/log/ap-call-intel.log 2>&1
0 13 * * * root cd /app && python3 -m call_intelligence.sync_deals >> /var/log/ap-call-intel.log 2>&1
CRONEOF
chmod 0644 /etc/cron.d/ap
cron

echo "AP automations running. Cron started."

# Keep alive with a simple sleep loop
while true; do sleep 3600; done
