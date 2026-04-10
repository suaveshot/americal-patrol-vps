#!/bin/bash
set -e

mkdir -p /app/data /app/pipeline_events

# Decode OAuth tokens from environment variables (first run only)
if [ ! -f /app/patrol_automation/token.json ] && [ -n "$GOOGLE_TOKEN_B64" ]; then
    echo "$GOOGLE_TOKEN_B64" | base64 -d > /app/patrol_automation/token.json
    echo "Decoded token.json from environment"
fi
if [ ! -f /app/patrol_automation/credentials.json ] && [ -n "$GOOGLE_CREDS_B64" ]; then
    echo "$GOOGLE_CREDS_B64" | base64 -d > /app/patrol_automation/credentials.json
    echo "Decoded credentials.json from environment"
fi

# Set up cron jobs (UTC times -- Pacific is UTC-7)
# 8 AM Pacific = 15:00 UTC
cat > /etc/cron.d/americal-patrol << 'CRONEOF'
# Americal Patrol VPS Automations (UTC times)
# Sales Pipeline - hourly lightweight check
0 * * * * root cd /app && python3 -m sales_pipeline.run_pipeline --hourly >> /var/log/ap-sales-hourly.log 2>&1
# Sales Pipeline - daily follow-ups + digest (Mon-Fri 8 AM Pacific = 15 UTC)
0 15 * * 1-5 root cd /app && python3 -m sales_pipeline.run_pipeline --daily >> /var/log/ap-sales-daily.log 2>&1
# Email Assistant - hourly inbox check
30 * * * * root cd /app && python3 email_assistant/email_monitor.py >> /var/log/ap-email.log 2>&1
# Watchdog - every 60 min
15 * * * * root cd /app && python3 watchdog/watchdog.py >> /var/log/ap-watchdog.log 2>&1

CRONEOF
chmod 0644 /etc/cron.d/americal-patrol

cron

echo "Americal Patrol VPS automations started."
echo "Cron: sales hourly, sales daily (Mon-Fri 15:00 UTC/8AM PT), email (hourly), watchdog (hourly)"

touch /var/log/ap-sales-hourly.log /var/log/ap-sales-daily.log /var/log/ap-email.log /var/log/ap-watchdog.log
tail -f /var/log/ap-sales-hourly.log /var/log/ap-sales-daily.log /var/log/ap-email.log /var/log/ap-watchdog.log
