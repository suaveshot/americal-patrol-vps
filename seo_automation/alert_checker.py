"""
Americal Patrol - Daily SEO Alert Checker
Runs every day at 10 AM. Sends an immediate email if any page drops
more than the configured threshold in a single day vs prior day average.

This is separate from the weekly report — it catches problems fast
(algorithm penalties, broken pages, etc.) before Monday's review.

Run manually: python alert_checker.py
"""

import base64
import json
import os
import sys
import traceback
import winreg
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from googleapiclient.discovery import build

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'
LOG_FILE    = SCRIPT_DIR / 'automation.log'


# ── Logging ───────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ALERT] {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


# ── Auth ──────────────────────────────────────────────────────────────────────

def _get_api_key() -> str:
    key = os.environ.get('ANTHROPIC_API_KEY')
    if key:
        return key
    try:
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, r'Environment') as reg_key:
            key, _ = winreg.QueryValueEx(reg_key, 'ANTHROPIC_API_KEY')
            return key
    except (FileNotFoundError, OSError):
        pass
    return ''


def _get_gmail_service():
    from auth_setup import get_credentials
    creds = get_credentials()
    return build('gmail', 'v1', credentials=creds)


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


# ── GA4 daily fetch ───────────────────────────────────────────────────────────

def _fetch_daily_sessions(property_id: str) -> tuple[dict, dict]:
    """
    Fetch today's sessions and yesterday's sessions per page from GA4.
    Returns (today_map, yesterday_map) — {page_path: sessions}
    """
    from auth_setup import get_credentials
    from google.analytics.data_v1beta import BetaAnalyticsDataClient
    from google.analytics.data_v1beta.types import (
        DateRange, Dimension, Metric, RunReportRequest
    )

    creds  = get_credentials()
    client = BetaAnalyticsDataClient(credentials=creds)

    def _run(start: str, end: str) -> dict:
        req = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name='pagePath')],
            metrics=[Metric(name='sessions')],
            date_ranges=[DateRange(start_date=start, end_date=end)],
        )
        resp = client.run_report(req)
        result = {}
        for row in resp.rows:
            page = row.dimension_values[0].value
            sess = int(row.metric_values[0].value)
            result[page] = sess
        return result

    today     = _run('today', 'today')
    yesterday = _run('yesterday', 'yesterday')
    return today, yesterday


# ── Alert logic ───────────────────────────────────────────────────────────────

def check_for_alerts(property_id: str, threshold_pct: float, log_fn=None) -> list:
    """
    Returns list of alert dicts for pages that dropped > threshold_pct vs yesterday.
    Only flags pages with meaningful traffic (>= 5 sessions yesterday).
    """
    today, yesterday = _fetch_daily_sessions(property_id)
    alerts = []

    for page, prev in yesterday.items():
        if prev < 5:
            continue
        cur = today.get(page, 0)
        drop_pct = ((prev - cur) / prev) * 100
        if drop_pct >= threshold_pct:
            alerts.append({
                'page':      page,
                'today':     cur,
                'yesterday': prev,
                'drop_pct':  round(drop_pct, 1),
            })

    alerts.sort(key=lambda x: x['drop_pct'], reverse=True)
    if log_fn and alerts:
        log_fn(f"{len(alerts)} traffic alert(s) detected")
    return alerts


# ── Email ─────────────────────────────────────────────────────────────────────

def _build_alert_html(alerts: list) -> str:
    today_str = datetime.now().strftime('%B %d, %Y')
    rows = ''
    for a in alerts:
        rows += (
            f'<tr>'
            f'<td style="padding:8px 12px;font-size:13px;">{a["page"]}</td>'
            f'<td style="padding:8px 12px;text-align:center;">{a["yesterday"]:,}</td>'
            f'<td style="padding:8px 12px;text-align:center;">{a["today"]:,}</td>'
            f'<td style="padding:8px 12px;text-align:center;color:#c62828;font-weight:bold;">'
            f'-{a["drop_pct"]}% ↓</td>'
            f'</tr>'
        )

    return f"""<html><body style="font-family:Arial,sans-serif;max-width:680px;margin:0 auto;color:#333;">
<div style="background:#c62828;padding:18px;border-radius:8px 8px 0 0;">
  <h1 style="color:white;margin:0;font-size:20px;">⚠️ Traffic Alert — Americal Patrol</h1>
  <p style="color:#fdd;margin:5px 0 0;font-size:13px;">{today_str} &nbsp;|&nbsp; americalpatrol.com</p>
</div>
<div style="padding:24px;background:#fff;border:1px solid #ddd;border-top:none;border-radius:0 0 8px 8px;">
  <p style="margin:0 0 16px;">The following pages had an unusually large traffic drop today compared to yesterday.
  This may indicate a Google algorithm update, a broken page, or a technical issue that needs immediate attention.</p>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead>
      <tr style="background:#1a3a5c;color:white;">
        <th style="padding:8px 12px;text-align:left;">Page</th>
        <th style="padding:8px 12px;text-align:center;">Yesterday</th>
        <th style="padding:8px 12px;text-align:center;">Today</th>
        <th style="padding:8px 12px;text-align:center;">Drop</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <p style="margin:20px 0 0;font-size:13px;">
    <strong>What to check:</strong><br>
    1. Open each page URL — is it loading correctly?<br>
    2. Check Google Search Console for any manual actions or crawl errors.<br>
    3. Search Google for your page's main keyword — do you still appear?<br>
    4. Check if any website edits were made in the last 24 hours.
  </p>
</div>
<p style="font-size:11px;color:#999;text-align:center;margin-top:12px;">
  Americal Patrol Daily SEO Alert &nbsp;|&nbsp; This email only sends when alerts are detected.
</p>
</body></html>"""


def send_alert_email(alerts: list, recipients: list) -> bool:
    html    = _build_alert_html(alerts)
    subject = f"⚠️ Traffic Alert — {len(alerts)} page(s) dropped significantly today"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['To']      = ', '.join(recipients)
    msg['From']    = 'me'
    msg.attach(MIMEText("Open in Gmail to view the traffic alert.", 'plain'))
    msg.attach(MIMEText(html, 'html'))

    raw   = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    gmail = _get_gmail_service()
    gmail.users().messages().send(userId='me', body={'raw': raw}).execute()
    return True


# ── Entry point ───────────────────────────────────────────────────────────────

def run() -> bool:
    log("Daily alert check starting...")
    try:
        config       = _load_config()
        property_id  = config.get('ga4_property_id', '')
        threshold    = config.get('alert_threshold_daily_drop_pct', 40)
        recipients   = config.get('recipients', [])

        alerts = check_for_alerts(property_id, threshold, log_fn=log)

        if not alerts:
            log("No significant drops detected — all clear.")
            return True

        log(f"Sending alert email to {', '.join(recipients)}...")
        send_alert_email(alerts, recipients)
        log(f"Alert email sent for {len(alerts)} page(s).")
        return True

    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()
        return False


if __name__ == '__main__':
    success = run()
    sys.exit(0 if success else 1)
