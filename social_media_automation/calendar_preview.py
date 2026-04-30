"""
Americal Patrol — Weekly Social Media Calendar Preview
Generates and emails a preview of the upcoming week's planned posts.
Runs Sunday night at 8 PM via Windows Task Scheduler.

Don/Sam can review the upcoming content types and adjust if needed
before the posts are generated on Tuesday, Thursday, and Saturday.

Usage:
    python calendar_preview.py
"""

import base64
import json
import os
import sys
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

SCRIPT_DIR   = Path(__file__).parent
CONFIG_FILE  = SCRIPT_DIR / "social_config.json"
STATE_FILE   = SCRIPT_DIR / "social_state.json"
LOG_FILE     = SCRIPT_DIR / "automation.log"
PATROL_DIR   = SCRIPT_DIR.parent / "patrol_automation"


def log(msg: str):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [CALENDAR] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"facebook_index": 0, "instagram_index": 0, "linkedin_index": 0, "gbp_index": 0}


def _build_client_config():
    return {
        "installed": {
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "project_id": "americal-patrol-automation",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost"],
        }
    }


def _get_gmail_service():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    scopes     = ["https://www.googleapis.com/auth/gmail.send"]
    token_path = PATROL_DIR / "token.json"

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_config(_build_client_config(), scopes)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("gmail", "v1", credentials=creds)


PLATFORM_COLORS = {
    "facebook":  "#1877F2",
    "instagram": "#E4405F",
    "linkedin":  "#0A66C2",
    "gbp":       "#4285F4",
}


def _find_posting_days() -> list[datetime]:
    """Find the next Tuesday, Thursday, Saturday from now."""
    config = _load_config()
    day_names = config.get("posting_schedule", {}).get("days", ["Tuesday", "Thursday", "Saturday"])
    day_map = {"Monday": 0, "Tuesday": 1, "Wednesday": 2, "Thursday": 3,
               "Friday": 4, "Saturday": 5, "Sunday": 6}

    target_days = [day_map[d] for d in day_names if d in day_map]
    now = datetime.now()
    upcoming = []

    for offset in range(1, 8):
        check = now + timedelta(days=offset)
        if check.weekday() in target_days:
            upcoming.append(check)

    return upcoming[:3]


def run():
    log("Generating weekly calendar preview...")

    config = _load_config()
    state  = _load_state()
    rotation = config.get("content_rotation", {})
    posting_days = _find_posting_days()

    if not posting_days:
        log("No posting days found in the next 7 days.")
        return

    # Build preview table
    rows = ""
    for i, day in enumerate(posting_days):
        day_str = day.strftime("%A, %B %d")
        row_cells = f'<td style="padding:12px 16px;font-weight:600;border-bottom:1px solid #eee;width:140px">{day_str}</td>'

        for platform in ["facebook", "instagram", "linkedin", "gbp"]:
            color = PLATFORM_COLORS.get(platform, "#333")
            slots = rotation.get(platform, [])
            if not slots:
                row_cells += '<td style="padding:12px;border-bottom:1px solid #eee">&mdash;</td>'
                continue

            # GBP only posts once per week — show content on first day only
            if platform == "gbp" and i > 0:
                row_cells += '<td style="padding:12px;border-bottom:1px solid #eee;color:#ccc;font-size:11px">&mdash;</td>'
                continue

            idx_key = f"{platform}_index"
            # Simulate rotation advancing for each posting day
            idx = (state.get(idx_key, 0) + i) % len(slots)
            slot = slots[idx]
            desc = slot.get('description', slot.get('subject', ''))

            row_cells += f"""
            <td style="padding:12px;border-bottom:1px solid #eee">
              <span style="display:inline-block;padding:3px 8px;background:{color}22;color:{color};border-radius:4px;font-size:12px;font-weight:600">
                {slot['type'].replace('_', ' ').title()}
              </span>
              <br><span style="font-size:11px;color:#888;margin-top:2px;display:inline-block">{desc[:60]}</span>
            </td>"""

        rows += f"<tr>{row_cells}</tr>"

    # Check for seasonal events
    seasonal_note = ""
    from content_planner import _check_seasonal
    seasonal = _check_seasonal(config)
    if seasonal:
        seasonal_note = f"""
        <div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:12px 16px;margin-bottom:20px">
          <strong>Seasonal Override:</strong> {seasonal['name']} is in {seasonal['days_until']} day(s).
          All posts will be themed for this event.
        </div>"""

    week_start = posting_days[0].strftime("%B %d")
    week_end   = posting_days[-1].strftime("%B %d, %Y")

    html = f"""<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;margin:0;padding:20px;background:#f4f4f4">
<div style="max-width:800px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)">
  <div style="background:#1a1a2e;color:#fff;padding:24px">
    <h2 style="margin:0;font-size:20px">Upcoming Social Media Calendar</h2>
    <p style="margin:6px 0 0;opacity:0.75;font-size:13px">{week_start} &mdash; {week_end}</p>
  </div>

  <div style="padding:24px">
    <p style="font-size:14px;color:#555;margin-top:0">
      Here's what's planned for this week's social media posts. Reply to this email if you'd like to adjust anything.
    </p>

    {seasonal_note}

    <table style="width:100%;border-collapse:collapse">
      <thead>
        <tr style="background:#f8f8f8">
          <th style="padding:10px 16px;text-align:left;font-size:13px;color:#555">Day</th>
          <th style="padding:10px 16px;text-align:left;font-size:13px;color:{PLATFORM_COLORS['facebook']}">Facebook</th>
          <th style="padding:10px 16px;text-align:left;font-size:13px;color:{PLATFORM_COLORS['instagram']}">Instagram</th>
          <th style="padding:10px 16px;text-align:left;font-size:13px;color:{PLATFORM_COLORS['linkedin']}">LinkedIn</th>
          <th style="padding:10px 16px;text-align:left;font-size:13px;color:{PLATFORM_COLORS['gbp']}">GBP</th>
        </tr>
      </thead>
      <tbody>
        {rows}
      </tbody>
    </table>
  </div>

  <div style="background:#f0f0f0;padding:12px 24px;font-size:11px;color:#999">
    Generated by Americal Patrol Social Media Automation
  </div>
</div>
</body>
</html>"""

    # Send email
    recipients = config.get("recipients", [])
    if not recipients:
        log("No recipients configured.")
        return

    subject = f"Social Media Calendar Preview — {week_start} to {week_end}"

    try:
        service = _get_gmail_service()

        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(html, "html"))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        log(f"Calendar preview sent to {', '.join(recipients)}")
    except Exception as e:
        log(f"ERROR sending calendar preview: {e}")


if __name__ == "__main__":
    run()
