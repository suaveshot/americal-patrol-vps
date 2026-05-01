"""
Americal Patrol — Social Media Draft Review Emailer
When draft_mode is enabled, sends an HTML email to Don/Sam showing all
planned social media posts side-by-side for review before publishing.
"""

import base64
import json
import os
from datetime import datetime
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
CONFIG_FILE  = SCRIPT_DIR / "social_config.json"
PATROL_DIR   = SCRIPT_DIR.parent / "patrol_automation"


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

PLATFORM_ICONS = {
    "facebook":  "FB",
    "instagram": "IG",
    "linkedin":  "LI",
    "gbp":       "GBP",
}


def send_draft_review_email(results: list[dict], plans: dict, log=None) -> None:
    """
    Send an HTML email showing all planned posts for review.

    Args:
        results: List of post result dicts with post_text, image_path, etc.
        plans: Full plans dict from content_planner
        log: Optional logging function
    """
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    recipients = config.get("recipients", [])
    if not recipients:
        if log:
            log("  No recipients configured for draft review email.")
        return

    date_str = datetime.now().strftime("%B %d, %Y")
    subject  = f"Social Media Posts for Review — {date_str}"

    # Build platform cards
    cards_html = ""
    image_attachments = []

    for result in results:
        platform = result.get("platform", "unknown")
        color    = PLATFORM_COLORS.get(platform, "#333")
        icon     = PLATFORM_ICONS.get(platform, "??")
        content_type = result.get("content_type", "")
        post_text    = result.get("post_text", "No content generated")
        image_path   = result.get("image_path")

        # Escape HTML in post text but preserve line breaks
        safe_text = (
            post_text
            .replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("\n", "<br>")
        )

        image_section = ""
        if image_path and Path(image_path).exists():
            cid = f"img_{platform}"
            image_section = f"""
            <div style="margin-top:12px">
              <img src="cid:{cid}" style="max-width:100%;border-radius:6px" alt="Post image">
            </div>"""
            image_attachments.append((cid, Path(image_path)))

        cards_html += f"""
        <div style="margin-bottom:24px;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
          <div style="background:{color};color:#fff;padding:12px 16px;display:flex;align-items:center">
            <span style="font-size:18px;font-weight:bold;margin-right:10px">{icon}</span>
            <span style="font-size:16px;font-weight:600">{platform.title()}</span>
            <span style="margin-left:auto;font-size:12px;opacity:0.8">{content_type}</span>
          </div>
          <div style="padding:16px">
            <div style="font-size:14px;line-height:1.6;color:#333">{safe_text}</div>
            {image_section}
          </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html>
<body style="font-family:Arial,sans-serif;margin:0;padding:20px;background:#f4f4f4">
<div style="max-width:700px;margin:auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)">
  <div style="background:#1a1a2e;color:#fff;padding:24px">
    <h2 style="margin:0;font-size:20px">Social Media Posts for Review</h2>
    <p style="margin:6px 0 0;opacity:0.75;font-size:13px">{date_str}</p>
  </div>

  <div style="padding:24px">
    <p style="font-size:14px;color:#555;margin-top:0">
      The following posts were generated for today. Review and approve before they go live.
      To switch to auto-publish, set <code>draft_mode: false</code> in social_config.json.
    </p>

    {cards_html}
  </div>

  <div style="background:#f0f0f0;padding:12px 24px;font-size:11px;color:#999">
    Generated by Americal Patrol Social Media Automation
  </div>
</div>
</body>
</html>"""

    # Build email
    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    # Attach images inline
    for cid, img_path in image_attachments:
        try:
            with open(img_path, "rb") as f:
                img_data = f.read()
            mime_img = MIMEImage(img_data)
            mime_img.add_header("Content-ID", f"<{cid}>")
            mime_img.add_header("Content-Disposition", "inline", filename=img_path.name)
            msg.attach(mime_img)
        except Exception:
            pass  # Skip if image can't be attached

    # Send
    try:
        service = _get_gmail_service()
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        service.users().messages().send(userId="me", body={"raw": raw}).execute()
        if log:
            log(f"  Draft review email sent to {', '.join(recipients)}")
    except Exception as e:
        if log:
            log(f"  ERROR sending draft review email: {e}")
        raise
