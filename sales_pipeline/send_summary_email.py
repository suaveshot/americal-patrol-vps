"""Send mobile-friendly summary of today's 2 PM send to Sam — includes message previews."""
import json
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sales_pipeline import config

config.validate_config()

with open("sales_pipeline/pipeline_drafts.json", "r", encoding="utf-8") as f:
    drafts = json.load(f)

approved = [d for d in drafts if d.get("status") == "approved"]
new_cold = [d for d in approved if not d.get("is_follow_up")]
follow_ups = [d for d in approved if d.get("is_follow_up")]


def _message_preview_html(body_text: str) -> str:
    """Convert plain text message to HTML preview with paragraph breaks."""
    if not body_text:
        return '<span style="color:#999;">No message</span>'
    paragraphs = body_text.strip().split("\n\n")
    html = ""
    for p in paragraphs:
        lines = p.strip().replace("\n", "<br>")
        html += f'<p style="margin:0 0 8px 0;line-height:1.4;">{lines}</p>'
    return html


def _build_card(d: dict, badge: str = "") -> str:
    name = (d.get("name") or "?").title()
    org = d.get("organization") or ""
    email = d.get("email") or ""
    subject = d.get("subject") or ""

    # Get the right message body
    selected = d.get("selected_variant", "a")
    if selected == "b":
        body = d.get("variant_b_plain") or d.get("variant_b_message") or ""
    else:
        body = d.get("message_plain") or d.get("message") or ""

    org_line = f'<div style="font-size:13px;color:#666;">{org}</div>' if org else ""
    badge_html = f'<span style="display:inline-block;background:#2563eb;color:#fff;font-size:10px;padding:2px 6px;border-radius:3px;margin-left:6px;">{badge}</span>' if badge else ""

    return f"""
    <div style="padding:14px;margin-bottom:12px;background:#ffffff;border:1px solid #e5e5e5;border-radius:8px;">
      <div style="font-size:16px;font-weight:bold;color:#1a3a5c;">{name}{badge_html}</div>
      {org_line}
      <div style="font-size:12px;color:#888;margin-bottom:8px;">{email}</div>
      <div style="font-size:12px;color:#1a3a5c;font-weight:bold;margin-bottom:6px;">Subject: {subject}</div>
      <div style="font-size:13px;color:#333;background:#f9f9f9;padding:10px;border-radius:4px;border-left:3px solid #2563eb;">
        {_message_preview_html(body)}
      </div>
    </div>"""


# Build cards
cards_new = ""
for d in new_cold:
    cards_new += _build_card(d)

cards_fu = ""
for d in follow_ups:
    touch = d.get("touch_number", "?")
    cards_fu += _build_card(d, badge=f"Follow-up #{touch}")

html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0;padding:0;background:#f0f0f0;font-family:Arial,Helvetica,sans-serif;">

<div style="max-width:500px;margin:0 auto;padding:12px;">

  <!-- Header -->
  <div style="background:#1a3a5c;color:#ffffff;padding:20px 16px;border-radius:8px 8px 0 0;text-align:center;">
    <div style="font-size:20px;font-weight:bold;">Sales Pipeline</div>
    <div style="font-size:14px;opacity:0.8;margin-top:4px;">Sending Today at 2:00 PM</div>
  </div>

  <div style="background:#f5f5f5;padding:16px;border-radius:0 0 8px 8px;">

    <!-- Summary -->
    <div style="background:#ffffff;border-radius:8px;padding:16px;margin-bottom:16px;text-align:center;border:1px solid #e5e5e5;">
      <div style="font-size:32px;font-weight:bold;color:#1a3a5c;">{len(approved)}</div>
      <div style="font-size:13px;color:#666;">total emails going out</div>
      <table width="100%" cellpadding="0" cellspacing="0" border="0" style="margin-top:12px;">
        <tr>
          <td width="50%" style="text-align:center;">
            <div style="font-size:22px;font-weight:bold;color:#1a3a5c;">{len(new_cold)}</div>
            <div style="font-size:11px;color:#888;">New Outreach</div>
          </td>
          <td width="50%" style="text-align:center;">
            <div style="font-size:22px;font-weight:bold;color:#2563eb;">{len(follow_ups)}</div>
            <div style="font-size:11px;color:#888;">Follow-ups</div>
          </td>
        </tr>
      </table>
    </div>

    <!-- New Cold Outreach -->
    <div style="margin-bottom:20px;">
      <div style="font-size:17px;font-weight:bold;color:#1a3a5c;padding:10px 0 8px 0;border-bottom:2px solid #1a3a5c;margin-bottom:12px;">
        New Cold Outreach ({len(new_cold)})
      </div>
      {cards_new}
    </div>

    <!-- Follow-ups -->
    <div style="margin-bottom:20px;">
      <div style="font-size:17px;font-weight:bold;color:#2563eb;padding:10px 0 8px 0;border-bottom:2px solid #2563eb;margin-bottom:12px;">
        Follow-ups ({len(follow_ups)})
      </div>
      {cards_fu}
    </div>

    <!-- Excluded -->
    <div style="background:#fff8f0;border-radius:8px;padding:14px;border:1px solid #fde68a;">
      <div style="font-size:14px;font-weight:bold;color:#b45309;">Removed from send list</div>
      <div style="font-size:13px;color:#92400e;margin-top:6px;">
        Mark McGaffin (Higher Vision Church) — deleted from GHL<br>
        Liz Monreal (Blackstock Jr High) — excluded
      </div>
      <div style="font-size:12px;color:#888;margin-top:8px;">
        Schools, churches, and campus accounts are now auto-excluded going forward.
      </div>
    </div>

  </div>

  <!-- Footer -->
  <div style="text-align:center;padding:12px;font-size:11px;color:#999;">
    Sales Pipeline — Americal Patrol
  </div>

</div>
</body>
</html>"""

# Send
sender = config.GMAIL_SENDER()
password = config.GMAIL_APP_PASSWORD()
recipient = "salarcon@americalpatrol.com"

msg = MIMEMultipart("alternative")
msg["Subject"] = f"Sales Pipeline — {len(approved)} Emails Sending at 2 PM Today"
msg["From"] = f"Larry (Sales Pipeline) <{sender}>"
msg["To"] = recipient
msg.attach(MIMEText(html, "html"))

with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
    srv.login(sender, password)
    srv.sendmail(sender, recipient, msg.as_string())

print(f"Summary sent to {recipient}")
print(f"  {len(new_cold)} new cold outreach")
print(f"  {len(follow_ups)} follow-ups")
print(f"  {len(approved)} total")
