"""
Americal Patrol — QBR Email Sender

Composes and sends (or drafts) QBR delivery emails with PDF attachments.
"""

import base64
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from config import (
    TOKEN_PATH, GMAIL_SCOPES, DRAFT_MODE, BCC_LIST, SIGNATURE, COMPANY_NAME,
)

log = logging.getLogger("qbr_generator")


def _get_gmail_service():
    """Get Gmail API service using patrol_automation's token."""
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), GMAIL_SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json())
        else:
            raise RuntimeError("Gmail credentials invalid — run patrol_automation auth first")
    return build("gmail", "v1", credentials=creds)


def send_qbr(group, pdf_path, quarter_label):
    """
    Send (or draft) the QBR email with PDF attachment.

    Returns result dict with mode, subject, and recipients.
    """
    service = _get_gmail_service()
    property_names = ", ".join(a["name"] for a in group["accounts"])
    first_name = group["recipients"][0].split("@")[0].split(".")[0].title()

    subject = f"{property_names} — {quarter_label} Security Patrol Review"

    plain_body = (
        f"Hi {first_name},\n\n"
        f"Please find attached your {quarter_label} Quarterly Business Review "
        f"for {property_names}.\n\n"
        f"This report covers patrol coverage, incident trends, and our recommendations "
        f"for the upcoming quarter. We hope you find it valuable.\n\n"
        f"If you'd like to discuss any of the findings or adjust coverage, "
        f"please don't hesitate to reach out.\n\n"
        f"{SIGNATURE}"
    )

    html_body = f"""<html><body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">
<p>Hi {first_name},</p>

<p>Please find attached your <strong>{quarter_label} Quarterly Business Review</strong>
for <strong>{property_names}</strong>.</p>

<p>This report covers patrol coverage, incident trends, and our recommendations
for the upcoming quarter. We hope you find it valuable.</p>

<p>If you'd like to discuss any of the findings or adjust coverage,
please don't hesitate to reach out.</p>

<p style="margin-top: 30px; color: #666; font-size: 13px;">
{SIGNATURE.replace(chr(10), '<br>')}
</p>
</body></html>"""

    msg = MIMEMultipart("mixed")
    msg["To"] = ", ".join(group["recipients"])
    msg["Bcc"] = ", ".join(BCC_LIST)
    msg["Subject"] = subject

    # Email body (alternative: plain + html)
    body_part = MIMEMultipart("alternative")
    body_part.attach(MIMEText(plain_body, "plain"))
    body_part.attach(MIMEText(html_body, "html"))
    msg.attach(body_part)

    # PDF attachment
    pdf_file = Path(pdf_path)
    if pdf_file.exists():
        with open(pdf_file, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition", "attachment", filename=pdf_file.name
        )
        msg.attach(part)
    else:
        log.warning("PDF not found at %s — sending email without attachment", pdf_path)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    if DRAFT_MODE:
        result = service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}},
        ).execute()
        log.info(
            "QBR draft created for %s: '%s' → %s",
            group["group_id"], subject, ", ".join(group["recipients"]),
        )
        return {"mode": "draft", "draft_id": result["id"], "subject": subject}
    else:
        result = service.users().messages().send(
            userId="me",
            body={"raw": raw},
        ).execute()
        log.info(
            "QBR SENT to %s: '%s' → %s",
            group["group_id"], subject, ", ".join(group["recipients"]),
        )
        return {"mode": "sent", "message_id": result["id"], "subject": subject}
