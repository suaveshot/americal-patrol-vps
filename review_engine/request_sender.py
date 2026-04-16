"""
Review Request Email + SMS Sender

Composes and sends (or drafts) review request emails to eligible clients.
Optionally sends SMS review requests via GHL alongside email.
"""

import base64
import logging
import os
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# Ensure project root is on sys.path so tenant_context is importable
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import tenant_context as tc

from review_engine.config import (
    TOKEN_PATH,
    GMAIL_SCOPES,
    DRAFT_MODE,
    GOOGLE_REVIEW_URL,
    FEEDBACK_FORM_URL,
    SENDER_NAME,
    COMPANY_NAME,
    BCC_LIST,
)

ENV_PATH = Path(__file__).resolve().parent.parent / ".env"

LOGO_PATH = Path(__file__).resolve().parent.parent / "Company Logos" / "logo.png"

log = logging.getLogger("review_engine")

SIGNATURE = (
    f"Best Regards,\n"
    f"{SENDER_NAME}\n\n"
    f"{COMPANY_NAME}\n"
    f"{tc.company_address()}\n"
    f"{tc.company_phone()}\n"
    f"{tc.company_website()}"
)


def _get_gmail_service():
    """Get Gmail API service using patrol_automation's token."""
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), GMAIL_SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            TOKEN_PATH.write_text(creds.to_json())
        else:
            raise RuntimeError("Gmail credentials invalid -- run patrol_automation auth first")
    return build("gmail", "v1", credentials=creds)


def _lookup_ghl_first_name(email):
    """Look up a contact's first name from GHL by email. Returns name or None."""
    try:
        ghl = _get_ghl_client()
        contact_id, _ = _find_ghl_contact_by_email(ghl, email)
        if contact_id:
            contact = ghl.get_contact(contact_id)
            first = contact.get("firstName", "").strip()
            if first:
                return first.title()
    except Exception as e:
        log.warning("GHL name lookup failed for %s: %s", email, e)
    return None


def _get_greeting_name(group):
    """
    Get the best greeting name for a client group.
    Priority: GHL firstName > email local part > 'there'
    """
    for email in group.get("recipients", []):
        # Try GHL first
        ghl_name = _lookup_ghl_first_name(email)
        if ghl_name:
            return ghl_name
        # Fall back to email parsing
        local_part = email.split("@")[0].split(".")[0]
        if len(local_part) > 3 and local_part.isalpha():
            return local_part.title()
    return "there"


def _compose_review_email(group, clean_days):
    """
    Build a personalized review request email.

    Returns (subject, html_body, plain_body) tuple.
    """
    property_names = ", ".join(a["name"] for a in group["accounts"])
    first_recipient_name = _get_greeting_name(group)

    subject = f"Would you recommend {tc.company_name()}?"

    plain_body = (
        f"Hi {first_recipient_name},\n\n"
        f"Thank you for trusting {tc.company_name()} with the security at {property_names}. "
        f"We've been proud to deliver {clean_days}+ consecutive incident-free days "
        f"of coverage.\n\n"
        f"We appreciate the opportunity to serve you! Is the service you received "
        f"something you would use again or recommend to others?\n\n"
        f"YES -- Leave a Google review: {GOOGLE_REVIEW_URL}\n"
        f"NO -- Share private feedback: {FEEDBACK_FORM_URL}\n\n"
        f"It only takes a minute, and we truly appreciate it.\n\n"
        f"{SIGNATURE}"
    )

    html_body = f"""<html><body style="font-family: Arial, sans-serif; font-size: 14px; color: #333;">

<p style="text-align: center; margin: 0 0 20px 0;">
  <img src="cid:company_logo" alt="{tc.company_name()}" style="height: 70px;">
</p>

<p>Hi {first_recipient_name},</p>

<p>Thank you for trusting {tc.company_name()} with the security at <strong>{property_names}</strong>.
We've been proud to deliver <strong>{clean_days}+ consecutive incident-free days</strong>
of coverage.</p>

<p style="text-align: center; font-size: 18px; font-weight: bold; margin: 20px 0 5px 0;">
  Would you recommend {tc.company_name()}?
</p>
<p style="text-align: center; color: #666; margin: 0 0 20px 0;">
  We appreciate the opportunity to serve you! Is the service you received
  something you would use again or recommend to others?
</p>

<p style="text-align: center; margin: 10px 0;">
  <a href="{GOOGLE_REVIEW_URL}"
     style="display: inline-block; width: 260px; background-color: #4CAF50; color: white;
            padding: 14px 0; text-decoration: none; border-radius: 5px;
            font-size: 18px; font-weight: bold; text-align: center;">
    YES
  </a>
</p>
<p style="text-align: center; margin: 10px 0;">
  <a href="{FEEDBACK_FORM_URL}"
     style="display: inline-block; width: 260px; background-color: #e53935; color: white;
            padding: 14px 0; text-decoration: none; border-radius: 5px;
            font-size: 18px; font-weight: bold; text-align: center;">
    NO
  </a>
</p>

<p style="margin-top: 30px; color: #666; font-size: 13px;">
{SIGNATURE.replace(chr(10), '<br>')}
</p>
</body></html>"""

    return subject, html_body, plain_body


def send_review_request(group, clean_days):
    """
    Send (or draft) a review request email to a client group.

    Returns dict with send result metadata.
    """
    service = _get_gmail_service()
    subject, html_body, plain_body = _compose_review_email(group, clean_days)

    # Build MIME: related > alternative (plain + html) + inline logo image
    msg = MIMEMultipart("related")
    msg["To"] = ", ".join(group["recipients"])
    msg["Bcc"] = ", ".join(BCC_LIST)
    msg["Subject"] = subject

    # Text alternatives
    alt_part = MIMEMultipart("alternative")
    alt_part.attach(MIMEText(plain_body, "plain"))
    alt_part.attach(MIMEText(html_body, "html"))
    msg.attach(alt_part)

    # Embed logo as inline attachment (referenced by cid:company_logo in HTML)
    if LOGO_PATH.exists() and LOGO_PATH.stat().st_size > 0:
        with open(LOGO_PATH, "rb") as f:
            logo_img = MIMEImage(f.read(), _subtype="png")
        logo_img.add_header("Content-ID", "<company_logo>")
        logo_img.add_header("Content-Disposition", "inline", filename="logo.png")
        msg.attach(logo_img)

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    if DRAFT_MODE:
        result = service.users().drafts().create(
            userId="me",
            body={"message": {"raw": raw}},
        ).execute()
        log.info(
            "Draft created for %s: '%s' -> %s",
            group["group_id"], subject, ", ".join(group["recipients"]),
        )
        return {"mode": "draft", "draft_id": result["id"], "subject": subject}
    else:
        result = service.users().messages().send(
            userId="me",
            body={"raw": raw},
        ).execute()
        log.info(
            "Email SENT to %s: '%s' -> %s",
            group["group_id"], subject, ", ".join(group["recipients"]),
        )
        return {"mode": "sent", "message_id": result["id"], "subject": subject}


def _get_ghl_client():
    """Get CRM client for SMS sending. Uses the provider factory."""
    import sys
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    load_dotenv(ENV_PATH)
    from providers import get_crm
    return get_crm()


def _find_ghl_contact_by_email(ghl, email):
    """Search GHL contacts by email to get contact_id + phone."""
    import requests as req
    url = f"{ghl._base}/contacts/search/duplicate"
    params = {"locationId": ghl._location_id, "email": email}
    r = req.get(url, headers=ghl._headers, params=params, timeout=15)
    if r.status_code == 200:
        contact = r.json().get("contact")
        if contact:
            return contact.get("id"), contact.get("phone", "")
    return None, None


def send_sms_review_request(group, clean_days):
    """
    Send an SMS review request via GHL to the first recipient with a phone number.
    Looks up the recipient's email in GHL to find their contact_id and phone.

    Returns dict with result, or None if no phone found.
    """
    property_names = ", ".join(a["name"] for a in group["accounts"])

    sms_body = (
        f"Hi! We've loved protecting {property_names}. "
        f"If you have 2 min, a Google review would mean a lot: "
        f"{GOOGLE_REVIEW_URL} "
        f"-- {tc.owner_name()}, {tc.company_name()}"
    )

    try:
        ghl = _get_ghl_client()
    except Exception as e:
        log.warning("Could not initialize GHL client for SMS: %s", e)
        return None

    # Try each recipient's email to find a GHL contact with a phone number
    for email in group.get("recipients", []):
        contact_id, phone = _find_ghl_contact_by_email(ghl, email)
        if contact_id and phone:
            if DRAFT_MODE:
                log.info(
                    "SMS DRAFT (not sent -- draft mode): %s -> %s (%s)",
                    group["group_id"], phone, email
                )
                return {"mode": "sms_draft", "phone": phone, "contact_id": contact_id}
            else:
                msg_id = ghl.send_sms(contact_id, sms_body)
                log.info(
                    "SMS SENT to %s: %s (%s) -- msgId=%s",
                    group["group_id"], phone, email, msg_id
                )
                return {"mode": "sms_sent", "message_id": msg_id, "phone": phone}

    log.info("No GHL contact with phone found for %s -- SMS skipped", group["group_id"])
    return None


def send_new_review_alert(new_reviews):
    """
    Email Sam when new Google reviews are detected.
    Includes reviewer name, star rating, comment, and link to respond.
    """
    if not new_reviews:
        return

    service = _get_gmail_service()

    # GBP review management URL
    respond_url = "https://business.google.com/reviews"

    lines = []
    for r in new_reviews:
        stars = r.get("star_rating", "UNKNOWN")
        name = r.get("reviewer_name", "Anonymous")
        comment = r.get("comment", "(no comment)")
        date = r.get("create_time", "")[:10]
        lines.append(f"  {stars} star -- {name} ({date})\n  \"{comment}\"\n")

    body = (
        f"New Google Review Alert\n"
        f"{'=' * 40}\n\n"
        f"{len(new_reviews)} new review(s) detected:\n\n"
        + "\n".join(lines)
        + f"\nRespond within 24 hours (ranking signal):\n"
        f"  {respond_url}\n\n"
        f"-- Review Engine"
    )

    msg = MIMEText(body)
    msg["to"] = tc.owner_email()
    msg["from"] = "me"
    msg["subject"] = f"New Google Review{'s' if len(new_reviews) > 1 else ''}: {new_reviews[0].get('star_rating', '')} star from {new_reviews[0].get('reviewer_name', 'someone')}"

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()

    if DRAFT_MODE:
        service.users().drafts().create(
            userId="me", body={"message": {"raw": raw}}
        ).execute()
        log.info("New review alert DRAFTED (draft mode) -- %d review(s)", len(new_reviews))
    else:
        service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()
        log.info("New review alert SENT to Sam -- %d review(s)", len(new_reviews))
