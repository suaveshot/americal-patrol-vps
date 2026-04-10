"""
Email Assistant (Larry) — Gmail Client
Reuses patrol_automation OAuth2 for inbox reading and draft creation.
"""

import base64
import sys
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from googleapiclient.discovery import build

# Reuse auth from patrol_automation
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "patrol_automation"))
from email_fetcher import get_credentials


def get_gmail_service():
    return build("gmail", "v1", credentials=get_credentials())


# ── Fetch unread emails ──────────────────────────────────────────────────────
def fetch_unread_emails(service, hours=2):
    """
    Fetch unread emails from the last `hours` hours.
    Excludes promotions, social, updates, and forums categories.
    Returns list of dicts with: id, thread_id, from, to, subject, date, body, labels, message_id.
    """
    query = (
        f"is:unread newer_than:{hours}h "
        "-category:promotions -category:social "
        "-category:updates -category:forums"
    )

    results = []
    page_token = None

    while True:
        resp = service.users().messages().list(
            userId="me", q=query, pageToken=page_token, maxResults=50
        ).execute()

        messages = resp.get("messages", [])
        if not messages:
            break

        for msg_ref in messages:
            msg = service.users().messages().get(
                userId="me", id=msg_ref["id"], format="full"
            ).execute()
            parsed = _parse_message(msg)
            if parsed:
                results.append(parsed)

        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return results


def _parse_message(msg):
    """Extract key fields from a Gmail API message."""
    headers = {h["name"].lower(): h["value"] for h in msg["payload"]["headers"]}
    labels = msg.get("labelIds", [])

    body = _extract_text_body(msg["payload"])

    return {
        "id": msg["id"],
        "thread_id": msg["threadId"],
        "from": headers.get("from", ""),
        "to": headers.get("to", ""),
        "subject": headers.get("subject", ""),
        "date": headers.get("date", ""),
        "message_id": headers.get("message-id", ""),
        "body": body,
        "labels": labels,
    }


def _extract_text_body(payload):
    """Get plain text body, falling back to stripped HTML."""
    parts = _flatten_parts(payload)

    # Prefer text/plain
    for part in parts:
        if part.get("mimeType") == "text/plain":
            data = part.get("body", {}).get("data", "")
            if data:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")

    # Fallback: strip HTML tags
    for part in parts:
        if part.get("mimeType") == "text/html":
            data = part.get("body", {}).get("data", "")
            if data:
                import re
                html = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                text = re.sub(r"<[^>]+>", " ", html)
                text = re.sub(r"\s+", " ", text).strip()
                return text

    return ""


def _flatten_parts(payload):
    """Recursively flatten MIME parts."""
    parts = []
    if "parts" in payload:
        for part in payload["parts"]:
            parts.extend(_flatten_parts(part))
    else:
        parts.append(payload)
    return parts


# ── Create reply draft ───────────────────────────────────────────────────────
def create_reply_draft(service, original_email, reply_body, signature):
    """
    Create a Gmail draft that replies to the original email.
    The draft appears in the correct thread and is ready for Sam to review/send.
    """
    # Build subject
    subject = original_email["subject"]
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"

    # Extract sender's email for the To field
    sender = original_email["from"]

    # Build MIME message
    msg = MIMEMultipart("alternative")
    msg["To"] = sender
    msg["Subject"] = subject
    msg["In-Reply-To"] = original_email.get("message_id", "")
    msg["References"] = original_email.get("message_id", "")

    # Plain text body with signature
    full_body = f"{reply_body}\n\n{signature}"
    msg.attach(MIMEText(full_body, "plain"))

    # Encode
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    draft_body = {
        "message": {
            "raw": raw,
            "threadId": original_email["thread_id"],
        }
    }

    draft = service.users().drafts().create(userId="me", body=draft_body).execute()
    return draft


# ── Send escalation email ────────────────────────────────────────────────────
def send_escalation_email(service, to_email, subject, body):
    """
    Send an email directly to Sam (not a draft).
    Used when Larry needs guidance — Sam gets notified immediately.
    """
    msg = MIMEText(body, "plain")
    msg["To"] = to_email
    msg["Subject"] = subject

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

    message_body = {
        "raw": raw,
    }

    sent = service.users().messages().send(userId="me", body=message_body).execute()
    return sent
