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


# -- Fetch full thread -------------------------------------------------------
def fetch_thread_messages(service, thread_id, max_messages=5):
    """Fetch up to max_messages from a thread for conversation context."""
    try:
        thread = service.users().threads().get(
            userId="me", id=thread_id, format="full"
        ).execute()
    except Exception:
        return []

    messages = thread.get("messages", [])
    messages = messages[-max_messages:]

    result = []
    for msg in messages:
        parsed = _parse_message(msg)
        if parsed:
            result.append({
                "from": parsed["from"],
                "date": parsed["date"],
                "body": parsed["body"][:1000],
            })
    return result


# -- Fetch Sam's replies to escalation emails --------------------------------
def fetch_sam_replies(service, sam_email, hours=2):
    """Fetch recent emails FROM Sam that are replies to Larry's escalations."""
    query = (
        f"from:{sam_email} newer_than:{hours}h "
        "subject:[Larry]"
    )
    results = []
    try:
        resp = service.users().messages().list(
            userId="me", q=query, maxResults=20
        ).execute()
        for msg_ref in resp.get("messages", []):
            msg = service.users().messages().get(
                userId="me", id=msg_ref["id"], format="full"
            ).execute()
            parsed = _parse_message(msg)
            if parsed:
                results.append(parsed)
    except Exception:
        pass
    return results


# -- Send HTML email ---------------------------------------------------------
def send_html_email(service, to_email, subject, html_body):
    """Send an HTML email (used for daily digest)."""
    msg = MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["Subject"] = subject
    msg.attach(MIMEText(html_body, "html"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()


# -- Send reply in thread ----------------------------------------------------
def send_reply_in_thread(service, original_email, reply_body, signature):
    """Send a reply directly (not a draft) in the original email's thread."""
    subject = original_email.get("subject", "")
    if not subject.lower().startswith("re:"):
        subject = f"Re: {subject}"
    sender = original_email.get("from", "")
    msg = MIMEMultipart("alternative")
    msg["To"] = sender
    msg["Subject"] = subject
    msg["In-Reply-To"] = original_email.get("message_id", "")
    msg["References"] = original_email.get("message_id", "")
    full_body = f"{reply_body}\n\n{signature}"
    msg.attach(MIMEText(full_body, "plain"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    return service.users().messages().send(
        userId="me",
        body={"raw": raw, "threadId": original_email.get("thread_id", "")},
    ).execute()


# -- Check if a draft was sent (for edit tracking) --------------------------
def find_sent_version_of_draft(service, original_to, original_subject, draft_created_after):
    """Search sent mail for a message matching a draft Larry created."""
    clean_subject = original_subject.replace("Re: ", "").replace("RE: ", "")
    query = (
        f"in:sent to:{original_to} subject:\"{clean_subject}\" "
        f"newer_than:7d"
    )
    try:
        resp = service.users().messages().list(
            userId="me", q=query, maxResults=5
        ).execute()
        for msg_ref in resp.get("messages", []):
            msg = service.users().messages().get(
                userId="me", id=msg_ref["id"], format="full"
            ).execute()
            internal_date = int(msg.get("internalDate", 0)) / 1000
            sent_time = datetime.fromtimestamp(internal_date)
            if sent_time < draft_created_after:
                continue
            parsed = _parse_message(msg)
            if parsed:
                return parsed["body"]
    except Exception:
        pass
    return None
