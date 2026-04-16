"""
WC Solns -- Gmail Email Provider

Adapts the Gmail API to the standard EmailProvider interface.
Uses Google OAuth2 credentials (same as patrol/SEO pipelines).

Config (provider_config.gmail in tenant_config.json):
    credentials_path:  Path to OAuth2 credentials.json (relative to project root)
    token_path:        Path to cached token.json (relative to project root)
    sender_email_env:  Env var holding the sender email address
    app_password_env:  Env var holding Gmail App Password (for SMTP fallback)
"""

import base64
import logging
import os
import sys
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email import encoders
from pathlib import Path

from providers.base import EmailProvider

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class GmailProvider(EmailProvider):
    """Gmail API email adapter."""

    def __init__(self, config: dict):
        self._creds_path = _PROJECT_ROOT / config.get(
            "credentials_path", "patrol_automation/credentials.json"
        )
        self._token_path = _PROJECT_ROOT / config.get(
            "token_path", "patrol_automation/token.json"
        )
        sender_env = config.get("sender_email_env", "WATCHDOG_EMAIL_FROM")
        self._sender = os.getenv(sender_env, "")
        self._service = None

    def _get_service(self):
        """Lazy-init the Gmail API service."""
        if self._service is not None:
            return self._service

        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build

        SCOPES = [
            "https://www.googleapis.com/auth/gmail.readonly",
            "https://www.googleapis.com/auth/gmail.compose",
            "https://www.googleapis.com/auth/gmail.send",
        ]

        creds = None
        if self._token_path.exists():
            creds = Credentials.from_authorized_user_file(
                str(self._token_path), SCOPES
            )
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                from google_auth_oauthlib.flow import InstalledAppFlow
                flow = InstalledAppFlow.from_client_secrets_file(
                    str(self._creds_path), SCOPES
                )
                creds = flow.run_local_server(port=0)
            with open(self._token_path, "w") as f:
                f.write(creds.to_json())

        self._service = build("gmail", "v1", credentials=creds)
        return self._service

    # -- EmailProvider interface ----------------------------------------

    def send_email(self, to, subject, html_body,
                   from_email="", from_name="",
                   reply_to="", bcc=None):
        service = self._get_service()

        if isinstance(to, list):
            to_str = ", ".join(to)
        else:
            to_str = to

        msg = MIMEMultipart("alternative")
        msg["To"] = to_str
        msg["Subject"] = subject
        msg["From"] = from_email or self._sender
        if reply_to:
            msg["Reply-To"] = reply_to
        if bcc:
            msg["Bcc"] = ", ".join(bcc) if isinstance(bcc, list) else bcc

        msg.attach(MIMEText(html_body, "html"))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        result = service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()

        msg_id = result.get("id", "")
        log.info("Gmail sent to %s: id=%s", to_str, msg_id)
        return {"success": True, "message_id": msg_id}

    def create_draft(self, to, subject, html_body,
                     from_email="", from_name=""):
        service = self._get_service()

        msg = MIMEMultipart("alternative")
        msg["To"] = to
        msg["Subject"] = subject
        msg["From"] = from_email or self._sender

        msg.attach(MIMEText(html_body, "html"))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        draft = service.users().drafts().create(
            userId="me", body={"message": {"raw": raw}}
        ).execute()

        draft_id = draft.get("id", "")
        log.info("Gmail draft created: id=%s", draft_id)
        return {"success": True, "draft_id": draft_id}

    def send_with_attachments(self, to, subject, html_body,
                              attachments, from_email="",
                              from_name=""):
        service = self._get_service()

        if isinstance(to, list):
            to_str = ", ".join(to)
        else:
            to_str = to

        msg = MIMEMultipart("mixed")
        msg["To"] = to_str
        msg["Subject"] = subject
        msg["From"] = from_email or self._sender

        body_part = MIMEText(html_body, "html")
        msg.attach(body_part)

        for att in attachments:
            maintype, subtype = att.get("mime_type", "application/octet-stream").split("/", 1)
            part = MIMEBase(maintype, subtype)
            part.set_payload(att["content"])
            encoders.encode_base64(part)
            part.add_header(
                "Content-Disposition", "attachment",
                filename=att.get("filename", "attachment")
            )
            msg.attach(part)

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        result = service.users().messages().send(
            userId="me", body={"raw": raw}
        ).execute()

        msg_id = result.get("id", "")
        log.info("Gmail sent with %d attachments to %s: id=%s",
                 len(attachments), to_str, msg_id)
        return {"success": True, "message_id": msg_id}

    # -- Gmail-specific methods (not part of standard interface) --------

    def create_reply_draft(self, original_email: dict, reply_body: str,
                           signature: str = "") -> dict:
        """Create a reply draft in the correct thread."""
        service = self._get_service()

        subject = original_email.get("subject", "")
        if not subject.lower().startswith("re:"):
            subject = f"Re: {subject}"

        full_body = f"{reply_body}\n\n{signature}" if signature else reply_body

        msg = MIMEMultipart("alternative")
        msg["To"] = original_email.get("from", "")
        msg["Subject"] = subject
        msg["In-Reply-To"] = original_email.get("message_id", "")
        msg["References"] = original_email.get("message_id", "")
        msg.attach(MIMEText(full_body, "plain"))

        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
        draft = service.users().drafts().create(
            userId="me",
            body={"message": {
                "raw": raw,
                "threadId": original_email.get("thread_id", ""),
            }}
        ).execute()
        return {"success": True, "draft_id": draft.get("id", "")}

    def fetch_unread_emails(self, hours: int = 2) -> list[dict]:
        """Fetch unread emails from the last N hours.
        Gmail-specific -- not part of the standard interface."""
        service = self._get_service()

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
                parsed = self._parse_message(msg)
                if parsed:
                    results.append(parsed)

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        return results

    @staticmethod
    def _parse_message(msg: dict) -> dict | None:
        """Extract key fields from a Gmail API message."""
        headers = {
            h["name"].lower(): h["value"]
            for h in msg["payload"]["headers"]
        }

        body = GmailProvider._extract_text_body(msg["payload"])

        return {
            "id": msg["id"],
            "thread_id": msg["threadId"],
            "from": headers.get("from", ""),
            "to": headers.get("to", ""),
            "subject": headers.get("subject", ""),
            "date": headers.get("date", ""),
            "message_id": headers.get("message-id", ""),
            "body": body,
            "labels": msg.get("labelIds", []),
        }

    @staticmethod
    def _extract_text_body(payload: dict) -> str:
        """Get plain text body, falling back to stripped HTML."""
        parts = GmailProvider._flatten_parts(payload)

        for part in parts:
            if part.get("mimeType") == "text/plain":
                data = part.get("body", {}).get("data", "")
                if data:
                    return base64.urlsafe_b64decode(data).decode(
                        "utf-8", errors="replace"
                    )

        for part in parts:
            if part.get("mimeType") == "text/html":
                data = part.get("body", {}).get("data", "")
                if data:
                    import re
                    html = base64.urlsafe_b64decode(data).decode(
                        "utf-8", errors="replace"
                    )
                    text = re.sub(r"<[^>]+>", " ", html)
                    text = re.sub(r"\s+", " ", text).strip()
                    return text

        return ""

    @staticmethod
    def _flatten_parts(payload: dict) -> list:
        """Recursively flatten MIME parts."""
        parts = []
        if "parts" in payload:
            for part in payload["parts"]:
                parts.extend(GmailProvider._flatten_parts(part))
        else:
            parts.append(payload)
        return parts
