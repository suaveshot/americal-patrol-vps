"""
Sales Pipeline — Unified GHL API Client
Combines sales_autopilot/ghl_sales_client.py (estimates, opportunities, conversations)
with cold_outreach_automation/ghl_client.py (contact pagination).
"""

import logging
import time
from datetime import datetime

import requests

from sales_pipeline.config import (
    GHL_API_KEY, GHL_LOCATION_ID, GHL_BASE_URL, GHL_API_VERSION, FROM_EMAIL,
)

log = logging.getLogger(__name__)


class GHLAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"GHL API {status_code}: {message}")


class GHLClient:
    """Unified GHL REST client for the full sales pipeline."""

    def __init__(self):
        self._base = GHL_BASE_URL
        self._headers = {
            "Authorization": f"Bearer {GHL_API_KEY()}",
            "Version": GHL_API_VERSION,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._location_id = GHL_LOCATION_ID()

    # ── HTTP helpers ──────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        r = requests.get(url, headers=self._headers, params=params or {}, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.get(url, headers=self._headers, params=params or {}, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def _post(self, path: str, body: dict) -> dict:
        url = f"{self._base}{path}"
        r = requests.post(url, headers=self._headers, json=body, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.post(url, headers=self._headers, json=body, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def _put(self, path: str, body: dict) -> dict:
        url = f"{self._base}{path}"
        r = requests.put(url, headers=self._headers, json=body, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.put(url, headers=self._headers, json=body, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def _delete(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        r = requests.delete(url, headers=self._headers, params=params or {}, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.delete(url, headers=self._headers, params=params or {}, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        if r.text:
            return r.json()
        return {}

    def _get_binary(self, path: str, params: dict | None = None) -> bytes | None:
        """Download binary data (e.g. call recordings). Returns bytes or None on 404."""
        url = f"{self._base}{path}"
        headers = {
            "Authorization": self._headers["Authorization"],
            "Version": self._headers["Version"],
            "Accept": "*/*",
        }
        r = requests.get(url, headers=headers, params=params or {}, timeout=60)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.get(url, headers=headers, params=params or {}, timeout=60)
        if r.status_code in (404, 422):
            return None
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.content

    def download_call_recording(self, message_id: str) -> bytes | None:
        """Download a call recording WAV file. Returns raw bytes or None."""
        path = f"/conversations/messages/{message_id}/locations/{self._location_id}/recording"
        data = self._get_binary(path)
        if data:
            log.info("Downloaded call recording for message %s (%d bytes)", message_id, len(data))
        return data

    # ── Contact operations (from cold_outreach_automation) ────────

    def get_contacts(self, page_size: int = 100) -> list:
        """Paginate all contacts for this location."""
        contacts = []
        params = {
            "locationId": self._location_id,
            "limit": page_size,
        }
        while True:
            data = self._get("/contacts/", params)
            batch = data.get("contacts", [])
            contacts.extend(batch)
            if not batch:
                break
            meta = data.get("meta", {})
            next_after = meta.get("startAfter")
            next_id = meta.get("startAfterId")
            if next_after is not None and next_id:
                params["startAfter"] = int(next_after)
                params["startAfterId"] = next_id
            elif meta.get("nextPage") is None:
                break
            else:
                break
        log.info(f"Fetched {len(contacts)} contacts from GHL")
        return contacts

    def get_contact(self, contact_id: str) -> dict:
        """Fetch a single contact by ID."""
        data = self._get(f"/contacts/{contact_id}")
        return data.get("contact", data)

    def get_contact_custom_field(self, contact_id: str, field_key: str) -> str | None:
        """Extract a custom field value from a contact. Returns None if not found."""
        contact = self.get_contact(contact_id)
        custom_fields = contact.get("customFields", contact.get("customField", []))

        if isinstance(custom_fields, list):
            for field in custom_fields:
                key = field.get("key", field.get("id", "")).lower()
                if field_key.lower() in key:
                    val = field.get("value", "").strip()
                    return val if val else None
        elif isinstance(custom_fields, dict):
            for key, val in custom_fields.items():
                if field_key.lower() in key.lower() and val:
                    return str(val).strip()
        return None

    def update_contact(self, contact_id: str, updates: dict) -> dict:
        """Update a contact's fields. Returns updated contact data."""
        return self._put(f"/contacts/{contact_id}", updates)

    # ── Conversation & messaging ─────────────────────────────────

    def search_conversations(self, contact_id: str) -> list:
        """Find conversations for a contact."""
        params = {
            "locationId": self._location_id,
            "contactId": contact_id,
        }
        data = self._get("/conversations/search", params=params)
        return data.get("conversations", [])

    def get_conversation_messages(self, conversation_id: str) -> list:
        """Get messages in a conversation, newest first."""
        data = self._get(f"/conversations/{conversation_id}/messages")
        msgs = data.get("messages", {})
        if isinstance(msgs, dict):
            return msgs.get("messages", [])
        return msgs

    def send_email(self, contact_id: str, subject: str, html_body: str,
                   attachment_urls: list[str] | None = None,
                   scheduled_at: datetime | None = None) -> str:
        """Send an email to a contact via GHL conversations. Returns messageId."""
        body = {
            "type": "Email",
            "contactId": contact_id,
            "subject": subject,
            "html": html_body,
            "emailFrom": FROM_EMAIL(),
        }
        if attachment_urls:
            body["attachments"] = attachment_urls
        if scheduled_at:
            body["scheduledTimestamp"] = int(scheduled_at.timestamp())
        data = self._post("/conversations/messages", body)
        msg_id = data.get("messageId", data.get("id", ""))
        if scheduled_at:
            log.info(f"Email scheduled for contact {contact_id} at {scheduled_at.isoformat()}: messageId={msg_id}")
        else:
            log.info(f"Email sent to contact {contact_id}: messageId={msg_id}")
        return msg_id

    def send_sms(self, contact_id: str, message: str,
                 scheduled_at: datetime | None = None) -> str:
        """Send an SMS to a contact via GHL conversations. Returns messageId."""
        body = {
            "type": "SMS",
            "contactId": contact_id,
            "message": message,
        }
        if scheduled_at:
            body["scheduledTimestamp"] = int(scheduled_at.timestamp())
        data = self._post("/conversations/messages", body)
        msg_id = data.get("messageId", data.get("id", ""))
        if scheduled_at:
            log.info(f"SMS scheduled for contact {contact_id} at {scheduled_at.isoformat()}: messageId={msg_id}")
        else:
            log.info(f"SMS sent to contact {contact_id}: messageId={msg_id}")
        return msg_id

    def cancel_scheduled_message(self, message_id: str) -> bool:
        """Cancel a previously scheduled message. Returns True if successful."""
        try:
            self._delete(f"/conversations/messages/{message_id}/schedule")
            log.info(f"Cancelled scheduled message: {message_id}")
            return True
        except GHLAPIError as e:
            log.warning(f"Failed to cancel scheduled message {message_id}: {e}")
            return False

    # ── Estimate API (version 2021-07-28) ─────────────────────────

    def _estimate_headers(self):
        return {**self._headers, "Version": "2021-07-28"}

    def _get_estimate(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        headers = self._estimate_headers()
        r = requests.get(url, headers=headers, params=params or {}, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.get(url, headers=headers, params=params or {}, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def _post_estimate(self, path: str, body: dict) -> dict:
        url = f"{self._base}{path}"
        headers = self._estimate_headers()
        r = requests.post(url, headers=headers, json=body, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.post(url, headers=headers, json=body, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def create_estimate(self, estimate_body: dict) -> dict:
        return self._post_estimate("/invoices/estimate", estimate_body)

    def send_estimate(self, estimate_id: str, action: str = "sms_and_email",
                      user_id: str = None, sent_from: dict = None) -> dict:
        body = {
            "altId": self._location_id,
            "altType": "location",
            "action": action,
            "liveMode": True,
        }
        if user_id:
            body["userId"] = user_id
        if sent_from:
            body["sentFrom"] = sent_from
        return self._post_estimate(
            f"/invoices/estimate/{estimate_id}/send", body
        )

    def get_estimate(self, estimate_id: str) -> dict:
        params = {"altId": self._location_id, "altType": "location"}
        return self._get_estimate(
            f"/invoices/estimate/{estimate_id}", params=params
        )

    def list_estimates(self, status: str = None, contact_id: str = None) -> list:
        params = {
            "altId": self._location_id,
            "altType": "location",
            "limit": 100,
            "offset": 0,
        }
        if status:
            params["status"] = status
        if contact_id:
            params["contactId"] = contact_id
        data = self._get_estimate("/invoices/estimate/list", params=params)
        return data.get("data", data.get("estimates", []))

    # ── Opportunity / pipeline operations ─────────────────────────

    def create_opportunity(self, contact_id: str, pipeline_id: str,
                           stage_id: str, name: str) -> str:
        body = {
            "pipelineId": pipeline_id,
            "pipelineStageId": stage_id,
            "locationId": self._location_id,
            "contactId": contact_id,
            "name": name,
            "status": "open",
        }
        data = self._post("/opportunities/", body)
        return data.get("opportunity", {}).get("id", data.get("id", ""))

    def search_opportunities(self, pipeline_id: str, stage_id: str | None = None) -> list:
        params = {
            "location_id": self._location_id,
            "pipeline_id": pipeline_id,
        }
        if stage_id:
            params["pipeline_stage_id"] = stage_id
        data = self._get("/opportunities/search", params=params)
        return data.get("opportunities", [])

    def update_opportunity(self, opportunity_id: str, updates: dict) -> None:
        self._put(f"/opportunities/{opportunity_id}", updates)

    def update_opportunity_stage(self, opportunity_id: str, stage_id: str) -> None:
        self._put(f"/opportunities/{opportunity_id}", {
            "pipelineStageId": stage_id,
        })

    # ── Full conversation history (for win/loss analysis) ────────

    def get_full_conversation_history(self, contact_id: str) -> list:
        """
        Pull all conversations for a contact and return a chronological
        list of messages with type, direction, subject, and body.
        """
        conversations = self.search_conversations(contact_id)
        all_messages = []

        for conv in conversations:
            conv_id = conv.get("id")
            if not conv_id:
                continue
            messages = self.get_conversation_messages(conv_id)
            for msg in messages:
                all_messages.append({
                    "timestamp": msg.get("dateAdded", ""),
                    "direction": msg.get("direction", ""),
                    "type": msg.get("type", ""),
                    "subject": msg.get("subject", ""),
                    "body": msg.get("body", msg.get("message", "")),
                    "status": msg.get("status", ""),
                })

        all_messages.sort(key=lambda m: m.get("timestamp", ""))
        return all_messages

    # ── Estimate view tracking ───────────────────────────────────

    def has_viewed_estimate(self, estimate_id: str) -> dict:
        """
        Check if a prospect has viewed the estimate.
        Returns {viewed: bool, viewed_at: str|None}.
        GHL estimate status transitions: draft -> sent -> viewed -> accepted.
        """
        try:
            data = self.get_estimate(estimate_id)
            estimate = data.get("estimate", data)
            status = estimate.get("status", "")
            viewed = status in ("viewed", "accepted")
            return {
                "viewed": viewed,
                "viewed_at": estimate.get("updatedAt") if viewed else None,
                "status": status,
            }
        except GHLAPIError:
            return {"viewed": False, "viewed_at": None, "status": "unknown"}
