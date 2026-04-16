"""
WC Solns -- GoHighLevel SMS Provider

Sends SMS via GHL's conversation messaging API.
Requires the GHL CRM to be configured (uses contact_id-based sending).

Note: GHL SMS sends to a contact by ID, not by phone number.
This adapter looks up the contact by phone number first.

Config: Uses the same GHL config as the CRM provider.
"""

import logging
import os

import requests

from providers.base import SMSProvider

log = logging.getLogger(__name__)


class GHLSMSProvider(SMSProvider):
    """GoHighLevel SMS adapter."""

    def __init__(self, config: dict):
        self._api_key = os.getenv(config.get("api_key_env", "GHL_API_KEY"), "")
        self._location_id = os.getenv(config.get("location_id_env", "GHL_LOCATION_ID"), "")
        self._base = config.get("base_url", "https://services.leadconnectorhq.com")

    def _headers(self):
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Version": "2021-04-15",
            "Content-Type": "application/json",
        }

    def _find_contact_by_phone(self, phone: str) -> str | None:
        """Look up a GHL contact ID by phone number."""
        r = requests.get(
            f"{self._base}/contacts/search/duplicate",
            headers=self._headers(),
            params={"locationId": self._location_id, "phone": phone},
            timeout=15,
        )
        if r.status_code == 200:
            contact = r.json().get("contact")
            if contact:
                return contact.get("id")
        return None

    def send_sms(self, to, message, from_number=""):
        # GHL sends SMS by contact_id, so look up the contact first
        contact_id = self._find_contact_by_phone(to)
        if not contact_id:
            return {
                "success": False,
                "message_id": "",
                "error": f"No GHL contact found for phone {to}",
            }

        body = {
            "type": "SMS",
            "contactId": contact_id,
            "message": message,
        }

        r = requests.post(
            f"{self._base}/conversations/messages",
            headers=self._headers(),
            json=body,
            timeout=30,
        )

        if r.status_code not in (200, 201):
            return {"success": False, "message_id": "", "error": r.text[:200]}

        msg_id = r.json().get("messageId", r.json().get("id", ""))
        log.info("GHL SMS sent to contact %s: messageId=%s", contact_id, msg_id)
        return {"success": True, "message_id": msg_id}
