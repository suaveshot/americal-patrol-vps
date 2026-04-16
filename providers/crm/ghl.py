"""
WC Solns -- GoHighLevel CRM Provider

Adapts the GHL REST API to the standard CRMProvider interface.
Extracted from sales_pipeline/ghl_client.py.

Config (provider_config.ghl in tenant_config.json):
    api_key_env:    env var name holding the API key (default: GHL_API_KEY)
    location_id_env: env var name holding location ID (default: GHL_LOCATION_ID)
    base_url:       GHL API base (default: https://services.leadconnectorhq.com)
    api_version:    API version header (default: 2021-04-15)
"""

import logging
import os
import time
from datetime import datetime

import requests

from providers.base import CRMProvider

log = logging.getLogger(__name__)


class GHLAPIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"GHL API {status_code}: {message}")


class GHLCRMProvider(CRMProvider):
    """GoHighLevel CRM adapter."""

    def __init__(self, config: dict):
        api_key_env = config.get("api_key_env", "GHL_API_KEY")
        location_id_env = config.get("location_id_env", "GHL_LOCATION_ID")

        self._api_key = os.getenv(api_key_env, "")
        self._location_id = os.getenv(location_id_env, "")
        self._base = config.get("base_url", "https://services.leadconnectorhq.com")
        self._api_version = config.get("api_version", "2021-04-15")

        self._headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Version": self._api_version,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    # -- HTTP helpers ---------------------------------------------------

    def _get(self, path: str, params: dict | None = None,
             headers: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        hdrs = headers or self._headers
        r = requests.get(url, headers=hdrs, params=params or {}, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.get(url, headers=hdrs, params=params or {}, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def _post(self, path: str, body: dict,
              headers: dict | None = None) -> dict:
        url = f"{self._base}{path}"
        hdrs = headers or self._headers
        r = requests.post(url, headers=hdrs, json=body, timeout=30)
        if r.status_code == 429:
            log.warning("GHL rate limited, sleeping 5s")
            time.sleep(5)
            r = requests.post(url, headers=hdrs, json=body, timeout=30)
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

    # -- Contact normalization ------------------------------------------

    @staticmethod
    def _normalize_contact(raw: dict) -> dict:
        """Convert GHL contact shape to standard shape."""
        return {
            "id": raw.get("id", ""),
            "name": f"{raw.get('firstName', '')} {raw.get('lastName', '')}".strip()
                    or raw.get("name", ""),
            "email": raw.get("email", ""),
            "phone": raw.get("phone", ""),
            "company": raw.get("companyName", ""),
            "tags": raw.get("tags", []),
            "created_at": raw.get("dateAdded", ""),
            "updated_at": raw.get("dateUpdated", ""),
            "custom_fields": {
                f.get("key", f.get("id", "")): f.get("value", "")
                for f in (raw.get("customFields", []) or [])
                if isinstance(f, dict)
            },
            "_raw": raw,
        }

    # -- CRMProvider: Contacts ------------------------------------------

    def list_contacts(self, limit: int = 100, offset: int = 0,
                      **filters) -> list[dict]:
        contacts = []
        params = {
            "locationId": self._location_id,
            "limit": min(limit, 100),
        }
        while True:
            data = self._get("/contacts/", params)
            batch = data.get("contacts", [])
            contacts.extend(self._normalize_contact(c) for c in batch)
            if not batch or len(contacts) >= limit:
                break
            meta = data.get("meta", {})
            next_after = meta.get("startAfter")
            next_id = meta.get("startAfterId")
            if next_after is not None and next_id:
                params["startAfter"] = int(next_after)
                params["startAfterId"] = next_id
            else:
                break
        log.info("Fetched %d contacts from GHL", len(contacts))
        return contacts[:limit]

    def get_contact(self, contact_id: str) -> dict:
        data = self._get(f"/contacts/{contact_id}")
        raw = data.get("contact", data)
        return self._normalize_contact(raw)

    def create_contact(self, data: dict) -> dict:
        body = {
            "locationId": self._location_id,
            "firstName": data.get("name", "").split()[0] if data.get("name") else "",
            "lastName": " ".join(data.get("name", "").split()[1:]) if data.get("name") else "",
            "email": data.get("email", ""),
            "phone": data.get("phone", ""),
            "companyName": data.get("company", ""),
            "tags": data.get("tags", []),
        }
        result = self._post("/contacts/", body)
        raw = result.get("contact", result)
        return self._normalize_contact(raw)

    def update_contact(self, contact_id: str, data: dict) -> dict:
        body = {}
        if "name" in data:
            parts = data["name"].split()
            body["firstName"] = parts[0] if parts else ""
            body["lastName"] = " ".join(parts[1:]) if len(parts) > 1 else ""
        if "email" in data:
            body["email"] = data["email"]
        if "phone" in data:
            body["phone"] = data["phone"]
        if "company" in data:
            body["companyName"] = data["company"]
        if "tags" in data:
            body["tags"] = data["tags"]
        result = self._put(f"/contacts/{contact_id}", body)
        raw = result.get("contact", result)
        return self._normalize_contact(raw)

    def search_contacts(self, query: str) -> list[dict]:
        params = {
            "locationId": self._location_id,
            "query": query,
            "limit": 50,
        }
        data = self._get("/contacts/", params)
        return [self._normalize_contact(c) for c in data.get("contacts", [])]

    # -- CRMProvider: Pipeline / Deals ----------------------------------

    def list_pipeline_stages(self, pipeline_id: str = "") -> list[dict]:
        data = self._get("/opportunities/pipelines")
        pipelines = data.get("pipelines", [])
        stages = []
        for p in pipelines:
            if pipeline_id and p.get("id") != pipeline_id:
                continue
            for i, s in enumerate(p.get("stages", [])):
                stages.append({
                    "id": s.get("id", ""),
                    "name": s.get("name", ""),
                    "order": i,
                    "pipeline_id": p.get("id", ""),
                    "pipeline_name": p.get("name", ""),
                })
        return stages

    def get_opportunities(self, pipeline_id: str = "",
                          stage_id: str = "",
                          **filters) -> list[dict]:
        params = {"location_id": self._location_id}
        if pipeline_id:
            params["pipeline_id"] = pipeline_id
        if stage_id:
            params["pipeline_stage_id"] = stage_id
        data = self._get("/opportunities/search", params=params)
        raw_opps = data.get("opportunities", [])
        return [
            {
                "id": o.get("id", ""),
                "name": o.get("name", ""),
                "value": o.get("monetaryValue", 0),
                "stage_id": o.get("pipelineStageId", ""),
                "stage_name": "",
                "contact_id": o.get("contactId", o.get("contact", {}).get("id", "")),
                "status": o.get("status", "open"),
                "created_at": o.get("createdAt", ""),
                "updated_at": o.get("updatedAt", ""),
                "_raw": o,
            }
            for o in raw_opps
        ]

    def create_opportunity(self, data: dict) -> dict:
        body = {
            "pipelineId": data["pipeline_id"] if "pipeline_id" in data else data.get("stage_id", "").split("/")[0] if "/" in data.get("stage_id", "") else "",
            "pipelineStageId": data.get("stage_id", ""),
            "locationId": self._location_id,
            "contactId": data.get("contact_id", ""),
            "name": data.get("name", ""),
            "status": data.get("status", "open"),
        }
        if "value" in data:
            body["monetaryValue"] = data["value"]
        if "pipeline_id" in data:
            body["pipelineId"] = data["pipeline_id"]
        result = self._post("/opportunities/", body)
        raw = result.get("opportunity", result)
        return {
            "id": raw.get("id", ""),
            "name": raw.get("name", ""),
            "value": raw.get("monetaryValue", 0),
            "stage_id": raw.get("pipelineStageId", ""),
            "contact_id": raw.get("contactId", ""),
            "status": raw.get("status", "open"),
        }

    def update_opportunity(self, opp_id: str, data: dict) -> dict:
        body = {}
        if "stage_id" in data:
            body["pipelineStageId"] = data["stage_id"]
        if "status" in data:
            body["status"] = data["status"]
        if "value" in data:
            body["monetaryValue"] = data["value"]
        if "name" in data:
            body["name"] = data["name"]
        self._put(f"/opportunities/{opp_id}", body)
        return {"id": opp_id, **data}

    # -- CRMProvider: Conversations -------------------------------------

    def get_conversations(self, contact_id: str) -> list[dict]:
        params = {
            "locationId": self._location_id,
            "contactId": contact_id,
        }
        data = self._get("/conversations/search", params=params)
        conversations = data.get("conversations", [])

        messages = []
        for conv in conversations:
            conv_id = conv.get("id")
            if not conv_id:
                continue
            msg_data = self._get(f"/conversations/{conv_id}/messages")
            raw_msgs = msg_data.get("messages", {})
            if isinstance(raw_msgs, dict):
                raw_msgs = raw_msgs.get("messages", [])
            for msg in raw_msgs:
                messages.append({
                    "id": msg.get("id", ""),
                    "direction": msg.get("direction", ""),
                    "channel": msg.get("type", "").lower(),
                    "body": msg.get("body", msg.get("message", "")),
                    "subject": msg.get("subject", ""),
                    "timestamp": msg.get("dateAdded", ""),
                })
        messages.sort(key=lambda m: m.get("timestamp", ""))
        return messages

    # -- CRMProvider: Calls ---------------------------------------------

    def get_calls(self, since: str = "", limit: int = 50) -> list[dict]:
        log.warning("GHL get_calls: use get_conversations and filter for calls")
        return []

    # -- Feature detection ----------------------------------------------

    def supports_feature(self, feature: str) -> bool:
        return feature in (
            "estimates", "conversations", "sms", "email_sending", "pipelines"
        )

    # -- GHL-specific methods (not part of standard interface) ----------

    def send_email(self, contact_id: str, subject: str, html_body: str,
                   from_email: str = "",
                   attachment_urls: list[str] | None = None) -> str:
        """Send email via GHL conversations. Returns messageId."""
        body = {
            "type": "Email",
            "contactId": contact_id,
            "subject": subject,
            "html": html_body,
        }
        if from_email:
            body["emailFrom"] = from_email
        if attachment_urls:
            body["attachments"] = attachment_urls
        data = self._post("/conversations/messages", body)
        msg_id = data.get("messageId", data.get("id", ""))
        log.info("GHL email sent to contact %s: messageId=%s", contact_id, msg_id)
        return msg_id

    def send_sms(self, contact_id: str, message: str) -> str:
        """Send SMS via GHL conversations. Returns messageId."""
        body = {
            "type": "SMS",
            "contactId": contact_id,
            "message": message,
        }
        data = self._post("/conversations/messages", body)
        msg_id = data.get("messageId", data.get("id", ""))
        log.info("GHL SMS sent to contact %s: messageId=%s", contact_id, msg_id)
        return msg_id

    def create_estimate(self, estimate_body: dict) -> dict:
        """Create a GHL estimate/invoice."""
        headers = {**self._headers, "Version": "2021-07-28"}
        return self._post("/invoices/estimate", estimate_body, headers=headers)

    def send_estimate(self, estimate_id: str, action: str = "sms_and_email",
                      user_id: str = None, sent_from: dict = None) -> dict:
        """Send a GHL estimate to the contact."""
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
        headers = {**self._headers, "Version": "2021-07-28"}
        return self._post(f"/invoices/estimate/{estimate_id}/send", body,
                          headers=headers)

    def get_estimate(self, estimate_id: str) -> dict:
        """Get a GHL estimate by ID."""
        params = {"altId": self._location_id, "altType": "location"}
        headers = {**self._headers, "Version": "2021-07-28"}
        url = f"{self._base}/invoices/estimate/{estimate_id}"
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        return r.json()

    def list_estimates(self, status: str = None,
                       contact_id: str = None) -> list:
        """List GHL estimates."""
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
        headers = {**self._headers, "Version": "2021-07-28"}
        url = f"{self._base}/invoices/estimate/list"
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code >= 400:
            raise GHLAPIError(r.status_code, r.text[:300])
        data = r.json()
        return data.get("data", data.get("estimates", []))

    def has_viewed_estimate(self, estimate_id: str) -> dict:
        """Check if a prospect has viewed the estimate."""
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

    def get_full_conversation_history(self, contact_id: str) -> list[dict]:
        """Pull all conversations for a contact chronologically."""
        return self.get_conversations(contact_id)
