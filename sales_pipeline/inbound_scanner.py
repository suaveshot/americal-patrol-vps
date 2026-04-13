"""
Sales Pipeline — Inbound Lead Scanner

Scans recently-created GHL contacts for inbound activity (web forms,
phone calls, SMS, email) that should be tracked in the sales pipeline
but weren't auto-ingested.

Runs as part of --hourly to catch leads that GHL workflows don't
sync to the Python pipeline state.
"""

import logging
from datetime import datetime, timezone, timedelta

from sales_pipeline.cold_outreach.lead_filter import _is_spam_or_job_seeker, is_in_service_area

log = logging.getLogger(__name__)

# How far back to look for new contacts (hours)
LOOKBACK_HOURS = 48


def _parse_iso(date_str: str) -> datetime | None:
    """Parse ISO8601 string to UTC-aware datetime."""
    if not date_str:
        return None
    try:
        s = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def _classify_engagement(messages: list) -> str | None:
    """
    Classify the engagement source from conversation messages.
    Returns: web_form, phone_call, sms, email, or None if no inbound activity.
    """
    for msg in messages:
        direction = msg.get("direction", "")
        msg_type = msg.get("type", "")
        body = msg.get("body", msg.get("message", ""))

        # Form submissions (GHL type varies — check for form-like content)
        if msg_type in ("FormSubmission", "form_submission"):
            return "web_form"

        # Inbound phone calls
        if direction == "inbound" and msg_type in (1, "1", "Call", "call"):
            return "phone_call"

        # Inbound SMS
        if direction == "inbound" and msg_type in (2, "2", "SMS", "sms"):
            return "sms"

        # Inbound email
        if direction == "inbound" and msg_type in (3, "3", "Email", "email"):
            return "email"

        # Any inbound message is a signal
        if direction == "inbound":
            return "email"

    return None


def scan_inbound_leads(ghl, state: dict, contacts: list | None = None) -> list:
    """
    Scan recently-created GHL contacts for inbound inquiries not yet in the pipeline.

    Args:
        ghl: GHLClient instance
        state: Current pipeline state dict
        contacts: Optional pre-fetched contact list (avoids extra API call)

    Returns:
        List of dicts ready for add_contact():
        [{contact_id, first_name, last_name, organization, email, phone,
          property_type, engagement_source, engagement_velocity}]
    """
    if contacts is None:
        contacts = ghl.get_contacts()

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=LOOKBACK_HOURS)
    already_tracked = set(state.get("contacts", {}).keys())

    candidates = []
    for c in contacts:
        contact_id = c.get("id", "")
        if not contact_id or contact_id in already_tracked:
            continue

        # Only look at recently created contacts
        created_str = c.get("dateAdded") or c.get("dateCreated", "")
        created_dt = _parse_iso(created_str)
        if not created_dt or created_dt < cutoff:
            continue

        # Must have the 'lead' tag (GHL applies this on form submissions and
        # manually for phone inquiries)
        tags = c.get("tags", [])
        if not isinstance(tags, list) or "lead" not in tags:
            continue

        # Apply spam/job-seeker filter
        if _is_spam_or_job_seeker(c):
            log.debug("Inbound scanner: %s filtered as spam/job seeker", contact_id)
            continue

        # Check service area
        city = c.get("city", "")
        if city and not is_in_service_area(city):
            log.debug("Inbound scanner: %s filtered — %s not in service area", contact_id, city)
            continue

        candidates.append((contact_id, c))

    if not candidates:
        return []

    # Check conversations for inbound activity
    qualified = []
    for contact_id, c in candidates:
        try:
            convs = ghl.search_conversations(contact_id)
        except Exception as e:
            log.warning("Inbound scanner: conversation search failed for %s: %s", contact_id, e)
            continue

        engagement_source = None
        for conv in convs:
            conv_id = conv.get("id")
            if not conv_id:
                continue
            try:
                messages = ghl.get_conversation_messages(conv_id)
            except Exception as e:
                log.warning("Inbound scanner: message fetch failed for conv %s: %s", conv_id, e)
                continue

            source = _classify_engagement(messages)
            if source:
                engagement_source = source
                break

        if not engagement_source:
            # Has 'lead' tag but no inbound conversation yet — still add as discovered
            # The tag itself indicates a legitimate inquiry
            engagement_source = "web_form"

        first_name = (c.get("firstName") or "").strip()
        last_name = (c.get("lastName") or "").strip()
        org = (c.get("companyName") or "").strip()
        email = (c.get("email") or "").strip()
        phone = c.get("phone", "")

        # Determine property type from org name or custom fields
        property_type = "other"
        org_lower = org.lower()
        if any(k in org_lower for k in ["hoa", "homeowner", "community association"]):
            property_type = "hoa"
        elif any(k in org_lower for k in ["storage", "warehouse", "industrial", "manufacturing"]):
            property_type = "industrial"
        elif any(k in org_lower for k in ["club", "hotel", "resort", "mall", "plaza", "retail"]):
            property_type = "commercial"
        elif any(k in org_lower for k in ["apartment", "residential", "condo"]):
            property_type = "residential"

        qualified.append({
            "contact_id": contact_id,
            "first_name": first_name,
            "last_name": last_name,
            "organization": org,
            "email": email,
            "phone": phone,
            "property_type": property_type,
            "engagement_source": engagement_source,
            "engagement_velocity": "fast",
        })

    log.info(
        "Inbound scanner: %d total contacts, %d recent candidates, %d qualified leads",
        len(contacts), len(candidates), len(qualified),
    )
    return qualified
