"""
Sales Pipeline — Cold Outreach: Channel Detector
Determines whether to reach a lead via SMS or email based on their
most recent conversation type in GHL.
"""

import logging
from datetime import datetime, timezone

log = logging.getLogger(__name__)


def _parse_date(date_str: str) -> datetime:
    if not date_str:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        s = date_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return datetime.min.replace(tzinfo=timezone.utc)


def detect_channel(conversations: list) -> str:
    """
    Return 'sms' or 'email' based on the most recently used conversation type.
    Defaults to 'email' if no conversations exist.

    GHL conversation `type` values: 'SMS', 'Email', 'GMB', 'FB', etc.
    We only recognise 'SMS' and 'Email'.
    """
    if not conversations:
        return "email"

    MIN_DT = datetime.min.replace(tzinfo=timezone.utc)
    sms_latest = MIN_DT
    email_latest = MIN_DT
    seen_sms = False
    seen_email = False

    for conv in conversations:
        conv_type = (conv.get("type") or "").upper()
        ts = _parse_date(conv.get("dateUpdated") or conv.get("lastMessageDate") or conv.get("dateCreated") or "")

        if conv_type == "SMS":
            seen_sms = True
            if ts > sms_latest:
                sms_latest = ts
        elif conv_type == "EMAIL":
            seen_email = True
            if ts > email_latest:
                email_latest = ts

    if not seen_sms and not seen_email:
        return "email"

    if seen_sms and not seen_email:
        return "sms"
    if seen_email and not seen_sms:
        return "email"

    # Both seen — compare most recent
    return "sms" if sms_latest >= email_latest else "email"
