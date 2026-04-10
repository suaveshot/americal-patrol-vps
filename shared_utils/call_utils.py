"""
Americal Patrol — Call Analysis Utilities

Shared functions for determining call status (answered vs missed)
and extracting call metadata from GHL conversation objects.

Used by:
  - google_ads_automation/lead_classifier.py
  - missed_call_tracker/run_missed_calls.py
"""


def get_call_duration(conv: dict) -> int:
    """Extract call duration in seconds from GHL conversation metadata."""
    meta = conv.get("callDuration", conv.get("duration", 0))
    try:
        return int(meta)
    except (TypeError, ValueError):
        return 0


def was_answered(conv: dict) -> bool:
    """
    Determine if a call was answered or missed/voicemail.
    GHL uses various fields depending on plan/version.
    """
    # Direct missed flag
    if conv.get("missed") is True:
        return False
    # Call status field
    status = str(conv.get("callStatus", conv.get("status", ""))).lower()
    if status in ("missed", "voicemail", "no-answer", "busy", "failed"):
        return False
    # Duration-based fallback: < 10s = almost certainly missed or voicemail
    duration = get_call_duration(conv)
    if duration < 10:
        return False
    return True
