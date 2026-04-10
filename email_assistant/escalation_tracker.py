"""
Email Assistant (Larry) — Escalation Tracker
Manages pending escalations in state so Sam's replies can be matched
back to the original client email.
"""

import re
from datetime import datetime, timedelta


def record_escalation(state, esc_msg_id, original_email, classifier_result):
    """Store a pending escalation keyed by the escalation email's message ID."""
    if "pending_escalations" not in state:
        state["pending_escalations"] = {}

    state["pending_escalations"][esc_msg_id] = {
        "original_email": {
            "id": original_email.get("id"),
            "thread_id": original_email.get("thread_id"),
            "from": original_email.get("from"),
            "to": original_email.get("to"),
            "subject": original_email.get("subject"),
            "date": original_email.get("date"),
            "message_id": original_email.get("message_id"),
            "body": original_email.get("body", "")[:3000],
        },
        "proposed_response": classifier_result.get("draft_body", ""),
        "category": classifier_result.get("category", "unknown"),
        "confidence": classifier_result.get("confidence", 0),
        "escalation_subject": f"[Larry] Need guidance: {original_email.get('subject', '')}",
        "escalated_at": datetime.now().isoformat(),
        "status": "pending",
    }


def find_escalation_for_reply(state, reply_email):
    """
    Given Sam's reply email, find the matching pending escalation.

    Matching strategy (in priority order):
    1. Parse ESCALATION_ID from the reference block in the quoted reply body
    2. Match by thread_id (Sam's reply is in the same thread as the escalation)
    3. Match by subject pattern — strip 'Re: ' prefixes and match against escalation_subject
    """
    pending = state.get("pending_escalations", {})
    if not pending:
        return None

    reply_body = reply_email.get("body", "")
    reply_subject = reply_email.get("subject", "")
    reply_thread_id = reply_email.get("thread_id", "")

    # Strategy 1: Look for ESCALATION_ID in the quoted body
    esc_id_match = re.search(r"ESCALATION_ID:\s*(\S+)", reply_body)
    if esc_id_match:
        esc_id = esc_id_match.group(1)
        if esc_id in pending and pending[esc_id]["status"] == "pending":
            return esc_id

    # Strategy 2: Match by thread_id
    if reply_thread_id:
        for esc_id, esc in pending.items():
            if esc["status"] != "pending":
                continue
            # The reply's thread_id should match the escalation thread
            # (escalation was sent as a new thread, Sam replies in that thread)
            if reply_thread_id == esc.get("escalation_thread_id"):
                return esc_id

    # Strategy 3: Match by subject
    # Strip leading "Re: " / "RE: " / "Fwd: " prefixes and compare
    normalized_subject = _strip_reply_prefixes(reply_subject).lower().strip()
    for esc_id, esc in pending.items():
        if esc["status"] != "pending":
            continue
        esc_subject = esc.get("escalation_subject", "").lower().strip()
        if normalized_subject == esc_subject or esc_subject in normalized_subject:
            return esc_id

    return None


def resolve_escalation(state, esc_key, resolution, resolution_detail=""):
    """Mark an escalation as resolved."""
    esc = state.get("pending_escalations", {}).get(esc_key)
    if not esc:
        return False

    esc["status"] = "resolved"
    esc["resolved_at"] = datetime.now().isoformat()
    esc["resolution"] = resolution  # "sent_proposed", "sent_custom", "skipped", "drafted"
    esc["resolution_detail"] = resolution_detail

    # Calculate response time
    try:
        escalated = datetime.fromisoformat(esc["escalated_at"])
        response_hours = (datetime.now() - escalated).total_seconds() / 3600
        esc["response_time_hours"] = round(response_hours, 1)
    except (ValueError, KeyError):
        pass

    return True


def get_pending_escalations(state):
    """Return all pending (unresolved) escalations."""
    return {
        esc_id: esc
        for esc_id, esc in state.get("pending_escalations", {}).items()
        if esc.get("status") == "pending"
    }


def prune_old_escalations(state, days=30):
    """Remove resolved escalations older than `days`."""
    if "pending_escalations" not in state:
        return

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    state["pending_escalations"] = {
        esc_id: esc
        for esc_id, esc in state["pending_escalations"].items()
        if esc.get("status") == "pending" or esc.get("resolved_at", "") > cutoff
    }


def _strip_reply_prefixes(subject):
    """Remove Re:/RE:/Fwd:/FWD: prefixes from a subject line."""
    return re.sub(r"^(Re|RE|Fwd|FWD|Fw|FW)\s*:\s*", "", subject).strip()
