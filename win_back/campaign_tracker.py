"""
Win-Back Campaign Tracker

Records outreach attempts and tracks outcomes (recovered, no response, unsubscribed).
"""

import logging
from datetime import datetime

from win_back.config import load_state, save_state

log = logging.getLogger(__name__)


def record_send(contact_id: str, contact_name: str, mode: str, channel: str):
    state = load_state()

    state["campaigns"][contact_id] = {
        "contact_name": contact_name,
        "last_sent": datetime.now().isoformat(),
        "mode": mode,
        "channel": channel,
        "outcome": "pending",
        "sent_count": state.get("campaigns", {}).get(contact_id, {}).get("sent_count", 0) + 1,
    }

    save_state(state)
    log.info("Recorded win-back send: %s (%s via %s)", contact_name, mode, channel)


def record_recovery(contact_id: str):
    state = load_state()
    campaign = state.get("campaigns", {}).get(contact_id, {})

    if campaign:
        campaign["outcome"] = "recovered"
        campaign["recovered_at"] = datetime.now().isoformat()
        state["campaigns"][contact_id] = campaign
        save_state(state)
        log.info("Customer recovered: %s", campaign.get("contact_name", contact_id))


def get_pending_count() -> int:
    state = load_state()
    return sum(1 for c in state.get("campaigns", {}).values() if c.get("outcome") == "pending")


def get_recovered_count(days: int = 30) -> int:
    from datetime import timedelta
    state = load_state()
    cutoff = datetime.now() - timedelta(days=days)
    count = 0
    for campaign in state.get("campaigns", {}).values():
        if campaign.get("outcome") == "recovered":
            recovered_at = campaign.get("recovered_at", "")
            if recovered_at:
                try:
                    dt = datetime.fromisoformat(recovered_at)
                    if dt > cutoff:
                        count += 1
                except ValueError:
                    pass
    return count


def increment_daily_ai_count() -> int:
    state = load_state()
    today = datetime.now().strftime("%Y-%m-%d")
    if state.get("daily_ai_date") != today:
        state["daily_ai_count"] = 0
        state["daily_ai_date"] = today
    state["daily_ai_count"] += 1
    save_state(state)
    return state["daily_ai_count"]
