"""
Sales Pipeline — Unified State Manager
Tracks contacts across the full lifecycle: discovery, cold outreach,
proposal, post-proposal follow-up, and terminal states.

Merges patterns from cold_outreach_automation/state.py and
sales_autopilot/follow_up_state.py into a single state file.

Atomic writes: write to .tmp then rename to prevent corruption.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

from sales_pipeline.config import STATE_FILE as _DEFAULT_STATE_FILE

# Day offsets for each touch number (both cold and post-proposal)
TOUCH_SCHEDULE = {
    1: 3,
    2: 7,
    3: 14,
    4: 21,
}

# Flexible day windows for day-of-week matching
# (min_days, max_days) — prefer the day matching optimal_send_day within this range
TOUCH_WINDOWS = {
    1: (3, 5),      # Day 3-5, slight flexibility for first follow-up
    2: (5, 9),      # Day 5-9, prefer matching day of week
    3: (12, 16),    # Day 12-16, prefer matching day of week
    4: (19, 23),    # Day 19-23, prefer matching day of week
}

MAX_TOUCHES = max(TOUCH_SCHEDULE.keys())  # 4

# Valid lifecycle stages
STAGES = [
    "discovered",
    "cold_drafted",
    "cold_sent",
    "cold_follow_up_1",
    "cold_follow_up_2",
    "cold_follow_up_3",
    "cold_follow_up_4",
    "engaged",
    "proposal_sent",
    "post_proposal_1",
    "post_proposal_2",
    "post_proposal_3",
    "post_proposal_4",
    "negotiating",
    "nurture_monthly",
    "won",
    "closed_lost",
    "sequence_done",
    "unsubscribed",
]

# Days between monthly nurture touches
NURTURE_INTERVAL_DAYS = 30


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(ts: str) -> datetime:
    """Parse an ISO-8601 string, handling trailing 'Z'."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


# ---------------------------------------------------------------------------
# State I/O
# ---------------------------------------------------------------------------

def load_state(path=None) -> dict:
    """Load state from JSON file. Returns default structure if missing."""
    file = Path(path) if path is not None else _DEFAULT_STATE_FILE
    if not file.exists():
        return {"version": 2, "contacts": {}}
    try:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
        if "contacts" not in data:
            data["contacts"] = {}
        data.setdefault("version", 2)
        return data
    except (json.JSONDecodeError, OSError):
        return {"version": 2, "contacts": {}}


def save_state(state: dict, path=None) -> None:
    """Atomically write state dict to JSON file."""
    file = Path(path) if path is not None else _DEFAULT_STATE_FILE
    tmp = file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, file)


# ---------------------------------------------------------------------------
# Contact management
# ---------------------------------------------------------------------------

def add_contact(state: dict, contact_id: str, *,
                stage: str = "discovered",
                phase: str = "cold_outreach",
                channel: str = "email",
                first_name: str = "",
                last_name: str = "",
                organization: str = "",
                property_type: str = "other",
                email: str = "",
                phone: str = "",
                enrichment_matched: bool = False,
                enrichment_company: str = "") -> None:
    """Add a new contact to the pipeline state."""
    state["contacts"][contact_id] = {
        "stage": stage,
        "phase": phase,
        "first_name": first_name,
        "last_name": last_name,
        "organization": organization,
        "property_type": property_type,
        "email": email,
        "phone": phone,
        "channel": channel,
        "path": None,  # Set during post-proposal (A or B)
        "discovered_at": _now_iso(),
        "draft_generated_at": None,
        "first_outreach_at": None,
        "touches_sent": 0,
        "last_touch_at": None,
        "proposal_sent_at": None,
        "proposal_viewed_at": None,
        "estimate_id": None,
        "opportunity_id": None,
        "replied": False,
        "replied_at": None,
        "completed": False,
        "completed_reason": None,
        "completed_at": None,
        "won_at": None,
        "lost_at": None,
        "nurture_started_at": None,
        "nurture_touches_sent": 0,
        "enrichment_matched": enrichment_matched,
        "enrichment_company": enrichment_company,
        # Smart timing fields
        "optimal_send_hour": None,      # 0-23, extracted from engagement time
        "optimal_send_minute": None,    # 0-59, rounded to nearest 15 min
        "optimal_send_day": None,       # "Monday"-"Friday", from engagement day
        "engagement_source": None,      # web_form, email, phone_call, sms, cold
        "engagement_velocity": "medium", # fast, medium, slow
        "scheduled_message_ids": [],    # GHL message IDs pending delivery
    }


def get_contact(state: dict, contact_id: str):
    """Return the contact entry dict, or None if not found."""
    return state["contacts"].get(contact_id)


def is_active(entry: dict) -> bool:
    """Check if a contact is still active in the pipeline (not terminal)."""
    if entry.get("completed"):
        return False
    if entry.get("stage") in ("won", "unsubscribed"):
        return False
    return True


# ---------------------------------------------------------------------------
# Stage transitions
# ---------------------------------------------------------------------------

def set_stage(state: dict, contact_id: str, stage: str) -> None:
    """Update the stage of a contact."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["stage"] = stage


def mark_drafted(state: dict, contact_id: str) -> None:
    """Mark that a draft message was generated for this contact."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["stage"] = "cold_drafted"
    entry["draft_generated_at"] = _now_iso()


def mark_outreached(state: dict, contact_id: str, channel: str) -> None:
    """Record that a contact has been outreached for the first time."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["stage"] = "cold_sent"
    entry["first_outreach_at"] = _now_iso()
    entry["channel"] = channel


def mark_replied(state: dict, contact_id: str) -> None:
    """Mark that the contact replied — transition to engaged/negotiating."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["replied"] = True
    entry["replied_at"] = _now_iso()
    if entry.get("phase") == "post_proposal":
        entry["stage"] = "negotiating"
        entry["completed"] = True
        entry["completed_reason"] = "replied"
    else:
        entry["stage"] = "engaged"


def mark_unsubscribed(state: dict, contact_id: str) -> None:
    """Mark contact as unsubscribed — terminal state, never contact again."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["stage"] = "unsubscribed"
    entry["completed"] = True
    entry["completed_reason"] = "unsubscribed"
    entry["completed_at"] = _now_iso()


def mark_completed(state: dict, contact_id: str, reason: str) -> None:
    """Mark the contact's sequence as completed for any reason."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["completed"] = True
    entry["completed_reason"] = reason
    entry["completed_at"] = _now_iso()
    if reason == "sequence_done":
        entry["stage"] = "sequence_done"


# ---------------------------------------------------------------------------
# Touch tracking
# ---------------------------------------------------------------------------

def record_touch(state: dict, contact_id: str, touch_number: int, channel: str) -> None:
    """Record that touch N was sent to this contact."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    now = _now_iso()
    entry["touches_sent"] = touch_number
    entry["last_touch_at"] = now
    entry[f"touch_{touch_number}_channel"] = channel
    entry[f"touch_{touch_number}_at"] = now
    # Update stage
    phase = entry.get("phase", "cold_outreach")
    if phase == "cold_outreach":
        entry["stage"] = f"cold_follow_up_{touch_number}"
    else:
        entry["stage"] = f"post_proposal_{touch_number}"


# ---------------------------------------------------------------------------
# Proposal transitions
# ---------------------------------------------------------------------------

def mark_proposal_sent(state: dict, contact_id: str, *,
                       estimate_id: str = "", opportunity_id: str = "") -> None:
    """Transition contact to post-proposal phase."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["stage"] = "proposal_sent"
    entry["phase"] = "post_proposal"
    entry["proposal_sent_at"] = _now_iso()
    entry["touches_sent"] = 0  # Reset for post-proposal sequence
    entry["last_touch_at"] = None
    if estimate_id:
        entry["estimate_id"] = estimate_id
    if opportunity_id:
        entry["opportunity_id"] = opportunity_id


def set_path(state: dict, contact_id: str, path: str) -> None:
    """Set Path A or B for post-proposal follow-up."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["path"] = path


# ---------------------------------------------------------------------------
# Proposal view tracking
# ---------------------------------------------------------------------------

def mark_proposal_viewed(state: dict, contact_id: str) -> None:
    """Record that the prospect viewed the proposal/estimate."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    if not entry.get("proposal_viewed_at"):
        entry["proposal_viewed_at"] = _now_iso()


# ---------------------------------------------------------------------------
# Won / Lost
# ---------------------------------------------------------------------------

def mark_won(state: dict, contact_id: str) -> None:
    """Mark deal as won — terminal state."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["stage"] = "won"
    entry["completed"] = True
    entry["completed_reason"] = "won"
    entry["completed_at"] = _now_iso()
    entry["won_at"] = _now_iso()


def mark_lost(state: dict, contact_id: str) -> None:
    """Mark deal as lost — transitions to nurture (can come back)."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["lost_at"] = _now_iso()
    _transition_to_nurture(state, contact_id)


# ---------------------------------------------------------------------------
# Nurture transitions
# ---------------------------------------------------------------------------

def _transition_to_nurture(state: dict, contact_id: str) -> None:
    """Move a contact into the monthly nurture phase."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    entry["phase"] = "nurture"
    entry["stage"] = "nurture_monthly"
    entry["nurture_started_at"] = _now_iso()
    entry["nurture_touches_sent"] = entry.get("nurture_touches_sent", 0)
    # Not completed — nurture is ongoing
    entry["completed"] = False
    entry["completed_reason"] = None
    entry["completed_at"] = None


def get_nurture_due_contacts(state: dict, now: datetime = None) -> list:
    """
    Return contacts in nurture phase due for their next monthly touch.
    Due when 30+ days since last_touch_at (or nurture_started_at if first).
    """
    if now is None:
        now = datetime.now(timezone.utc)

    due = []
    for contact_id, entry in state["contacts"].items():
        if entry.get("phase") != "nurture":
            continue
        if entry.get("completed") or entry.get("stage") == "unsubscribed":
            continue

        ref_ts = entry.get("last_touch_at") or entry.get("nurture_started_at")
        if not ref_ts:
            continue

        ref_dt = _parse_iso(ref_ts)
        days_elapsed = (now - ref_dt).total_seconds() / 86400

        if days_elapsed >= NURTURE_INTERVAL_DAYS:
            due.append({
                "contact_id": contact_id,
                "nurture_touch": entry.get("nurture_touches_sent", 0) + 1,
                "channel": "email",
                "property_type": entry.get("property_type", "other"),
            })

    return due


def record_nurture_touch(state: dict, contact_id: str) -> None:
    """Record that a monthly nurture touch was sent."""
    entry = state["contacts"].get(contact_id)
    if entry is None:
        return
    count = entry.get("nurture_touches_sent", 0) + 1
    entry["nurture_touches_sent"] = count
    entry["last_touch_at"] = _now_iso()


def backfill_smart_timing(state: dict) -> int:
    """Add smart timing fields to existing contacts that don't have them.
    Returns count of contacts backfilled."""
    count = 0
    for contact_id, entry in state["contacts"].items():
        if "optimal_send_hour" in entry:
            continue  # Already has smart timing fields

        # Extract time from best available timestamp
        ref_ts = (entry.get("first_outreach_at")
                  or entry.get("proposal_sent_at")
                  or entry.get("discovered_at"))

        if ref_ts:
            ref_dt = _parse_iso(ref_ts)
            hour = ref_dt.hour
            minute = (ref_dt.minute // 15) * 15  # Round to nearest 15
            day_name = ref_dt.strftime("%A")
            # Clamp to business hours (7 AM - 7 PM) — quiet hours guard
            if hour < 7:
                hour, minute = 7, 0
            elif hour >= 19:
                hour, minute = 10, 0  # Default to mid-morning
            # Weekend shift
            if day_name in ("Saturday", "Sunday"):
                day_name = "Monday"
        else:
            hour, minute, day_name = 10, 0, "Tuesday"

        entry["optimal_send_hour"] = hour
        entry["optimal_send_minute"] = minute
        entry["optimal_send_day"] = day_name
        entry["engagement_source"] = "cold"
        entry["engagement_velocity"] = "medium"
        entry["scheduled_message_ids"] = []
        count += 1

    return count


# ---------------------------------------------------------------------------
# Pipeline stats & queries
# ---------------------------------------------------------------------------

def get_needs_decision(state: dict, days: int = 7) -> list:
    """Contacts that replied 7+ days ago but aren't marked won or lost."""
    now = datetime.now(timezone.utc)
    results = []
    for contact_id, entry in state["contacts"].items():
        if not entry.get("replied"):
            continue
        if entry.get("stage") in ("won", "closed_lost", "unsubscribed"):
            continue
        if entry.get("won_at") or entry.get("lost_at"):
            continue
        replied_at = entry.get("replied_at")
        if not replied_at:
            continue
        replied_dt = _parse_iso(replied_at)
        if (now - replied_dt).total_seconds() / 86400 >= days:
            results.append({
                "contact_id": contact_id,
                "first_name": entry.get("first_name", ""),
                "organization": entry.get("organization", ""),
                "replied_at": replied_at,
                "days_since_reply": int((now - replied_dt).total_seconds() / 86400),
            })
    return results


def get_pipeline_stats(state: dict) -> dict:
    """Compute pipeline-wide statistics for the digest scorecard."""
    counts = {
        "cold_outreach": 0,
        "post_proposal": 0,
        "nurture": 0,
        "won": 0,
        "lost": 0,
        "total_active": 0,
    }
    last_won_at = None

    for entry in state["contacts"].values():
        phase = entry.get("phase", "cold_outreach")
        stage = entry.get("stage", "")

        if stage == "won":
            counts["won"] += 1
            won_ts = entry.get("won_at")
            if won_ts:
                if last_won_at is None or won_ts > last_won_at:
                    last_won_at = won_ts
        elif stage in ("closed_lost",):
            counts["lost"] += 1
        elif stage == "unsubscribed":
            pass
        elif phase == "nurture":
            counts["nurture"] += 1
            counts["total_active"] += 1
        elif phase == "post_proposal":
            counts["post_proposal"] += 1
            counts["total_active"] += 1
        elif phase == "cold_outreach" and not entry.get("completed"):
            counts["cold_outreach"] += 1
            counts["total_active"] += 1

    total_decided = counts["won"] + counts["lost"]
    counts["win_rate"] = (counts["won"] / total_decided * 100) if total_decided else None
    counts["last_won_at"] = last_won_at

    if last_won_at:
        now = datetime.now(timezone.utc)
        counts["days_since_last_win"] = int(
            (now - _parse_iso(last_won_at)).total_seconds() / 86400
        )
    else:
        counts["days_since_last_win"] = None

    return counts


# ---------------------------------------------------------------------------
# Due-contact logic
# ---------------------------------------------------------------------------

def get_due_contacts(state: dict, phase: str = None, now: datetime = None) -> list:
    """
    Return list of dicts for contacts due for their next touch.

    Each result: {contact_id, touch_number, path, phase, channel}.

    For cold_outreach: days since first_outreach_at >= schedule
    For post_proposal: days since proposal_sent_at >= schedule

    Optionally filter by phase ("cold_outreach" or "post_proposal").
    """
    if now is None:
        now = datetime.now(timezone.utc)

    due = []

    for contact_id, entry in state["contacts"].items():
        if entry.get("replied") or entry.get("completed"):
            continue
        if entry.get("stage") == "unsubscribed":
            continue

        entry_phase = entry.get("phase", "cold_outreach")
        if phase and entry_phase != phase:
            continue

        touches_sent = entry.get("touches_sent", 0)

        # All touches sent — transition to monthly nurture
        if touches_sent >= MAX_TOUCHES:
            _transition_to_nurture(state, contact_id)
            continue

        next_touch = touches_sent + 1
        required_days = TOUCH_SCHEDULE[next_touch]

        # Determine reference timestamp
        if entry_phase == "post_proposal":
            ref_ts = entry.get("proposal_sent_at")
        else:
            ref_ts = entry.get("first_outreach_at")

        if not ref_ts:
            continue

        ref_dt = _parse_iso(ref_ts)
        days_elapsed = (now - ref_dt).total_seconds() / 86400

        if days_elapsed >= required_days:
            due.append({
                "contact_id": contact_id,
                "touch_number": next_touch,
                "path": entry.get("path"),
                "phase": entry_phase,
                "channel": entry.get("channel", "email"),
            })

    return due


# ---------------------------------------------------------------------------
# Migration from old state files
# ---------------------------------------------------------------------------

def migrate_cold_outreach_state(old_state: dict) -> dict:
    """Convert cold_outreach_state.json contacts to unified schema."""
    new_state = {"version": 2, "contacts": {}}
    for cid, old in old_state.get("contacts", {}).items():
        stage = "cold_sent"
        touches = 0
        if old.get("follow_up_sent_at"):
            stage = "cold_follow_up_1"
            touches = 1
        if old.get("reply_detected"):
            stage = "engaged"

        new_state["contacts"][cid] = {
            "stage": stage,
            "phase": "cold_outreach",
            "first_name": "",
            "last_name": "",
            "organization": "",
            "property_type": "other",
            "email": "",
            "phone": "",
            "channel": old.get("first_outreach_channel", "email"),
            "path": None,
            "discovered_at": old.get("first_outreach_at", _now_iso()),
            "draft_generated_at": None,
            "first_outreach_at": old.get("first_outreach_at"),
            "touches_sent": touches,
            "last_touch_at": old.get("follow_up_sent_at"),
            "proposal_sent_at": None,
            "estimate_id": None,
            "opportunity_id": None,
            "replied": old.get("reply_detected", False),
            "replied_at": None,
            "completed": old.get("reply_detected", False),
            "completed_reason": "replied" if old.get("reply_detected") else None,
            "completed_at": None,
            "enrichment_matched": False,
            "enrichment_company": "",
        }
    return new_state


def migrate_follow_up_state(old_state: dict) -> dict:
    """Convert follow_up_state.json contacts to unified schema."""
    new_state = {"version": 2, "contacts": {}}
    for cid, old in old_state.get("contacts", {}).items():
        touches = old.get("touches_sent", 0)
        replied = old.get("replied", False)
        completed = old.get("completed", False)

        if replied:
            stage = "negotiating"
        elif completed:
            stage = old.get("completed_reason", "sequence_done")
            if stage not in STAGES:
                stage = "sequence_done"
        elif touches > 0:
            stage = f"post_proposal_{touches}"
        else:
            stage = "proposal_sent"

        entry = {
            "stage": stage,
            "phase": "post_proposal",
            "first_name": "",
            "last_name": "",
            "organization": "",
            "property_type": "other",
            "email": "",
            "phone": "",
            "channel": "email",
            "path": old.get("path"),
            "discovered_at": old.get("added_at", _now_iso()),
            "draft_generated_at": None,
            "first_outreach_at": None,
            "touches_sent": touches,
            "last_touch_at": old.get("last_touch_at"),
            "proposal_sent_at": old.get("proposal_sent_at"),
            "estimate_id": None,
            "opportunity_id": None,
            "replied": replied,
            "replied_at": old.get("replied_at"),
            "completed": completed,
            "completed_reason": old.get("completed_reason"),
            "completed_at": old.get("completed_at"),
            "enrichment_matched": False,
            "enrichment_company": "",
        }
        # Copy touch details
        for i in range(1, touches + 1):
            ch_key = f"touch_{i}_channel"
            at_key = f"touch_{i}_at"
            if ch_key in old:
                entry[ch_key] = old[ch_key]
            if at_key in old:
                entry[at_key] = old[at_key]

        new_state["contacts"][cid] = entry
    return new_state


def merge_states(cold_state: dict, follow_up_state: dict) -> dict:
    """
    Merge both old state files into unified state.
    Contacts in both get follow-up state priority (more data).
    """
    merged = {"version": 2, "contacts": {}}
    cold_migrated = migrate_cold_outreach_state(cold_state)
    fu_migrated = migrate_follow_up_state(follow_up_state)

    # Add cold outreach contacts first
    merged["contacts"].update(cold_migrated.get("contacts", {}))
    # Follow-up state overwrites (has richer data if contact exists in both)
    merged["contacts"].update(fu_migrated.get("contacts", {}))

    return merged
