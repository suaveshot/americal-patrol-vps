# guard_compliance/compliance_engine.py
"""
Guard Compliance — Core Engine
Calculates expiry status, determines pending notifications, manages tier history.
"""

import json
import logging
import os
from datetime import datetime, date
from pathlib import Path

log = logging.getLogger(__name__)

# Alert tier names in escalation order
TIERS = ["first_notice", "reminder", "urgent", "critical", "expired"]

DEFAULT_THRESHOLDS = {
    "first_notice": 90,
    "reminder": 60,
    "urgent": 30,
    "critical": 14,
    "expired": 0,
}


def calculate_status(expiry_date_str: str | None, thresholds: dict = None) -> tuple[str, int | None]:
    """
    Calculate the compliance status and days remaining.

    Returns:
        (status, days_remaining) where status is one of:
        "valid", "first_notice", "reminder", "urgent", "critical", "expired", "unknown"
    """
    if not expiry_date_str:
        return ("unknown", None)

    thresholds = thresholds or DEFAULT_THRESHOLDS

    try:
        expiry = datetime.strptime(expiry_date_str, "%Y-%m-%d").date()
    except ValueError:
        return ("unknown", None)

    today = date.today()
    days_remaining = (expiry - today).days

    if days_remaining < 0:
        return ("expired", days_remaining)

    # Check tiers from most urgent to least
    for tier in ["critical", "urgent", "reminder", "first_notice"]:
        if days_remaining <= thresholds.get(tier, DEFAULT_THRESHOLDS[tier]):
            return (tier, days_remaining)

    return ("valid", days_remaining)


def get_pending_notifications(officer: dict, config: dict) -> list[dict]:
    """
    Check each credential against thresholds and notification history.
    Returns a list of notifications that need to be sent.

    Each item: {
        "credential_type": "guard_card_expiry",
        "tier": "urgent",
        "days_remaining": 28,
        "expiry_date": "2026-04-29",
        "officer": {...}
    }
    """
    thresholds = config.get("alert_thresholds_days", DEFAULT_THRESHOLDS)
    notifications_history = officer.get("notifications", {})
    credentials = officer.get("credentials", {})
    pending = []

    for cred_name, cred_data in credentials.items():
        # Only check expiry/date fields
        if not cred_name.endswith(("_expiry", "_date", "_renewal")):
            continue

        expiry_date = cred_data if isinstance(cred_data, str) else None
        if isinstance(cred_data, dict):
            expiry_date = cred_data.get("expiry")

        status, days_remaining = calculate_status(expiry_date, thresholds)

        if status in ("valid", "unknown"):
            continue

        # Check if this tier has already been notified
        cred_history = notifications_history.get(cred_name, {})
        tier_key = f"{status}_sent"

        if cred_history.get(tier_key):
            continue  # Already sent this tier

        # Also skip if a MORE urgent tier was already sent
        # (e.g., don't send "reminder" if "urgent" was already sent)
        tier_index = TIERS.index(status) if status in TIERS else -1
        already_sent_more_urgent = False
        for i in range(tier_index + 1, len(TIERS)):
            if cred_history.get(f"{TIERS[i]}_sent"):
                already_sent_more_urgent = True
                break
        if already_sent_more_urgent:
            continue

        pending.append({
            "credential_type": cred_name,
            "tier": status,
            "days_remaining": days_remaining,
            "expiry_date": expiry_date,
            "officer": officer,
        })

    return pending


def check_bsis_verification(officer: dict, bsis_result: dict) -> list[dict]:
    """
    Generate alerts for BSIS verification issues.
    Returns list of alert dicts for any mismatches found.
    """
    alerts = []
    if not bsis_result or bsis_result.get("verified") is None:
        return alerts  # No data to verify against

    if bsis_result["verified"] is False:
        for issue in bsis_result.get("issues", []):
            alerts.append({
                "credential_type": "bsis_verification",
                "tier": "bsis_mismatch",
                "issue": issue,
                "dca_status": bsis_result.get("dca_status"),
                "officer": officer,
            })

    elif bsis_result.get("issues"):
        # Verified but with warnings (e.g., expiry mismatch)
        for issue in bsis_result["issues"]:
            alerts.append({
                "credential_type": "bsis_verification",
                "tier": "bsis_warning",
                "issue": issue,
                "dca_status": bsis_result.get("dca_status"),
                "officer": officer,
            })

    return alerts


def update_notification_history(state: dict, officer_id: str,
                                credential_type: str, tier: str) -> None:
    """Mark a notification tier as sent with current timestamp."""
    officers = state.setdefault("officers", {})
    officer = officers.setdefault(officer_id, {})
    notifications = officer.setdefault("notifications", {})
    cred_notifs = notifications.setdefault(credential_type, {})
    cred_notifs[f"{tier}_sent"] = datetime.now().isoformat()


def reset_notification_history(state: dict, officer_id: str,
                               credential_type: str) -> None:
    """
    Clear all sent timestamps for a credential (called when renewal detected).
    """
    try:
        state["officers"][officer_id]["notifications"][credential_type] = {}
        log.info(f"Reset notification history for {officer_id}/{credential_type} (renewal detected)")
    except KeyError:
        pass


def detect_renewals(old_state: dict, new_officers: dict) -> list[tuple[str, str]]:
    """
    Compare previous state to new data and find credentials that were renewed
    (new expiry date is later than old expiry date).

    Returns: list of (officer_id, credential_type) tuples that were renewed.
    """
    renewals = []
    old_officers = old_state.get("officers", {})

    for oid, new_data in new_officers.items():
        old_data = old_officers.get(oid, {})
        old_creds = old_data.get("credentials", {})
        new_creds = new_data.get("credentials", {})

        for cred_name in new_creds:
            if not cred_name.endswith(("_expiry", "_date", "_renewal")):
                continue

            old_expiry = old_creds.get(cred_name)
            new_expiry = new_creds.get(cred_name)

            if isinstance(old_expiry, dict):
                old_expiry = old_expiry.get("expiry")
            if isinstance(new_expiry, dict):
                new_expiry = new_expiry.get("expiry")

            if old_expiry and new_expiry and new_expiry > old_expiry:
                renewals.append((oid, cred_name))
                log.info(f"Renewal detected: {oid}/{cred_name}: {old_expiry} → {new_expiry}")

    return renewals


# ── State I/O (atomic writes) ────────────────────────────────────────────────

def load_state(state_file: Path) -> dict:
    """Load compliance state from JSON file."""
    if not state_file.exists():
        return {"version": 1, "officers": {}, "discovered_fields": []}
    try:
        with open(state_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.error(f"Error loading state file: {e}")
        return {"version": 1, "officers": {}, "discovered_fields": []}


def save_state(state: dict, state_file: Path) -> None:
    """Save compliance state atomically (temp file + os.replace)."""
    state["last_sync"] = datetime.now().isoformat()
    tmp = str(state_file) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(state_file))
    log.info(f"State saved to {state_file.name}")
