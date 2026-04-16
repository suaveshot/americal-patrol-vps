"""
Review Request Eligibility Checker

Determines which clients qualify for a review request based on:
1. Not already reviewed on Google (permanently excluded)
2. Consecutive incident-free days >= threshold
3. Not asked within cooldown period
4. Not manually excluded
"""

import logging
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared_utils.event_bus import read_events_since
from review_engine.config import (
    CLEAN_DAYS_THRESHOLD,
    NEW_CLIENT_THRESHOLD_DAYS,
    NEW_CLIENT_COOLDOWN_DAYS,
    EXISTING_CLIENT_COOLDOWN_DAYS,
    MAX_REQUESTS_PER_RUN,
    MANUAL_EXCLUSIONS,
    ONBOARDING_MIN_DAYS,
    ONBOARDING_MAX_DAYS,
    load_clients,
    load_state,
)
from review_engine.gbp_review_checker import fetch_reviews, find_reviewed_clients

log = logging.getLogger("review_engine")


def _get_incident_history(days=90):
    """
    Read patrol_daily_summary events and build a dict of:
    property_name -> set of dates with incidents.
    """
    events = read_events_since("patrol", "daily_summary", days=days)
    incident_dates = {}  # property_name -> set of date strings

    for evt in events:
        date_str = evt.get("report_date", "")
        incident_accounts = evt.get("incident_accounts", [])
        for prop_name in incident_accounts:
            prop_lower = prop_name.lower().strip()
            if prop_lower not in incident_dates:
                incident_dates[prop_lower] = set()
            incident_dates[prop_lower].add(date_str)

    return incident_dates


def _consecutive_clean_days(group, incident_dates):
    """
    Count consecutive incident-free days for a client group,
    working backwards from today.
    """
    property_names = [
        a["name"].lower().strip() for a in group.get("accounts", [])
    ]

    today = datetime.now().date()
    clean_days = 0

    for days_ago in range(0, 90):
        check_date = today - timedelta(days=days_ago)
        # Format to match event date format: "March 29, 2026"
        date_str = f"{check_date.strftime('%B')} {check_date.day}, {check_date.year}"

        had_incident = False
        for prop_name in property_names:
            dates_with_incidents = incident_dates.get(prop_name, set())
            if date_str in dates_with_incidents:
                had_incident = True
                break

        if had_incident:
            break
        clean_days += 1

    return clean_days


def _find_first_patrol_date(group, days_lookback=90):
    """
    Find the earliest patrol daily_summary event that mentions this client's
    properties. Returns the number of days since first patrol, or None if
    no patrol events found.
    """
    property_names = [
        a["name"].lower().strip() for a in group.get("accounts", [])
    ]
    events = read_events_since("patrol", "daily_summary", days=days_lookback)

    # Events are newest-first; we need the oldest match
    earliest_date = None
    for evt in events:
        report_date = evt.get("report_date", "")
        # Check if any of this group's properties appear in the event
        all_accounts = evt.get("all_accounts", [])
        all_lower = [a.lower().strip() for a in all_accounts]
        for prop in property_names:
            if prop in all_lower:
                try:
                    dt = datetime.strptime(report_date, "%B %d, %Y").date()
                    if earliest_date is None or dt < earliest_date:
                        earliest_date = dt
                except ValueError:
                    continue
                break

    if earliest_date is None:
        return None
    return (datetime.now().date() - earliest_date).days


def check_eligibility(dry_run=False):
    """
    Returns list of eligible client groups for review requests.

    Each entry: {
        "group": <group dict from clients.json>,
        "clean_days": int,
        "reason": str (why eligible),
    }

    Also returns a report dict with skip reasons for logging.
    """
    clients = load_clients()
    state = load_state()
    now = datetime.now()

    # Step 1: Fetch Google reviews and find already-reviewed clients
    log.info("Fetching Google reviews from GBP...")
    reviews = fetch_reviews()
    reviewed_ids = find_reviewed_clients(reviews, clients)

    # Merge with permanently excluded from state
    permanently_excluded = set(state.get("permanently_excluded", []))
    newly_excluded = reviewed_ids - permanently_excluded
    if newly_excluded:
        log.info("Newly detected reviewed clients: %s", newly_excluded)
        permanently_excluded.update(newly_excluded)
        state["permanently_excluded"] = sorted(permanently_excluded)

    # Step 2: Check each client
    incident_dates = _get_incident_history(days=90)
    eligible = []
    report = {"skipped": {}, "eligible": []}

    for group in clients:
        gid = group["group_id"]

        # Check permanent exclusion (already reviewed)
        if gid in permanently_excluded:
            report["skipped"][gid] = "Already left a Google review"
            continue

        # Check manual exclusion
        skip_manual = False
        for account in group.get("accounts", []):
            if account["name"] in MANUAL_EXCLUSIONS:
                report["skipped"][gid] = f"Manually excluded ({account['name']})"
                skip_manual = True
                break
        if skip_manual:
            continue

        # Check cooldown -- two-tier: new clients monthly, existing quarterly
        req_data = state.get("requests", {}).get(gid, {})
        last_asked = req_data.get("last_asked")
        if last_asked:
            last_dt = datetime.fromisoformat(last_asked)
            days_since = (now - last_dt).days

            # Determine if this is a new or existing client
            client_age = _find_first_patrol_date(group, days_lookback=365)
            if client_age is not None and client_age <= NEW_CLIENT_THRESHOLD_DAYS:
                cooldown = NEW_CLIENT_COOLDOWN_DAYS  # monthly for new clients
            else:
                cooldown = EXISTING_CLIENT_COOLDOWN_DAYS  # quarterly for existing

            if days_since < cooldown:
                tier_label = "new" if cooldown == NEW_CLIENT_COOLDOWN_DAYS else "existing"
                report["skipped"][gid] = (
                    f"Cooldown ({tier_label}): asked {days_since}d ago (need {cooldown}d)"
                )
                continue

        # Check clean streak
        clean = _consecutive_clean_days(group, incident_dates)
        if clean < CLEAN_DAYS_THRESHOLD:
            report["skipped"][gid] = (
                f"Only {clean} clean days (need {CLEAN_DAYS_THRESHOLD})"
            )
            continue

        # Eligible!
        property_names = ", ".join(a["name"] for a in group["accounts"])
        eligible.append({
            "group": group,
            "clean_days": clean,
            "reason": f"{clean} consecutive clean days at {property_names}",
        })
        report["eligible"].append(gid)

    # Cap at max per run
    if len(eligible) > MAX_REQUESTS_PER_RUN:
        log.info(
            "Capping eligible clients from %d to %d",
            len(eligible), MAX_REQUESTS_PER_RUN,
        )
        # Prioritize longest clean streaks
        eligible.sort(key=lambda x: x["clean_days"], reverse=True)
        eligible = eligible[:MAX_REQUESTS_PER_RUN]

    # Log report
    log.info("=== Eligibility Report ===")
    log.info("Total clients: %d", len(clients))
    log.info("Eligible: %d", len(eligible))
    for e in eligible:
        log.info("  + %s -- %s", e["group"]["group_id"], e["reason"])
    log.info("Skipped: %d", len(report["skipped"]))
    for gid, reason in report["skipped"].items():
        log.info("  - %s -- %s", gid, reason)

    return eligible, state, report


def check_onboarding_eligibility():
    """
    Find clients whose first patrol was 14-30 days ago (new clients at
    peak satisfaction). These get a review request immediately, separate
    from the monthly batch.

    Returns (eligible_list, state).
    Each entry: {"group": ..., "days_active": int, "reason": str}
    """
    clients = load_clients()
    state = load_state()
    now = datetime.now()

    eligible = []
    for group in clients:
        gid = group["group_id"]

        # Skip if already asked (any trigger)
        req_data = state.get("requests", {}).get(gid, {})
        if req_data.get("last_asked"):
            last_dt = datetime.fromisoformat(req_data["last_asked"])
            days_since = (now - last_dt).days
            if days_since < NEW_CLIENT_COOLDOWN_DAYS:
                continue

        # Skip manual exclusions
        if any(a["name"] in MANUAL_EXCLUSIONS for a in group.get("accounts", [])):
            continue

        # Skip permanently excluded (already reviewed)
        if gid in state.get("permanently_excluded", []):
            continue

        # Check how long this client has been active
        days_active = _find_first_patrol_date(group, days_lookback=ONBOARDING_MAX_DAYS + 5)
        if days_active is None:
            continue  # No patrol events found -- not an active client yet

        if ONBOARDING_MIN_DAYS <= days_active <= ONBOARDING_MAX_DAYS:
            property_names = ", ".join(a["name"] for a in group["accounts"])
            eligible.append({
                "group": group,
                "days_active": days_active,
                "reason": f"New client -- {days_active} days active at {property_names}",
            })
            log.info("Onboarding eligible: %s -- %d days active", gid, days_active)

    log.info("Onboarding check: %d new client(s) eligible", len(eligible))
    return eligible, state
