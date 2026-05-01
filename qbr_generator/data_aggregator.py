"""
Americal Patrol — QBR Data Aggregator

Collects 90 days of patrol data per client from the event bus
and structures it for trend analysis and report generation.
"""

import logging
from collections import defaultdict
from datetime import datetime, timedelta

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from shared_utils.event_bus import read_events_since

log = logging.getLogger("qbr_generator")


def aggregate_client_data(group, quarter_months, quarter_year):
    """
    Aggregate patrol data for a single client group over a quarter.

    Args:
        group: Client group dict from clients.json
        quarter_months: List of month numbers [1,2,3] for the quarter
        quarter_year: Year of the quarter

    Returns dict:
    {
        "group_id": str,
        "property_names": [str],
        "total_patrol_days": int,
        "total_incidents": int,
        "incidents_by_type": {"trespassing": 3, ...},  # placeholder until we have type data
        "incidents_by_week": {iso_week: count},
        "incidents_by_day_of_week": {0: count, 1: count, ...},  # 0=Monday
        "incident_dates": [str],
        "clean_streaks": [int],  # lengths of consecutive clean-day runs
        "longest_clean_streak": int,
        "coverage_days": int,
        "quarter_label": str,
    }
    """
    property_names = [a["name"] for a in group.get("accounts", [])]
    property_names_lower = [n.lower().strip() for n in property_names]
    gid = group["group_id"]

    # Fetch all patrol summaries from the quarter (up to 95 days to cover full quarter)
    events = read_events_since("patrol", "daily_summary", days=95)

    # Filter to events within the target quarter
    quarter_events = []
    for evt in events:
        date_str = evt.get("report_date", "")
        try:
            evt_date = datetime.strptime(date_str, "%B %d, %Y")
        except ValueError:
            continue
        if evt_date.year == quarter_year and evt_date.month in quarter_months:
            quarter_events.append((evt_date, evt))

    quarter_events.sort(key=lambda x: x[0])

    # Count incidents and build distributions
    total_incidents = 0
    incident_dates = []
    incidents_by_week = defaultdict(int)
    incidents_by_dow = defaultdict(int)
    clean_streak = 0
    clean_streaks = []
    coverage_days = 0

    for evt_date, evt in quarter_events:
        coverage_days += 1
        incident_accounts = [a.lower().strip() for a in evt.get("incident_accounts", [])]

        had_incident = any(
            prop in incident_accounts
            for prop in property_names_lower
        )

        if had_incident:
            total_incidents += 1
            incident_dates.append(evt_date.strftime("%Y-%m-%d"))
            iso_week = evt_date.isocalendar()[1]
            incidents_by_week[iso_week] += 1
            incidents_by_dow[evt_date.weekday()] += 1

            if clean_streak > 0:
                clean_streaks.append(clean_streak)
            clean_streak = 0
        else:
            clean_streak += 1

    # Capture trailing clean streak
    if clean_streak > 0:
        clean_streaks.append(clean_streak)

    # Quarter label
    q_num = {(1, 2, 3): "Q1", (4, 5, 6): "Q2", (7, 8, 9): "Q3", (10, 11, 12): "Q4"}
    q_label = q_num.get(tuple(quarter_months), "Q?")
    quarter_label = f"{q_label} {quarter_year}"

    return {
        "group_id": gid,
        "property_names": property_names,
        "total_patrol_days": coverage_days,
        "total_incidents": total_incidents,
        "incidents_by_week": dict(incidents_by_week),
        "incidents_by_day_of_week": dict(incidents_by_dow),
        "incident_dates": incident_dates,
        "clean_streaks": clean_streaks,
        "longest_clean_streak": max(clean_streaks) if clean_streaks else coverage_days,
        "coverage_days": coverage_days,
        "quarter_label": quarter_label,
    }


def get_prior_quarter_data(group, quarter_months, quarter_year):
    """
    Get the same aggregation for the PRIOR quarter (for comparison).
    Returns None if no data available (first quarter of tracking).
    """
    # Calculate prior quarter
    q_index = {(1, 2, 3): 0, (4, 5, 6): 1, (7, 8, 9): 2, (10, 11, 12): 3}
    idx = q_index.get(tuple(quarter_months), 0)

    if idx == 0:
        prior_months = [10, 11, 12]
        prior_year = quarter_year - 1
    elif idx == 1:
        prior_months = [1, 2, 3]
        prior_year = quarter_year
    elif idx == 2:
        prior_months = [4, 5, 6]
        prior_year = quarter_year
    else:
        prior_months = [7, 8, 9]
        prior_year = quarter_year

    # Check if we even have data that far back (events only go 30 days by default)
    # For first run, prior quarter data won't be available
    data = aggregate_client_data(group, prior_months, prior_year)
    if data["coverage_days"] == 0:
        return None
    return data
