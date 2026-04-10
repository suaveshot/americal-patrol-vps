"""
Sales Pipeline — Smart Timing Module

Calculates per-contact optimal send times based on:
1. Engagement time matching (same hour they first engaged)
2. Day-of-week matching (prefer same day within touch window)
3. Quiet hours guard (clamp to 7 AM - 7 PM Pacific)
4. Weekend-to-Monday shift
5. Engagement velocity multiplier (compress/expand intervals)
6. Recency boost (fast response to new leads/replies)
7. Evolving optimal time (updates on reply)
8. Proposal view time capture
"""

import logging
from datetime import datetime, timedelta, timezone

from sales_pipeline.state import (
    _parse_iso,
    TOUCH_WINDOWS,
    TOUCH_SCHEDULE,
    NURTURE_INTERVAL_DAYS,
)

log = logging.getLogger(__name__)

# Pacific timezone offset (UTC-7 PDT, UTC-8 PST)
# Using fixed -7 for simplicity; production could use pytz/zoneinfo
PACIFIC_OFFSET = timedelta(hours=-7)
PACIFIC_TZ = timezone(PACIFIC_OFFSET)

QUIET_HOURS_START = 7   # 7 AM
QUIET_HOURS_END = 19    # 7 PM

VELOCITY_MULTIPLIER = {
    "fast": 0.7,
    "medium": 1.0,
    "slow": 1.25,
}

DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]


def get_optimal_send_time(
    contact: dict,
    touch_number: int,
    ref_timestamp: str,
    phase: str = "post_proposal",
    now: datetime | None = None,
) -> datetime:
    """
    Calculate the optimal send datetime for a contact's next touch.

    Args:
        contact: Contact entry from pipeline_state.json
        touch_number: Which touch this is (1-4)
        ref_timestamp: ISO timestamp of reference event (proposal_sent_at or first_outreach_at)
        phase: "cold_outreach" or "post_proposal"
        now: Override current time (for testing)

    Returns:
        datetime in UTC for when the message should be scheduled
    """
    if now is None:
        now = datetime.now(timezone.utc)

    # Get the contact's preferred send time
    send_hour = contact.get("optimal_send_hour", 10)
    send_minute = contact.get("optimal_send_minute", 0)
    preferred_day = contact.get("optimal_send_day", "Tuesday")
    velocity = contact.get("engagement_velocity", "medium")

    if send_hour is None:
        send_hour = 10
    if send_minute is None:
        send_minute = 0

    # Calculate the base target date using velocity-adjusted touch schedule
    ref_dt = _parse_iso(ref_timestamp)
    multiplier = VELOCITY_MULTIPLIER.get(velocity, 1.0)

    if touch_number in TOUCH_WINDOWS:
        min_days, max_days = TOUCH_WINDOWS[touch_number]
        # Apply velocity to the center of the window
        center = (min_days + max_days) / 2
        adjusted_center = center * multiplier
        base_days = max(min_days, int(adjusted_center))
    else:
        base_days = TOUCH_SCHEDULE.get(touch_number, 7)
        base_days = int(base_days * multiplier)

    target_date = ref_dt + timedelta(days=base_days)

    # Day-of-week matching: find the closest matching day within the window
    if preferred_day and preferred_day in DAY_NAMES and touch_number in TOUCH_WINDOWS:
        min_days, max_days = TOUCH_WINDOWS[touch_number]
        min_days = int(min_days * multiplier) if multiplier < 1 else min_days
        max_days = int(max_days * multiplier) if multiplier > 1 else max_days

        window_start = ref_dt + timedelta(days=min_days)
        window_end = ref_dt + timedelta(days=max_days)

        # Find the preferred day of week within the window
        target_weekday = DAY_NAMES.index(preferred_day)
        candidate = window_start
        best = None

        while candidate <= window_end:
            if candidate.weekday() == target_weekday and candidate >= now:
                best = candidate
                break
            candidate += timedelta(days=1)

        if best:
            target_date = best

    # Ensure target is in the future
    if target_date.date() < now.date():
        target_date = now

    # Ensure target is a weekday (Mon-Fri)
    while target_date.weekday() >= 5:  # Saturday=5, Sunday=6
        target_date += timedelta(days=1)

    # Build the final datetime with preferred time
    # Convert send_hour/minute (Pacific) to UTC
    target_pacific = target_date.replace(
        hour=send_hour, minute=send_minute, second=0, microsecond=0,
        tzinfo=PACIFIC_TZ,
    )

    # Apply quiet hours clamp
    target_pacific = _clamp_quiet_hours(target_pacific)

    # Convert to UTC
    target_utc = target_pacific.astimezone(timezone.utc)

    # If the calculated time is in the past, push to next business day
    if target_utc <= now:
        target_utc = _next_business_day_at_time(now, send_hour, send_minute)

    return target_utc


def get_nurture_send_time(contact: dict, now: datetime | None = None) -> datetime:
    """Calculate optimal send time for a nurture touch."""
    if now is None:
        now = datetime.now(timezone.utc)

    send_hour = contact.get("optimal_send_hour", 10) or 10
    send_minute = contact.get("optimal_send_minute", 0) or 0

    # Nurture sends today at the preferred time if possible
    target_pacific = now.astimezone(PACIFIC_TZ).replace(
        hour=send_hour, minute=send_minute, second=0, microsecond=0,
    )
    target_pacific = _clamp_quiet_hours(target_pacific)
    target_utc = target_pacific.astimezone(timezone.utc)

    if target_utc <= now:
        target_utc = _next_business_day_at_time(now, send_hour, send_minute)

    return target_utc


def calculate_engagement_time(
    ghl_contact: dict | None = None,
    state_entry: dict | None = None,
) -> dict:
    """
    Extract engagement time from a contact's GHL data or state entry.

    Returns dict with: optimal_send_hour, optimal_send_minute,
                       optimal_send_day, engagement_source
    """
    result = {
        "optimal_send_hour": 10,
        "optimal_send_minute": 0,
        "optimal_send_day": "Tuesday",
        "engagement_source": "cold",
    }

    # Try GHL contact timestamps first
    timestamp = None
    source = "cold"

    if ghl_contact:
        # dateAdded is when the contact was created (often form submission time)
        for field in ("dateAdded", "dateCreated"):
            ts_str = ghl_contact.get(field, "")
            if ts_str:
                try:
                    timestamp = _parse_iso(ts_str)
                    source = "web_form"
                    break
                except (ValueError, TypeError):
                    continue

    # Fall back to state entry timestamps
    if not timestamp and state_entry:
        for field, src in [
            ("replied_at", "email"),
            ("proposal_sent_at", "cold"),
            ("first_outreach_at", "cold"),
            ("discovered_at", "cold"),
        ]:
            ts_str = state_entry.get(field)
            if ts_str:
                try:
                    timestamp = _parse_iso(ts_str)
                    source = src
                    break
                except (ValueError, TypeError):
                    continue

    if timestamp:
        # Convert to Pacific for business hours analysis
        pacific_dt = timestamp.astimezone(PACIFIC_TZ)
        hour = pacific_dt.hour
        minute = (pacific_dt.minute // 15) * 15  # Round to nearest 15 min
        day_name = pacific_dt.strftime("%A")

        # Quiet hours clamp
        if hour < QUIET_HOURS_START:
            hour, minute = QUIET_HOURS_START, 0
        elif hour >= QUIET_HOURS_END:
            hour, minute = 10, 0  # Default to mid-morning

        # Weekend shift
        if day_name in ("Saturday", "Sunday"):
            day_name = "Monday"

        result["optimal_send_hour"] = hour
        result["optimal_send_minute"] = minute
        result["optimal_send_day"] = day_name
        result["engagement_source"] = source

    return result


def update_optimal_time_from_reply(contact: dict, reply_timestamp: str) -> bool:
    """
    Update optimal send time based on a reply timestamp.
    Only updates if the reply is within quiet hours.
    Returns True if updated.
    """
    try:
        reply_dt = _parse_iso(reply_timestamp)
        pacific_dt = reply_dt.astimezone(PACIFIC_TZ)
        hour = pacific_dt.hour

        # Only update from replies within business hours
        if hour < QUIET_HOURS_START or hour >= QUIET_HOURS_END:
            return False

        minute = (pacific_dt.minute // 15) * 15
        day_name = pacific_dt.strftime("%A")

        # Weekend replies don't update day preference
        if day_name in ("Saturday", "Sunday"):
            return False

        contact["optimal_send_hour"] = hour
        contact["optimal_send_minute"] = minute
        contact["optimal_send_day"] = day_name
        log.info(
            "Updated optimal time from reply: %s %d:%02d for %s",
            day_name, hour, minute,
            contact.get("first_name", "unknown"),
        )
        return True
    except (ValueError, TypeError):
        return False


def classify_velocity(contact: dict) -> str:
    """
    Classify engagement velocity based on reply timing.
    Returns "fast", "medium", or "slow".
    """
    replied_at = contact.get("replied_at")
    if not replied_at:
        return "medium"

    # Compare reply time to last touch
    last_touch = contact.get("last_touch_at")
    ref = last_touch or contact.get("first_outreach_at") or contact.get("proposal_sent_at")
    if not ref:
        return "medium"

    try:
        reply_dt = _parse_iso(replied_at)
        ref_dt = _parse_iso(ref)
        hours_to_reply = (reply_dt - ref_dt).total_seconds() / 3600

        if hours_to_reply <= 24:
            return "fast"
        elif hours_to_reply <= 72:
            return "medium"
        else:
            return "slow"
    except (ValueError, TypeError):
        return "medium"


def get_proposal_view_send_time(contact: dict, now: datetime | None = None) -> datetime | None:
    """
    If the contact viewed a proposal, return a send time matching the view hour.
    Returns None if no view recorded.
    """
    viewed_at = contact.get("proposal_viewed_at")
    if not viewed_at:
        return None

    if now is None:
        now = datetime.now(timezone.utc)

    try:
        view_dt = _parse_iso(viewed_at)
        pacific_dt = view_dt.astimezone(PACIFIC_TZ)
        hour = pacific_dt.hour
        minute = (pacific_dt.minute // 15) * 15

        if hour < QUIET_HOURS_START or hour >= QUIET_HOURS_END:
            hour, minute = 10, 0

        return _next_business_day_at_time(now, hour, minute)
    except (ValueError, TypeError):
        return None


def should_recency_boost(contact: dict, now: datetime | None = None) -> tuple:
    """
    Check if a contact should get a recency-boosted follow-up.

    Returns (should_boost: bool, target_time: datetime | None, reason: str)
    """
    if now is None:
        now = datetime.now(timezone.utc)

    source = contact.get("engagement_source")

    # New form submissions: boost within 1-2 hours
    discovered_at = contact.get("discovered_at")
    if (source == "web_form" and discovered_at
            and not contact.get("first_outreach_at")
            and contact.get("touches_sent", 0) == 0):
        try:
            disc_dt = _parse_iso(discovered_at)
            hours_since = (now - disc_dt).total_seconds() / 3600
            if hours_since <= 2:
                # Schedule for 1 hour from now
                target = now + timedelta(hours=1)
                target_pacific = target.astimezone(PACIFIC_TZ)
                target_pacific = _clamp_quiet_hours(target_pacific)
                return True, target_pacific.astimezone(timezone.utc), "web_form"
        except (ValueError, TypeError):
            pass

    # Recent replies: boost within 2-4 hours
    replied_at = contact.get("replied_at") or contact.get("soft_replied_at")
    if replied_at:
        try:
            reply_dt = _parse_iso(replied_at)
            hours_since = (now - reply_dt).total_seconds() / 3600
            if hours_since <= 4:
                target = now + timedelta(hours=2)
                target_pacific = target.astimezone(PACIFIC_TZ)
                target_pacific = _clamp_quiet_hours(target_pacific)
                return True, target_pacific.astimezone(timezone.utc), "reply"
        except (ValueError, TypeError):
            pass

    return False, None, ""


def get_channel_for_source(engagement_source: str, default: str = "email") -> str:
    """Channel mirroring: return the channel matching the lead's engagement source."""
    mapping = {
        "web_form": "email",
        "email": "email",
        "phone_call": "sms",
        "sms": "sms",
        "cold": default,
    }
    return mapping.get(engagement_source, default)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clamp_quiet_hours(dt: datetime) -> datetime:
    """Clamp a datetime to business hours (7 AM - 7 PM)."""
    if dt.hour < QUIET_HOURS_START:
        return dt.replace(hour=QUIET_HOURS_START, minute=0, second=0, microsecond=0)
    elif dt.hour >= QUIET_HOURS_END:
        # Push to next business day at 10 AM
        next_day = dt + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        return next_day.replace(hour=10, minute=0, second=0, microsecond=0)
    return dt


def _next_business_day_at_time(now: datetime, hour: int, minute: int) -> datetime:
    """Return the next business day at the specified Pacific time, converted to UTC."""
    pacific_now = now.astimezone(PACIFIC_TZ)
    candidate = pacific_now + timedelta(days=1)
    candidate = candidate.replace(hour=hour, minute=minute, second=0, microsecond=0)

    # Skip weekends
    while candidate.weekday() >= 5:
        candidate += timedelta(days=1)

    # Clamp to quiet hours
    candidate = _clamp_quiet_hours(candidate)

    return candidate.astimezone(timezone.utc)
