"""
Sales Pipeline — Cold Outreach: Lead Filter
Returns cold leads: contacts with no activity for >= threshold_days
that have not already been outreached (per state).
Filters out job seekers, spam, non-lead contacts, and contacts outside service area.
Sorted coldest-first, capped at daily cap.
"""

import logging
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

# Build served cities list from city pages directory
_CITY_PAGES_DIR = (
    Path(__file__).resolve().parent.parent.parent
    / "Website Refresh" / "ghl-build" / "city-pages"
)

def _load_served_cities() -> dict:
    """Load served cities from city page filenames. Returns {normalized_city: url_slug}."""
    cities = {}
    if not _CITY_PAGES_DIR.exists():
        return cities
    for f in os.listdir(_CITY_PAGES_DIR):
        if f.startswith("city-") and f.endswith(".html"):
            slug = f.replace("city-", "").replace(".html", "")
            city_name = slug.replace("-", " ").lower()
            cities[city_name] = slug
    return cities

SERVED_CITIES = _load_served_cities()


def get_city_page_url(city: str) -> str:
    """Return the city page URL if we serve this city, else empty string."""
    if not city:
        return ""
    normalized = city.lower().strip()
    slug = SERVED_CITIES.get(normalized)
    if slug:
        return f"https://americalpatrol.com/{slug}-security-guards"
    # Try partial match (e.g., "Los Angeles" -> "los angeles")
    for served_city, served_slug in SERVED_CITIES.items():
        if normalized in served_city or served_city in normalized:
            return f"https://americalpatrol.com/{served_slug}-security-guards"
    return ""


def is_in_service_area(city: str) -> bool:
    """Check if a city is in our service area."""
    if not city:
        return True  # Don't filter if no city data — give benefit of the doubt
    return bool(get_city_page_url(city))

# Keywords that indicate a job seeker or applicant, not a lead
JOB_SEEKER_KEYWORDS = [
    "guard card", "position", "employment", "job", "resume", "apply",
    "hiring", "application", "looking for work", "work opportunity",
    "laborales", "considerar",  # Spanish job-seeking phrases
]

# Known internal/test contacts and spam patterns to skip
BLOCKLIST_EMAILS = [
    "americalpatrol.com",  # Internal company emails
    "leadlinc",            # Lead Linc Pro test system
    "msg.americalpatrol",  # Sending domain
]

BLOCKLIST_NAMES = [
    "test", "demo", "sample", "polesmoker", "asdf", "xxx",
    "bait", "bate",
]

# Fake/placeholder org names that indicate spam
BLOCKLIST_ORGS = [
    "nil", "n/a", "none", "na", "unknown", "no company", "no org",
    "test", "demo", "asdf", "xxx",
]

# Organization types we don't service — auto-exclude from cold outreach
EXCLUDED_ORG_KEYWORDS = [
    "school", "academy", "university", "college", "campus",
    "elementary", "middle school", "high school", "preschool",
    "kindergarten", "charter school", "school district",
    "church", "chapel", "ministry", "ministries", "cathedral",
    "temple", "mosque", "synagogue", "parish", "congregation",
    "worship", "fellowship", "bible",
]


def _parse_iso(date_str: str) -> datetime | None:
    """Parse ISO8601 string to UTC-aware datetime. Returns None on failure."""
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


def _last_activity_dt(contact: dict) -> datetime | None:
    """Return the best available last-activity datetime for a contact."""
    for field in ("lastActivity", "dateLastActivity", "dateAdded", "dateCreated"):
        dt = _parse_iso(contact.get(field, ""))
        if dt:
            return dt
    return None


def get_cold_leads(
    contacts: list,
    threshold_days: int,
    state: dict,
    daily_cap: int = 20,
) -> list:
    """
    Filter contacts to those that are 'cold':
      - No activity for >= threshold_days
      - Not already in state (previously outreached)
      - Not unsubscribed (DND / unsubscribed stage)
    Returns list of contacts with 'days_since_contact' and 'last_contact_at' added.
    Sorted coldest-first, capped at daily_cap.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=threshold_days)
    already_outreached = set(state.get("contacts", {}).keys())

    # Also skip contacts that are unsubscribed
    unsubscribed = set()
    for cid, entry in state.get("contacts", {}).items():
        if entry.get("stage") == "unsubscribed":
            unsubscribed.add(cid)

    cold = []
    filtered_spam = 0
    for c in contacts:
        contact_id = c.get("id", "")
        if contact_id in already_outreached:
            continue
        if contact_id in unsubscribed:
            continue
        # Skip contacts with DND enabled in GHL
        dnd = c.get("dnd", False)
        if dnd is True or (isinstance(dnd, dict) and any(dnd.values())):
            continue
        # Skip job seekers, spam, and non-leads
        if _is_spam_or_job_seeker(c):
            filtered_spam += 1
            log.debug(f"Contact {contact_id}: filtered as spam/job seeker")
            continue
        # Skip contacts outside service area
        contact_city = c.get("city", "")
        if contact_city and not is_in_service_area(contact_city):
            filtered_spam += 1
            log.debug(f"Contact {contact_id}: filtered — {contact_city} not in service area")
            continue
        last_dt = _last_activity_dt(c)
        if last_dt is None:
            log.debug(f"Contact {contact_id}: no date fields, skipping")
            continue
        if last_dt <= cutoff:
            days_cold = (now - last_dt).days
            enriched = dict(c)
            enriched["days_since_contact"] = days_cold
            enriched["last_contact_at"] = last_dt.isoformat()
            cold.append(enriched)

    # Sort coldest first
    cold.sort(key=lambda x: x["days_since_contact"], reverse=True)
    result = cold[:daily_cap]
    log.info(
        f"Lead filter: {len(contacts)} total, {len(cold)} cold "
        f"(threshold={threshold_days}d), {filtered_spam} filtered (spam/job), "
        f"returning {len(result)} (cap={daily_cap})"
    )
    return result


def _is_spam_or_job_seeker(contact: dict) -> bool:
    """
    Detect job seekers, applicants, spam, internal test contacts,
    and contacts with no real security inquiry.
    """
    email = (contact.get("email") or "").lower()
    phone = contact.get("phone", "")
    first = (contact.get("firstName") or "").strip()
    last = (contact.get("lastName") or "").strip()
    org = (contact.get("companyName") or "").strip()
    full_name = f"{first} {last}".lower()

    # Block internal/test emails
    for blocked in BLOCKLIST_EMAILS:
        if blocked in email:
            return True

    # Block known spam/fake names
    for blocked in BLOCKLIST_NAMES:
        if blocked in full_name:
            return True

    # International phone numbers — only allow US/Canada (+1)
    if phone and not phone.startswith("+1"):
        return True

    # No email and no phone — likely spam/incomplete
    if not email and not phone:
        return True

    # No first name or single character
    if not first or len(first) <= 1:
        return True

    # Check tags — contacts with 'lead' or 'proposal' tag are legitimate
    tags = contact.get("tags", [])
    if isinstance(tags, list) and ("lead" in tags or "proposal" in tags):
        return False

    # No organization AND no lead/proposal tag — almost always not a real inquiry
    if not org:
        return True

    # Block fake/placeholder org names (e.g., "Nil", "N/A", "None")
    org_lower = org.lower().strip()
    if org_lower in BLOCKLIST_ORGS:
        return True

    # Exclude organization types we don't service (schools, churches, campuses)
    for keyword in EXCLUDED_ORG_KEYWORDS:
        if keyword in org_lower:
            return True

    # Build text blob for job-seeker detection
    custom_fields = contact.get("customFields", contact.get("customField", []))
    field_text = ""
    if isinstance(custom_fields, list):
        for cf in custom_fields:
            val = cf.get("value", "")
            if val:
                field_text += " " + str(val)
    elif isinstance(custom_fields, dict):
        for val in custom_fields.values():
            if val:
                field_text += " " + str(val)

    field_text_lower = field_text.lower()

    # Check for job-seeker keywords in custom fields
    for keyword in JOB_SEEKER_KEYWORDS:
        if keyword in field_text_lower:
            if keyword == "guard card" and any(
                w in field_text_lower for w in ["patrol", "property", "building", "community"]
            ):
                continue
            return True

    return False
