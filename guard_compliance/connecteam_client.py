# guard_compliance/connecteam_client.py
"""
Connecteam API client for fetching employee/user data.

API: https://api.connecteam.com/users/v1/users
Auth: X-API-KEY header
Rate limits (Expert plan): 100 req/min, 10,000 req/day
"""

import logging
import re
import time
from datetime import datetime

import requests

from shared_utils.retry import with_retry

log = logging.getLogger(__name__)

BASE_URL = "https://api.connecteam.com"
PAGE_SIZE = 500          # Max allowed by Connecteam
PAGE_DELAY = 0.7         # Seconds between paginated requests (stay under 100/min)

# Date formats Connecteam might return — try each in order
DATE_FORMATS = [
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%m/%d/%y",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%SZ",
    "%d/%m/%Y",
]


def parse_date(value: str) -> str | None:
    """Try multiple date formats and return YYYY-MM-DD or None."""
    if not value or not isinstance(value, str):
        return None
    value = value.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    log.warning(f"Could not parse date: '{value}'")
    return None


@with_retry(max_attempts=3, base_delay=5, exceptions=(requests.Timeout, requests.ConnectionError))
def _fetch_page(api_key: str, offset: int = 0, status: str = "active") -> dict:
    """Fetch a single page of users from Connecteam."""
    headers = {
        "X-API-KEY": api_key,
        "Accept": "application/json",
    }
    params = {
        "limit": PAGE_SIZE,
        "offset": offset,
        "userStatus": status,
    }
    resp = requests.get(f"{BASE_URL}/users/v1/users", headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    try:
        return resp.json()
    except requests.exceptions.JSONDecodeError:
        log.error(f"Connecteam API returned non-JSON response (HTTP {resp.status_code}). "
                  f"Check your CONNECTEAM_API_KEY in .env. Response: {resp.text[:200]}")
        raise RuntimeError("Connecteam API returned invalid response — check API key")


def get_all_users(api_key: str, status: str = "active") -> list[dict]:
    """
    Fetch all users from Connecteam with pagination.
    Returns a flat list of user dicts.
    """
    all_users = []
    offset = 0

    while True:
        raw = _fetch_page(api_key, offset=offset, status=status)
        # Response structure: {"requestId": "...", "data": {"users": [...]}}
        users = raw.get("data", {}).get("users", [])
        if not users:
            break

        all_users.extend(users)
        log.info(f"Fetched {len(users)} users (offset={offset}, total so far={len(all_users)})")

        if len(users) < PAGE_SIZE:
            break

        offset += PAGE_SIZE
        time.sleep(PAGE_DELAY)

    return all_users


def discover_custom_fields(users: list[dict]) -> set[str]:
    """
    Scan all users and return the set of custom field names found.
    Useful for initial setup — run with --discover to see what fields exist.
    """
    field_names = set()
    for user in users:
        for field in user.get("customFields", []):
            name = field.get("name") or field.get("label") or field.get("title", "")
            if name:
                field_names.add(name)
    return field_names


# ── Guard Card Field Parser ──────────────────────────────────────────────────
#
# Connecteam stores all credentials in a SINGLE free-text field:
#   "Guard Card License Number and Expiration Date"
#
# Examples:
#   "G6473162 12/31/27"
#   "G1187108 12/31/27   Baton- 112957 12/31/25   Firearm- 235767  12/31/25"
#   "G1223656 8/31/25   Firearm- FQ243035"
#   "G6339708 9/30/27    Baton-G1546726  8/31/25"
#   "excempt"
#   "Dnp"
#
# Pattern: guard_card_number guard_card_expiry [Baton- baton_number baton_expiry]
#          [Firearm- firearm_number firearm_expiry]

# Regex for a date: M/D/YY or MM/DD/YY or MM/DD/YYYY
_DATE_RE = r'\d{1,2}/\d{1,2}/(?:\d{4}|\d{2})'

# Guard card number: starts with G followed by digits
_GUARD_CARD_RE = re.compile(
    r'(G\d{5,8})\s+(' + _DATE_RE + r')',
    re.IGNORECASE,
)

# Baton: "Baton-" followed by optional G prefix + number + optional date
_BATON_RE = re.compile(
    r'Baton-\s*(G?\d{4,8})\s*(' + _DATE_RE + r')?',
    re.IGNORECASE,
)

# Firearm: "Firearm-" followed by optional FQ/G prefix + number + optional date
_FIREARM_RE = re.compile(
    r'Firearm-\s*([A-Z]*\d{4,8})\s*(' + _DATE_RE + r')?',
    re.IGNORECASE,
)


def parse_credential_field(raw_value: str) -> dict:
    """
    Parse the combined "Guard Card License Number and Expiration Date" field
    into structured credential data.

    Returns dict with keys like:
        guard_card_number, guard_card_expiry,
        baton_number, baton_expiry,
        firearm_number, firearm_expiry,
        raw_value, exempt
    """
    result = {
        "guard_card_number": None,
        "guard_card_expiry": None,
        "baton_number": None,
        "baton_expiry": None,
        "firearm_number": None,
        "firearm_expiry": None,
        "raw_value": raw_value,
        "exempt": False,
    }

    if not raw_value or not isinstance(raw_value, str):
        return result

    text = raw_value.strip()

    # Check for exempt/DNP/special cases
    if text.lower() in ("exempt", "excempt", "dnp", "n/a", "na", "none", ""):
        result["exempt"] = True
        return result

    # Parse guard card (first G-number + date)
    gc_match = _GUARD_CARD_RE.search(text)
    if gc_match:
        result["guard_card_number"] = gc_match.group(1).upper()
        result["guard_card_expiry"] = parse_date(gc_match.group(2))

    # Parse baton
    baton_match = _BATON_RE.search(text)
    if baton_match:
        result["baton_number"] = baton_match.group(1).upper()
        if baton_match.group(2):
            result["baton_expiry"] = parse_date(baton_match.group(2))

    # Parse firearm
    firearm_match = _FIREARM_RE.search(text)
    if firearm_match:
        result["firearm_number"] = firearm_match.group(1).upper()
        if firearm_match.group(2):
            result["firearm_expiry"] = parse_date(firearm_match.group(2))

    return result


def extract_officer_data(user: dict, field_mappings: dict) -> dict:
    """
    Extract officer compliance data from a Connecteam user dict.

    The field_mappings config points to the combined credential field.
    We parse it into individual credential types.
    """
    # Build a lookup: Connecteam field name → value
    custom_values = {}
    for field in user.get("customFields", []):
        label = field.get("name") or field.get("label") or field.get("title", "")
        value = field.get("value", "")
        if label:
            custom_values[label] = value

    # Get the combined credential field
    cred_field_name = field_mappings.get(
        "guard_card_combined",
        "Guard Card License Number and Expiration Date"
    )
    raw_creds = str(custom_values.get(cred_field_name, ""))

    # Parse the combined field into individual credentials
    parsed = parse_credential_field(raw_creds)

    credentials = {
        "guard_card_number": parsed["guard_card_number"],
        "guard_card_expiry": parsed["guard_card_expiry"],
        "baton_number": parsed["baton_number"],
        "baton_expiry": parsed["baton_expiry"],
        "firearm_number": parsed["firearm_number"],
        "firearm_expiry": parsed["firearm_expiry"],
        "raw_value": parsed["raw_value"],
        "exempt": parsed["exempt"],
    }

    officer = {
        "connecteam_id": str(user.get("userId", user.get("id", ""))),
        "first_name": user.get("firstName", ""),
        "last_name": user.get("lastName", ""),
        "name": f"{user.get('firstName', '')} {user.get('lastName', '')}".strip(),
        "email": user.get("email") or user.get("emailAddress") or "",
        "phone": user.get("phoneNumber") or user.get("phone") or "",
        "status": user.get("userStatus", "active"),
        "credentials": credentials,
    }

    return officer
