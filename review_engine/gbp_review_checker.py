"""
GBP Review Checker

Fetches existing Google reviews from the Google Business Profile API
and matches reviewer names against client contact names to identify
clients who have already left a review.

GBP Reviews API: mybusiness.googleapis.com/v4/accounts/{account}/locations/{location}/reviews
"""

import json
import logging
from difflib import SequenceMatcher

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request, AuthorizedSession

from review_engine.config import GBP_TOKEN_PATH, GBP_CONFIG_PATH, GBP_SCOPES

log = logging.getLogger("review_engine")

GBP_BASE = "https://mybusiness.googleapis.com/v4"


def _get_gbp_session():
    """Get an AuthorizedSession for GBP API calls."""
    if not GBP_TOKEN_PATH.exists():
        log.warning("GBP token not found at %s -- skipping review scan", GBP_TOKEN_PATH)
        return None

    creds = Credentials.from_authorized_user_file(str(GBP_TOKEN_PATH), GBP_SCOPES)
    if not creds.valid:
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            GBP_TOKEN_PATH.write_text(creds.to_json())
        else:
            log.error("GBP credentials invalid and cannot refresh")
            return None

    return AuthorizedSession(creds)


def _load_gbp_ids():
    """Load account_id and location_id from gbp_config.json."""
    with open(GBP_CONFIG_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg.get("account_id", ""), cfg.get("location_id", "")


def fetch_reviews():
    """
    Fetch all Google reviews for the GBP listing.
    Returns list of review dicts with 'reviewer_name' and 'star_rating'.
    Returns empty list if API unavailable or no reviews.
    """
    session = _get_gbp_session()
    if not session:
        return []

    account_id, location_id = _load_gbp_ids()
    if not account_id or not location_id:
        log.warning("GBP account_id or location_id not configured -- skipping review scan")
        return []

    url = f"{GBP_BASE}/accounts/{account_id}/locations/{location_id}/reviews"
    reviews = []
    page_token = None

    while True:
        params = {"pageSize": 50}
        if page_token:
            params["pageToken"] = page_token

        resp = session.get(url, params=params)
        if resp.status_code != 200:
            log.warning("GBP reviews API returned %s: %s", resp.status_code, resp.text[:200])
            break

        data = resp.json()
        for r in data.get("reviews", []):
            reviewer = r.get("reviewer", {})
            reviews.append({
                "reviewer_name": reviewer.get("displayName", ""),
                "star_rating": r.get("starRating", ""),
                "comment": r.get("comment", ""),
                "create_time": r.get("createTime", ""),
            })

        page_token = data.get("nextPageToken")
        if not page_token:
            break

    log.info("Fetched %d Google reviews from GBP", len(reviews))
    return reviews


def _name_similarity(name1, name2):
    """Fuzzy match two names (case-insensitive). Returns 0.0-1.0."""
    return SequenceMatcher(
        None,
        name1.lower().strip(),
        name2.lower().strip(),
    ).ratio()


def find_reviewed_clients(reviews, client_groups, threshold=0.75):
    """
    Cross-reference Google reviewer names against client contact info.

    For each review, checks:
    1. Reviewer name against recipient email local parts
    2. Reviewer name fuzzy match against known contact names in group

    Returns set of group_ids that have at least one matching reviewer.
    """
    reviewed_group_ids = set()

    reviewer_names = [r["reviewer_name"] for r in reviews if r["reviewer_name"]]

    for group in client_groups:
        gid = group["group_id"]

        # Build list of name-like strings from this group's recipients
        contact_names = []
        for email in group.get("recipients", []):
            local = email.split("@")[0]
            name_from_email = local.replace(".", " ").replace("_", " ").replace("-", " ")
            contact_names.append(name_from_email)

        for account in group.get("accounts", []):
            contact_names.append(account["name"].lower())

        # Check each reviewer against this group's contacts
        for reviewer in reviewer_names:
            for contact in contact_names:
                if _name_similarity(reviewer, contact) >= threshold:
                    reviewed_group_ids.add(gid)
                    log.info(
                        "Matched reviewer '%s' to client group '%s' (via '%s')",
                        reviewer, gid, contact,
                    )
                    break
            if gid in reviewed_group_ids:
                break

    return reviewed_group_ids


def check_for_new_reviews(state):
    """
    Compare current GBP reviews against last-known review IDs in state.
    Returns list of new review dicts (may be empty).
    Updates state["known_review_times"] with current set.
    """
    reviews = fetch_reviews()
    if not reviews:
        return []

    known_times = set(state.get("known_review_times", []))

    new_reviews = []
    current_times = set()
    for r in reviews:
        key = f"{r['reviewer_name']}|{r['create_time']}"
        current_times.add(key)
        if key not in known_times:
            new_reviews.append(r)

    state["known_review_times"] = sorted(current_times)
    return new_reviews
