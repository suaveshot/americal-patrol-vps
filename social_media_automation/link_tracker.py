"""
Americal Patrol — UTM Link Tracker
Appends UTM parameters to URLs so GA4 can track which social media
platform drives the most website traffic.

Example:
    https://americalpatrol.com/blog/security-oxnard
    →  https://americalpatrol.com/blog/security-oxnard?utm_source=facebook&utm_medium=social&utm_campaign=weekly_post

This data flows back to the SEO pipeline for richer analytics.
"""

import json
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode, urlparse, urlunparse, parse_qs

SCRIPT_DIR     = Path(__file__).parent
TRACKER_FILE   = SCRIPT_DIR / "link_tracker.json"


def _load_tracker() -> dict:
    if TRACKER_FILE.exists():
        with open(TRACKER_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"links": [], "last_updated": None}


def _save_tracker(data: dict):
    data["last_updated"] = datetime.now().isoformat()
    with open(TRACKER_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def add_utm_params(url: str, platform: str, campaign: str = "weekly_post",
                   content_type: str = "") -> str:
    """
    Add UTM tracking parameters to a URL.

    Args:
        url: Original URL
        platform: Social platform name (facebook, instagram, linkedin)
        campaign: Campaign name (default: weekly_post)
        content_type: Content type for utm_content (optional)

    Returns:
        URL with UTM parameters appended
    """
    if not url:
        return url

    parsed = urlparse(url)
    existing_params = parse_qs(parsed.query)

    utm_params = {
        "utm_source": platform,
        "utm_medium": "social",
        "utm_campaign": campaign,
    }
    if content_type:
        utm_params["utm_content"] = content_type

    # Merge with existing params
    existing_params.update(utm_params)

    # Flatten single-value lists
    flat_params = {k: v[0] if isinstance(v, list) and len(v) == 1 else v
                   for k, v in existing_params.items()}

    new_query = urlencode(flat_params, doseq=True)
    tracked_url = urlunparse(parsed._replace(query=new_query))

    # Record the tracked link
    tracker = _load_tracker()
    tracker["links"].append({
        "original_url": url,
        "tracked_url": tracked_url,
        "platform": platform,
        "campaign": campaign,
        "content_type": content_type,
        "created_at": datetime.now().isoformat(),
    })

    # Keep only last 200 entries
    tracker["links"] = tracker["links"][-200:]
    _save_tracker(tracker)

    return tracked_url


def get_links_by_platform(platform: str, days: int = 30) -> list[dict]:
    """Get tracked links for a specific platform within N days."""
    tracker = _load_tracker()
    cutoff = datetime.now().replace(hour=0, minute=0, second=0)

    return [
        link for link in tracker.get("links", [])
        if link.get("platform") == platform
    ][-50:]  # Last 50 for the platform
