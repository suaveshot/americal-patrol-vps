"""
Americal Patrol — Hashtag Intelligence Engine
Tracks which hashtags correlate with higher reach on Instagram.
Rotates hashtag sets to avoid Instagram's shadowban on repetitive usage.

Pulls trending keywords from SEO pipeline for hashtag ideas.
Maintains hashtag_performance.json for tracking.
"""

import json
import random
from datetime import datetime
from pathlib import Path

SCRIPT_DIR       = Path(__file__).parent
PERFORMANCE_FILE = SCRIPT_DIR / "hashtag_performance.json"
CONFIG_FILE      = SCRIPT_DIR / "social_config.json"


def _load_performance() -> dict:
    if PERFORMANCE_FILE.exists():
        with open(PERFORMANCE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"hashtags": {}, "last_updated": None}


def _save_performance(data: dict):
    data["last_updated"] = datetime.now().isoformat()
    with open(PERFORMANCE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def _load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# Categorized hashtag pools
HASHTAG_POOLS = {
    "security": [
        "#SecurityPatrol", "#SecurityGuard", "#PropertySecurity",
        "#SecurityServices", "#PatrolServices", "#SecurityCompany",
        "#SecuritySolutions", "#SafetyFirst", "#SecureProperty",
        "#CommercialSecurity", "#HOASecurity", "#RetailSecurity",
    ],
    "location": [
        "#OxnardCA", "#VenturaCounty", "#Camarillo", "#PortHueneme",
        "#SantaPaula", "#Ojai", "#VenturaCalifornia", "#OxnardCalifornia",
        "#SoCalSecurity", "#CaliforniaSecurity",
    ],
    "brand": [
        "#VeteranOwned", "#VeteranBusiness", "#VeteranOwnedBusiness",
        "#AmericalPatrol", "#Since1986", "#FamilyOwned",
    ],
    "industry": [
        "#LossPrevention", "#CrimePrevention", "#PropertyManagement",
        "#HOAManagement", "#CommercialRealEstate", "#FacilityManagement",
        "#BuildingSecurity", "#MobilePatrol", "#NightPatrol",
    ],
}


def select_hashtags(content_type: str, seo_keywords: list[str] = None,
                    count: int = 12) -> list[str]:
    """
    Select an optimized set of hashtags for a post.

    Combines:
      - 3-4 base brand/location tags (always included)
      - 3-4 category-specific tags (rotated)
      - 2-3 SEO keyword tags (from trending data)
      - 2-3 random rotation tags (to avoid repetition)

    Args:
        content_type: Post content type for category selection
        seo_keywords: Trending keywords from SEO pipeline
        count: Target number of hashtags (max 15)

    Returns:
        List of hashtag strings (e.g., ["#SecurityPatrol", "#OxnardCA"])
    """
    perf = _load_performance()
    selected = []

    # Always include 2-3 brand/location tags
    brand_tags = random.sample(HASHTAG_POOLS["brand"], min(2, len(HASHTAG_POOLS["brand"])))
    location_tags = random.sample(HASHTAG_POOLS["location"], min(2, len(HASHTAG_POOLS["location"])))
    selected.extend(brand_tags)
    selected.extend(location_tags)

    # Add category-specific tags
    security_tags = random.sample(HASHTAG_POOLS["security"], min(3, len(HASHTAG_POOLS["security"])))
    selected.extend(security_tags)

    # Add industry tags
    industry_tags = random.sample(HASHTAG_POOLS["industry"], min(2, len(HASHTAG_POOLS["industry"])))
    selected.extend(industry_tags)

    # Convert SEO keywords to hashtags
    if seo_keywords:
        for kw in seo_keywords[:3]:
            tag = "#" + kw.replace(" ", "").replace("-", "").title()
            if tag not in selected and len(tag) <= 30:
                selected.append(tag)

    # Trim to count and deduplicate
    seen = set()
    unique = []
    for tag in selected:
        if tag.lower() not in seen:
            seen.add(tag.lower())
            unique.append(tag)

    return unique[:count]


def record_performance(hashtags: list[str], reach: int):
    """Record reach metrics for a set of hashtags."""
    perf = _load_performance()

    for tag in hashtags:
        tag_lower = tag.lower()
        if tag_lower not in perf["hashtags"]:
            perf["hashtags"][tag_lower] = {
                "tag": tag,
                "total_reach": 0,
                "uses": 0,
                "avg_reach": 0,
            }

        entry = perf["hashtags"][tag_lower]
        entry["total_reach"] += reach
        entry["uses"] += 1
        entry["avg_reach"] = entry["total_reach"] / entry["uses"]

    _save_performance(perf)


def get_top_hashtags(n: int = 10) -> list[dict]:
    """Get the top performing hashtags by average reach."""
    perf = _load_performance()
    entries = list(perf.get("hashtags", {}).values())
    entries.sort(key=lambda x: x.get("avg_reach", 0), reverse=True)
    return entries[:n]
