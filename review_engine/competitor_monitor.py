"""
Competitor Review Monitor
Tracks competitor review counts and ratings over time via Google Places API.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import tenant_context as tc
from shared_utils.event_bus import publish_event
from shared_utils.usage_tracker import log_usage

log = logging.getLogger(__name__)

DATA_FILE = Path(__file__).parent / "competitor_data.json"


def _load_data() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"competitors": {}, "snapshots": []}


def _save_data(data: dict):
    tmp = str(DATA_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, str(DATA_FILE))


def fetch_competitor_metrics(competitors: list[str], city: str,
                             industry: str) -> list[dict]:
    api_key = os.getenv("GOOGLE_PLACES_API_KEY", "")
    if not api_key:
        log.warning("GOOGLE_PLACES_API_KEY not set -- competitor monitoring disabled")
        return []

    results = []
    for name in competitors:
        try:
            search_url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
            params = {
                "input": f"{name} {city}",
                "inputtype": "textquery",
                "fields": "name,rating,user_ratings_total,place_id",
                "key": api_key,
            }
            r = requests.get(search_url, params=params, timeout=15)
            data = r.json()

            log_usage("review_engine", "google_places", {"cost_usd": 0.017},
                      client_id=tc.client_id())

            candidates = data.get("candidates", [])
            if candidates:
                place = candidates[0]
                results.append({
                    "name": place.get("name", name),
                    "rating": place.get("rating", 0.0),
                    "review_count": place.get("user_ratings_total", 0),
                    "place_id": place.get("place_id", ""),
                })
            else:
                log.warning("Competitor '%s' not found in Google Places", name)
        except Exception as e:
            log.error("Failed to fetch competitor '%s': %s", name, e)

    return results


def run_monitor(competitors: list[str] = None, city: str = "",
                industry: str = "") -> dict:
    config = tc.get_review_engine_config()
    comp_config = config.get("competitor_tracking", {})

    if not comp_config.get("enabled", False):
        log.info("Competitor tracking disabled")
        return {"status": "disabled"}

    if competitors is None:
        competitors = comp_config.get("competitors", [])
    if not city:
        city = comp_config.get("city", tc.company_city())
    if not industry:
        industry = comp_config.get("industry", tc.company_industry())

    if not competitors:
        log.info("No competitors configured for tracking")
        return {"status": "no_competitors"}

    current = fetch_competitor_metrics(competitors, city, industry)
    if not current:
        return {"status": "fetch_failed"}

    data = _load_data()

    snapshot = {
        "date": datetime.now().isoformat(),
        "metrics": {c["name"]: {"rating": c["rating"], "reviews": c["review_count"]}
                    for c in current},
    }

    trends = []
    prev_snapshots = data.get("snapshots", [])
    if prev_snapshots:
        prev = prev_snapshots[-1].get("metrics", {})
        for comp in current:
            name = comp["name"]
            prev_data = prev.get(name, {})
            if prev_data:
                review_diff = comp["review_count"] - prev_data.get("reviews", 0)
                rating_diff = round(comp["rating"] - prev_data.get("rating", 0), 1)
                if review_diff != 0 or rating_diff != 0:
                    trends.append({
                        "name": name,
                        "review_change": review_diff,
                        "rating_change": rating_diff,
                        "current_reviews": comp["review_count"],
                        "current_rating": comp["rating"],
                    })

    data["snapshots"].append(snapshot)
    data["snapshots"] = data["snapshots"][-12:]
    _save_data(data)

    publish_event("reviews", "competitor_update", {
        "competitors": current,
        "trends": trends,
    })

    log.info("Competitor monitor complete: %d competitors, %d trends detected",
             len(current), len(trends))
    return {"status": "ok", "competitors": current, "trends": trends}
