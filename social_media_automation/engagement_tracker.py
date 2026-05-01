"""
Americal Patrol — Social Media Engagement Tracker
Pulls post performance metrics from each platform ~48 hours after posting.
Writes an engagement_report event to the pipeline event bus.

Sends a weekly email digest with performance stats to Don/Sam.

Schedule: Daily at 10 AM (checks for posts published 48h ago)

Usage:
    python engagement_tracker.py
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared_utils.event_bus import publish_event, read_events_since

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

SCRIPT_DIR  = Path(__file__).parent
STATE_FILE  = SCRIPT_DIR / "social_state.json"
LOG_FILE    = SCRIPT_DIR / "automation.log"


def log(msg: str):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [ENGAGE] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _fetch_platform_metrics(platform: str, post_id: str) -> dict:
    """Fetch post details from GoHighLevel."""
    try:
        from ghl_publisher import get_post_details
        return get_post_details(post_id)
    except Exception as e:
        return {"error": str(e)}


def run():
    log("Checking for posts to track engagement...")

    # Find social posts published ~48 hours ago
    events = read_events_since("social", "posts_published", days=3)

    if not events:
        log("No recent social media posts found.")
        return

    metrics_collected = []

    for event in events:
        published_at = event.get("published_at", "")
        try:
            pub_time = datetime.fromisoformat(published_at)
            hours_ago = (datetime.now() - pub_time).total_seconds() / 3600

            # Only track posts that are 24-72 hours old
            if hours_ago < 24 or hours_ago > 72:
                continue
        except (ValueError, TypeError):
            continue

        posts = event.get("posts", [])
        for post in posts:
            if post.get("status") != "published":
                continue

            post_id  = post.get("post_id", "")
            platform = post.get("platform", "")

            if not post_id:
                continue

            log(f"  Fetching metrics for {platform} post {post_id}...")
            metrics = _fetch_platform_metrics(platform, post_id)

            metrics_collected.append({
                "platform": platform,
                "post_id": post_id,
                "content_type": post.get("content_type", ""),
                "published_at": published_at,
                "metrics": metrics,
            })

    if metrics_collected:
        publish_event("social", "engagement_report", {
            "tracked_posts": len(metrics_collected),
            "metrics": metrics_collected,
        })
        log(f"Engagement report published for {len(metrics_collected)} post(s).")
    else:
        log("No posts ready for engagement tracking at this time.")


if __name__ == "__main__":
    run()
