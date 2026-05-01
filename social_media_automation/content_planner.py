"""
Americal Patrol — Social Media Content Planner
Reads event bus data and selects unique content for each platform.

Each platform gets a completely different post — different topic, different angle.
The planner checks:
  - Blog events (auto-promote new posts)
  - SEO events (trending keywords for hashtags)
  - GBP events (avoid duplicate topics)
  - Seasonal calendar (override rotation for themed posts)
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

from shared_utils.event_bus import read_latest_event, read_events_since

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "social_config.json"
STATE_FILE  = SCRIPT_DIR / "social_state.json"


def _load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"facebook_index": 0, "instagram_index": 0, "linkedin_index": 0,
            "gbp_index": 0, "gbp_last_posted": None}


def _check_seasonal(config: dict) -> dict | None:
    """Check if a seasonal/holiday date is within 7 days."""
    now = datetime.now()
    for event in config.get("seasonal_calendar", []):
        try:
            event_date = datetime(now.year, event["month"], event["day"])
            delta = (event_date - now).days
            if 0 <= delta <= 7:
                return {
                    "name": event["name"],
                    "description": event["description"],
                    "days_until": delta,
                }
        except (ValueError, KeyError):
            continue
    return None


def _get_blog_event() -> dict | None:
    """Get the most recent blog post published this week."""
    event = read_latest_event("blog", "post_published")
    if not event:
        return None
    try:
        published = datetime.fromisoformat(event["published_at"])
        if (datetime.now() - published).days <= 7:
            return event
    except (KeyError, ValueError):
        pass
    return None


def _get_seo_context() -> dict:
    """Pull trending keywords and topics from SEO analysis."""
    event = read_latest_event("seo", "analysis_results")
    if not event:
        return {"top_keywords": [], "trending_topics": [], "priority_topics": []}
    return {
        "top_keywords": event.get("top_keywords", []),
        "trending_topics": event.get("trending_topics", []),
        "priority_topics": event.get("priority_topics", []),
    }


def _get_gbp_topics() -> list[str]:
    """Get recent GBP post topics to avoid duplication."""
    events = read_events_since("gbp", "post_published", days=14)
    return [e.get("topic_subject", "") for e in events if e.get("topic_subject")]


def plan_posts(log=None) -> dict:
    """
    Plan unique content for each enabled platform.

    Returns:
        {
            "facebook": {"content_type": ..., "context": ..., "rotation_slot": ...},
            "instagram": {"content_type": ..., "context": ..., "rotation_slot": ...},
            "linkedin": {"content_type": ..., "context": ..., "rotation_slot": ...},
            "seo_context": {...},
            "seasonal": {...} or None,
            "blog_event": {...} or None,
        }
    """
    config = _load_config()
    state  = _load_state()

    # Gather context from other pipelines
    seo_context = _get_seo_context()
    blog_event  = _get_blog_event()
    gbp_topics  = _get_gbp_topics()
    seasonal    = _check_seasonal(config)

    if log and seasonal:
        log(f"Seasonal event detected: {seasonal['name']} (in {seasonal['days_until']} days)")
    if log and blog_event:
        log(f"Blog post available for promotion: {blog_event.get('title', 'N/A')}")

    plans = {
        "seo_context": seo_context,
        "seasonal": seasonal,
        "blog_event": blog_event,
        "gbp_recent_topics": gbp_topics,
    }

    # Plan each platform independently
    for platform in ["facebook", "instagram", "linkedin", "gbp"]:
        platform_config = config.get("platforms", {}).get(platform, {})
        if not platform_config.get("enabled", False):
            continue

        # GBP: weekly frequency gate — skip if posted within last 7 days
        if platform == "gbp":
            gbp_last = state.get("gbp_last_posted")
            if gbp_last:
                try:
                    last_dt = datetime.fromisoformat(gbp_last)
                    seconds_ago = (datetime.now() - last_dt).total_seconds()
                    days_ago = int(seconds_ago / 86400)
                    if seconds_ago < 6 * 86400:
                        if log:
                            log(f"  gbp: Skipped — last posted {days_ago} day(s) ago (weekly)")
                        continue
                except (ValueError, TypeError):
                    pass  # Invalid date, allow posting

            # GBP priority topics (written by SEO pipeline)
            priority = config.get("gbp_priority_post_topics", [])
            if priority:
                slot = priority.pop(0)
                config["gbp_priority_post_topics"] = priority
                with open(CONFIG_FILE, "w", encoding="utf-8") as f:
                    json.dump(config, f, indent=2)
                if log:
                    log(f"  gbp: Priority topic (SEO): {slot.get('subject', '')}")
                plans[platform] = {
                    "content_type": slot.get("type", "seo_priority"),
                    "description": slot.get("subject", ""),
                    "subject": slot.get("subject", ""),
                    "context": slot,
                    "rotation_index": state.get("gbp_index", 0),
                    "max_chars": 1500,
                }
                continue

        rotation = config.get("content_rotation", {}).get(platform, [])
        if not rotation:
            continue

        idx_key = f"{platform}_index"
        idx = state.get(idx_key, 0) % len(rotation)
        slot = rotation[idx]

        # Seasonal override: replace the normal rotation with themed content
        if seasonal:
            slot = {
                "type": f"seasonal_{seasonal['name'].lower().replace(' ', '_')}",
                "description": seasonal["description"],
                "seasonal_event": seasonal,
            }

        # Blog promotion override for specific slots
        if blog_event and slot["type"] in ("blog_share", "blog_promotion"):
            slot["blog_event"] = blog_event

        plans[platform] = {
            "content_type": slot["type"],
            "description": slot.get("description", slot.get("subject", "")),
            "subject": slot.get("subject", ""),
            "context": slot,
            "rotation_index": idx,
            "max_chars": platform_config.get("max_chars", 2000),
        }

        if log:
            desc = slot.get("description", slot.get("subject", ""))
            log(f"  {platform}: {slot['type']} — {desc}")

    return plans


def advance_rotation(active_platforms: set | None = None, log=None) -> None:
    """Advance the rotation index for each platform that posted.

    Args:
        active_platforms: Set of platform names that had content planned this run.
            If None, advances all platforms (backward-compatible default).
    """
    config = _load_config()
    state  = _load_state()

    for platform in ["facebook", "instagram", "linkedin", "gbp"]:
        if active_platforms is not None and platform not in active_platforms:
            continue
        rotation = config.get("content_rotation", {}).get(platform, [])
        if not rotation:
            continue
        idx_key = f"{platform}_index"
        current = state.get(idx_key, 0)
        state[idx_key] = (current + 1) % len(rotation)

    state["last_run"] = datetime.now().isoformat()

    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

    if log:
        log("Content rotation advanced for all platforms.")
