"""
Americal Patrol — Pipeline Event Bus
Lightweight file-based event system for inter-pipeline communication.

Each pipeline publishes JSON event files to pipeline_events/.
Other pipelines read those events to inform their own processing.

Usage:
    from shared_utils.event_bus import publish_event, read_latest_event, read_events_since

    # Publish an event after pipeline completes
    publish_event("blog", "post_published", {
        "title": "Industrial Security in Oxnard",
        "slug": "industrial-security-oxnard",
    })

    # Read the most recent event of a given type
    event = read_latest_event("blog", "post_published")

    # Read all events from the last 7 days
    events = read_events_since("seo", "analysis_results", days=7)
"""

import json
import glob
from datetime import datetime, timedelta
from pathlib import Path

EVENTS_DIR = Path(__file__).resolve().parent.parent / "pipeline_events"


def publish_event(pipeline: str, event_type: str, data: dict) -> Path:
    """
    Write an event file to pipeline_events/.

    File naming: {pipeline}_{event_type}_{YYYYMMDD}.json
    Automatically adds published_at timestamp and pipeline source.

    Returns the path to the created event file.
    """
    EVENTS_DIR.mkdir(exist_ok=True)

    today = datetime.now().strftime("%Y%m%d")
    filename = f"{pipeline}_{event_type}_{today}.json"
    filepath = EVENTS_DIR / filename

    event = {
        "pipeline": pipeline,
        "event_type": event_type,
        "published_at": datetime.now().isoformat(),
        **data,
    }

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(event, f, indent=2, default=str)

    return filepath


def read_latest_event(pipeline: str, event_type: str) -> dict | None:
    """
    Read the most recent event file matching the given pipeline and event type.
    Returns the parsed event dict, or None if no matching event exists.
    """
    pattern = str(EVENTS_DIR / f"{pipeline}_{event_type}_*.json")
    files = sorted(glob.glob(pattern), reverse=True)

    if not files:
        return None

    with open(files[0], "r", encoding="utf-8") as f:
        return json.load(f)


def read_events_since(pipeline: str, event_type: str, days: int = 7) -> list[dict]:
    """
    Read all event files matching the given pipeline and event type
    that were created within the last N days.
    Returns a list of parsed event dicts, newest first.
    """
    pattern = str(EVENTS_DIR / f"{pipeline}_{event_type}_*.json")
    files = sorted(glob.glob(pattern), reverse=True)

    cutoff = datetime.now() - timedelta(days=days)
    results = []

    for filepath in files:
        # Extract date from filename: {pipeline}_{event_type}_{YYYYMMDD}.json
        stem = Path(filepath).stem
        date_str = stem.rsplit("_", 1)[-1]
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            continue

        if file_date < cutoff:
            break  # Files are sorted newest-first, so we can stop early

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                results.append(json.load(f))
        except (json.JSONDecodeError, OSError):
            continue

    return results


def cleanup_old_events(days: int = 30) -> int:
    """
    Delete event files older than N days.
    Returns the number of files deleted.
    """
    cutoff = datetime.now() - timedelta(days=days)
    deleted = 0

    for filepath in EVENTS_DIR.glob("*.json"):
        stem = filepath.stem
        date_str = stem.rsplit("_", 1)[-1]
        try:
            file_date = datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            continue

        if file_date < cutoff:
            filepath.unlink()
            deleted += 1

    return deleted
