"""
Email Assistant (Larry) -- Client Interaction Tracker
Tracks per-client email interaction history, sentiment trends,
response time SLAs, and inactive client detection.
"""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path

CLIENT_FILE = Path(__file__).resolve().parent.parent / "data" / "client_interactions.json"


def load_clients():
    if not CLIENT_FILE.exists():
        return {}
    try:
        return json.loads(CLIENT_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_clients(data):
    CLIENT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CLIENT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(str(tmp), str(CLIENT_FILE))


def _client_key(email_data):
    """Derive a stable key for the sender (domain for business, full address for public)."""
    sender = (email_data.get("from") or "").lower()
    domain = sender.split("@")[-1].rstrip(">").strip()
    if not domain or domain in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com"):
        return sender.split("<")[-1].rstrip(">").strip() if "<" in sender else sender
    return domain


def record_interaction(email_data, category, action, sentiment=None,
                       response_time_sec=None):
    """Record an email interaction for the sender's domain."""
    key = _client_key(email_data)
    clients = load_clients()

    if key not in clients:
        clients[key] = {
            "first_contact": datetime.now().isoformat(),
            "contact_count": 0,
            "categories": {},
            "last_contact": None,
            "last_category": None,
            "last_subject": None,
            "sentiment_history": [],
            "response_times": [],
        }

    entry = clients[key]
    entry["contact_count"] += 1
    entry["last_contact"] = datetime.now().isoformat()
    entry["last_category"] = category
    entry["last_subject"] = email_data.get("subject", "")[:100]

    cats = entry.get("categories", {})
    cats[category] = cats.get(category, 0) + 1
    entry["categories"] = cats

    # Track sentiment (keep last 20 data points)
    if sentiment is not None:
        history = entry.setdefault("sentiment_history", [])
        history.append({
            "ts": datetime.now().isoformat(),
            "score": sentiment,
            "subject": email_data.get("subject", "")[:60],
        })
        if len(history) > 20:
            entry["sentiment_history"] = history[-20:]

    # Track response time in seconds (keep last 20)
    if response_time_sec is not None:
        times = entry.setdefault("response_times", [])
        times.append({
            "ts": datetime.now().isoformat(),
            "seconds": response_time_sec,
        })
        if len(times) > 20:
            entry["response_times"] = times[-20:]

    save_clients(clients)
    return entry


def get_client_context(email_data):
    """Get context about a client for the classifier prompt."""
    sender = (email_data.get("from") or "").lower()
    domain = sender.split("@")[-1].rstrip(">").strip()
    clients = load_clients()
    key = sender.split("<")[-1].rstrip(">").strip() if "<" in sender else sender
    entry = clients.get(domain) or clients.get(key)
    if not entry or entry.get("contact_count", 0) < 2:
        return ""
    top_cats = sorted(entry.get("categories", {}).items(), key=lambda x: -x[1])[:3]
    cat_str = ", ".join(f"{c} ({n}x)" for c, n in top_cats)
    return (
        f"\nCLIENT HISTORY: This sender has contacted us {entry['contact_count']} times. "
        f"Typical topics: {cat_str}. "
        f"Last contact: {entry.get('last_contact', 'unknown')[:10]}."
    )


def get_sentiment_trend(key):
    """
    Return (avg_sentiment, trend_direction) for a client.
    trend_direction: 'declining', 'stable', 'improving', or None if insufficient data.
    """
    clients = load_clients()
    entry = clients.get(key)
    if not entry:
        return None, None

    history = entry.get("sentiment_history", [])
    if len(history) < 2:
        return (history[0]["score"] if history else None), None

    scores = [h["score"] for h in history]
    avg = sum(scores) / len(scores)

    # Compare first half vs second half
    mid = len(scores) // 2
    first_half = sum(scores[:mid]) / mid
    second_half = sum(scores[mid:]) / (len(scores) - mid)
    diff = second_half - first_half

    if diff < -0.2:
        trend = "declining"
    elif diff > 0.2:
        trend = "improving"
    else:
        trend = "stable"

    return avg, trend


def get_avg_response_time(key):
    """Return average response time in minutes for a client, or None."""
    clients = load_clients()
    entry = clients.get(key)
    if not entry:
        return None
    times = entry.get("response_times", [])
    if not times:
        return None
    avg_sec = sum(t["seconds"] for t in times) / len(times)
    return avg_sec / 60  # return minutes


def get_inactive_clients(days_threshold=30):
    """
    Return list of (key, entry) for clients with no contact in the given number of days.
    Only returns clients with at least 2 prior contacts (real relationships, not one-offs).
    """
    clients = load_clients()
    cutoff = (datetime.now() - timedelta(days=days_threshold)).isoformat()
    inactive = []

    for key, entry in clients.items():
        last = entry.get("last_contact")
        count = entry.get("contact_count", 0)
        if last and last < cutoff and count >= 2:
            inactive.append((key, entry))

    # Sort by longest silence first
    inactive.sort(key=lambda x: x[1].get("last_contact", ""))
    return inactive


def get_all_client_stats():
    """Return all client entries with computed metrics for reporting."""
    clients = load_clients()
    stats = []
    for key, entry in clients.items():
        avg_sentiment, trend = get_sentiment_trend(key)
        avg_response = get_avg_response_time(key)
        stats.append({
            "key": key,
            "contact_count": entry.get("contact_count", 0),
            "first_contact": entry.get("first_contact"),
            "last_contact": entry.get("last_contact"),
            "last_category": entry.get("last_category"),
            "avg_sentiment": avg_sentiment,
            "sentiment_trend": trend,
            "avg_response_min": avg_response,
        })
    return stats
