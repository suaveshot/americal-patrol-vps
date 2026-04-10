"""
Email Assistant (Larry) -- Client Interaction Tracker
Tracks per-client email interaction history for smarter responses.
"""

import json
import os
from datetime import datetime
from pathlib import Path

CLIENT_FILE = Path(__file__).resolve().parent / "client_interactions.json"


def load_clients():
    if not CLIENT_FILE.exists():
        return {}
    try:
        return json.loads(CLIENT_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}


def save_clients(data):
    tmp = CLIENT_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(str(tmp), str(CLIENT_FILE))


def record_interaction(email_data, category, action):
    """Record an email interaction for the sender's domain."""
    sender = (email_data.get("from") or "").lower()
    domain = sender.split("@")[-1].rstrip(">").strip()
    if not domain or domain in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com"):
        key = sender.split("<")[-1].rstrip(">").strip() if "<" in sender else sender
    else:
        key = domain

    clients = load_clients()
    if key not in clients:
        clients[key] = {
            "first_contact": datetime.now().isoformat(),
            "contact_count": 0,
            "categories": {},
            "last_contact": None,
            "last_category": None,
            "last_subject": None,
        }

    entry = clients[key]
    entry["contact_count"] += 1
    entry["last_contact"] = datetime.now().isoformat()
    entry["last_category"] = category
    entry["last_subject"] = email_data.get("subject", "")[:100]
    cats = entry.get("categories", {})
    cats[category] = cats.get(category, 0) + 1
    entry["categories"] = cats

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
