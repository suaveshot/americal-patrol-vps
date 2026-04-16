"""
Win-Back Pipeline Configuration

Reads settings from tenant_config.json win_back section.
Manages campaign state (who was contacted, when, outcomes).
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import tenant_context as tc

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
STATE_FILE = SCRIPT_DIR / "winback_state.json"
LOG_FILE = SCRIPT_DIR / "automation.log"


def get_config() -> dict:
    """Return the full win_back config section with defaults."""
    cfg = tc.get_win_back()
    return {
        "enabled": cfg.get("enabled", False),
        "inactivity_days": cfg.get("inactivity_days", 90),
        "mode": cfg.get("mode", "simple"),
        "simple_template_email": cfg.get("simple_template_email",
            "Hi {first_name}, it's been a while since your last service with {company_name}. "
            "We'd love to have you back! Call us at {phone}."),
        "simple_template_sms": cfg.get("simple_template_sms",
            "Hi {first_name}, we miss you at {company_name}! Call us at {phone}."),
        "discount_percentage": cfg.get("discount_percentage", 10),
        "discount_code": cfg.get("discount_code", "COMEBACK"),
        "discount_expiry_days": cfg.get("discount_expiry_days", 30),
        "channels": cfg.get("channels", ["email"]),
        "max_per_run": cfg.get("max_per_run", 10),
        "exclude_tags": cfg.get("exclude_tags", ["do_not_contact"]),
        "recovery_window_days": cfg.get("recovery_window_days", 30),
        "cooldown_days": cfg.get("cooldown_days", 90),
        "max_ai_messages_per_day": cfg.get("limits", {}).get("max_ai_messages_per_day", 25),
    }


def load_state() -> dict:
    """Load win-back campaign state."""
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "campaigns": {},
        "daily_ai_count": 0,
        "daily_ai_date": "",
    }


def save_state(state: dict):
    """Atomic write of campaign state."""
    tmp = str(STATE_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(STATE_FILE))
