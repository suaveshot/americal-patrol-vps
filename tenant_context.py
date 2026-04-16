"""
Tenant Context (AP VPS)
Minimal tenant context for Americal Patrol VPS deployment.
Provides is_active(), client_id(), and basic company accessors.
"""

import json
import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent
_CONFIG_PATH = _PROJECT_ROOT / "tenant_config.json"

_config: dict | None = None


def _load() -> dict:
    global _config
    if _config is None:
        if not _CONFIG_PATH.exists():
            # Fallback defaults for AP
            _config = {
                "active": True,
                "client_id": "americal_patrol",
                "company": {"name": "Americal Patrol"},
                "contact": {"owner_name": "Sam Alarcon",
                            "owner_email": "salarcon@americalpatrol.com"},
            }
        else:
            with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
                _config = json.load(f)
    return _config


def reload():
    global _config
    _config = None
    return _load()


def is_active() -> bool:
    return _load().get("active", True)


def client_id() -> str:
    return _load().get("client_id", "americal_patrol")


def usage_thresholds() -> dict:
    return _load().get("usage_thresholds", {})


def company_name() -> str:
    return _load().get("company", {}).get("name", "Americal Patrol")


def company_legal_name() -> str:
    return _load().get("company", {}).get("legal_name", company_name())


def company_phone() -> str:
    return _load().get("company", {}).get("phone", "")


def owner_name() -> str:
    return _load().get("contact", {}).get("owner_name", "Sam Alarcon")


def owner_email() -> str:
    return _load().get("contact", {}).get("owner_email", "salarcon@americalpatrol.com")


def gmail_account() -> str:
    return _load().get("contact", {}).get("owner_email", "americalpatrol@gmail.com")
