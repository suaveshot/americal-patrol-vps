"""
Call Intelligence — Configuration
Loads .env from project root and validates required keys.
Detects Docker vs local environment for persistent data paths.
"""

import json
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent

try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")
except ImportError:
    pass

REQUIRED_KEYS = [
    "GHL_API_KEY",
    "GHL_LOCATION_ID",
    "ANTHROPIC_API_KEY",
]


def validate_config() -> None:
    """Raise EnvironmentError listing ALL missing required keys."""
    missing = [k for k in REQUIRED_KEYS if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Call intelligence missing required keys: {', '.join(missing)}"
        )


def get(key: str, default: str = "") -> str:
    return os.getenv(key) or default


# ── Convenience accessors ────────────────────────────────────────
GHL_API_KEY       = lambda: get("GHL_API_KEY")
GHL_LOCATION_ID   = lambda: get("GHL_LOCATION_ID")
ANTHROPIC_API_KEY = lambda: get("ANTHROPIC_API_KEY")
PIPELINE_ID       = lambda: get("GHL_INQUIRIES_PIPELINE_ID")

# ── Persistent data paths (Docker-aware) ─────────────────────────
_DOCKER_DATA = Path("/app/data")
DATA_DIR = _DOCKER_DATA / "call_intelligence" if _DOCKER_DATA.exists() else BASE_DIR

DB_FILE        = DATA_DIR / "calls.db"
STATE_FILE     = DATA_DIR / "pipeline_state.json"
RECORDINGS_DIR = DATA_DIR / "recordings"

# Config stays in code dir (checked into git)
CONFIG_FILE = BASE_DIR / "config.json"

# Log path: Docker writes to /var/log, local writes to module dir
LOG_FILE = (
    Path("/var/log/ap-call-intel.log")
    if Path("/var/log").exists() and os.access("/var/log", os.W_OK)
    else BASE_DIR / "automation.log"
)


def load_config() -> dict:
    """Load operational config from config.json."""
    if not CONFIG_FILE.exists():
        return {}
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(cfg: dict) -> None:
    """Write config.json atomically."""
    tmp = CONFIG_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(cfg, f, indent=2)
    os.replace(tmp, CONFIG_FILE)
