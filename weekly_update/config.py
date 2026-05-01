# weekly_update/config.py
"""
Weekly Update — Configuration
Loads .env from project root. Same pattern as sales_autopilot/config.py.
"""

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
    "GHL_INQUIRIES_PIPELINE_ID",
    "WATCHDOG_EMAIL_FROM",
    "GMAIL_APP_PASSWORD",
]

OPTIONAL_DEFAULTS = {
    "GHL_PROPOSAL_SENT_STAGE_ID": "",
    "WEEKLY_UPDATE_TO_EMAIL": "don@americalpatrol.com",
    # Google Ads keys are optional — collect_ads_data() handles missing gracefully
    "GOOGLE_ADS_DEVELOPER_TOKEN": "",
    "GOOGLE_CLIENT_ID": "",
    "GOOGLE_CLIENT_SECRET": "",
    "GOOGLE_ADS_REFRESH_TOKEN": "",
    "GOOGLE_ADS_LOGIN_CUSTOMER_ID": "",
    "GOOGLE_ADS_CLIENT_CUSTOMER_ID": "",
}


def validate_config() -> None:
    missing = [k for k in REQUIRED_KEYS if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Weekly update pipeline missing required .env keys: {', '.join(missing)}"
        )


def get(key: str) -> str:
    return os.getenv(key) or OPTIONAL_DEFAULTS.get(key, "")


# Convenience accessors
GHL_API_KEY        = lambda: get("GHL_API_KEY")
GHL_LOCATION_ID    = lambda: get("GHL_LOCATION_ID")
PIPELINE_ID        = lambda: get("GHL_INQUIRIES_PIPELINE_ID")
PROPOSAL_SENT_STAGE = lambda: get("GHL_PROPOSAL_SENT_STAGE_ID")
GMAIL_SENDER       = lambda: get("WATCHDOG_EMAIL_FROM")
GMAIL_APP_PASSWORD = lambda: get("GMAIL_APP_PASSWORD")
TO_EMAIL           = lambda: get("WEEKLY_UPDATE_TO_EMAIL")

LOG_FILE    = BASE_DIR / "automation.log"
STATE_FILE  = BASE_DIR / "weekly_state.json"
