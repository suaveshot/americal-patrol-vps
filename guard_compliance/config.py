# guard_compliance/config.py
"""
Guard Compliance — Configuration
Loads .env from project root. Same pattern as weekly_update/config.py.
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
    "CONNECTEAM_API_KEY",
    "WATCHDOG_EMAIL_FROM",
    "GMAIL_APP_PASSWORD",
]

OPTIONAL_DEFAULTS = {
    "SAM_EMAIL": "salarcon@americalpatrol.com",
    "SAM_CARRIER_GATEWAY": "5629684474@vtext.com",
    "BUSINESS_CARRIER_GATEWAY": "8058449433@vtext.com",
    "COMPLIANCE_TEST_MODE": "true",
}


def validate_config() -> None:
    missing = [k for k in REQUIRED_KEYS if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Guard compliance pipeline missing required .env keys: {', '.join(missing)}"
        )


def get(key: str) -> str:
    return os.getenv(key) or OPTIONAL_DEFAULTS.get(key, "")


# Convenience accessors
CONNECTEAM_API_KEY = lambda: get("CONNECTEAM_API_KEY")
GMAIL_SENDER       = lambda: get("WATCHDOG_EMAIL_FROM")
GMAIL_APP_PASSWORD = lambda: get("GMAIL_APP_PASSWORD")
SAM_EMAIL          = lambda: get("SAM_EMAIL")
SAM_CARRIER_GATEWAY      = lambda: get("SAM_CARRIER_GATEWAY")
BUSINESS_CARRIER_GATEWAY = lambda: get("BUSINESS_CARRIER_GATEWAY")

def is_test_mode() -> bool:
    return get("COMPLIANCE_TEST_MODE").lower() in ("true", "1", "yes")

LOG_FILE    = BASE_DIR / "automation.log"
STATE_FILE  = BASE_DIR / "compliance_state.json"
CONFIG_FILE = BASE_DIR / "compliance_config.json"
BSIS_DIR    = BASE_DIR / "bsis_data"
