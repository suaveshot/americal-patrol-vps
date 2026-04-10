"""
Sales Pipeline — Unified Configuration
Merges config from cold_outreach_automation + sales_autopilot.
Loads .env from project root and validates required keys.
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
    "ANTHROPIC_API_KEY",
    "GHL_CALENDAR_LINK",
    "WATCHDOG_EMAIL_FROM",
    "GMAIL_APP_PASSWORD",
]

OPTIONAL_DEFAULTS = {
    # Cold outreach settings
    "COLD_OUTREACH_THRESHOLD_DAYS": "180",
    "COLD_OUTREACH_DAILY_CAP": "20",
    "COLD_OUTREACH_REVIEW_EMAIL": "",
    "COLD_OUTREACH_FROM_EMAIL": "",
    # Sales autopilot settings
    "FOLLOW_UP_CHECK_HOURS": "48",
    "GHL_INQUIRIES_PIPELINE_ID": "",
    "GHL_PROPOSAL_SENT_STAGE_ID": "",
    "GHL_NEGOTIATING_STAGE_ID": "",
    "SALES_DIGEST_TO_EMAIL": "",
    "GHL_USER_ID": "",
    # Unsubscribe
    "GHL_UNSUBSCRIBE_TRIGGER_URL": "",
    # Cold outreach sending domain (separate from main domain for reputation)
    "COLD_OUTREACH_SENDER_EMAIL": "salarcon@msg.americalpatrol.com",
    "COLD_OUTREACH_SENDER_NAME": "Sam Alarcon",
}


def validate_config() -> None:
    """Raise EnvironmentError listing ALL missing required keys."""
    missing = [k for k in REQUIRED_KEYS if not os.getenv(k)]
    if missing:
        raise EnvironmentError(
            f"Sales pipeline missing required .env keys: {', '.join(missing)}"
        )


def get(key: str) -> str:
    val = os.getenv(key) or OPTIONAL_DEFAULTS.get(key, "")
    return val


def get_int(key: str) -> int:
    return int(get(key))


# ── Convenience accessors ────────────────────────────────────────
GHL_API_KEY          = lambda: get("GHL_API_KEY")
GHL_LOCATION_ID      = lambda: get("GHL_LOCATION_ID")
ANTHROPIC_API_KEY    = lambda: get("ANTHROPIC_API_KEY")
CALENDAR_LINK        = lambda: get("GHL_CALENDAR_LINK")
GMAIL_SENDER         = lambda: get("WATCHDOG_EMAIL_FROM")
GMAIL_APP_PASSWORD   = lambda: get("GMAIL_APP_PASSWORD")

# Cold outreach
REVIEW_EMAIL         = lambda: get("COLD_OUTREACH_REVIEW_EMAIL") or get("SALES_DIGEST_TO_EMAIL")
FROM_EMAIL           = lambda: get("COLD_OUTREACH_FROM_EMAIL") or get("COLD_OUTREACH_SENDER_EMAIL")
SENDER_NAME          = lambda: get("COLD_OUTREACH_SENDER_NAME")
THRESHOLD_DAYS       = lambda: get_int("COLD_OUTREACH_THRESHOLD_DAYS")
DAILY_CAP            = lambda: get_int("COLD_OUTREACH_DAILY_CAP")

# Sales autopilot
PIPELINE_ID          = lambda: get("GHL_INQUIRIES_PIPELINE_ID")
PROPOSAL_SENT_STAGE  = lambda: get("GHL_PROPOSAL_SENT_STAGE_ID")
NEGOTIATING_STAGE    = lambda: get("GHL_NEGOTIATING_STAGE_ID")
DIGEST_TO_EMAIL      = lambda: get("SALES_DIGEST_TO_EMAIL") or REVIEW_EMAIL()
GHL_USER_ID          = lambda: get("GHL_USER_ID")
FOLLOW_UP_CHECK_HOURS = lambda: get_int("FOLLOW_UP_CHECK_HOURS")

# Unsubscribe
UNSUBSCRIBE_TRIGGER_URL = lambda: get("GHL_UNSUBSCRIBE_TRIGGER_URL")

# ── Constants ────────────────────────────────────────────────────
GHL_BASE_URL    = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-04-15"

# ── File paths ───────────────────────────────────────────────────
STATE_FILE    = BASE_DIR / "pipeline_state.json"
DRAFTS_FILE   = BASE_DIR / "pipeline_drafts.json"
LOG_FILE      = BASE_DIR / "automation.log"
TEMPLATE_FILE = BASE_DIR / "proposal" / "proposal_template.docx"
ENRICHMENT_DIR = BASE_DIR / "enrichment"

# ── Call transcripts ────────────────────────────────────────────
TRANSCRIPTS_FILE = BASE_DIR / "call_transcripts.json"

# ── Learning system paths ───────────────────────────────────────
LEARNING_DIR       = BASE_DIR / "learning"
OUTCOME_LOG_FILE   = LEARNING_DIR / "outcome_log.jsonl"
INSIGHTS_FILE      = LEARNING_DIR / "insights.json"
WIN_LOSS_LOG_FILE  = LEARNING_DIR / "win_loss_log.jsonl"
EXIT_ANALYSIS_FILE = LEARNING_DIR / "exit_analysis_log.jsonl"
WEEKLY_REVIEW_FILE = LEARNING_DIR / "weekly_review_state.json"

# Ensure learning directory exists
LEARNING_DIR.mkdir(exist_ok=True)
