"""
Americal Patrol -- Review Engine Configuration

Adapted for VPS deployment. Uses tenant_context for company-specific values.
Paths are relative to the VPS project root (/app/ in Docker).
"""

import json
import os
import sys
from pathlib import Path

# Ensure project root is on sys.path so tenant_context is importable
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import tenant_context as tc

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# -- Paths ----------------------------------------------------------------
CLIENTS_FILE = PROJECT_ROOT / "patrol_automation" / "clients.json"
STATE_FILE = SCRIPT_DIR / "review_state.json"
LOG_FILE = SCRIPT_DIR / "automation.log"

# Gmail OAuth -- reuse patrol_automation token (has compose + send scopes)
TOKEN_PATH = PROJECT_ROOT / "patrol_automation" / "token.json"

# GBP OAuth -- reuse gbp_automation token (has business.manage scope)
GBP_TOKEN_PATH = PROJECT_ROOT / "gbp_automation" / "gbp_token.json"
GBP_CONFIG_PATH = PROJECT_ROOT / "gbp_automation" / "gbp_config.json"

# -- Thresholds -------------------------------------------------------------
CLEAN_DAYS_THRESHOLD = 14       # Consecutive incident-free days to qualify
MAX_REQUESTS_PER_RUN = 10       # Cap per execution

# -- Two-tier review cadence ------------------------------------------------
NEW_CLIENT_THRESHOLD_DAYS = 90  # Client is "new" for this many days after first patrol
NEW_CLIENT_COOLDOWN_DAYS = 30   # Ask new clients every 30 days (monthly)
EXISTING_CLIENT_COOLDOWN_DAYS = 60  # Ask existing clients every 60 days (bimonthly)

# -- Onboarding trigger ----------------------------------------------------
ONBOARDING_MIN_DAYS = 14        # Earliest to ask a new client
ONBOARDING_MAX_DAYS = 30        # Latest to ask

# -- GHL Review Funnel (AP-specific URLs) -----------------------------------
GOOGLE_REVIEW_URL = "https://g.page/r/CRLwh6tepYwDEBM/review"
FEEDBACK_FORM_URL = "https://api.leadconnectorhq.com/widget/form/OO8JYEs8c9hH8dMCt9i9"

# -- Draft mode -------------------------------------------------------------
DRAFT_MODE = True               # True = Gmail drafts; False = send directly

# -- SMS -------------------------------------------------------------------
SEND_SMS = True                 # Also send SMS review request via GHL alongside email

# -- Email settings (from tenant_context) -----------------------------------
SENDER_NAME = tc.owner_name()
SENDER_TITLE = tc.owner_title()
COMPANY_NAME = tc.company_legal_name()
BCC_LIST = [tc.owner_email()]

# -- Exclusions -------------------------------------------------------------
MANUAL_EXCLUSIONS = []

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.send",
]

GBP_SCOPES = [
    "https://www.googleapis.com/auth/business.manage",
]


def load_clients():
    """Load client groups from patrol_automation/clients.json."""
    with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("groups", [])


def load_state():
    """Load review engine state (who was asked, when, permanent exclusions)."""
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "requests": {},
        "permanently_excluded": [],
    }


def save_state(state):
    """Atomic write of review state."""
    tmp = str(STATE_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(STATE_FILE))
