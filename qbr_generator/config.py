"""
Americal Patrol — QBR Generator Configuration
"""

import json
import os
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent

# ── Paths ────────────────────────────────────────────────────────────────────
CLIENTS_FILE = PROJECT_ROOT / "patrol_automation" / "clients.json"
STATE_FILE = SCRIPT_DIR / "qbr_state.json"
LOG_FILE = SCRIPT_DIR / "automation.log"
TEMPLATE_DIR = SCRIPT_DIR / "templates"
OUTPUT_DIR = SCRIPT_DIR / "output"

# Gmail OAuth — reuse patrol_automation token
TOKEN_PATH = PROJECT_ROOT / "patrol_automation" / "token.json"

# ── Branding ─────────────────────────────────────────────────────────────────
LOGO_PATH = PROJECT_ROOT / "Company Logos" / "AmericalLogo.png"
COMPANY_NAME = "Americal Patrol, Inc."
PRIMARY_COLOR = "#1a3a5c"
ACCENT_COLOR = "#2c7bb6"
LIGHT_BG = "#f5f7fa"

# ── Email ────────────────────────────────────────────────────────────────────
DRAFT_MODE = True
BCC_LIST = ["salarcon@americalpatrol.com", "don@americalpatrol.com"]
SENDER_NAME = "Sam Alarcon"

GMAIL_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.compose",
    "https://www.googleapis.com/auth/gmail.send",
]

# ── Quarter config ───────────────────────────────────────────────────────────
QUARTER_MONTHS = {
    "Q1": [1, 2, 3],
    "Q2": [4, 5, 6],
    "Q3": [7, 8, 9],
    "Q4": [10, 11, 12],
}

SIGNATURE = (
    f"Best Regards,\n"
    f"{SENDER_NAME}\n\n"
    f"{COMPANY_NAME}\n"
    "Mailing: 3301 Harbor Blvd., Oxnard, CA 93035\n"
    "VC Office: (805) 844-9433  |  LA & OC Office: (714) 521-0855\n"
    "www.americalpatrol.com"
)


def current_quarter():
    """Return (quarter_label, month_list) for the PREVIOUS quarter (the one being reported on)."""
    from datetime import datetime
    month = datetime.now().month
    year = datetime.now().year

    # Determine current quarter, then report on the PREVIOUS one
    if month in [1, 2, 3]:
        return f"Q4 {year - 1}", [10, 11, 12], year - 1
    elif month in [4, 5, 6]:
        return f"Q1 {year}", [1, 2, 3], year
    elif month in [7, 8, 9]:
        return f"Q2 {year}", [4, 5, 6], year
    else:
        return f"Q3 {year}", [7, 8, 9], year


def load_clients():
    """Load client groups from patrol_automation/clients.json."""
    with open(CLIENTS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("groups", [])


def load_state():
    """Load QBR state (last QBR dates, history)."""
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"reports": {}}


def save_state(state):
    """Atomic write of QBR state."""
    tmp = str(STATE_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(STATE_FILE))
