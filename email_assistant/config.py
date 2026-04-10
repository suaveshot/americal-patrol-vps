"""
Email Assistant (Larry) — Configuration
Settings, filter rules, company context, and signature.
"""

import json
from pathlib import Path

# ── Paths ────────────────────────────────────────────────────────────────────
BASE_DIR    = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
STATE_FILE  = BASE_DIR / "email_state.json"
LOG_FILE    = BASE_DIR / "automation.log"
CLIENTS_JSON = PROJECT_DIR / "patrol_automation" / "clients.json"

# ── Email addresses ──────────────────────────────────────────────────────────
LARRY_EMAIL = "americalpatrol@gmail.com"
SAM_EMAIL   = "salarcon@americalpatrol.com"

# ── Classifier settings ─────────────────────────────────────────────────────
CONFIDENCE_THRESHOLD = 0.85
SEARCH_WINDOW_HOURS  = 2
CLAUDE_MODEL         = "claude-sonnet-4-6"
CLAUDE_MAX_TOKENS    = 1500

# ── Signature (matches patrol_automation/draft_composer.py) ──────────────────
SIGNATURE = (
    "Best Regards,\n"
    "Larry\n\n"
    "Americal Patrol, Inc.\n"
    "Mailing: 3301 Harbor Blvd., Oxnard, CA 93035\n"
    "VC Office: (805) 844-9433  |  LA & OC Office: (714) 521-0855  |  FAX: (866) 526-8472\n"
    "www.americalpatrol.com"
)

# ── Noise filters ────────────────────────────────────────────────────────────
NOISE_SENDER_PATTERNS = [
    "noreply@",
    "no-reply@",
    "notifications@",
    "notification@",
    "mailer-daemon@",
    "postmaster@",
    "noreply@reports.connecteam.com",
    "calendar-notification@google.com",
    "notifications@github.com",
    "donotreply@",
    "info@mail.",
    "news@",
    "newsletter@",
    "updates@",
    "support@google.com",
    "notify@",
]

INTERNAL_DOMAINS = [
    "americalpatrol.com",
    "msg.americalpatrol.com",
]

NOISE_SUBJECT_PATTERNS = [
    "unsubscribe",
    "out of office",
    "automatic reply",
    "auto-reply",
    "autoreply",
    "order confirmation",
    "your receipt",
    "payment received",
    "invoice #",
    "weekly digest",
    "daily summary",
    "delivery notification",
]

NOISE_GMAIL_LABELS = [
    "CATEGORY_PROMOTIONS",
    "CATEGORY_SOCIAL",
    "CATEGORY_UPDATES",
    "CATEGORY_FORUMS",
    "SPAM",
    "TRASH",
]


# ── Client domain lookup ────────────────────────────────────────────────────
_PUBLIC_DOMAINS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
    "icloud.com", "me.com", "live.com", "msn.com", "protonmail.com",
}


def _load_client_domains():
    """Extract unique email domains from clients.json recipient list.
    Excludes public email providers (gmail, yahoo, etc.) to avoid false matches."""
    if not CLIENTS_JSON.exists():
        return set()
    data = json.loads(CLIENTS_JSON.read_text(encoding="utf-8"))
    domains = set()
    for group in data.get("groups", []):
        for email in group.get("recipients", []):
            parts = email.split("@")
            if len(parts) == 2:
                domain = parts[1].lower()
                if domain not in _PUBLIC_DOMAINS:
                    domains.add(domain)
    return domains


CLIENT_DOMAINS = _load_client_domains()


def is_client_email(email_data):
    """
    Return True if this email should be analyzed by the classifier.
    Returns False for noise (newsletters, noreply, internal, etc.).
    """
    sender = (email_data.get("from") or "").lower()
    subject = (email_data.get("subject") or "").lower()
    labels = email_data.get("labels", [])

    # Reject: noise Gmail labels
    for label in labels:
        if label in NOISE_GMAIL_LABELS:
            return False

    # Reject: noise sender patterns
    for pattern in NOISE_SENDER_PATTERNS:
        if pattern in sender:
            return False

    # Reject: internal domains
    sender_domain = sender.split("@")[-1].rstrip(">").strip()
    for domain in INTERNAL_DOMAINS:
        if sender_domain == domain:
            return False

    # Reject: noise subject patterns
    for pattern in NOISE_SUBJECT_PATTERNS:
        if pattern in subject:
            return False

    # Accept: known client domain (boost)
    if sender_domain in CLIENT_DOMAINS:
        return True

    # Accept: anything that passed all reject filters
    return True


# ── Company context for Claude system prompt ─────────────────────────────────
COMPANY_CONTEXT = """
Americal Patrol, Inc. is a BSIS-licensed (PPO #16968) security patrol company
headquartered in Oxnard, California. Founded in 1986, veteran-owned.

SERVICES:
- Armed and unarmed security patrol officers
- 24/7 mobile patrol and standing guard coverage
- Daily Activity Reports (DARs) and Incident Reports
- HOA and residential complex patrol
- Commercial and industrial property security
- Fire watch services
- Special event security
- Vehicle inspection and parking enforcement

SERVICE AREAS:
- Ventura County: Oxnard, Ventura, Camarillo, Thousand Oaks, Simi Valley, Moorpark,
  Santa Paula, Fillmore, Ojai, Port Hueneme
- Orange County: Anaheim, Fullerton, Placentia, Brea, Tustin
- Los Angeles County: LA, Gardena, Vernon, El Monte, City of Industry, Manhattan Beach, El Segundo

CONTACTS:
- Sam Alarcon, Vice President: salarcon@americalpatrol.com, (805) 515-3834
- Don Alarcon: don@americalpatrol.com
- VC Office: (805) 844-9433
- LA & OC Office: (714) 521-0855

CURRENT CLIENTS (do not share pricing):
- Transwestern (Stadium Plaza, Towers Industrial, Buena Park Business Center, etc.)
- Pacific Corinthian Yacht Club
- Peninsula Yacht Marina (Suntex)
- Simpson Strong Tie
- Harbor Lights HOA
- Manhattan Plaza
- Nexon
- LAX Logistics (Prologis)
- Assisted Home Health & Hospice
- John Reed Industrial Park (Longpoint)
- Westside Plaza
- Maulhardt Farm House

RESPONSE GUIDELINES:
- Sign all emails as "Larry"
- Professional but approachable tone
- Never commit to pricing, contracts, or scheduling without Sam's approval
- For service inquiries: acknowledge interest, provide general info, offer to schedule a call with Sam
- For existing client questions about reports or incidents: be helpful, reference the relevant DAR/incident report
- For complaints: acknowledge professionally, assure follow-up, escalate to Sam
- For billing/invoice questions: always escalate to Sam
- For scheduling changes: acknowledge receipt, escalate to Sam for confirmation
"""
