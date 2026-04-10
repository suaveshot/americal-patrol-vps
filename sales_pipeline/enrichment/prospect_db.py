"""
Sales Pipeline — Enrichment: Prospect Database
Parses outreach_leads.csv and outreach_emails.md into lookup dicts.
Provides fuzzy matching so GHL contacts can be enriched with
hand-researched company details, decision-maker names, and strategic angles.
"""

import csv
import re
import logging
from pathlib import Path

log = logging.getLogger(__name__)

ENRICHMENT_DIR = Path(__file__).resolve().parent
CSV_PATH = ENRICHMENT_DIR / "outreach_leads.csv"
EMAILS_PATH = ENRICHMENT_DIR / "outreach_emails.md"

# Cache loaded data
_prospects: dict | None = None
_email_hooks: dict | None = None


# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace."""
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9\s]", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _make_aliases(company: str) -> list[str]:
    """
    Generate normalized aliases for a company name.
    E.g., "Community Property Management" -> ["community property management", "cpm"]
    """
    norm = _normalize(company)
    aliases = [norm]

    # Generate acronym (first letter of each word)
    words = norm.split()
    if len(words) > 1:
        acronym = "".join(w[0] for w in words)
        aliases.append(acronym)

    # Common shortenings
    for suffix in ["management", "property management", "of california",
                   "group", "properties", "realty"]:
        if norm.endswith(suffix):
            short = norm[: -len(suffix)].strip()
            if short and short not in aliases:
                aliases.append(short)

    return aliases


# ---------------------------------------------------------------------------
# CSV parser
# ---------------------------------------------------------------------------

def load_prospects() -> dict:
    """
    Parse outreach_leads.csv into a dict keyed by normalized company name.
    Each value contains the full prospect record.
    """
    global _prospects
    if _prospects is not None:
        return _prospects

    _prospects = {}
    if not CSV_PATH.exists():
        log.warning(f"Prospect CSV not found at {CSV_PATH}")
        return _prospects

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get("Company", "").strip()
            if not company:
                continue

            prospect = {
                "company": company,
                "type": row.get("Type", "").strip(),
                "phone": row.get("Phone", "").strip(),
                "email": row.get("Email", "").strip(),
                "address": row.get("Address", "").strip(),
                "cities_served": row.get("Cities Served", "").strip(),
                "property_types": row.get("Property Types", "").strip(),
                "website": row.get("Website", "").strip(),
                "notes": row.get("Notes", "").strip(),
                "aliases": _make_aliases(company),
                # These get populated from emails.md
                "decision_maker": "",
                "priority": "",
                "email_hook": "",
                "subject_line": "",
            }

            # Store under normalized name
            _prospects[_normalize(company)] = prospect

    log.info(f"Loaded {len(_prospects)} prospects from CSV")

    # Enrich with email data
    _load_email_hooks()

    return _prospects


# ---------------------------------------------------------------------------
# Emails.md parser
# ---------------------------------------------------------------------------

def _load_email_hooks() -> None:
    """Parse outreach_emails.md to extract decision-maker names, priorities, and hooks."""
    global _email_hooks
    if _email_hooks is not None:
        return

    _email_hooks = {}
    if not EMAILS_PATH.exists():
        log.warning(f"Outreach emails not found at {EMAILS_PATH}")
        return

    content = EMAILS_PATH.read_text(encoding="utf-8")

    # Split into sections by "## N." headers
    sections = re.split(r"^## \d+\.\s+", content, flags=re.MULTILINE)

    for section in sections[1:]:  # Skip preamble
        lines = section.strip().split("\n")
        if not lines:
            continue

        # First line: "Company Name — Decision Maker"
        header = lines[0].strip()
        match = re.match(r"(.+?)\s*[—–-]\s*(.+)", header)
        if not match:
            continue

        company_part = match.group(1).strip()
        decision_maker = match.group(2).strip()

        # Extract priority
        priority = ""
        for line in lines[1:5]:
            pmatch = re.search(r"\*\*Priority:\s*(HIGH|MEDIUM|LOWER)", line, re.IGNORECASE)
            if pmatch:
                priority = pmatch.group(1).upper()
                break

        # Extract subject line
        subject = ""
        for line in lines:
            smatch = re.search(r"\*\*Subject:\*\*\s*(.+)", line)
            if smatch:
                subject = smatch.group(1).strip()
                break

        # Extract the first paragraph after the greeting as the "hook"
        # (the personalized opening that references company-specific details)
        hook = ""
        in_body = False
        for line in lines:
            if line.startswith("Hi ") and not in_body:
                in_body = True
                continue
            if in_body and line.strip():
                # Skip the "I'm Don Burt" intro line
                if "I'm Don Burt" in line or "I'm Sam" in line:
                    continue
                hook = line.strip()
                break

        # Match to CSV prospect
        norm_company = _normalize(company_part)
        # Try exact match first
        if norm_company in _prospects:
            _prospects[norm_company]["decision_maker"] = decision_maker
            _prospects[norm_company]["priority"] = priority
            _prospects[norm_company]["email_hook"] = hook
            _prospects[norm_company]["subject_line"] = subject
        else:
            # Try substring match
            for key, prospect in _prospects.items():
                if norm_company in key or key in norm_company:
                    prospect["decision_maker"] = decision_maker
                    prospect["priority"] = priority
                    prospect["email_hook"] = hook
                    prospect["subject_line"] = subject
                    break
                # Check aliases
                for alias in prospect["aliases"]:
                    if alias in norm_company or norm_company in alias:
                        prospect["decision_maker"] = decision_maker
                        prospect["priority"] = priority
                        prospect["email_hook"] = hook
                        prospect["subject_line"] = subject
                        break


# ---------------------------------------------------------------------------
# Fuzzy matching
# ---------------------------------------------------------------------------

def find_prospect_match(organization: str, city: str = "") -> dict | None:
    """
    Fuzzy match a GHL contact's organization name against the prospect database.
    Returns the enrichment dict or None if no match.
    """
    prospects = load_prospects()
    if not prospects or not organization:
        return None

    norm_org = _normalize(organization)
    if not norm_org:
        return None

    # 1. Exact match on normalized name
    if norm_org in prospects:
        return prospects[norm_org]

    # 2. Check if org matches any prospect's aliases
    for key, prospect in prospects.items():
        for alias in prospect["aliases"]:
            if alias == norm_org:
                return prospect

    # 3. Substring match (either direction)
    for key, prospect in prospects.items():
        if norm_org in key or key in norm_org:
            return prospect
        for alias in prospect["aliases"]:
            if len(alias) >= 3 and (alias in norm_org or norm_org in alias):
                return prospect

    # 4. City-based matching (if org didn't match, check if city matches a prospect's service area)
    # This is a weaker match — only used if city is provided
    if city:
        norm_city = _normalize(city)
        for key, prospect in prospects.items():
            if norm_city and norm_city in _normalize(prospect.get("cities_served", "")):
                # City match alone isn't enough — need some org overlap
                org_words = set(norm_org.split())
                company_words = set(key.split())
                if org_words & company_words:
                    return prospect

    return None


# ---------------------------------------------------------------------------
# Strategic angle builder
# ---------------------------------------------------------------------------

def get_strategic_angle(prospect: dict) -> str:
    """
    Build a strategic context string from prospect data for Claude prompts.
    Combines CSV notes, decision-maker info, email hooks, and property details.
    """
    parts = []

    if prospect.get("decision_maker"):
        parts.append(f"Decision maker: {prospect['decision_maker']}.")

    if prospect.get("priority"):
        parts.append(f"Priority: {prospect['priority']}.")

    if prospect.get("notes"):
        parts.append(prospect["notes"])

    if prospect.get("property_types"):
        parts.append(f"Manages: {prospect['property_types']}.")

    if prospect.get("cities_served"):
        parts.append(f"Serves: {prospect['cities_served']}.")

    if prospect.get("email_hook"):
        parts.append(f"Personalization hook: {prospect['email_hook']}")

    return " ".join(parts)


def get_all_prospects() -> list[dict]:
    """Return all prospects as a list, sorted by priority (HIGH first)."""
    prospects = load_prospects()
    priority_order = {"HIGH": 0, "MEDIUM": 1, "LOWER": 2, "": 3}

    items = list(prospects.values())
    items.sort(key=lambda p: priority_order.get(p.get("priority", ""), 3))
    return items
