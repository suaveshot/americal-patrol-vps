# patrol_automation/report_polisher.py
"""
Report Polisher — Claude-powered text cleanup for Connecteam reports.

Fixes grammar, spelling, ALL CAPS, translates Spanish → English,
generates executive summaries, extracts report numbers, classifies severity.
"""

import json
import logging
import os
import re
from pathlib import Path

import anthropic

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent

# ── Severity classification ──────────────────────────────────────────────────

SEVERITY_MAP = {
    # Red — serious
    "trespassing": "red",
    "trespass": "red",
    "theft": "red",
    "stolen": "red",
    "burglary": "red",
    "break-in": "red",
    "break in": "red",
    "vandalism": "red",
    "assault": "red",
    "weapon": "red",
    "fight": "red",
    "fire": "red",
    "arson": "red",
    # Orange — elevated
    "suspicious": "orange",
    "suspicious activity": "orange",
    "suspicious person": "orange",
    "disturbance": "orange",
    "altercation": "orange",
    "drug": "orange",
    "drugs": "orange",
    "intoxicated": "orange",
    "alarm": "orange",
    # Blue — low
    "noise": "blue",
    "noise complaint": "blue",
    "parking": "blue",
    "parking violation": "blue",
    "overnight parking": "blue",
    "traffic": "blue",
    "vehicle": "blue",
    "key issue": "blue",
    "key": "blue",
    "lock": "blue",
    "lockout": "blue",
    # Green — routine
    "maintenance": "green",
    "maintenance issue": "green",
    "light out": "green",
    "lights": "green",
    "lighting issue": "green",
    "water leak": "green",
    "water leak / flooding": "green",
    "cleanup": "green",
    "routine": "green",
    "routine observation": "green",
    "observation": "green",
    "property check": "green",
    "gate": "green",
    "gate issue": "green",
    "escort / access": "green",
    "animal / pest": "green",
    "welfare check": "blue",
    "soliciting": "orange",
    "loitering": "orange",
    "homeless / encampment": "orange",
    "police response": "red",
    "fire department response": "red",
    "medical emergency": "red",
    "attempted theft": "orange",
    "attempted break-in": "orange",
    "unauthorized parking": "blue",
    "vehicle accident": "orange",
    "traffic incident": "blue",
    "fire / smoke": "red",
    "alarm activation": "orange",
    "intoxicated person": "orange",
    "drug activity": "red",
}

SEVERITY_COLORS = {
    "red":    {"bg": "#fef2f2", "border": "#dc2626", "text": "#991b1b", "label": "High"},
    "orange": {"bg": "#fff7ed", "border": "#ea580c", "text": "#9a3412", "label": "Elevated"},
    "blue":   {"bg": "#eff6ff", "border": "#2563eb", "text": "#1e40af", "label": "Low"},
    "green":  {"bg": "#f0fdf4", "border": "#16a34a", "text": "#166534", "label": "Routine"},
}


def classify_severity(incident_type: str) -> str:
    """Classify incident type into a severity level. Returns color key."""
    if not incident_type:
        return "blue"
    lower = incident_type.lower().strip()
    # Try exact match first, then substring
    if lower in SEVERITY_MAP:
        return SEVERITY_MAP[lower]
    for keyword, severity in SEVERITY_MAP.items():
        if keyword in lower:
            return severity
    return "blue"  # default


def extract_report_number(raw_text: str) -> str | None:
    """Extract Connecteam report number (#XXXX) from PDF text."""
    if not raw_text:
        return None
    match = re.search(r'#(\d{2,6})\b', raw_text)
    return f"#{match.group(1)}" if match else None


def has_freetext(parsed_data: dict) -> bool:
    """Check if a DAR has any freetext notes worth polishing (beyond standard checklist values)."""
    if not parsed_data:
        return False
    standard_values = {"completed", "yes", "no", "n/a", "unknown", ""}
    for rnd in parsed_data.get("rounds", []):
        for note in rnd.get("incident_notes", []):
            if note and note.strip().lower() not in standard_values:
                return True
        for key, val in rnd.get("checks", {}).items():
            if val and val.strip().lower() not in standard_values:
                return True
    return False


# ── Standardized Incident Type Word Bank ──────────────────────────────────────
# Claude classifies each incident into one of these categories based on the
# narrative, not the guard's dropdown selection. This ensures consistent
# categorization across all reports.

INCIDENT_TYPE_WORDBANK = [
    "Trespassing",
    "Suspicious Person",
    "Suspicious Vehicle",
    "Suspicious Activity",
    "Theft",
    "Attempted Theft",
    "Vandalism",
    "Property Damage",
    "Break-In",
    "Attempted Break-In",
    "Burglary",
    "Assault",
    "Disturbance",
    "Noise Complaint",
    "Parking Violation",
    "Overnight Parking",
    "Unauthorized Parking",
    "Vehicle Accident",
    "Traffic Incident",
    "Fire / Smoke",
    "Water Leak / Flooding",
    "Alarm Activation",
    "Medical Emergency",
    "Drug Activity",
    "Intoxicated Person",
    "Homeless / Encampment",
    "Soliciting",
    "Loitering",
    "Key / Lock Issue",
    "Gate Issue",
    "Lighting Issue",
    "Maintenance Issue",
    "Animal / Pest",
    "Welfare Check",
    "Police Response",
    "Fire Department Response",
    "Escort / Access",
    "Routine Observation",
    "Other",
]


# ── Claude polishing ─────────────────────────────────────────────────────────

INCIDENT_PROMPT = """You are editing a security patrol incident report for Americal Patrol, Inc., a professional security company.

The text below is from an officer's incident report. Clean it up following these rules:

1. Fix all spelling and grammar errors
2. If the text is written in ALL CAPS, convert to proper sentence case. Keep these uppercase: acronyms (BSIS, LAPD, HOA, PCYC, PD), abbreviations, license plate numbers
3. Translate any Spanish text to English (preserve meaning exactly)
4. Use professional, past-tense language appropriate for a security report
5. Do NOT change names, times, dates, addresses, license plates, or factual events
6. Do NOT add information that was not in the original
7. Do NOT remove any information
8. Keep section structure (Type of Incident, Address, Report)

Also:
- Write a 2-3 sentence executive summary suitable for the top of the report.
- Classify the incident into the BEST matching category from this list (pick exactly one):
  """ + ", ".join(INCIDENT_TYPE_WORDBANK) + """
- Return a "polished_incidents" array with one entry per incident, each containing the polished version of that incident's type, address, and report narrative.

Return ONLY valid JSON (no markdown, no explanation):
{
  "polished_text": "the full corrected report text",
  "executive_summary": "2-3 sentence professional summary",
  "classified_type": "the best matching incident category from the list above",
  "polished_incidents": [
    {"type": "polished incident type", "address": "address if provided", "report": "polished narrative text"}
  ],
  "changes_made": ["brief description of each fix"]
}

ORIGINAL REPORT TEXT:
"""

DAR_PROMPT = """You are editing a security patrol Daily Activity Report for Americal Patrol, Inc.

These are mostly checklist items. Clean up ONLY the freetext notes and comments:

1. Fix spelling and grammar in any freetext notes or comments
2. If text is in ALL CAPS, convert to proper sentence case (keep acronyms uppercase)
3. Translate any Spanish text to English
4. Do NOT modify checklist status values (Completed, Yes, No, etc.)
5. Do NOT change names, times, or factual details
6. Keep the exact same structure

Also write a 2-3 sentence executive summary of the patrol activity (how many rounds, any incidents, overall status).

Return ONLY valid JSON (no markdown, no explanation):
{
  "polished_text": "the full corrected text",
  "executive_summary": "2-3 sentence patrol summary",
  "changes_made": ["brief description of each fix"]
}

ORIGINAL REPORT TEXT:
"""


def polish_report_text(raw_text: str, report_type: str = "incident",
                       parsed_data: dict = None) -> dict | None:
    """
    Send report text to Claude for grammar, spelling, caps, and translation fixes.

    Args:
        raw_text: Raw text extracted from the Connecteam PDF
        report_type: "incident" or "dar"
        parsed_data: Optional parsed report data (used to check if DAR has freetext)

    Returns:
        {
            "polished_text": str,
            "executive_summary": str,
            "changes_made": [str]
        }
        or None on failure (caller should use original text)
    """
    if not raw_text or not raw_text.strip():
        return None

    # Skip Claude call for DARs with no freetext (just standard checklist values)
    if report_type == "dar" and parsed_data and not has_freetext(parsed_data):
        # Still generate an executive summary from structured data
        return _build_dar_summary_from_data(parsed_data)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("ANTHROPIC_API_KEY not set — skipping polish")
        return None

    prompt = INCIDENT_PROMPT if report_type == "incident" else DAR_PROMPT
    full_prompt = prompt + raw_text[:8000]  # Cap input to avoid huge token costs

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4000,
            messages=[{"role": "user", "content": full_prompt}],
        )
        result_text = response.content[0].text.strip()

        # Parse JSON response — handle markdown code blocks if Claude wraps it
        if result_text.startswith("```"):
            result_text = re.sub(r'^```(?:json)?\s*', '', result_text)
            result_text = re.sub(r'\s*```$', '', result_text)

        result = json.loads(result_text)

        if "polished_text" not in result:
            log.warning("Claude response missing 'polished_text' key")
            return None

        result.setdefault("executive_summary", "")
        result.setdefault("changes_made", [])
        return result

    except json.JSONDecodeError as e:
        log.warning(f"Could not parse Claude polish response as JSON: {e}")
        return None
    except anthropic.APIError as e:
        log.warning(f"Claude API error during polish: {e}")
        return None
    except Exception as e:
        log.warning(f"Unexpected error during polish: {e}")
        return None


def _build_dar_summary_from_data(parsed_data: dict) -> dict:
    """Build a basic executive summary for a DAR that has no freetext to polish."""
    prop = parsed_data.get("property", "the property")
    # Clean date prefixes and underscores from property name
    prop = re.sub(r'^\d{8}[\s_]*', '', prop).replace('_', ' ').strip() or prop
    date = parsed_data.get("date", "")
    total = parsed_data.get("total_rounds", 0)
    officers = parsed_data.get("officers", [])
    first = parsed_data.get("first_time", "")
    last = parsed_data.get("last_time", "")
    has_inc = parsed_data.get("has_incidents", False)

    officer_str = ", ".join(officers) if officers else "patrol officers"
    if first and last:
        time_str = f" at {first}" if first == last else f" between {first} and {last}"
    else:
        time_str = ""

    if has_inc:
        status = "Incidents were reported during this period."
    else:
        status = "All security checks were completed satisfactorily. No incidents were reported."

    summary = (
        f"On {date}, Americal Patrol conducted {total} patrol round{'s' if total != 1 else ''} "
        f"of {prop}{time_str}. {status}"
    )

    return {
        "polished_text": None,  # No text to polish — use original
        "executive_summary": summary,
        "changes_made": [],
    }


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(SCRIPT_DIR.parent / ".env")

    # Test with sample text
    test_text = """Stadium Plaza Incident Report
Nicholas Noell
NN
04/01/2026, 01:09 AM | America/Los_Angeles
#3081
Type of Incident: Overnight Parking
Address: 1520 South Harris Court
Suite:
Report: OFFICER SPOTTED VEHICLE PARKED OVERNIGHT OUTSIDE 1520 S HARRIS CT. OFFICER LEFT WARNING TICKET ON FRONT WINDOW ADVISING TO NOT PARKED OVERNIGHT ON PRIVATE PROPERTY."""

    print("=== Testing Report Polisher ===")
    print(f"Report number: {extract_report_number(test_text)}")
    print(f"Severity: {classify_severity('Overnight Parking')}")
    print()

    result = polish_report_text(test_text, "incident")
    if result:
        print("Executive Summary:")
        print(f"  {result['executive_summary']}")
        print()
        print("Polished Text:")
        print(f"  {result['polished_text'][:500]}")
        print()
        print(f"Changes: {result['changes_made']}")
    else:
        print("Polish failed (check ANTHROPIC_API_KEY)")
