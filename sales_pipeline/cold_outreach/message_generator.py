"""
Sales Pipeline — Cold Outreach: Message Generator
Uses Claude API to generate hyperpersonalized re-engagement messages.
Returns (subject, body) tuples.

2026 Best Practices Baked In:
  - 50-125 word email body (first touch ideally under 80 words)
  - PAS framework for known pain points, AIDA for cold discovery
  - Conversational tone (no formal business language)
  - Single CTA per message
  - Timeline hooks preferred over problem statements
  - No signature in output (appended separately by signature.py)
  - A/B variant generation for initial touches
"""

import logging

import anthropic

from sales_pipeline.config import ANTHROPIC_API_KEY, CALENDAR_LINK
from sales_pipeline.enrichment.prospect_db import find_prospect_match, get_strategic_angle
from sales_pipeline.learning.learning_analyzer import get_prompt_guidance
from sales_pipeline.cold_outreach.lead_filter import get_city_page_url

log = logging.getLogger(__name__)


class MessageGenerationError(Exception):
    pass


_BASE_SYSTEM_PROMPT = """You are writing on behalf of Sam Alarcon, Vice President at Americal Patrol, a licensed security patrol company
based in Oxnard, CA. Key facts about Americal Patrol:
- California BSIS-licensed PPO (fully insured)
- Armed and unarmed officer options available
- 24/7 patrol coverage available
- HOA and residential complex specialists
- Serves Ventura County, Orange County, Los Angeles County, and surrounding areas
- Veteran-owned, established 1986

CRITICAL LOCATION RULE:
- ALWAYS reference the correct county/area based on the prospect's city
- Ventura County cities: Oxnard, Ventura, Camarillo, Thousand Oaks, Simi Valley, Moorpark, Santa Paula, Fillmore, Ojai, Port Hueneme, Oak View, Carpinteria
- Orange County cities: Anaheim, Fullerton, Placentia, Brea, Tustin, Yorba Linda, La Mirada, Westminster, Garden Grove
- Los Angeles County cities: LA, Gardena, Vernon, Pasadena, Baldwin Park, Long Beach
- If the city is not listed, say "Southern California" — NEVER default to Ventura County
- Do NOT say "Ventura County" for a prospect in Orange County or LA County

PROPERTY LANGUAGE RULE:
- NEVER say "your other property", "the other property", or "another property" — it sounds vague and confusing.
- If you know the address or property name, reference it specifically (e.g., "your Birch St. property").
- If you know the company name, reference it (e.g., "security for Colour Republic").
- If you only know the city, reference the area (e.g., "your Oxnard property").
- As a last resort, say "your property" or "your site" — never "other".

WRITING RULES:
- Write conversationally but STRUCTURED — this is a professional email, not a text message.
- Keep emails between 60-125 words.
- Use 2-3 SHORT paragraphs separated by blank lines — NEVER one run-on block of text.
- Start with a greeting on its own line (e.g., "Hey Dan,").
- Paragraph 1: Context or hook (1-2 sentences).
- Paragraph 2: Value proposition or insight (1-2 sentences).
- Paragraph 3: Clear CTA / ask (1 sentence with booking link).
- Use the PAS framework (Problem-Agitate-Solution) when you know their pain points.
- ONE clear call-to-action per message — always end with a specific ask.
- Reference specific details about their company when provided.
- Never sound like a mass email or template.
- Do NOT include a full signature block — it is added separately.
- DO end with a short sign-off like "Best," or "Talk soon," followed by "Sam" on the next line.
- Do NOT mention that they previously inquired, filled out a form, or "went cold" — write as if this is a fresh introduction.
- NEVER use markdown formatting. No [brackets](links), no **bold**, no *italics*, no bullet points.
- Write URLs as plain text on their own line — never wrap them in markdown link syntax.
- Example of WRONG: [americalpatrol.com/oxnard](https://americalpatrol.com/oxnard)
- Example of RIGHT: americalpatrol.com/oxnard-security-guards
- NEVER use em dashes (—). Use commas, periods, or rewrite the sentence instead."""


def _build_system_prompt() -> str:
    """Build system prompt with learning insights appended."""
    base = _BASE_SYSTEM_PROMPT
    try:
        guidance = get_prompt_guidance()
        if guidance:
            base += "\n" + guidance
    except Exception:
        pass
    return base

PROPERTY_ANGLES = {
    "commercial":  "After-hours patrol, access control, reducing theft and vandalism liability for commercial properties.",
    "industrial":  "Perimeter monitoring, cargo and equipment theft prevention, and 24/7 coverage for industrial and warehouse facilities.",
    "retail":      "Visible uniformed deterrence, loss prevention partnership, and fast incident response for retail environments.",
    "hoa":         "Resident safety, parking enforcement, gate and common area patrol. HOA and apartment complex specialists.",
    "other":       "A customized security assessment tailored to their specific needs and inquiry details.",
}


def _build_prompt(contact: dict, channel: str, is_followup: bool) -> str:
    name        = contact.get("first_name") or "there"
    org         = contact.get("organization") or "your organization"
    prop_type   = contact.get("property_type", "other")
    address     = contact.get("property_address", "")
    city        = contact.get("property_city", "")
    details     = contact.get("inquiry_details", "")
    calendly    = CALENDAR_LINK()

    location_str = ", ".join(filter(None, [address, city]))
    angle = PROPERTY_ANGLES.get(prop_type, PROPERTY_ANGLES["other"])

    # Enrichment: look up prospect in research database
    enrichment_context = ""
    prospect = find_prospect_match(org, city)
    if prospect:
        enrichment_context = get_strategic_angle(prospect)
        # Use decision maker name if available and we don't have a first name
        if prospect.get("decision_maker") and name == "there":
            dm_first = prospect["decision_maker"].split()[0]
            name = dm_first

    if is_followup:
        if channel == "sms":
            return (
                f"Write a SHORT follow-up text message (≤160 characters) to {name} at {org}. "
                f"A gentle check-in about security services. "
                f"Casual, friendly, 1-2 sentences max. End with this exact booking link: {calendly}"
            )
        else:
            return (
                f"Write a SHORT follow-up email body (≤75 words) to {name} at {org}. "
                f"A brief check-in about security patrol services. "
                f"Warm, no pressure, single CTA. End with this exact booking link: {calendly}\n"
                + (f"\nContext about this company: {enrichment_context}" if enrichment_context else "")
            )

    # Build location context for county-aware messaging
    city_str = city or ""
    location_note = ""
    city_page_note = ""
    if city_str:
        location_note = f"Their property is in {city_str}. Reference the correct county for this city. "
        city_url = get_city_page_url(city_str)
        if city_url:
            city_page_note = (
                f"We have a dedicated page for their city: {city_url}. "
                f"naturally mention or link to it if it fits the message flow. "
            )

    if channel == "sms":
        return (
            f"Write a cold outreach SMS (≤300 characters) to {name} at {org}. "
            f"Introduce Americal Patrol's security services"
            + (f" for their {prop_type} property" if prop_type != "other" else "") +
            (f" in {city_str}" if city_str else "") + ". "
            f"{location_note}"
            f"Focus: {angle} "
            + (f"Property details: {details}. " if details else "") +
            (f"Company context: {enrichment_context} " if enrichment_context else "") +
            f"End with this exact booking link: {calendly}"
        )
    else:
        return (
            f"SUBJECT LINE: Write a 2-4 word lowercase subject line (curiosity-driven, "
            f"no company name). Put it on the first line prefixed with 'SUBJECT: '.\n\n"
            f"BODY: Write a cold outreach email body (50-80 words, NO signature) to {name} at {org}. "
            f"Introduce Americal Patrol's security patrol services"
            + (f" for their {prop_type} property" if prop_type != "other" else "") +
            (f" in {city_str}" if city_str else "") + ". "
            f"{location_note}"
            f"{city_page_note}"
            f"Focus: {angle} "
            + (f"Property details: {details}. " if details else "") +
            (f"\nCompany research context (use this to deeply personalize): {enrichment_context} " if enrichment_context else "") +
            f"\nEnd with this exact booking link: {calendly}. Invite them to schedule a quick call. "
            f"Conversational tone, single CTA, no sign-off or signature."
        )


def _build_subject(contact: dict, is_followup: bool) -> str:
    """Generate a personalized subject line following best practices."""
    name = contact.get("first_name") or ""
    org = contact.get("organization") or "your property"
    prop_type = contact.get("property_type", "other")

    # Check enrichment for decision maker name
    if not name:
        prospect = find_prospect_match(org, contact.get("property_city", ""))
        if prospect and prospect.get("decision_maker"):
            name = prospect["decision_maker"].split()[0]

    type_labels = {
        "commercial":  "commercial security",
        "industrial":  "industrial security",
        "retail":      "retail security",
        "hoa":         "HOA patrol",
        "other":       "security services",
    }
    label = type_labels.get(prop_type, "security services")

    if is_followup:
        if name:
            return f"Hi {name}, following up on {label}"
        return f"Following up on {label} for {org}"

    # Initial subject: personalized with name or company
    if name:
        return f"Hi {name}, {label} for {org}"
    return f"{label.title()} for {org} - quick question"


def _parse_claude_response(text: str, channel: str) -> tuple:
    """Parse Claude's response to extract subject and body when subject is inline."""
    if channel == "sms":
        return "", text.strip()

    # Check if Claude included a SUBJECT: line
    lines = text.strip().split("\n")
    subject = ""
    body_lines = []
    found_subject = False

    for line in lines:
        stripped = line.strip()
        if stripped.upper().startswith("SUBJECT:") and not found_subject:
            subject = stripped.split(":", 1)[1].strip().strip('"').strip("'")
            found_subject = True
        elif stripped.upper().startswith("BODY:") and found_subject:
            # Skip the "BODY:" label
            rest = stripped.split(":", 1)[1].strip()
            if rest:
                body_lines.append(rest)
        else:
            body_lines.append(line)

    body = "\n".join(body_lines).strip()

    # Fallback if Claude didn't include SUBJECT: line
    if not subject:
        subject = _build_subject_fallback(body)

    return subject, body


def _build_subject_fallback(body: str) -> str:
    """Fallback subject line when Claude doesn't generate one."""
    return "quick question"


def generate_message(contact: dict, channel: str, is_followup: bool = False) -> tuple:
    """
    Generate a personalized message via Claude.
    Returns (subject, body). Subject is empty string for SMS.
    Raises MessageGenerationError on API failure.

    For email: Claude generates both subject and body inline.
    For SMS: subject is always empty.
    For follow-ups: uses template subjects (shorter, less important).

    Note: Body does NOT include signature or unsubscribe footer —
    those are appended by draft_builder.py using templates/signature.py
    and templates/unsubscribe.py.
    """
    prompt = _build_prompt(contact, channel, is_followup)

    # Follow-up emails still use template subjects (simpler)
    use_template_subject = is_followup and channel != "sms"

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY())
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=_build_system_prompt(),
            messages=[{"role": "user", "content": prompt}],
        )
        raw = response.content[0].text.strip()

        if use_template_subject:
            subject = _build_subject(contact, is_followup)
            body = raw
        else:
            subject, body = _parse_claude_response(raw, channel)

        log.info(
            f"Generated {'follow-up' if is_followup else 'initial'} {channel} message "
            f"for {contact.get('id')} ({len(body)} chars)"
        )
        return subject, body
    except anthropic.APIError as e:
        raise MessageGenerationError(f"Claude API error: {e}") from e
    except Exception as e:
        raise MessageGenerationError(f"Message generation failed: {e}") from e


def generate_ab_variants(contact: dict, channel: str) -> list[tuple]:
    """
    Generate two message variants (A/B) for initial cold outreach.
    Returns list of 2 (subject, body) tuples.
    Reviewer picks the better one in the draft JSON.
    """
    variants = []
    for i in range(2):
        try:
            variants.append(generate_message(contact, channel, is_followup=False))
        except MessageGenerationError:
            if variants:
                return variants  # Return what we got
            raise
    return variants
