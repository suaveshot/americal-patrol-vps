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

All company-specific values loaded from tenant_context.
"""

import logging

import anthropic

from shared_utils.usage_tracker import tracked_create
from sales_pipeline.config import ANTHROPIC_API_KEY, CALENDAR_LINK
from sales_pipeline.enrichment.prospect_db import find_prospect_match, get_strategic_angle
from sales_pipeline.learning.learning_analyzer import get_prompt_guidance
from sales_pipeline.cold_outreach.lead_filter import get_city_page_url
import tenant_context as tc

log = logging.getLogger(__name__)


class MessageGenerationError(Exception):
    pass


def _build_base_system_prompt() -> str:
    """Build the system prompt dynamically from tenant_config.json."""
    name = tc.owner_name()
    title = tc.owner_title()
    company = tc.company_name()
    description = tc.company_description()

    # Build selling points as bullet list
    points = tc.selling_points()
    points_str = "\n".join(f"- {p}" for p in points)

    # Location rules (tenant-specific)
    loc_rules = tc.location_rules()
    loc_section = f"\n{loc_rules}\n" if loc_rules else ""

    return f"""You are writing on behalf of {name}, {title} at {company}, {description}
Key facts about {company}:
{points_str}
{loc_section}
WRITING RULES:
- Write conversationally, like texting a business acquaintance — NOT like a formal business letter
- Keep emails between 50-125 words (the sweet spot for cold email reply rates)
- Use the PAS framework (Problem-Agitate-Solution) when you know their pain points
- ONE clear call-to-action per message — always end with a specific ask
- Prefer timeline hooks ("I noticed X recently") over generic problem statements
- Reference specific details about their company when provided
- Never sound like a mass email or template
- Do NOT include a signature block — it is added separately
- Do NOT include "Best regards", "Sincerely", or any sign-off — just end with the CTA
- Do NOT mention that they previously inquired, filled out a form, or "went cold" — write as if this is a fresh introduction"""


def _build_system_prompt() -> str:
    """Build system prompt with learning insights appended."""
    base = _build_base_system_prompt()
    try:
        guidance = get_prompt_guidance()
        if guidance:
            base += "\n" + guidance
    except Exception:
        pass
    return base


def _get_property_angles() -> dict:
    """Get property/client type talking points from tenant config, with fallback."""
    angles = tc.property_angles()
    if angles:
        return angles
    # Generic fallback if not configured
    industry = tc.company_industry()
    return {
        "commercial": f"Professional {industry} services for commercial properties.",
        "residential": f"Reliable {industry} services for homeowners and renters.",
        "hoa": f"Dedicated {industry} services for HOA and property management companies.",
        "other": f"A customized {industry} plan tailored to their specific needs.",
    }


def _build_prompt(contact: dict, channel: str, is_followup: bool) -> str:
    name        = contact.get("first_name") or "there"
    org         = contact.get("organization") or "your organization"
    prop_type   = contact.get("property_type", "other")
    address     = contact.get("property_address", "")
    city        = contact.get("property_city", "")
    details     = contact.get("inquiry_details", "")
    calendly    = CALENDAR_LINK()

    company = tc.company_name()
    industry = tc.company_industry()
    angles = _get_property_angles()

    location_str = ", ".join(filter(None, [address, city]))
    angle = angles.get(prop_type, angles.get("other", f"Professional {industry} services."))

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
                f"A gentle check-in about {industry} services. "
                f"Casual, friendly, 1-2 sentences max. End with this exact booking link: {calendly}"
            )
        else:
            return (
                f"Write a SHORT follow-up email body (≤75 words) to {name} at {org}. "
                f"A brief check-in about {industry} services. "
                f"Warm, no pressure, single CTA. End with this exact booking link: {calendly}\n"
                + (f"\nContext about this company: {enrichment_context}" if enrichment_context else "")
            )

    # Build location context for county-aware messaging
    city_str = city or ""
    location_note = ""
    city_page_note = ""
    if city_str:
        location_note = f"Their property is in {city_str}. Reference the correct area for this city. "
        city_url = get_city_page_url(city_str)
        if city_url:
            city_page_note = (
                f"We have a dedicated page for their city: {city_url} — "
                f"naturally mention or link to it if it fits the message flow. "
            )

    if channel == "sms":
        return (
            f"Write a cold outreach SMS (≤300 characters) to {name} at {org}. "
            f"Introduce {company}'s {industry} services"
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
            f"Introduce {company}'s {industry} services for their "
            f"{prop_type} property"
            + (f" in {city_str}" if city_str else "") + ". "
            f"{location_note}"
            f"{city_page_note}"
            f"Focus: {angle} "
            + (f"Property details: {details}. " if details else "") +
            (f"\nCompany research context (use this to deeply personalize): {enrichment_context} " if enrichment_context else "") +
            f"\nEnd with this exact booking link: {calendly} — invite them to schedule a quick call. "
            f"Conversational tone, single CTA, no sign-off or signature."
        )


def _build_subject(contact: dict, is_followup: bool) -> str:
    """Generate a personalized subject line following best practices."""
    name = contact.get("first_name") or ""
    org = contact.get("organization") or "your property"
    prop_type = contact.get("property_type", "other")
    industry = tc.company_industry()

    # Check enrichment for decision maker name
    if not name:
        prospect = find_prospect_match(org, contact.get("property_city", ""))
        if prospect and prospect.get("decision_maker"):
            name = prospect["decision_maker"].split()[0]

    type_labels = {k: f"{k} {industry}" for k in _get_property_angles()}
    type_labels.setdefault("other", f"{industry} services")
    label = type_labels.get(prop_type, f"{industry} services")

    if is_followup:
        if name:
            return f"Hi {name} — following up on {label}"
        return f"Following up — {label} for {org}"

    # Initial subject: personalized with name or company
    if name:
        return f"Hi {name} — {label} for {org}"
    return f"{label.title()} for {org} — quick question"


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
        response = tracked_create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=_build_system_prompt(),
            messages=[{"role": "user", "content": prompt}],
            pipeline="sales",
            client_id=tc.client_id(),
            api_key=ANTHROPIC_API_KEY(),
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
