"""
Sales Pipeline — Unified Adaptive Follow-Up Engine

Handles follow-ups for both phases:
  - Cold outreach: generates drafts (human review required)
  - Post-proposal: auto-sends (proposal already reviewed)

Post-proposal uses Path A/B detection:
  Path A (email-engaged): prospect replied within 48h of proposal
  Path B (email-silent): no reply — lean on SMS first

Cold outreach uses a simplified matrix:
  Same-channel, same-channel, alternate, alternate
"""

import logging
from datetime import datetime, timedelta, timezone

from shared_utils.usage_tracker import tracked_create

from sales_pipeline.config import (
    ANTHROPIC_API_KEY,
    CALENDAR_LINK,
    NEGOTIATING_STAGE,
    WIN_LOSS_LOG_FILE,
)
from sales_pipeline.state import (
    _parse_iso,
    load_state,
    save_state,
    record_touch,
    mark_replied,
    mark_completed,
    mark_proposal_viewed,
    get_due_contacts,
    get_nurture_due_contacts,
    record_nurture_touch,
    get_contact as get_state_contact,
    set_path,
    MAX_TOUCHES,
)
from sales_pipeline.templates.signature import wrap_email_body
from sales_pipeline.templates.unsubscribe import (
    wrap_email_with_unsubscribe, build_sms_opt_out_text,
)
from sales_pipeline.enrichment.prospect_db import find_prospect_match, get_strategic_angle
from sales_pipeline.learning.learning_analyzer import get_prompt_guidance
import tenant_context as tc

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SMS_MAX_LENGTH = 320

# Touch types
TOUCH_TYPES = {
    "check_in": "check_in",
    "value_add": "value_add",
    "calendar": "calendar",
    "final": "final",
}

# Post-proposal touch matrix (Path A/B)
POST_PROPOSAL_MATRIX = {
    "A": {
        1: {"channel": "email", "type": "check_in"},
        2: {"channel": "email", "type": "value_add"},
        3: {"channel": "email", "type": "calendar"},
        4: {"channel": "sms",   "type": "final"},
    },
    "B": {
        1: {"channel": "sms",   "type": "check_in"},
        2: {"channel": "sms",   "type": "calendar"},
        3: {"channel": "email", "type": "value_add"},
        4: {"channel": "email", "type": "final"},
    },
}

# Cold outreach touch matrix (no Path A/B — uses initial channel)
def cold_touch_matrix(initial_channel: str) -> dict:
    """Generate touch matrix for cold outreach based on initial channel."""
    alt = "sms" if initial_channel == "email" else "email"
    return {
        1: {"channel": initial_channel, "type": "check_in"},
        2: {"channel": initial_channel, "type": "value_add"},
        3: {"channel": alt,             "type": "calendar"},
        4: {"channel": alt,             "type": "final"},
    }

# Subject templates per touch type (email only) — uses tenant industry
def _subject_templates() -> dict:
    ind = tc.company_industry()
    return {
        "check_in": f"Following up \u2014 {ind} for {{company}}",
        "value_add": f"Quick thought on {{property_type}} {ind} \u2014 {{company}}",
        "calendar": f"10 minutes to discuss {ind} for {{company}}?",
        "final": "Keeping the door open \u2014 {company}",
    }

SUBJECT_TEMPLATES = None  # Lazy-loaded; use _get_subject_templates()

def _get_subject_templates() -> dict:
    global SUBJECT_TEMPLATES
    if SUBJECT_TEMPLATES is None:
        SUBJECT_TEMPLATES = _subject_templates()
    return SUBJECT_TEMPLATES

# Claude prompt templates — uses tenant industry
def _prompt_templates() -> dict:
    ind = tc.company_industry()
    return {
        "check_in": (
            "Write a brief, warm follow-up (under 60 words) to {first_name} at {company}. "
            f"This is a check-in about {ind} services for their {{property_type}} property. "
            "Conversational tone. Single CTA. No subject line, no signature, no sign-off."
            "{enrichment}"
        ),
        "value_add": (
            "Write a follow-up (under 75 words) to {first_name} at {company}. "
            f"Include a brief, relevant insight about {ind} for {{property_type}} properties. "
            "Be helpful, not pushy. Conversational. No subject line, no signature, no sign-off."
            "{enrichment}"
        ),
        "calendar": (
            "Write a follow-up (under 60 words) to {first_name} at {company}. "
            f"Offer a brief 10-minute call about {ind} for their {{property_type}} property. "
            "Include this booking link: {calendar_link}. "
            "Conversational. No subject line, no signature, no sign-off."
            "{enrichment}"
        ),
        "final": (
            "Write a final follow-up (under 50 words) to {first_name} at {company}. "
            f"Let them know the door remains open for {{property_type}} {ind} services. "
            "Gracious, not pushy. No subject line, no signature, no sign-off."
        ),
    }

PROMPT_TEMPLATES = None  # Lazy-loaded

def _get_prompt_templates() -> dict:
    global PROMPT_TEMPLATES
    if PROMPT_TEMPLATES is None:
        PROMPT_TEMPLATES = _prompt_templates()
    return PROMPT_TEMPLATES

# Post-proposal variants — reference the proposal already sent
def _post_proposal_prompt_templates() -> dict:
    ind = tc.company_industry()
    return {
        "check_in": (
            "Write a brief, warm follow-up (under 60 words) to {first_name} at {company}. "
            f"We already sent them a {ind} services proposal for their {{property_type}} property. "
            "Check in on whether they've had a chance to review it and if they have any questions. "
            "Conversational tone. Single CTA. No subject line, no signature, no sign-off."
            "{enrichment}"
        ),
        "value_add": (
            "Write a follow-up (under 75 words) to {first_name} at {company}. "
            f"We already sent them a {ind} services proposal for their {{property_type}} property. "
            f"Include a brief, relevant insight about {ind} for {{property_type}} properties "
            "that reinforces why our proposal is worth considering. "
            "Be helpful, not pushy. Conversational. No subject line, no signature, no sign-off."
            "{enrichment}"
        ),
        "calendar": (
            "Write a follow-up (under 60 words) to {first_name} at {company}. "
            f"We already sent them a {ind} services proposal for their {{property_type}} property. "
            "Offer a brief 10-minute call to walk through the proposal or answer questions. "
            "Include this booking link: {calendar_link}. "
            "Conversational. No subject line, no signature, no sign-off."
            "{enrichment}"
        ),
        "final": (
            "Write a final follow-up (under 50 words) to {first_name} at {company}. "
            f"We sent them a {ind} services proposal for their {{property_type}} property but haven't heard back. "
            "Let them know the door remains open. "
            "Gracious, not pushy. No subject line, no signature, no sign-off."
        ),
    }

POST_PROPOSAL_PROMPT_TEMPLATES = None  # Lazy-loaded

def _get_post_proposal_prompt_templates() -> dict:
    global POST_PROPOSAL_PROMPT_TEMPLATES
    if POST_PROPOSAL_PROMPT_TEMPLATES is None:
        POST_PROPOSAL_PROMPT_TEMPLATES = _post_proposal_prompt_templates()
    return POST_PROPOSAL_PROMPT_TEMPLATES

# Monthly nurture content rotation (5 types, cycles)
def _nurture_rotation() -> list:
    ind = tc.company_industry()
    return [
        {
            "type": "industry_insight",
            "prompt": (
                "Write a brief email (50-75 words) to {first_name} at {company} sharing "
                f"a relevant {ind} insight for {{property_type}} properties. "
                "Reference a real trend or consideration for the current season ({month}). "
                "Helpful, not salesy. Single soft CTA. No subject line, no signature."
                "{win_context}{proposal_context}"
            ),
            "subject_hint": f"{ind} tip",
        },
        {
            "type": "provider_check",
            "prompt": (
                "Write a brief email (50-75 words) to {first_name} at {company}. "
                "{proposal_context}"
                f"Ask how their current {ind} situation is going for their {{property_type}} property. "
                "Be genuinely curious, not competitive. If they're happy with their provider, great. "
                "If not, offer to have a quick call to discuss what's not working and how we might help. "
                "Conversational, zero pressure. Include this booking link: {calendar_link}. "
                "No subject line, no signature."
            ),
            "subject_hint": "checking in",
        },
        {
            "type": "case_study",
            "prompt": (
                "Write a brief email (50-75 words) to {first_name} at {company}. "
                f"Share a quick success story about how professional {ind} services "
                "helped a {property_type} property. Keep it conversational and credible. "
                "Single soft CTA. No subject line, no signature."
                "{win_context}{proposal_context}"
            ),
            "subject_hint": "quick story",
        },
        {
            "type": "seasonal_tip",
            "prompt": (
                "Write a brief email (50-75 words) to {first_name} at {company}. "
                f"Share a seasonal {ind} tip relevant to {{property_type}} properties "
                "for {month}. Be specific and practical. Single soft CTA. "
                "No subject line, no signature."
                "{proposal_context}"
            ),
            "subject_hint": "seasonal tip",
        },
        {
            "type": "re_engagement",
            "prompt": (
                "Write a brief re-engagement email (50-75 words) to {first_name} at {company}. "
                f"Offer a free, no-obligation {ind} assessment for their {{property_type}} property. "
                "Low pressure, keep the door open. Include this booking link: {calendar_link}. "
                "No subject line, no signature."
                "{proposal_context}"
            ),
            "subject_hint": "free assessment",
        },
    ]

NURTURE_ROTATION = None  # Lazy-loaded

def _get_nurture_rotation() -> list:
    global NURTURE_ROTATION
    if NURTURE_ROTATION is None:
        NURTURE_ROTATION = _nurture_rotation()
    return NURTURE_ROTATION


# ---------------------------------------------------------------------------
# Channel detection (post-proposal only)
# ---------------------------------------------------------------------------

def detect_channel_path(ghl_client, contact_id: str, proposal_sent_at: str) -> str:
    """
    Determine Path A or B by checking if prospect replied within 48h
    of the proposal being sent.
    Returns "A" if inbound reply found, "B" otherwise.
    """
    conversations = ghl_client.search_conversations(contact_id)
    if not conversations:
        return "B"

    proposal_dt = _parse_iso(proposal_sent_at)
    cutoff = proposal_dt + timedelta(hours=48)

    for conv in conversations:
        conv_id = conv.get("id")
        if not conv_id:
            continue
        messages = ghl_client.get_conversation_messages(conv_id)
        for msg in messages:
            if msg.get("direction") != "inbound":
                continue
            msg_dt = _parse_iso(msg.get("dateAdded", ""))
            if proposal_dt < msg_dt <= cutoff:
                return "A"

    return "B"


# ---------------------------------------------------------------------------
# Claude content generation
# ---------------------------------------------------------------------------

def _build_follow_up_system_prompt_base() -> str:
    """Build the follow-up system prompt from tenant config."""
    name = tc.owner_name()
    title = tc.owner_title()
    company = tc.company_name()
    description = tc.company_description()
    loc_rules = tc.location_rules()
    loc_section = f"\n{loc_rules}\n" if loc_rules else ""

    return f"""You are writing follow-up messages on behalf of {name}, {title} at {company},
{description}
{loc_section}
Write conversationally — like texting a business acquaintance. Keep messages brief and personal.
ONE clear CTA per message. Never sound like a template or mass email.
Do NOT include a signature, sign-off, or "Best regards"."""

FOLLOW_UP_SYSTEM_PROMPT = None  # Lazy-loaded


def _build_system_prompt() -> str:
    """Build system prompt with learning insights appended."""
    global FOLLOW_UP_SYSTEM_PROMPT
    if FOLLOW_UP_SYSTEM_PROMPT is None:
        FOLLOW_UP_SYSTEM_PROMPT = _build_follow_up_system_prompt_base()
    base = FOLLOW_UP_SYSTEM_PROMPT
    try:
        guidance = get_prompt_guidance()
        if guidance:
            base += "\n" + guidance
    except Exception:
        pass
    return base


def _call_claude(prompt: str) -> str:
    response = tracked_create(
        model="claude-sonnet-4-6",
        max_tokens=300,
        system=_build_system_prompt(),
        messages=[{"role": "user", "content": prompt}],
        pipeline="sales",
        client_id=tc.client_id(),
        api_key=ANTHROPIC_API_KEY(),
    )
    return response.content[0].text.strip()


def _get_conversation_since(ghl_client, contact_id: str, since_iso: str = None) -> list:
    """
    Fetch conversation messages, optionally filtered to only those after since_iso.
    Returns list of message dicts sorted chronologically.
    """
    try:
        messages = ghl_client.get_full_conversation_history(contact_id)
    except Exception:
        return []

    if not messages:
        return []

    if since_iso:
        since_dt = _parse_iso(since_iso)
        messages = [m for m in messages if _parse_iso(m.get("timestamp", "")) > since_dt]

    return messages


def _build_conversation_context(
    ghl_client, contact_id: str, since_iso: str = None, max_messages: int = 10,
) -> str:
    """
    Fetch conversation history since the last touch and format as prompt context.
    Only pulls messages from the window since since_iso (last touch or proposal sent).
    """
    messages = _get_conversation_since(ghl_client, contact_id, since_iso)

    if not messages:
        return ""

    # Take the most recent messages in the window
    recent = messages[-max_messages:]

    lines = []
    for msg in recent:
        direction = "THEM" if msg.get("direction") == "inbound" else "US"
        body = (msg.get("body") or "").strip()
        if not body:
            continue
        # Truncate long messages to keep context manageable
        if len(body) > 200:
            body = body[:200] + "..."
        msg_type = msg.get("type", "").upper()
        lines.append(f"[{direction} via {msg_type}]: {body}")

    if not lines:
        return ""

    return (
        "\nRecent conversation history (use this to continue the conversation naturally — "
        "acknowledge what they said, don't repeat yourself or ignore their responses):\n"
        + "\n".join(lines)
    )


def _check_active_conversation(ghl_client, contact_id: str, since_iso: str = None,
                                cooldown_days: int = 7) -> dict:
    """
    Check if there's an active conversation that should delay the next automated touch.

    Returns:
        {
            "active": bool — True if we should delay the touch,
            "not_interested": bool — True if lead expressed disinterest,
            "last_message_at": str — ISO timestamp of most recent message,
        }
    """
    messages = _get_conversation_since(ghl_client, contact_id, since_iso)

    if not messages:
        return {"active": False, "not_interested": False, "last_message_at": None}

    # Check for not-interested signals in inbound messages
    not_interested_phrases = [
        "not interested", "no thank", "no thanks", "pass on this",
        "don't need", "don't want", "already have", "went with",
        "going with", "chose another", "found someone", "remove me",
        "stop contacting", "not looking", "no longer need",
        "we're good", "we are good", "all set", "no need",
    ]
    for msg in messages:
        if msg.get("direction") != "inbound":
            continue
        body = str(msg.get("body") or "").lower()
        if any(phrase in body for phrase in not_interested_phrases):
            return {
                "active": False,
                "not_interested": True,
                "last_message_at": msg.get("timestamp"),
            }

    # Check if there's been recent back-and-forth (any message in the window)
    last_msg = messages[-1]
    last_msg_dt = _parse_iso(last_msg.get("timestamp", ""))
    now = datetime.now(timezone.utc)
    days_since_last = (now - last_msg_dt).total_seconds() / 86400

    # If last message was within the cooldown period, conversation is still active
    if days_since_last < cooldown_days:
        return {
            "active": True,
            "not_interested": False,
            "last_message_at": last_msg.get("timestamp"),
        }

    return {"active": False, "not_interested": False, "last_message_at": last_msg.get("timestamp")}


def _build_contact_context(contact: dict, state_entry: dict | None = None) -> str:
    """Build a context block with all available details about the contact."""
    parts = []

    # Location info from GHL contact
    city = contact.get("city") or ""
    state = contact.get("state") or ""
    address = contact.get("address1") or ""
    if city:
        loc = city
        if state:
            loc += f", {state}"
        parts.append(f"Location: {loc}")
    if address:
        parts.append(f"Property address: {address}")

    # Custom fields from GHL (property name, property address, etc.)
    custom = contact.get("customField", {})
    if isinstance(custom, dict):
        for key in ("property_name", "property_address", "property_city", "notes"):
            val = custom.get(key)
            if val:
                parts.append(f"{key.replace('_', ' ').title()}: {val}")

    # Tags can indicate context (e.g., "HOA", "retail", "industrial")
    tags = contact.get("tags") or []
    if tags:
        parts.append(f"Tags: {', '.join(tags)}")

    # Enrichment from prospect database
    company = contact.get("companyName") or contact.get("company_name") or ""
    if not company and state_entry:
        company = state_entry.get("organization", "")
    prospect = find_prospect_match(company) if company else None
    if prospect:
        angle = get_strategic_angle(prospect)
        parts.append(f"Company context: {angle}")

    if not parts:
        return ""
    return "\nContact details — use these to personalize the message:\n" + "\n".join(f"- {p}" for p in parts)


def get_touch_content(
    touch_info: dict,
    contact: dict,
    property_type: str,
    phase: str = "cold_outreach",
    state_entry: dict | None = None,
    ghl_client=None,
    contact_id: str = "",
) -> tuple[str, str]:
    """
    Generate subject and body for a follow-up touch.
    Returns (subject, body) — raw text without signature/unsubscribe.
    """
    channel = touch_info["channel"]
    touch_type = touch_info["type"]

    first_name = contact.get("firstName") or contact.get("first_name") or "there"
    company = contact.get("companyName") or contact.get("company_name") or contact.get("organization") or "your company"

    # Build rich context from all available contact data
    enrichment = _build_contact_context(contact, state_entry)

    # Append conversation history since last touch so Claude can continue naturally
    if ghl_client and contact_id:
        # Determine the window start: last touch, or proposal/first outreach date
        since_iso = None
        if state_entry:
            since_iso = (
                state_entry.get("last_touch_at")
                or state_entry.get("proposal_sent_at")
                or state_entry.get("first_outreach_at")
            )
        conv_context = _build_conversation_context(ghl_client, contact_id, since_iso=since_iso)
        enrichment += conv_context

    # Use post-proposal templates when contact already received a proposal
    templates = _get_post_proposal_prompt_templates() if phase == "post_proposal" else _get_prompt_templates()

    prompt = templates[touch_type].format(
        first_name=first_name,
        company=company,
        property_type=property_type,
        calendar_link=CALENDAR_LINK() if touch_type == "calendar" else "",
        enrichment=enrichment,
    )

    body = _call_claude(prompt)

    if channel == "sms":
        subject = ""
        body = body[:SMS_MAX_LENGTH]
    else:
        subject = _get_subject_templates()[touch_type].format(
            company=company,
            property_type=property_type,
        )

    return subject, body


# ---------------------------------------------------------------------------
# Reply detection helper
# ---------------------------------------------------------------------------

def _check_for_reply(ghl_client, contact_id: str, since_iso: str) -> bool:
    """Check GHL conversations for any inbound message newer than since_iso."""
    result = _classify_reply(ghl_client, contact_id, since_iso)
    return result["replied"]


# Reply quality levels
REPLY_HOT = "hot"       # "Let's schedule a call", "Send me the contract"
REPLY_SOFT = "soft"     # "Thanks, I'll look at it", "Need more time"
REPLY_NONE = "none"     # No inbound messages


def _classify_reply(ghl_client, contact_id: str, since_iso: str) -> dict:
    """
    Check for inbound replies and classify their intent.

    Returns:
        {
            "replied": bool,
            "quality": "hot" | "soft" | "none",
            "message": str — the reply text (for context),
        }
    """
    conversations = ghl_client.search_conversations(contact_id)
    if not conversations:
        return {"replied": False, "quality": REPLY_NONE, "message": ""}

    since_dt = _parse_iso(since_iso)
    inbound_messages = []

    for conv in conversations:
        conv_id = conv.get("id")
        if not conv_id:
            continue
        messages = ghl_client.get_conversation_messages(conv_id)
        for msg in messages:
            if msg.get("direction") != "inbound":
                continue
            msg_dt = _parse_iso(msg.get("dateAdded", ""))
            if msg_dt > since_dt:
                inbound_messages.append({
                    "body": msg.get("body", msg.get("message", "")),
                    "timestamp": msg.get("dateAdded", ""),
                })

    if not inbound_messages:
        return {"replied": False, "quality": REPLY_NONE, "message": ""}

    # Use the most recent inbound message for classification
    latest = max(inbound_messages, key=lambda m: m.get("timestamp", ""))
    body = str(latest.get("body") or "").lower().strip()

    # Hot signals — ready to move forward
    hot_phrases = [
        "schedule", "call me", "let's talk", "let's meet", "set up a time",
        "send me the contract", "send the contract", "move forward",
        "let's do it", "ready to go", "sign me up", "interested",
        "send me a quote", "sounds good", "i'm in", "let's proceed",
        "when can you start", "how soon", "send over",
    ]

    # Soft signals — acknowledged but not ready
    soft_phrases = [
        "thanks", "thank you", "got it", "received", "i'll look",
        "will review", "need more time", "need to think", "let me check",
        "i'll get back", "will get back", "need to discuss", "checking with",
        "talk to my", "run it by", "busy right now", "give me a",
        "couple weeks", "couple of weeks", "few weeks", "end of month",
        "not right now", "maybe later", "circle back",
    ]

    quality = REPLY_SOFT  # Default to soft if replied but can't classify

    if any(phrase in body for phrase in hot_phrases):
        quality = REPLY_HOT
    elif any(phrase in body for phrase in soft_phrases):
        quality = REPLY_SOFT

    return {
        "replied": True,
        "quality": quality,
        "message": latest.get("body", "")[:200],
    }


def _detect_lead_channel(ghl_client, contact_id: str) -> str | None:
    """
    Check the lead's inbound messages and return the channel they're
    actively using ('sms' or 'email'). Returns None if no inbound messages.
    """
    try:
        conversations = ghl_client.search_conversations(contact_id)
    except Exception:
        return None
    if not conversations:
        return None

    # Collect all inbound messages with timestamps
    inbound = []
    for conv in conversations:
        conv_id = conv.get("id")
        if not conv_id:
            continue
        try:
            messages = ghl_client.get_conversation_messages(conv_id)
        except Exception:
            continue
        for msg in messages:
            if msg.get("direction") == "inbound":
                inbound.append({
                    "timestamp": msg.get("dateAdded", ""),
                    "type": str(msg.get("type") or "").lower(),
                })

    if not inbound:
        return None

    # Use the most recent inbound message's channel
    inbound.sort(key=lambda m: m.get("timestamp", ""))
    last_type = inbound[-1].get("type", "")

    if last_type in ("sms", "sms/mms"):
        return "sms"
    elif last_type in ("email",):
        return "email"

    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_follow_ups(ghl_client, state: dict = None) -> dict:
    """
    Daily entry point for the unified follow-up engine.

    1. Load state (or use provided state)
    2. Check all active contacts for replies (auto-stop)
    3. Get due contacts (both phases)
    4. Post-proposal: generate content, send via email/SMS, record touch
    5. Cold outreach: generate drafts (written to drafts file for review)
    6. Save state
    7. Return summary dict for digest
    """
    owns_state = state is None
    if owns_state:
        state = load_state()

    summary = {
        "sent": [],
        "replied": [],
        "completed": [],
        "cold_drafts_generated": [],
        "nurture_sent": [],
        "proposal_views": [],
        "errors": [],
    }

    # --- Phase 1: Reply detection with quality classification ---
    for contact_id, entry in list(state["contacts"].items()):
        if entry.get("replied") or entry.get("completed"):
            continue
        if entry.get("stage") == "unsubscribed":
            continue

        # Check since last outreach
        phase = entry.get("phase", "cold_outreach")
        if phase == "post_proposal":
            since = entry.get("proposal_sent_at")
        else:
            since = entry.get("first_outreach_at")

        if not since:
            continue

        try:
            reply_info = _classify_reply(ghl_client, contact_id, since)

            if reply_info["replied"]:
                quality = reply_info["quality"]

                if quality == REPLY_HOT:
                    # Hot reply — stop sequence, move to Negotiating
                    mark_replied(state, contact_id)
                    summary["replied"].append(contact_id)
                    log.info("HOT reply from %s — stopping sequence, moving to Negotiating", contact_id)

                    if phase == "post_proposal":
                        negotiating_stage = NEGOTIATING_STAGE()
                        if negotiating_stage:
                            try:
                                opp_id = entry.get("opportunity_id")
                                if opp_id:
                                    ghl_client.update_opportunity_stage(opp_id, negotiating_stage)
                            except Exception as e:
                                log.warning("Could not move %s to Negotiating: %s", contact_id, e)

                elif quality == REPLY_SOFT:
                    # Soft reply — don't stop sequence, but slow cadence
                    # Mark as soft_replied so the active conversation check delays the next touch
                    entry["soft_replied"] = True
                    entry["soft_replied_at"] = datetime.now(timezone.utc).isoformat()
                    entry["soft_reply_message"] = reply_info["message"]
                    log.info(
                        "SOFT reply from %s ('%s') — slowing cadence, keeping in sequence",
                        contact_id, reply_info["message"][:80],
                    )
                    summary.setdefault("soft_replies", []).append({
                        "contact_id": contact_id,
                        "message": reply_info["message"][:100],
                    })

        except Exception as e:
            log.warning("Error checking replies for %s: %s", contact_id, e)

    # --- Phase 1.5: Check proposal views and send accelerated follow-up ---
    for contact_id, entry in list(state["contacts"].items()):
        if entry.get("phase") != "post_proposal":
            continue
        if entry.get("completed") or entry.get("replied"):
            continue
        estimate_id = entry.get("estimate_id")
        if not estimate_id or entry.get("proposal_viewed_at"):
            continue
        try:
            view_info = ghl_client.has_viewed_estimate(estimate_id)
            if view_info.get("viewed"):
                mark_proposal_viewed(state, contact_id)
                summary["proposal_views"].append({
                    "contact_id": contact_id,
                    "viewed_at": view_info.get("viewed_at", ""),
                })
                log.info("Proposal viewed by %s — sending accelerated follow-up", contact_id)

                # Send an accelerated check-in if no touch has been sent yet
                # or if the last touch was 2+ days ago (avoid double-messaging)
                last_touch = entry.get("last_touch_at")
                should_send = True
                if last_touch:
                    days_since_touch = (datetime.now(timezone.utc) - _parse_iso(last_touch)).total_seconds() / 86400
                    if days_since_touch < 2:
                        should_send = False
                        log.info("Skipping view follow-up for %s — last touch was %.1f days ago", contact_id, days_since_touch)

                if should_send:
                    try:
                        contact = ghl_client.get_contact(contact_id)
                        property_type = (
                            contact.get("customField", {}).get("property_type")
                            or entry.get("property_type", "commercial")
                        )

                        # Detect which channel the lead prefers
                        lead_channel = _detect_lead_channel(ghl_client, contact_id)
                        channel = lead_channel or entry.get("channel", "email")

                        # Use check_in touch type with post-proposal context
                        touch_info = {"channel": channel, "type": "check_in"}
                        subject, body = get_touch_content(
                            touch_info=touch_info,
                            contact=contact,
                            property_type=property_type,
                            phase="post_proposal",
                            state_entry=entry,
                            ghl_client=ghl_client,
                            contact_id=contact_id,
                        )

                        if channel == "email":
                            html_body = wrap_email_body(body, include_signature=True)
                            html_body = wrap_email_with_unsubscribe(html_body, contact_id)
                            ghl_client.send_email(contact_id, subject, html_body)
                        else:
                            sms_body = body + "\n" + build_sms_opt_out_text()
                            ghl_client.send_sms(contact_id, sms_body)

                        log.info("Sent proposal-view follow-up (%s) to %s", channel, contact_id)
                        summary["sent"].append({
                            "contact_id": contact_id,
                            "touch": "view_trigger",
                            "channel": channel,
                            "phase": "post_proposal",
                        })
                    except Exception as e:
                        log.warning("Error sending view follow-up for %s: %s", contact_id, e)

        except Exception as e:
            log.warning("Error checking proposal view for %s: %s", contact_id, e)

    # --- Phase 2: Send due post-proposal follow-ups (auto-send) ---
    post_proposal_due = get_due_contacts(state, phase="post_proposal")

    for item in post_proposal_due:
        contact_id = item["contact_id"]
        touch_number = item["touch_number"]
        path = item["path"]
        entry = get_state_contact(state, contact_id)

        try:
            # Check for active conversation or not-interested signal
            since_iso = entry.get("last_touch_at") or entry.get("proposal_sent_at") if entry else None
            if since_iso:
                conv_status = _check_active_conversation(ghl_client, contact_id, since_iso)

                if conv_status["not_interested"]:
                    mark_completed(state, contact_id, reason="not_interested")
                    summary["completed"].append({
                        "contact_id": contact_id,
                        "reason": "not_interested",
                    })
                    log.info("Lead %s expressed disinterest — removed from pipeline", contact_id)
                    try:
                        from sales_pipeline.learning.exit_analyzer import run_exit_analysis
                        run_exit_analysis(ghl_client, contact_id, reason="not_interested")
                    except Exception as e:
                        log.warning("Exit analysis failed for %s: %s", contact_id, e)
                    continue

                if conv_status["active"]:
                    log.info(
                        "Active conversation with %s (last msg: %s) — delaying touch %d",
                        contact_id, conv_status["last_message_at"], touch_number,
                    )
                    continue

            # Detect path if not yet set
            if not path and entry:
                proposal_sent_at = entry.get("proposal_sent_at", "")
                if proposal_sent_at:
                    path = detect_channel_path(ghl_client, contact_id, proposal_sent_at)
                    set_path(state, contact_id, path)
                else:
                    path = "B"
                    set_path(state, contact_id, path)

            # Fetch contact details from GHL
            contact = ghl_client.get_contact(contact_id)
            property_type = (
                contact.get("customField", {}).get("property_type")
                or entry.get("property_type", "commercial")
            )

            touch_info = POST_PROPOSAL_MATRIX[path][touch_number]
            channel = touch_info["channel"]

            # If the lead is actively responding on a specific channel, match it
            lead_channel = _detect_lead_channel(ghl_client, contact_id)
            if lead_channel:
                channel = lead_channel
                touch_info = {**touch_info, "channel": channel}

            subject, body = get_touch_content(
                touch_info=touch_info,
                contact=contact,
                property_type=property_type,
                phase="post_proposal",
                state_entry=entry,
                ghl_client=ghl_client,
                contact_id=contact_id,
            )

            # Wrap with signature + unsubscribe
            if channel == "email":
                html_body = wrap_email_body(body, include_signature=True)
                html_body = wrap_email_with_unsubscribe(html_body, contact_id)
                ghl_client.send_email(contact_id, subject, html_body)
            else:
                sms_body = body + "\n" + build_sms_opt_out_text()
                ghl_client.send_sms(contact_id, sms_body)

            record_touch(state, contact_id, touch_number, channel)
            summary["sent"].append({
                "contact_id": contact_id,
                "touch": touch_number,
                "channel": channel,
                "phase": "post_proposal",
            })
            log.info(
                "Sent post-proposal touch %d (%s/%s) to %s",
                touch_number, path, channel, contact_id,
            )

            # Record outcome for learning
            try:
                from sales_pipeline.learning.outcome_tracker import record_outcome
                record_outcome(
                    contact_id=contact_id, channel=channel,
                    touch_number=touch_number, phase="post_proposal",
                    subject=subject, body=body,
                    property_type=property_type,
                    enrichment_used=bool(find_prospect_match(
                        contact.get("companyName", ""))),
                )
            except Exception:
                pass

        except Exception as e:
            error_msg = f"Error sending touch {touch_number} to {contact_id}: {e}"
            summary["errors"].append(error_msg)
            log.error(error_msg)

    # --- Phase 2.5: Monthly nurture sends ---
    nurture_due = get_nurture_due_contacts(state)

    for item in nurture_due:
        contact_id = item["contact_id"]
        nurture_touch = item["nurture_touch"]
        property_type = item.get("property_type", "other")

        try:
            # Check for active conversation or not-interested signal
            n_entry = get_state_contact(state, contact_id)
            nurture_since = n_entry.get("last_touch_at") if n_entry else None
            if nurture_since:
                conv_status = _check_active_conversation(ghl_client, contact_id, nurture_since)

                if conv_status["not_interested"]:
                    mark_completed(state, contact_id, reason="not_interested")
                    summary["completed"].append({
                        "contact_id": contact_id,
                        "reason": "not_interested",
                    })
                    log.info("Lead %s expressed disinterest — removed from nurture", contact_id)
                    try:
                        from sales_pipeline.learning.exit_analyzer import run_exit_analysis
                        run_exit_analysis(ghl_client, contact_id, reason="not_interested")
                    except Exception as e:
                        log.warning("Exit analysis failed for %s: %s", contact_id, e)
                    continue

                if conv_status["active"]:
                    log.info(
                        "Active conversation with %s — delaying nurture touch %d",
                        contact_id, nurture_touch,
                    )
                    continue

            contact = ghl_client.get_contact(contact_id)
            first_name = contact.get("firstName") or "there"
            company = (contact.get("companyName")
                       or get_state_contact(state, contact_id).get("organization", "")
                       or "your company")

            # Smart content rotation
            nurture_list = _get_nurture_rotation()
            rotation_idx = (nurture_touch - 1) % len(nurture_list)
            rotation = nurture_list[rotation_idx]

            # Build win context for case_study/industry_insight types
            win_context = ""
            if rotation["type"] in ("case_study", "industry_insight"):
                try:
                    import json as _json
                    if WIN_LOSS_LOG_FILE.exists():
                        wins = []
                        for line in WIN_LOSS_LOG_FILE.read_text(encoding="utf-8").strip().split("\n"):
                            if line.strip():
                                rec = _json.loads(line)
                                if rec.get("outcome") == "won":
                                    wins.append(rec)
                        if wins:
                            recent = wins[-1]
                            win_context = (
                                f"\nRecent success to reference: "
                                f"Helped a {recent.get('property_type', 'commercial')} "
                                f"property with security patrol."
                            )
                except Exception:
                    pass

            # Build proposal context for contacts that previously received a proposal
            proposal_context = ""
            state_entry = get_state_contact(state, contact_id)
            if state_entry:
                has_proposal = (
                    contact_id.startswith("proposal_")
                    or state_entry.get("estimate_id")
                    or state_entry.get("lost_at")
                )
                if has_proposal:
                    proposal_context = (
                        "\nIMPORTANT CONTEXT: We previously sent this contact a security proposal. "
                        "They may have gone with another provider. Be aware of this — "
                        "don't pretend we've never talked. Instead, position us as a knowledgeable "
                        "alternative if their current situation isn't working out. "
                    )

            # Add rich contact details for personalization
            contact_context = _build_contact_context(contact, state_entry)
            if contact_context:
                proposal_context += contact_context

            # Add conversation history since last nurture touch
            nurture_since = state_entry.get("last_touch_at") if state_entry else None
            conv_context = _build_conversation_context(ghl_client, contact_id, since_iso=nurture_since)
            if conv_context:
                proposal_context += conv_context

            month = datetime.now().strftime("%B %Y")
            prompt = rotation["prompt"].format(
                first_name=first_name,
                company=company,
                property_type=property_type,
                month=month,
                calendar_link=CALENDAR_LINK(),
                win_context=win_context,
                proposal_context=proposal_context,
            )

            body = _call_claude(prompt)
            subject = f"{first_name}, {rotation['subject_hint']}"

            html_body = wrap_email_body(body, include_signature=True)
            html_body = wrap_email_with_unsubscribe(html_body, contact_id)
            ghl_client.send_email(contact_id, subject, html_body)

            record_nurture_touch(state, contact_id)
            summary["nurture_sent"].append({
                "contact_id": contact_id,
                "nurture_touch": nurture_touch,
                "content_type": rotation["type"],
            })
            log.info(
                "Sent nurture touch %d (%s) to %s",
                nurture_touch, rotation["type"], contact_id,
            )

            # Record outcome for learning
            try:
                from sales_pipeline.learning.outcome_tracker import record_outcome
                record_outcome(
                    contact_id=contact_id, channel="email",
                    touch_number=nurture_touch, phase="nurture",
                    subject=subject, body=body,
                    property_type=property_type,
                )
            except Exception:
                pass

        except Exception as e:
            error_msg = f"Error sending nurture touch {nurture_touch} to {contact_id}: {e}"
            summary["errors"].append(error_msg)
            log.error(error_msg)

    # --- Phase 3: Generate cold outreach follow-up drafts (human review) ---
    cold_due = get_due_contacts(state, phase="cold_outreach")

    for item in cold_due:
        contact_id = item["contact_id"]
        touch_number = item["touch_number"]
        entry = get_state_contact(state, contact_id)

        try:
            # Check for active conversation or not-interested signal
            since_iso = entry.get("last_touch_at") or entry.get("first_outreach_at") if entry else None
            if since_iso:
                conv_status = _check_active_conversation(ghl_client, contact_id, since_iso)

                if conv_status["not_interested"]:
                    mark_completed(state, contact_id, reason="not_interested")
                    summary["completed"].append({
                        "contact_id": contact_id,
                        "reason": "not_interested",
                    })
                    log.info("Lead %s expressed disinterest — removed from pipeline", contact_id)
                    try:
                        from sales_pipeline.learning.exit_analyzer import run_exit_analysis
                        run_exit_analysis(ghl_client, contact_id, reason="not_interested")
                    except Exception as e:
                        log.warning("Exit analysis failed for %s: %s", contact_id, e)
                    continue

                if conv_status["active"]:
                    log.info(
                        "Active conversation with %s (last msg: %s) — delaying touch %d",
                        contact_id, conv_status["last_message_at"], touch_number,
                    )
                    continue

            contact = ghl_client.get_contact(contact_id)
            property_type = entry.get("property_type", "other") if entry else "other"
            initial_channel = entry.get("channel", "email") if entry else "email"

            matrix = cold_touch_matrix(initial_channel)
            touch_info = matrix[touch_number]
            channel = touch_info["channel"]

            # If the lead is actively responding on a specific channel, match it
            lead_channel = _detect_lead_channel(ghl_client, contact_id)
            if lead_channel:
                channel = lead_channel
                touch_info = {**touch_info, "channel": channel}

            subject, body = get_touch_content(
                touch_info=touch_info,
                contact=contact,
                property_type=property_type,
                state_entry=entry,
                ghl_client=ghl_client,
                contact_id=contact_id,
            )

            # Cold follow-ups go to drafts for review (not auto-sent)
            summary["cold_drafts_generated"].append({
                "contact_id": contact_id,
                "touch": touch_number,
                "channel": channel,
                "subject": subject,
                "body": body,
            })
            log.info(
                "Generated cold follow-up draft touch %d (%s) for %s",
                touch_number, channel, contact_id,
            )

        except Exception as e:
            error_msg = f"Error generating cold follow-up {touch_number} for {contact_id}: {e}"
            summary["errors"].append(error_msg)
            log.error(error_msg)

    # --- Phase 4: Save state ---
    if owns_state:
        save_state(state)

    return summary
