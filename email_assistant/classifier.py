"""
Email Assistant (Larry) -- Classifier V2
Uses Claude API to analyze incoming emails with thread context,
two-tier confidence, near-zero skip policy, and style learning.
"""

import json
import logging
import os
from pathlib import Path

import anthropic

from email_assistant.config import (
    COMPANY_CONTEXT,
    CONFIDENCE_THRESHOLD_KNOWN,
    CONFIDENCE_THRESHOLD_UNKNOWN,
    CLAUDE_MODEL,
    CLAUDE_MAX_TOKENS,
)

log = logging.getLogger(__name__)

STYLE_GUIDE_FILE = Path(__file__).resolve().parent / "learning" / "style_guide.json"


def _load_style_guidance():
    """Load learned style patterns from Sam's edits, if available."""
    if not STYLE_GUIDE_FILE.exists():
        return ""
    try:
        data = json.loads(STYLE_GUIDE_FILE.read_text(encoding="utf-8"))
        patterns = data.get("patterns", [])
        if not patterns:
            return ""
        lines = "\n".join(f"- {p}" for p in patterns[:10])
        return (
            f"\n\nSTYLE GUIDANCE (learned from Sam's past edits -- follow these):\n"
            f"{lines}\n"
        )
    except (json.JSONDecodeError, OSError):
        return ""


def _build_system_prompt(is_known_client):
    """Build the system prompt with appropriate confidence threshold."""
    threshold = CONFIDENCE_THRESHOLD_KNOWN if is_known_client else CONFIDENCE_THRESHOLD_UNKNOWN
    style = _load_style_guidance()

    return f"""You are Larry, the business assistant for Americal Patrol, Inc.
Your job is to analyze incoming emails to americalpatrol@gmail.com and decide how to respond.

{COMPANY_CONTEXT}
{style}

INSTRUCTIONS:
1. Determine if this email is from a client or potential client needing a response.
2. Assess your confidence (0.0-1.0) in drafting an appropriate response.
3. If confident (>= {threshold}): draft a professional, approachable reply.
4. If uncertain (< {threshold}), or the email involves pricing, contracts, billing,
   complaints, or anything requiring Sam's judgment: prepare an escalation summary.

CRITICAL -- NEAR-ZERO SKIP POLICY:
This email already passed our noise filters (newsletters, noreply, internal, spam).
If it reached you, it almost certainly deserves a response or escalation.
Only use action="skip" for TRUE edge cases:
- Out-of-office replies that somehow slipped through filters
- Duplicate forwards of content already processed
- Clearly misdirected emails (wrong company entirely)
You MUST provide a specific skip_reason if you skip. When in doubt, ESCALATE -- never skip.

ALWAYS ESCALATE (regardless of confidence):
- Pricing or quote requests
- Contract or agreement questions
- Billing, invoice, or payment issues
- Complaints or service issues
- Scheduling changes or new service requests
- Anything legal or HR-related
- Anything you're not 100% sure about

RESPOND IN VALID JSON ONLY -- no markdown fencing, no extra text:
{{
  "action": "draft_response" | "escalate" | "skip",
  "confidence": 0.0 to 1.0,
  "category": "service_inquiry" | "scheduling" | "billing" | "complaint" | "report_question" | "general" | "new_inquiry" | "spam" | "not_applicable",
  "reasoning": "Brief explanation of your decision",
  "draft_subject": "Re: [original subject]",
  "draft_body": "The full email body you would send (WITHOUT signature -- it is added automatically). Leave empty if action is skip.",
  "escalation_summary": "If escalating: summary for Sam of what the email says, your proposed response, and what you need guidance on. Leave empty if not escalating.",
  "skip_reason": "REQUIRED if action is skip. Specific reason this email should be ignored."
}}"""


def analyze_and_draft(email_data, is_known_client=False, thread_context=None):
    """
    Send email to Claude for analysis with optional thread context.
    Returns parsed JSON result. On failure, returns escalation action.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set")
        return _fallback_escalation(email_data, "ANTHROPIC_API_KEY not set")

    parts = []
    if thread_context and len(thread_context) > 1:
        parts.append("--- CONVERSATION HISTORY (oldest first) ---")
        for i, msg in enumerate(thread_context[:-1]):
            parts.append(f"\n[Message {i+1}] From: {msg['from']} | Date: {msg['date']}")
            parts.append(msg["body"])
        parts.append("\n--- CURRENT EMAIL (respond to this) ---")

    parts.append(
        f"From: {email_data['from']}\n"
        f"To: {email_data['to']}\n"
        f"Subject: {email_data['subject']}\n"
        f"Date: {email_data['date']}\n"
        f"\n--- Email Body ---\n"
        f"{email_data['body'][:3000]}"
    )

    user_prompt = "\n".join(parts)
    system_prompt = _build_system_prompt(is_known_client)

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=CLAUDE_MAX_TOKENS,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3].strip()
        if text.startswith("json"):
            text = text[4:].strip()

        result = json.loads(text)

        for field in ("action", "confidence", "category"):
            if field not in result:
                log.warning(f"Missing field '{field}' in classifier response")
                return _fallback_escalation(email_data, f"Malformed response: missing {field}")

        threshold = CONFIDENCE_THRESHOLD_KNOWN if is_known_client else CONFIDENCE_THRESHOLD_UNKNOWN
        if result["action"] == "draft_response" and result["confidence"] < threshold:
            log.info(f"Confidence {result['confidence']:.2f} below threshold {threshold} -- escalating")
            result["action"] = "escalate"
            if not result.get("escalation_summary"):
                result["escalation_summary"] = (
                    f"Confidence was {result['confidence']:.2f} (below {threshold}).\n\n"
                    f"Proposed response:\n{result.get('draft_body', '(none)')}\n\n"
                    f"Reasoning: {result.get('reasoning', '(none)')}"
                )

        return result

    except json.JSONDecodeError as e:
        log.error(f"JSON parse error from classifier: {e}")
        return _fallback_escalation(email_data, f"Could not parse classifier response: {e}")

    except Exception as e:
        log.error(f"Classifier error: {e}")
        return _fallback_escalation(email_data, str(e))


def _fallback_escalation(email_data, reason):
    """When the classifier fails, return an escalation so the email isn't lost."""
    return {
        "action": "escalate",
        "confidence": 0.0,
        "category": "general",
        "reasoning": f"Classifier error -- escalating to Sam. Reason: {reason}",
        "draft_subject": "",
        "draft_body": "",
        "escalation_summary": (
            f"[AUTOMATED ESCALATION -- Classifier Error]\n\n"
            f"Reason: {reason}\n\n"
            f"Original email from: {email_data.get('from', 'unknown')}\n"
            f"Subject: {email_data.get('subject', 'unknown')}\n\n"
            f"Body preview:\n{email_data.get('body', '')[:500]}"
        ),
    }
