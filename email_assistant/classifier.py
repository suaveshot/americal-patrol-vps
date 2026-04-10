"""
Email Assistant (Larry) — Classifier
Uses Claude API to analyze incoming emails and draft responses.
"""

import json
import logging
import os

import anthropic

from email_assistant.config import (
    COMPANY_CONTEXT,
    CONFIDENCE_THRESHOLD,
    CLAUDE_MODEL,
    CLAUDE_MAX_TOKENS,
)

log = logging.getLogger(__name__)

_SYSTEM_PROMPT = f"""You are Larry, the business assistant for Americal Patrol, Inc.
Your job is to analyze incoming emails to americalpatrol@gmail.com and decide how to respond.

{COMPANY_CONTEXT}

INSTRUCTIONS:
1. Determine if this email is from a client or potential client needing a response.
2. Assess your confidence (0.0-1.0) in drafting an appropriate response.
3. If confident (>= {CONFIDENCE_THRESHOLD}): draft a professional, approachable reply.
4. If uncertain (< {CONFIDENCE_THRESHOLD}), or the email involves pricing, contracts, billing,
   complaints, or anything requiring Sam's judgment: prepare an escalation summary.
5. If the email is spam, a newsletter that slipped through, or doesn't need a response: skip it.

ALWAYS ESCALATE (regardless of confidence):
- Pricing or quote requests
- Contract or agreement questions
- Billing, invoice, or payment issues
- Complaints or service issues
- Scheduling changes or new service requests
- Anything legal or HR-related
- Anything you're not 100% sure about

RESPOND IN VALID JSON ONLY — no markdown fencing, no extra text:
{{
  "action": "draft_response" | "escalate" | "skip",
  "confidence": 0.0 to 1.0,
  "category": "service_inquiry" | "scheduling" | "billing" | "complaint" | "report_question" | "general" | "new_inquiry" | "spam" | "not_applicable",
  "reasoning": "Brief explanation of your decision",
  "draft_subject": "Re: [original subject]",
  "draft_body": "The full email body you would send (WITHOUT signature — it is added automatically). Leave empty if action is skip.",
  "escalation_summary": "If escalating: summary for Sam of what the email says, your proposed response, and what you need guidance on. Leave empty if not escalating."
}}"""


def analyze_and_draft(email_data):
    """
    Send email to Claude for analysis. Returns parsed JSON result.
    On failure, returns an escalation action so the email isn't lost.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set")
        return _fallback_escalation(email_data, "ANTHROPIC_API_KEY not set")

    user_prompt = (
        f"From: {email_data['from']}\n"
        f"To: {email_data['to']}\n"
        f"Subject: {email_data['subject']}\n"
        f"Date: {email_data['date']}\n"
        f"\n--- Email Body ---\n"
        f"{email_data['body'][:3000]}"
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=CLAUDE_MAX_TOKENS,
            system=_SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_prompt}],
        )

        text = response.content[0].text.strip()

        # Strip markdown fencing if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3].strip()
        if text.startswith("json"):
            text = text[4:].strip()

        result = json.loads(text)

        # Validate required fields
        for field in ("action", "confidence", "category"):
            if field not in result:
                log.warning(f"Missing field '{field}' in classifier response")
                return _fallback_escalation(email_data, f"Malformed classifier response: missing {field}")

        # Force escalation if confidence below threshold even when action is draft_response
        if result["action"] == "draft_response" and result["confidence"] < CONFIDENCE_THRESHOLD:
            log.info(f"Confidence {result['confidence']:.2f} below threshold — escalating")
            result["action"] = "escalate"
            if not result.get("escalation_summary"):
                result["escalation_summary"] = (
                    f"Confidence was {result['confidence']:.2f} (below {CONFIDENCE_THRESHOLD}).\n\n"
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
        "reasoning": f"Classifier error — escalating to Sam. Reason: {reason}",
        "draft_subject": "",
        "draft_body": "",
        "escalation_summary": (
            f"[AUTOMATED ESCALATION — Classifier Error]\n\n"
            f"Reason: {reason}\n\n"
            f"Original email from: {email_data.get('from', 'unknown')}\n"
            f"Subject: {email_data.get('subject', 'unknown')}\n\n"
            f"Body preview:\n{email_data.get('body', '')[:500]}"
        ),
    }
