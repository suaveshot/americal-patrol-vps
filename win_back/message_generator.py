"""
Win-Back Message Generator

Generates re-engagement messages in three modes:
- simple: Template-based with variable substitution
- discount: Template with configurable offer
- ai_personalized: Claude-generated based on contact history
"""

import logging
from datetime import datetime, timedelta

from shared_utils.usage_tracker import tracked_create

log = logging.getLogger(__name__)


def _first_name(full_name: str) -> str:
    return full_name.strip().split()[0] if full_name else "there"


def _fill_template(template: str, contact: dict,
                   company_name: str, company_phone: str,
                   company_website: str, **extra) -> str:
    replacements = {
        "{first_name}": _first_name(contact.get("name", "")),
        "{full_name}": contact.get("name", ""),
        "{company_name}": company_name,
        "{phone}": company_phone,
        "{website}": company_website,
        "{service_type}": extra.get("service_type", "service"),
        "{discount}": str(extra.get("discount_percentage", "")),
        "{code}": extra.get("discount_code", ""),
        "{expiry}": extra.get("expiry_date", ""),
    }
    result = template
    for key, val in replacements.items():
        result = result.replace(key, val)
    return result


def generate_message(
    contact: dict,
    config: dict,
    channel: str = "email",
    company_name: str = "",
    company_phone: str = "",
    company_website: str = "",
    client_id: str = "",
) -> dict:
    """
    Generate a win-back message for a contact.

    Returns:
        {"body": str, "subject": str or None (SMS), "mode": str}
    """
    mode = config.get("mode", "simple")
    first_name = _first_name(contact.get("name", ""))

    if mode == "simple":
        if channel == "sms":
            body = _fill_template(
                config.get("simple_template_sms", f"Hi {first_name}, we miss you!"),
                contact, company_name, company_phone, company_website)
        else:
            body = _fill_template(
                config.get("simple_template_email", f"Hi {first_name}, we miss you!"),
                contact, company_name, company_phone, company_website)
        subject = f"We miss you, {first_name}!" if channel == "email" else None

    elif mode == "discount":
        pct = config.get("discount_percentage", 10)
        code = config.get("discount_code", "COMEBACK")
        expiry_days = config.get("discount_expiry_days", 30)
        expiry_date = (datetime.now() + timedelta(days=expiry_days)).strftime("%B %d, %Y")

        template = (
            f"Hi {{first_name}}, we miss you at {{company_name}}! "
            f"Here's {pct}% off your next {{service_type}}. "
            f"Use code {code} or just reply to this message. "
            f"Offer valid through {expiry_date}."
        )
        body = _fill_template(
            template, contact, company_name, company_phone, company_website,
            discount_percentage=pct, discount_code=code, expiry_date=expiry_date)
        subject = f"{first_name}, here's {pct}% off your next visit" if channel == "email" else None

    elif mode == "ai_personalized":
        system_prompt = (
            f"You are writing a short, warm re-engagement message for {company_name}. "
            f"The customer's name is {contact.get('name', 'there')}. "
            f"They haven't used the service in a while. "
            f"Write a {'text message (under 160 chars, no subject line)' if channel == 'sms' else 'brief email body (3-4 sentences)'}. "
            f"Be conversational and human. No em dashes. Don't sound scripted. "
            f"Include the company phone: {company_phone}. "
            f"Do not include a greeting line like 'Subject:' -- just the message body."
        )

        response = tracked_create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=system_prompt,
            messages=[{"role": "user", "content": f"Write a {channel} win-back message for {contact.get('name', 'this customer')}."}],
            pipeline="win_back",
            client_id=client_id or "unknown",
        )
        body = response.content[0].text.strip()
        subject = f"We've been thinking about you, {first_name}" if channel == "email" else None

    else:
        body = f"Hi {first_name}, we miss you at {company_name}! Call us at {company_phone}."
        subject = f"We miss you, {first_name}!" if channel == "email" else None

    return {"body": body, "subject": subject, "mode": mode}
