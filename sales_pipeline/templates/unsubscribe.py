"""
Sales Pipeline — Unsubscribe Footer & DND Integration
Generates CAN-SPAM compliant unsubscribe links for outgoing emails.
Connects to Sam's existing GHL unsubscribe workflow via trigger link.

Every outgoing cold email gets:
  1. A visible "Unsubscribe" link in the footer
  2. A List-Unsubscribe header for one-click unsubscribe in Gmail/Yahoo

Every outgoing SMS gets:
  "Reply STOP to opt out" (GHL handles SMS opt-out natively)
"""

from sales_pipeline.config import UNSUBSCRIBE_TRIGGER_URL


def build_unsubscribe_footer(contact_id: str = "") -> str:
    """
    Generate HTML unsubscribe footer for email messages.
    Uses GHL trigger link URL from config — connects to existing workflow
    that sets DND ON and tags the contact.
    """
    trigger_url = UNSUBSCRIBE_TRIGGER_URL()

    if trigger_url:
        # Append contact_id as query param if available (for tracking)
        if contact_id:
            separator = "&" if "?" in trigger_url else "?"
            unsub_url = f"{trigger_url}{separator}contact_id={contact_id}"
        else:
            unsub_url = trigger_url
    else:
        # Fallback: mailto unsubscribe (CAN-SPAM minimum compliance)
        unsub_url = "mailto:salarcon@americalpatrol.com?subject=Unsubscribe"

    return (
        f'<div style="margin-top:24px;padding-top:12px;'
        f'border-top:1px solid #e5e5e5;font-family:Arial,sans-serif;'
        f'font-size:11px;color:#999999;line-height:1.4;">'
        f'Americal Patrol, Inc. &bull; Oxnard, CA<br>'
        f'Not interested? '
        f'<a href="{unsub_url}" style="color:#999999;text-decoration:underline;">'
        f'Unsubscribe</a> &mdash; no hard feelings.'
        f'</div>'
    )


def build_list_unsubscribe_header(contact_id: str = "") -> dict:
    """
    Generate List-Unsubscribe and List-Unsubscribe-Post headers
    for one-click unsubscribe compliance (RFC 8058).

    Returns a dict of headers to add to the outgoing email.
    Google and Yahoo require these for bulk senders since June 2024.
    """
    trigger_url = UNSUBSCRIBE_TRIGGER_URL()
    if not trigger_url:
        return {}

    if contact_id:
        separator = "&" if "?" in trigger_url else "?"
        unsub_url = f"{trigger_url}{separator}contact_id={contact_id}"
    else:
        unsub_url = trigger_url

    return {
        "List-Unsubscribe": f"<{unsub_url}>",
        "List-Unsubscribe-Post": "List-Unsubscribe=One-Click",
    }


def build_sms_opt_out_text() -> str:
    """
    Returns opt-out text to append to SMS messages.
    GHL handles SMS STOP natively, but including the text is best practice.
    """
    return "\nReply STOP to opt out"


def wrap_email_with_unsubscribe(html_body: str, contact_id: str = "") -> str:
    """
    Append unsubscribe footer to an HTML email body.
    Call this after signature has been added.
    """
    footer = build_unsubscribe_footer(contact_id)
    return f"{html_body}\n{footer}"


def build_plain_text_unsubscribe(contact_id: str = "") -> str:
    """Plain text unsubscribe line for cold outreach emails (no HTML)."""
    trigger_url = UNSUBSCRIBE_TRIGGER_URL()
    if trigger_url:
        if contact_id:
            separator = "&" if "?" in trigger_url else "?"
            unsub_url = f"{trigger_url}{separator}contact_id={contact_id}"
        else:
            unsub_url = trigger_url
        return f"Not interested? Unsubscribe: {unsub_url}"
    return "Not interested? Reply 'unsubscribe' and we'll remove you from our list."
