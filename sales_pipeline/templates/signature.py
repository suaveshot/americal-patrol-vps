"""
Sales Pipeline — Email Signature Generator
Produces an HTML signature with Americal Patrol shield logo, Sam's contact info,
company office numbers, and booking CTA. Cross-client compatible (HTML tables + inline CSS).
"""

from sales_pipeline.config import CALENDAR_LINK

# ── Image URLs ───────────────────────────────────────────────────
LOGO_URL = "https://assets.cdn.filesafe.space/pI579KjSOGiSQA5cvy35/media/69bc45faa37cc2c63b0a7ff1.png"

# ── Contact details ──────────────────────────────────────────────
SENDER_NAME = "Sam Alarcon"
SENDER_TITLE = "Vice President"
COMPANY_NAME = "Americal Patrol, Inc."
DIRECT_PHONE = "(805) 515-3834"
MAILING_ADDRESS = "3301 Harbor Blvd., Oxnard, CA 93035"
FAX = "(866) 526-8472"
WEBSITE = "www.americalpatrol.com"
WEBSITE_URL = "https://americalpatrol.com"


def build_signature_html() -> str:
    """
    Generate a professional HTML email signature.
    Uses inline CSS and HTML tables for maximum email client compatibility.
    Layout: shield logo left, contact info right, divider line between.
    """
    calendly = CALENDAR_LINK()

    return f"""<table cellpadding="0" cellspacing="0" border="0" style="max-width:480px;font-family:Arial,Helvetica,sans-serif;border-top:2px solid #000000;padding-top:14px;margin-top:20px;">
  <tr>
    <td style="vertical-align:top;padding-right:14px;">
      <img src="{LOGO_URL}" alt="Americal Patrol" width="70" height="82"
           style="display:block;border:0;" />
    </td>
    <td style="vertical-align:top;border-left:2px solid #000000;padding-left:14px;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr><td style="font-size:15px;font-weight:bold;color:#1a3a5c;padding-bottom:1px;">{SENDER_NAME}</td></tr>
        <tr><td style="font-size:12px;color:#555555;padding-bottom:6px;">{SENDER_TITLE}, {COMPANY_NAME}</td></tr>
        <tr><td style="font-size:11px;color:#333333;padding-bottom:2px;">
          Call or Text: <a href="tel:+18055153834" style="color:#2563eb;text-decoration:none;font-weight:bold;">{DIRECT_PHONE}</a>
        </td></tr>
        <tr><td style="font-size:11px;color:#333333;padding-bottom:2px;">
          Fax: {FAX}
        </td></tr>
        <tr><td style="font-size:11px;color:#888888;padding-bottom:4px;">
          {MAILING_ADDRESS}
        </td></tr>
        <tr><td style="font-size:12px;">
          <a href="{WEBSITE_URL}" style="color:#2563eb;text-decoration:none;font-weight:bold;">{WEBSITE}</a>
        </td></tr>
      </table>
    </td>
  </tr>
  <tr>
    <td colspan="2" style="padding-top:10px;">
      <table cellpadding="0" cellspacing="0" border="0">
        <tr><td style="background-color:#1a3a5c;border-radius:4px;padding:8px 18px;">
          <a href="{calendly}" target="_blank"
             style="color:#ffffff;text-decoration:none;font-size:12px;font-family:Arial,sans-serif;font-weight:bold;">
            Schedule a Free Security Assessment &rarr;</a>
        </td></tr>
      </table>
    </td>
  </tr>
</table>"""


def build_sms_signature() -> str:
    """Plain text signature for SMS messages."""
    return f"-- {SENDER_NAME}, {COMPANY_NAME} | {DIRECT_PHONE}"


def build_plain_text_signature() -> str:
    """Plain text signature for cold outreach emails (no HTML)."""
    return (
        f"{SENDER_NAME}\n"
        f"{SENDER_TITLE}, {COMPANY_NAME}\n"
        f"Call or Text: {DIRECT_PHONE}\n"
        f"{WEBSITE}"
    )


def wrap_email_body(body_text: str, include_signature: bool = True) -> str:
    """
    Wrap a plain-text email body in minimal HTML with signature.
    Converts newlines to <br> for proper rendering.
    """
    # Convert plain text to HTML paragraphs
    paragraphs = body_text.strip().split("\n\n")
    html_body = ""
    for p in paragraphs:
        lines = p.strip().replace("\n", "<br>")
        html_body += f"<p style=\"font-family:Arial,sans-serif;font-size:14px;color:#1a1a1a;line-height:1.5;margin:0 0 12px 0;\">{lines}</p>\n"

    if include_signature:
        html_body += f"\n<br>\n{build_signature_html()}"

    return html_body
