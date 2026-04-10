"""Send a sample cold outreach follow-up email to Sam for review."""

import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sales_pipeline import config
from sales_pipeline.templates.signature import wrap_email_body
from sales_pipeline.templates.unsubscribe import wrap_email_with_unsubscribe

config.validate_config()

# Generate a LIVE message using the actual Claude prompts
import anthropic
from sales_pipeline.follow_up.follow_up_engine import (
    PROMPT_TEMPLATES, FOLLOW_UP_SYSTEM_PROMPT, _build_system_prompt,
)

print("Generating message via Claude...")
client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY())

prompt = PROMPT_TEMPLATES["check_in"].format(
    first_name="Dan",
    company="Sandvik- Artisan Vehicles",
    property_type="commercial",
    enrichment="\nContact details — use these to personalize the message:\n- Property address: 742 Pancho Rd\n- Location: Camarillo, CA",
)

response = client.messages.create(
    model="claude-sonnet-4-6",
    max_tokens=300,
    system=_build_system_prompt(),
    messages=[{"role": "user", "content": prompt}],
)

SAMPLE_BODY = response.content[0].text.strip()
SAMPLE_SUBJECT = "Following up — security for Sandvik- Artisan Vehicles"
print(f"Generated body:\n{SAMPLE_BODY}\n")

# Build the full HTML email exactly as the pipeline would
html_body = wrap_email_body(SAMPLE_BODY, include_signature=True)
html_body = wrap_email_with_unsubscribe(html_body, "SAMPLE_CONTACT_ID")

# Wrap in a full HTML document for proper rendering
full_html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="margin:0;padding:20px;background:#ffffff;">
{html_body}
</body>
</html>"""

# Send via Gmail
sender = config.GMAIL_SENDER()
password = config.GMAIL_APP_PASSWORD()
recipient = "salarcon@americalpatrol.com"

msg = MIMEMultipart("alternative")
msg["Subject"] = SAMPLE_SUBJECT
msg["From"] = f"Sam Alarcon <{sender}>"
msg["To"] = recipient
msg.attach(MIMEText(full_html, "html"))

try:
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
        srv.login(sender, password)
        srv.sendmail(sender, recipient, msg.as_string())
    print(f"Test email sent to {recipient}")
    print(f"Subject: {SAMPLE_SUBJECT}")
except smtplib.SMTPException as e:
    print(f"Failed to send: {e}")
    sys.exit(1)
