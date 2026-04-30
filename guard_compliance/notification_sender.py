# guard_compliance/notification_sender.py
"""
Guard Compliance — Notification Sender
Sends email alerts via Gmail SMTP and SMS via carrier gateway.
Follows the watchdog.py email pattern (smtplib.SMTP_SSL).
"""

import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from guard_compliance import config
from guard_compliance.templates.sam_alert import (
    build_sam_alert_html, build_sam_alert_subject,
)
from guard_compliance.templates.officer_alert import (
    build_officer_alert_html, build_officer_alert_subject, build_officer_sms,
)

log = logging.getLogger(__name__)


def _send_email(to: str, subject: str, html_body: str) -> bool:
    """Send an HTML email via Gmail SMTP. Returns True on success."""
    sender = config.GMAIL_SENDER()
    password = config.GMAIL_APP_PASSWORD()

    if not all([sender, password, to]):
        log.warning(f"Email credentials incomplete — cannot send to {to}")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = to
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(sender, password)
            srv.sendmail(sender, to, msg.as_string())
        log.info(f"Email sent to {to}: {subject}")
        return True
    except Exception as e:
        log.error(f"Failed to send email to {to}: {e}")
        return False


def _send_sms(gateway_address: str, message: str) -> bool:
    """
    Send SMS via carrier email-to-SMS gateway using Gmail SMTP.
    Same approach as voice_agent/emergency_alert.py but using SMTP directly.
    """
    sender = config.GMAIL_SENDER()
    password = config.GMAIL_APP_PASSWORD()

    if not all([sender, password, gateway_address]):
        log.warning(f"SMS credentials incomplete — cannot send to {gateway_address}")
        return False

    msg = MIMEText(message)
    msg["To"] = gateway_address
    msg["Subject"] = ""  # Carriers prepend subject to body — leave blank

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(sender, password)
            srv.sendmail(sender, gateway_address, msg.as_string())
        log.info(f"SMS sent to {gateway_address}")
        return True
    except Exception as e:
        log.error(f"Failed to send SMS to {gateway_address}: {e}")
        return False


def send_sam_alerts(alerts_by_tier: dict[str, list[dict]], test_mode: bool = True) -> int:
    """
    Send consolidated alerts to Sam, one email per tier.
    Also sends SMS to Sam's personal phone for critical/expired/bsis_mismatch tiers.
    Returns total number of notifications sent.
    """
    sam_email = config.SAM_EMAIL()
    sam_sms = config.SAM_CARRIER_GATEWAY()
    sms_tiers = {"critical", "expired", "bsis_mismatch"}
    sent = 0

    for tier, alerts in alerts_by_tier.items():
        if not alerts:
            continue

        subject = build_sam_alert_subject(tier, len(alerts))
        html = build_sam_alert_html(alerts, tier, test_mode=test_mode)

        if _send_email(sam_email, subject, html):
            sent += 1

        # SMS for critical tiers
        if tier in sms_tiers and sam_sms:
            names = ", ".join(a.get("officer", {}).get("name", "?") for a in alerts[:3])
            if len(alerts) > 3:
                names += f" +{len(alerts)-3} more"
            sms_text = f"[{tier.upper()}] Guard compliance: {names}. Check email for details."
            _send_sms(sam_sms, sms_text)

    return sent


def send_officer_notifications(officer_alerts: dict[str, list[dict]],
                               test_mode: bool = True) -> int:
    """
    Send individual notifications to officers about their expiring credentials.
    Uses email when available, falls back to SMS via carrier gateway.
    In test mode, all notifications redirect to Sam.

    Args:
        officer_alerts: {officer_id: [alert_dicts]}

    Returns total notifications sent.
    """
    sam_email = config.SAM_EMAIL()
    sent = 0

    for officer_id, alerts in officer_alerts.items():
        if not alerts:
            continue

        officer = alerts[0].get("officer", {})
        name = officer.get("name", "Officer")
        email = officer.get("email", "")
        phone = officer.get("phone", "")

        # Determine most urgent tier for subject line
        most_urgent = alerts[0]
        tier = most_urgent.get("tier", "reminder")
        cred_type = most_urgent.get("credential_type", "guard_card_expiry")
        days = most_urgent.get("days_remaining")

        if test_mode:
            # Redirect everything to Sam
            subject = build_officer_alert_subject(tier, cred_type, days)
            html = build_officer_alert_html(
                name, alerts, test_mode=True,
                original_recipient=email or phone or "no contact"
            )
            if _send_email(sam_email, f"[TEST] {subject}", html):
                sent += 1
        elif email:
            # Send email to officer
            subject = build_officer_alert_subject(tier, cred_type, days)
            html = build_officer_alert_html(name, alerts)
            if _send_email(email, subject, html):
                sent += 1
        elif phone:
            # SMS to officer via business carrier gateway (8058449433@vtext.com)
            business_gateway = config.BUSINESS_CARRIER_GATEWAY()
            sms_text = build_officer_sms(name, cred_type,
                                         most_urgent.get("expiry_date", ""),
                                         days)
            if business_gateway:
                # Send SMS from the business phone to the officer's number
                if _send_sms(business_gateway, f"To {name} ({phone}):\n{sms_text}"):
                    sent += 1
                    log.info(f"Officer SMS sent via business gateway for {name}")
            else:
                log.warning(f"Officer {name} has no email and no business gateway configured")
                subject = f"Officer {name} needs manual notification (no email)"
                html = build_officer_alert_html(
                    name, alerts, test_mode=True,
                    original_recipient=f"SMS to {phone} (manual)"
                )
                if _send_email(sam_email, subject, html):
                    sent += 1
        else:
            log.warning(f"Officer {name} has no email or phone — cannot notify, alerting Sam")
            subject = f"Officer {name} has no contact method on file"
            html = build_officer_alert_html(
                name, alerts, test_mode=True,
                original_recipient="NO CONTACT METHOD"
            )
            _send_email(sam_email, subject, html)

    return sent


def send_compliance_report(html: str, subject: str, test_mode: bool = True) -> bool:
    """Send the weekly compliance report email."""
    recipient = config.SAM_EMAIL()
    return _send_email(recipient, subject, html)
