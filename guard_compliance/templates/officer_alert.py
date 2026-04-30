# guard_compliance/templates/officer_alert.py
"""HTML email templates for officer compliance notifications."""

from datetime import datetime

TIER_MESSAGING = {
    "first_notice": {
        "urgency": "Heads Up",
        "color": "#3b82f6",
        "message": "Your {cred_name} is set to expire on {expiry_date}. Please begin the renewal process soon.",
    },
    "reminder": {
        "urgency": "Reminder",
        "color": "#f59e0b",
        "message": "Your {cred_name} expires on {expiry_date} ({days} days). Please schedule your renewal.",
    },
    "urgent": {
        "urgency": "Urgent",
        "color": "#f97316",
        "message": "Your {cred_name} expires on {expiry_date} -- only {days} days remaining. Please renew immediately.",
    },
    "critical": {
        "urgency": "Critical",
        "color": "#ef4444",
        "message": "Your {cred_name} expires on {expiry_date} -- only {days} days left. Please renew immediately.",
    },
    "expired": {
        "urgency": "Expired",
        "color": "#991b1b",
        "message": "Your {cred_name} expired on {expiry_date}. Please renew as soon as possible.",
    },
}

# Guard card is the ONLY credential that prevents an officer from working.
# Baton, firearm, tear gas/pepper spray being expired just means don't bring that item.
GUARD_CARD_OVERRIDES = {
    "critical": "Your {cred_name} expires on {expiry_date} -- only {days} days left. You may be pulled from the schedule if not renewed.",
    "expired": "Your {cred_name} expired on {expiry_date}. You cannot work until this is renewed.",
}

EQUIPMENT_OVERRIDES = {
    "critical": "Your {cred_name} expires on {expiry_date} -- only {days} days left. Do not bring this item to work until renewed.",
    "expired": "Your {cred_name} expired on {expiry_date}. Do not bring this item to work until it is renewed.",
}

EQUIPMENT_CREDENTIALS = {
    "baton_cert_expiry", "firearm_permit_expiry", "pepper_spray_cert_expiry",
}

CRED_DISPLAY_NAMES = {
    "guard_card_expiry": "BSIS Guard Card",
    "firearm_permit_expiry": "Firearms Permit",
    "baton_cert_expiry": "Baton Certification",
    "pepper_spray_cert_expiry": "Pepper Spray Certification",
    "cpr_cert_expiry": "CPR Certification",
    "background_check_renewal": "Background Check",
}

CRED_RENEWAL_INFO = {
    "guard_card_expiry": (
        "To renew your BSIS Guard Card, visit the Bureau of Security and "
        "Investigative Services at <a href='https://www.bsis.ca.gov'>www.bsis.ca.gov</a> "
        "or contact the office at (805) 844-9433 for assistance."
    ),
    "firearm_permit_expiry": (
        "Firearms permit renewal requires completing a qualifying course. "
        "Contact the office at (805) 844-9433 to schedule."
    ),
    "baton_cert_expiry": "Contact the office at (805) 844-9433 to schedule your baton recertification training.",
    "pepper_spray_cert_expiry": "Contact the office at (805) 844-9433 to schedule your pepper spray recertification.",
    "cpr_cert_expiry": "Schedule a CPR recertification course through an approved provider.",
    "background_check_renewal": "Contact the office at (805) 844-9433 — we will coordinate your background check renewal.",
}


def build_officer_alert_html(officer_name: str, alerts: list[dict],
                             test_mode: bool = False,
                             original_recipient: str = "") -> str:
    """
    Build HTML email for an individual officer about their expiring credentials.
    """
    # Use the most urgent tier for the header
    most_urgent = alerts[0] if alerts else {}
    tier = most_urgent.get("tier", "reminder")
    tier_info = TIER_MESSAGING.get(tier, TIER_MESSAGING["reminder"])

    test_banner = ""
    if test_mode:
        test_banner = (
            f'<div style="background:#fef3c7;border:2px solid #f59e0b;padding:12px;'
            f'margin-bottom:16px;border-radius:6px;text-align:center;font-weight:bold">'
            f'TEST MODE — Would have been sent to: {original_recipient}'
            f'</div>'
        )

    cred_blocks = ""
    for alert in alerts:
        cred_type = alert.get("credential_type", "guard_card_expiry")
        cred_name = CRED_DISPLAY_NAMES.get(cred_type, cred_type.replace("_", " ").title())
        expiry_date = alert.get("expiry_date", "N/A")
        days = alert.get("days_remaining")
        alert_tier = alert.get("tier", "reminder")
        info = TIER_MESSAGING.get(alert_tier, TIER_MESSAGING["reminder"])

        # Use credential-specific messaging for critical/expired tiers
        msg_template = info["message"]
        if alert_tier in ("critical", "expired"):
            if cred_type == "guard_card_expiry":
                msg_template = GUARD_CARD_OVERRIDES[alert_tier]
            elif cred_type in EQUIPMENT_CREDENTIALS:
                msg_template = EQUIPMENT_OVERRIDES[alert_tier]
        msg = msg_template.format(
            cred_name=cred_name,
            expiry_date=expiry_date,
            days=abs(days) if days is not None else "?",
        )
        renewal = CRED_RENEWAL_INFO.get(cred_type, "Contact the office for renewal instructions.")

        cred_blocks += f"""
        <div style="background:#f9fafb;border-left:4px solid {info['color']};
                     padding:12px 16px;margin:12px 0;border-radius:0 6px 6px 0">
          <p style="margin:0 0 8px;font-weight:bold;color:{info['color']}">{cred_name}</p>
          <p style="margin:0 0 8px">{msg}</p>
          <p style="margin:0;font-size:13px;color:#6b7280">{renewal}</p>
        </div>"""

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:600px;margin:0 auto;padding:20px;color:#1f2937">
{test_banner}
<div style="background:{tier_info['color']};color:white;padding:16px 20px;border-radius:8px 8px 0 0">
  <h2 style="margin:0;font-size:18px">Americal Patrol &mdash; Credential {tier_info['urgency']}</h2>
</div>
<div style="border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px;padding:20px">
  <p>Hi {officer_name},</p>
  {cred_blocks}
  <p style="margin-top:16px">If you need assistance or have questions about the renewal process,
     please contact the office at (805) 844-9433.</p>
  <p style="margin-top:8px">Thank you,<br>Americal Patrol Management</p>
  <p style="color:#9ca3af;font-size:11px;margin-top:20px;border-top:1px solid #e5e7eb;padding-top:12px">
    This is an automated message from the Americal Patrol Guard Compliance system.
  </p>
</div>
</body></html>"""

    return html


def build_officer_alert_subject(tier: str, cred_type: str, days: int | None) -> str:
    """Build email subject line for officer alert."""
    cred_name = CRED_DISPLAY_NAMES.get(cred_type, "Credential")
    if days is not None and days >= 0:
        return f"Americal Patrol — Your {cred_name} Expires in {days} Days"
    elif days is not None and days < 0:
        return f"Americal Patrol — Your {cred_name} Has Expired"
    else:
        return f"Americal Patrol — {cred_name} Renewal Notice"


def build_officer_sms(officer_name: str, cred_type: str,
                      expiry_date: str, days: int | None) -> str:
    """Build SMS message for officer (160 char limit per segment)."""
    cred_name = CRED_DISPLAY_NAMES.get(cred_type, "credential")
    if days is not None and days >= 0:
        return f"Americal Patrol: {officer_name}, your {cred_name} expires {expiry_date} ({days} days). Please renew. Questions? Call (805) 844-9433."
    elif cred_type == "guard_card_expiry":
        return f"Americal Patrol: {officer_name}, your {cred_name} expired {expiry_date}. You cannot work until renewed. Call (805) 844-9433."
    else:
        return f"Americal Patrol: {officer_name}, your {cred_name} expired {expiry_date}. Do not bring this item to work. Call (805) 844-9433 to renew."
