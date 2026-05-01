# guard_compliance/templates/sam_alert.py
"""HTML email templates for Sam's compliance alerts."""

from datetime import datetime

TIER_LABELS = {
    "first_notice": ("FYI", "#3b82f6", "Upcoming expirations"),
    "reminder":     ("Action Needed", "#f59e0b", "Schedule renewals"),
    "urgent":       ("URGENT", "#f97316", "Renewals due this month"),
    "critical":     ("CRITICAL", "#ef4444", "Renewals critically overdue"),
    "expired":      ("EXPIRED", "#991b1b", "Expired credentials -- review needed"),
    "bsis_mismatch":("BSIS ALERT", "#991b1b", "State record mismatch"),
    "bsis_warning": ("BSIS Warning", "#f59e0b", "Verification issue"),
}


def build_sam_alert_html(alerts: list[dict], tier: str, test_mode: bool = False) -> str:
    """
    Build consolidated HTML email for Sam listing officers with compliance issues.
    Groups alerts by tier for a single email.
    """
    label, color, action = TIER_LABELS.get(tier, ("Alert", "#6b7280", "Review needed"))
    now = datetime.now().strftime("%B %d, %Y at %I:%M %p")

    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background:#fef3c7;border:2px solid #f59e0b;padding:12px;'
            'margin-bottom:16px;border-radius:6px;text-align:center;font-weight:bold">'
            'TEST MODE — No real notifications were sent to officers'
            '</div>'
        )

    rows = ""
    for alert in alerts:
        officer = alert.get("officer", {})
        name = officer.get("name", "Unknown")

        if tier in ("bsis_mismatch", "bsis_warning"):
            issue = alert.get("issue", "Unknown issue")
            dca_status = alert.get("dca_status", "N/A")
            rows += (
                f'<tr>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{name}</td>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{dca_status}</td>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{issue}</td>'
                f'</tr>'
            )
        else:
            cred_type = alert.get("credential_type", "").replace("_", " ").title()
            expiry = alert.get("expiry_date", "N/A")
            days = alert.get("days_remaining")
            days_text = f"{days} days" if days is not None and days >= 0 else "EXPIRED"
            rows += (
                f'<tr>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{name}</td>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{cred_type}</td>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{expiry}</td>'
                f'<td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;'
                f'font-weight:bold;color:{color}">{days_text}</td>'
                f'</tr>'
            )

    if tier in ("bsis_mismatch", "bsis_warning"):
        header_row = (
            '<tr style="background:#f3f4f6">'
            '<th style="padding:8px 12px;text-align:left">Officer</th>'
            '<th style="padding:8px 12px;text-align:left">DCA Status</th>'
            '<th style="padding:8px 12px;text-align:left">Issue</th>'
            '</tr>'
        )
    else:
        header_row = (
            '<tr style="background:#f3f4f6">'
            '<th style="padding:8px 12px;text-align:left">Officer</th>'
            '<th style="padding:8px 12px;text-align:left">Credential</th>'
            '<th style="padding:8px 12px;text-align:left">Expiry Date</th>'
            '<th style="padding:8px 12px;text-align:left">Remaining</th>'
            '</tr>'
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto;padding:20px;color:#1f2937">
{test_banner}
<div style="background:{color};color:white;padding:16px 20px;border-radius:8px 8px 0 0">
  <h2 style="margin:0;font-size:20px">[{label}] Guard Compliance Alert</h2>
  <p style="margin:4px 0 0;opacity:0.9;font-size:14px">{action} &mdash; {len(alerts)} officer(s)</p>
</div>
<div style="border:1px solid #e5e7eb;border-top:none;border-radius:0 0 8px 8px;padding:16px 20px">
  <table style="width:100%;border-collapse:collapse;font-size:14px">
    {header_row}
    {rows}
  </table>
  <p style="color:#6b7280;font-size:12px;margin-top:16px">
    Generated {now} by Guard Compliance Tracker
  </p>
</div>
</body></html>"""

    return html


def build_sam_alert_subject(tier: str, count: int) -> str:
    """Build email subject line for Sam's alert."""
    label = TIER_LABELS.get(tier, ("Alert",))[0]
    plural = "Officer" if count == 1 else "Officers"
    return f"[{label}] Guard Compliance — {count} {plural} Need Attention"
