# guard_compliance/templates/compliance_report.py
"""HTML template for the weekly compliance status report."""

from datetime import datetime


STATUS_COLORS = {
    "valid":        ("#059669", "#ecfdf5", "Valid"),
    "first_notice": ("#3b82f6", "#eff6ff", "90 days"),
    "reminder":     ("#f59e0b", "#fffbeb", "60 days"),
    "urgent":       ("#f97316", "#fff7ed", "30 days"),
    "critical":     ("#ef4444", "#fef2f2", "14 days"),
    "expired":      ("#991b1b", "#fef2f2", "EXPIRED"),
    "unknown":      ("#6b7280", "#f9fafb", "N/A"),
}

BSIS_COLORS = {
    True:  ("#059669", "Verified"),
    False: ("#ef4444", "Mismatch"),
    None:  ("#6b7280", "N/A"),
}


def build_compliance_report_html(officers: list[dict], test_mode: bool = False) -> str:
    """
    Build a full HTML compliance dashboard report.

    officers: list of dicts with keys:
        name, guard_card_status, guard_card_days, guard_card_expiry,
        bsis_verified, bsis_status, other credential statuses...
    """
    now = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    total = len(officers)
    compliant = sum(1 for o in officers if o.get("guard_card_status") == "valid")

    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background:#fef3c7;border:2px solid #f59e0b;padding:12px;'
            'margin-bottom:16px;border-radius:6px;text-align:center;font-weight:bold">'
            'TEST MODE'
            '</div>'
        )

    # Summary bar
    pct = round(compliant / total * 100) if total > 0 else 0
    bar_color = "#059669" if pct >= 90 else "#f59e0b" if pct >= 70 else "#ef4444"

    # Build rows
    rows = ""
    for o in officers:
        name = o.get("name", "Unknown")

        # Guard card status cell
        gc_status = o.get("guard_card_status", "unknown")
        gc_color, gc_bg, gc_label = STATUS_COLORS.get(gc_status, STATUS_COLORS["unknown"])
        gc_days = o.get("guard_card_days")
        gc_expiry = o.get("guard_card_expiry", "N/A")
        if gc_days is not None and gc_status != "expired":
            gc_text = f"{gc_days}d ({gc_expiry})"
        elif gc_status == "expired":
            gc_text = f"EXPIRED ({gc_expiry})"
        else:
            gc_text = gc_expiry or "N/A"

        # BSIS verification cell
        bsis_ok = o.get("bsis_verified")
        bsis_color, bsis_label = BSIS_COLORS.get(bsis_ok, BSIS_COLORS[None])
        bsis_note = o.get("bsis_status", "")
        if bsis_note:
            bsis_label = f"{bsis_label} ({bsis_note})"

        # Contact method
        contact = "Email" if o.get("email") else ("SMS" if o.get("phone") else "None")

        rows += f"""<tr>
  <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb">{name}</td>
  <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;background:{gc_bg};color:{gc_color};font-weight:600">{gc_text}</td>
  <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:{bsis_color};font-weight:600">{bsis_label}</td>
  <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:#6b7280">{contact}</td>
</tr>"""

    # Find next expiring officer
    expiring_soon = [o for o in officers
                     if o.get("guard_card_days") is not None
                     and o.get("guard_card_status") != "valid"]
    expiring_soon.sort(key=lambda x: x.get("guard_card_days", 999))
    next_up = ""
    if expiring_soon:
        nxt = expiring_soon[0]
        next_up = (
            f'<div style="background:#fff7ed;border:1px solid #f97316;padding:12px;'
            f'border-radius:6px;margin-top:16px">'
            f'<strong>Next due:</strong> {nxt["name"]} &mdash; '
            f'guard card expires {nxt.get("guard_card_expiry", "N/A")} '
            f'({nxt.get("guard_card_days", "?")} days)'
            f'</div>'
        )

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="font-family:Arial,sans-serif;max-width:700px;margin:0 auto;padding:20px;color:#1f2937">
{test_banner}
<div style="background:#1e3a5f;color:white;padding:20px;border-radius:8px 8px 0 0">
  <h1 style="margin:0;font-size:22px">Americal Patrol &mdash; Guard Compliance Status</h1>
  <p style="margin:6px 0 0;opacity:0.85;font-size:14px">{now}</p>
</div>
<div style="border:1px solid #e5e7eb;border-top:none;padding:20px;border-radius:0 0 8px 8px">
  <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
    <div style="font-size:32px;font-weight:bold;color:{bar_color}">{compliant}/{total}</div>
    <div style="font-size:14px;color:#6b7280">officers fully compliant ({pct}%)</div>
  </div>
  <div style="background:#e5e7eb;border-radius:4px;height:8px;margin-bottom:20px">
    <div style="background:{bar_color};border-radius:4px;height:8px;width:{pct}%"></div>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:14px">
    <tr style="background:#f3f4f6">
      <th style="padding:8px 12px;text-align:left">Officer</th>
      <th style="padding:8px 12px;text-align:left">Guard Card</th>
      <th style="padding:8px 12px;text-align:left">BSIS Verified</th>
      <th style="padding:8px 12px;text-align:left">Contact</th>
    </tr>
    {rows}
  </table>
  {next_up}
  <p style="color:#9ca3af;font-size:12px;margin-top:20px">
    Auto-generated by Guard Compliance Tracker &middot;
    Run: <code>python -m guard_compliance.run_compliance --report</code>
  </p>
</div>
</body></html>"""

    return html


def build_report_subject(compliant: int, total: int) -> str:
    """Build email subject for weekly compliance report."""
    return f"Guard Compliance Report — {compliant}/{total} Officers Compliant"
