# weekly_update/email_composer.py
"""
Build an HTML email body from collected metrics + week-over-week deltas.
"""

from datetime import datetime


# ---------------------------------------------------------------------------
# Delta formatting helpers
# ---------------------------------------------------------------------------

def _delta_html(value_str: str, delta: dict) -> str:
    """
    Format a metric value with its delta indicator.
    delta dict has keys: direction ("up","down","flat","new"), pct (float)
    """
    d = delta.get("direction", "flat")
    pct = delta.get("pct", 0)

    if d == "new":
        return f'{value_str} <span style="color:#888;font-size:12px">(new)</span>'
    elif d == "up":
        return (f'{value_str} <span style="color:#16a34a;font-size:12px">'
                f'(&#x2191; {pct:.0f}%)</span>')
    elif d == "down":
        return (f'{value_str} <span style="color:#dc2626;font-size:12px">'
                f'(&#x2193; {pct:.0f}%)</span>')
    else:  # flat
        return (f'{value_str} <span style="color:#888;font-size:12px">'
                f'(&#x2192;)</span>')


def _fmt_usd(amount: float) -> str:
    return f"${amount:,.2f}"


# ---------------------------------------------------------------------------
# Section builders
# ---------------------------------------------------------------------------

def _estimates_section(estimates: list, total: float) -> str:
    if not estimates:
        return '<p style="color:#666">No new estimates sent this week.</p>'

    rows = ""
    for e in estimates:
        rows += f"""
        <tr style="background-color:#f9f9f9">
          <td style="padding:10px;border-bottom:1px solid #eee">{e['name']}</td>
          <td style="padding:10px;border-bottom:1px solid #eee">{e['service']}</td>
          <td style="padding:10px;border-bottom:1px solid #eee;text-align:right">
            <strong>{_fmt_usd(e['amount'])}</strong></td>
        </tr>"""

    return f"""
    <table style="border-collapse:collapse;width:100%;margin:12px 0">
      <tr style="background-color:#1a3c5e;color:white">
        <th style="padding:10px;text-align:left">Client</th>
        <th style="padding:10px;text-align:left">Service</th>
        <th style="padding:10px;text-align:right">Monthly Amount</th>
      </tr>
      {rows}
    </table>
    <p><strong>Total pending monthly revenue: {_fmt_usd(total)}</strong></p>"""


def _deals_section(deals: list, total: float) -> str:
    if not deals:
        return '<p style="color:#666">No new deals closed this week.</p>'

    rows = ""
    for d in deals:
        rows += f"""
        <tr style="background-color:#f9f9f9">
          <td style="padding:10px;border-bottom:1px solid #eee">{d['name']}</td>
          <td style="padding:10px;border-bottom:1px solid #eee">{d['service']}</td>
          <td style="padding:10px;border-bottom:1px solid #eee;text-align:right">
            <strong>{_fmt_usd(d['amount'])}</strong></td>
        </tr>"""

    return f"""
    <table style="border-collapse:collapse;width:100%;margin:12px 0">
      <tr style="background-color:#1a3c5e;color:white">
        <th style="padding:10px;text-align:left">Client</th>
        <th style="padding:10px;text-align:left">Service</th>
        <th style="padding:10px;text-align:right">Monthly Amount</th>
      </tr>
      {rows}
    </table>
    <p><strong>Total new monthly revenue: {_fmt_usd(total)}</strong></p>"""


def _ads_section(ads: dict, deltas: dict) -> str:
    if ads.get("error"):
        return '<p style="color:#cc0000">Google Ads data unavailable this week.</p>'

    spend_html = _delta_html(_fmt_usd(ads["spend"]), deltas.get("ads_spend", {}))
    calls_html = _delta_html(str(ads["calls"]), deltas.get("ads_calls", {}))
    cpl_html = _delta_html(_fmt_usd(ads["cost_per_lead"]), deltas.get("ads_cost_per_lead", {}))

    return f"""
    <ul style="list-style:none;padding:0">
      <li style="padding:6px 0"><strong>Weekly Spend:</strong> {spend_html}</li>
      <li style="padding:6px 0"><strong>Qualified Calls:</strong> {calls_html}</li>
      <li style="padding:6px 0"><strong>Cost Per Lead:</strong> {cpl_html}</li>
    </ul>"""


def _voice_section(voice: dict, deltas: dict) -> str:
    total_html = _delta_html(str(voice["total"]), deltas.get("voice_total_calls", {}))
    intake_html = _delta_html(str(voice["intake"]), deltas.get("voice_intake_calls", {}))

    return f"""
    <ul style="list-style:none;padding:0">
      <li style="padding:6px 0"><strong>Total Calls:</strong> {total_html}</li>
      <li style="padding:6px 0"><strong>Intake / Lead Calls:</strong> {intake_html}</li>
      <li style="padding:6px 0"><strong>Emergency Calls:</strong> {voice['emergency']}</li>
      <li style="padding:6px 0"><strong>Dropped Calls:</strong> {voice['dropped']}</li>
    </ul>"""


# ---------------------------------------------------------------------------
# Main composer
# ---------------------------------------------------------------------------

def compose_email(data: dict, deltas: dict) -> tuple[str, str]:
    """
    Build the full HTML email.

    Args:
        data:   Output of collect_all() — keys: ghl, ads, voice
        deltas: Output of build_deltas() — metric key -> delta dict

    Returns:
        (subject, html_body)
    """
    date_str = datetime.now().strftime("%b %d, %Y")
    subject = f"Americal Patrol \u2014 Weekly Update ({date_str})"

    ghl = data["ghl"]
    ads = data["ads"]
    voice = data["voice"]

    hr = '<hr style="border:none;border-top:1px solid #ddd;margin:24px 0">'

    body = f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;color:#333;line-height:1.6;max-width:700px">

<p>Hey Don,</p>
<p>Here's this week's business update.</p>

{hr}

<h2 style="color:#1a3c5e;font-size:18px">Estimates Sent This Week</h2>
{_estimates_section(ghl['estimates'], ghl['estimates_total'])}

<h2 style="color:#1a3c5e;font-size:18px">Deals Closed This Week</h2>
{_deals_section(ghl['deals_closed'], ghl['deals_closed_total'])}

{hr}

<h2 style="color:#1a3c5e;font-size:18px">Google Ads Performance</h2>
{_ads_section(ads, deltas)}

<h2 style="color:#1a3c5e;font-size:18px">Voice Agent</h2>
{_voice_section(voice, deltas)}

{hr}

<p>Sam</p>

</body></html>"""

    return subject, body
