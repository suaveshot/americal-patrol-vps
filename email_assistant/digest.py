"""
Email Assistant (Larry) — Daily Digest & Escalation Aging
Sends a daily summary email and reminder nudges for stale escalations.
"""

from datetime import datetime, timedelta

from email_assistant.config import (
    SAM_EMAIL,
    AGING_HOURS_BUSINESS,
    AGING_HOURS_OFFHOURS,
    AGING_URGENT_HOURS,
    BUSINESS_HOURS_START,
    BUSINESS_HOURS_END,
)
from email_assistant.escalation_tracker import get_pending_escalations


# ── Escalation Aging ────────────────────────────────────────────────────────

def check_escalation_aging(service, state, send_fn, log_fn):
    """
    Check pending escalations and send reminders for stale ones.

    Args:
        service: Gmail API service
        state: pipeline state dict
        send_fn: function(service, to, subject, body) to send email
        log_fn: logging function
    """
    pending = get_pending_escalations(state)
    if not pending:
        return

    now = datetime.now()
    is_business = BUSINESS_HOURS_START <= now.hour < BUSINESS_HOURS_END
    threshold_hours = AGING_HOURS_BUSINESS if is_business else AGING_HOURS_OFFHOURS

    for esc_id, esc in pending.items():
        try:
            escalated_at = datetime.fromisoformat(esc["escalated_at"])
        except (ValueError, KeyError):
            continue

        age_hours = (now - escalated_at).total_seconds() / 3600

        # Skip if not yet stale
        if age_hours < threshold_hours:
            continue

        # Skip if already reminded
        if esc.get("reminded_at"):
            # But check for urgent upgrade (>24h and not yet marked urgent)
            if age_hours >= AGING_URGENT_HOURS and not esc.get("urgent_reminded"):
                _send_reminder(service, esc, age_hours, urgent=True, send_fn=send_fn, log_fn=log_fn)
                esc["urgent_reminded"] = True
            continue

        # Send first reminder
        urgent = age_hours >= AGING_URGENT_HOURS
        _send_reminder(service, esc, age_hours, urgent=urgent, send_fn=send_fn, log_fn=log_fn)
        esc["reminded_at"] = now.isoformat()
        if urgent:
            esc["urgent_reminded"] = True


def _send_reminder(service, esc, age_hours, urgent, send_fn, log_fn):
    """Send a reminder email to Sam about a stale escalation."""
    original_subject = esc.get("original_email", {}).get("subject", "(unknown)")
    original_from = esc.get("original_email", {}).get("from", "unknown")

    prefix = "[Larry] URGENT REMINDER" if urgent else "[Larry] REMINDER"
    subject = f"{prefix}: Pending guidance on {original_subject} ({age_hours:.0f}h ago)"

    body = (
        f"Hi Sam,\n\n"
        f"Just a reminder — I'm still waiting for your guidance on this email:\n\n"
        f"From: {original_from}\n"
        f"Subject: {original_subject}\n"
        f"Escalated: {age_hours:.0f} hours ago\n\n"
    )

    proposed = esc.get("proposed_response", "").strip()
    if proposed:
        body += (
            f"My proposed response:\n"
            f"{proposed[:500]}\n\n"
        )

    body += (
        f"Reply to the original escalation email with:\n"
        f"  1 = send as-is  |  2 = your edits  |  3 = skip\n\n"
        f"Thanks,\n"
        f"Larry"
    )

    try:
        send_fn(service, SAM_EMAIL, subject, body)
        log_fn(f"  Aging reminder sent: {subject}")
    except Exception as e:
        log_fn(f"  ERROR sending aging reminder: {e}")


# ── Daily Digest ────────────────────────────────────────────────────────────

def should_send_digest(state):
    """Check if the daily digest should be sent (once per calendar day)."""
    last_sent = state.get("digest_last_sent", "")
    if not last_sent:
        return True
    try:
        last_date = datetime.fromisoformat(last_sent).date()
        return last_date < datetime.now().date()
    except ValueError:
        return True


def build_daily_digest(state):
    """Build the HTML digest email summarizing Larry's activity."""
    now = datetime.now()
    stats = state.get("stats", {})
    pending = get_pending_escalations(state)

    # Gather resolved escalations from today
    today_resolved = []
    for esc_id, esc in state.get("pending_escalations", {}).items():
        if esc.get("status") != "resolved":
            continue
        resolved_at = esc.get("resolved_at", "")
        try:
            if datetime.fromisoformat(resolved_at).date() == now.date():
                today_resolved.append(esc)
        except ValueError:
            pass

    # Build HTML
    html = f"""<!DOCTYPE html>
<html>
<head>
<style>
body {{ font-family: Arial, sans-serif; color: #333; max-width: 640px; margin: 0 auto; }}
h1 {{ color: #1a365d; font-size: 20px; border-bottom: 2px solid #1a365d; padding-bottom: 8px; }}
h2 {{ color: #2d3748; font-size: 16px; margin-top: 24px; }}
table {{ border-collapse: collapse; width: 100%; margin: 12px 0; }}
th {{ background: #1a365d; color: #fff; text-align: left; padding: 8px 12px; font-size: 13px; }}
td {{ padding: 8px 12px; border-bottom: 1px solid #e2e8f0; font-size: 13px; }}
tr:nth-child(even) {{ background: #f7fafc; }}
.stat-box {{ display: inline-block; background: #ebf4ff; border-radius: 8px; padding: 12px 20px; margin: 4px 8px 4px 0; text-align: center; }}
.stat-num {{ font-size: 24px; font-weight: bold; color: #1a365d; }}
.stat-label {{ font-size: 11px; color: #718096; text-transform: uppercase; }}
.pending {{ color: #c53030; font-weight: bold; }}
.resolved {{ color: #276749; }}
.aged {{ color: #c53030; }}
.footer {{ margin-top: 24px; padding-top: 12px; border-top: 1px solid #e2e8f0; font-size: 11px; color: #a0aec0; }}
</style>
</head>
<body>

<h1>Larry's Daily Email Digest — {now.strftime('%A, %B %d, %Y')}</h1>

<h2>Today's Activity</h2>
<div>
  <div class="stat-box">
    <div class="stat-num">{stats.get('total_drafted', 0)}</div>
    <div class="stat-label">Drafts Created</div>
  </div>
  <div class="stat-box">
    <div class="stat-num">{stats.get('total_escalated', 0)}</div>
    <div class="stat-label">Escalated</div>
  </div>
  <div class="stat-box">
    <div class="stat-num">{stats.get('total_feedback_processed', 0)}</div>
    <div class="stat-label">Feedback Processed</div>
  </div>
  <div class="stat-box">
    <div class="stat-num">{stats.get('total_skipped', 0)}</div>
    <div class="stat-label">Filtered / Skipped</div>
  </div>
</div>
"""

    # Pending escalations
    if pending:
        html += f"""
<h2 class="pending">Pending Escalations ({len(pending)})</h2>
<p>These escalations are still waiting for your response:</p>
<table>
<tr><th>From</th><th>Subject</th><th>Age</th><th>Category</th></tr>
"""
        for esc_id, esc in sorted(pending.items(), key=lambda x: x[1].get("escalated_at", ""), reverse=True):
            orig = esc.get("original_email", {})
            try:
                age = datetime.now() - datetime.fromisoformat(esc["escalated_at"])
                age_str = _format_age(age)
                age_class = ' class="aged"' if age.total_seconds() / 3600 >= AGING_URGENT_HOURS else ""
            except (ValueError, KeyError):
                age_str = "?"
                age_class = ""

            html += (
                f'<tr><td>{_esc_html(orig.get("from", "?"))}</td>'
                f'<td>{_esc_html(orig.get("subject", "?"))}</td>'
                f'<td{age_class}>{age_str}</td>'
                f'<td>{_esc_html(esc.get("category", "?"))}</td></tr>\n'
            )
        html += "</table>\n"
    else:
        html += '<h2 class="resolved">No Pending Escalations</h2>\n'

    # Resolved today
    if today_resolved:
        html += f"""
<h2>Resolved Today ({len(today_resolved)})</h2>
<table>
<tr><th>From</th><th>Subject</th><th>Resolution</th><th>Response Time</th></tr>
"""
        for esc in today_resolved:
            orig = esc.get("original_email", {})
            resolution = esc.get("resolution", "?")
            resp_time = esc.get("response_time_hours")
            resp_str = f"{resp_time:.1f}h" if resp_time is not None else "?"
            html += (
                f'<tr><td>{_esc_html(orig.get("from", "?"))}</td>'
                f'<td>{_esc_html(orig.get("subject", "?"))}</td>'
                f'<td>{_esc_html(resolution)}</td>'
                f'<td>{resp_str}</td></tr>\n'
            )
        html += "</table>\n"

    # All-time stats
    html += f"""
<h2>All-Time Stats</h2>
<table>
<tr><th>Metric</th><th>Count</th></tr>
<tr><td>Total Processed</td><td>{stats.get('total_processed', 0)}</td></tr>
<tr><td>Drafts Created</td><td>{stats.get('total_drafted', 0)}</td></tr>
<tr><td>Escalated to Sam</td><td>{stats.get('total_escalated', 0)}</td></tr>
<tr><td>Feedback Processed</td><td>{stats.get('total_feedback_processed', 0)}</td></tr>
<tr><td>Filtered / Skipped</td><td>{stats.get('total_skipped', 0)}</td></tr>
</table>

<div class="footer">
  Generated by Larry (Email Assistant) at {now.strftime('%I:%M %p')} — Americal Patrol, Inc.
</div>
</body>
</html>"""

    return html


def send_daily_digest(service, state, send_html_fn, log_fn):
    """
    Send the daily digest if it hasn't been sent today.

    Args:
        service: Gmail API service
        state: pipeline state dict
        send_html_fn: function(service, to, subject, html_body) to send HTML email
        log_fn: logging function
    """
    if not should_send_digest(state):
        return

    html = build_daily_digest(state)
    subject = f"[Larry] Daily Email Digest — {datetime.now().strftime('%b %d, %Y')}"

    try:
        send_html_fn(service, SAM_EMAIL, subject, html)
        state["digest_last_sent"] = datetime.now().isoformat()
        log_fn(f"Daily digest sent to {SAM_EMAIL}")
    except Exception as e:
        log_fn(f"ERROR sending daily digest: {e}")


# ── Helpers ─────────────────────────────────────────────────────────────────

def _format_age(td):
    """Format a timedelta as a human-readable age string."""
    hours = td.total_seconds() / 3600
    if hours < 1:
        return f"{int(td.total_seconds() / 60)}m"
    elif hours < 24:
        return f"{hours:.1f}h"
    else:
        days = int(hours // 24)
        remaining_hours = hours % 24
        return f"{days}d {remaining_hours:.0f}h"


def _esc_html(text):
    """Basic HTML escaping."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )
