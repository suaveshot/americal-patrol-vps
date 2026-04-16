"""
Pipeline Exit & Win Analyzer

When contacts leave the pipeline (not interested) or are won, runs a full
Claude analysis on the conversation history and logs findings. Generates
a weekly digest email with analysis and pipeline recommendations for Sam.
"""

import json
import logging
import smtplib
from datetime import datetime, timezone, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from shared_utils.usage_tracker import tracked_create

from sales_pipeline import config
from sales_pipeline.state import load_state, get_contact, _parse_iso
import tenant_context as tc

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Exit analysis (runs when a contact is removed from pipeline)
# ---------------------------------------------------------------------------

def run_exit_analysis(ghl_client, contact_id: str, reason: str = "not_interested") -> dict:
    """
    Pull full conversation history for an exited contact, run Claude analysis
    to understand what went wrong, and log to exit_analysis_log.jsonl.
    """
    state = load_state()
    entry = get_contact(state, contact_id)

    # Pull conversation history
    messages = ghl_client.get_full_conversation_history(contact_id)
    contact = ghl_client.get_contact(contact_id)

    first_name = contact.get("firstName", "")
    company = contact.get("companyName", "") or (entry.get("organization", "") if entry else "")
    property_type = entry.get("property_type", "other") if entry else "other"
    touches_sent = entry.get("touches_sent", 0) if entry else 0

    # Build conversation timeline
    timeline = _build_timeline(messages)

    # Calculate days in pipeline
    days_in_pipeline = _calc_days(entry)

    # Pull estimate info if available
    estimate_info = _get_estimate_info(ghl_client, entry)

    prompt = f"""Analyze why this prospect left {tc.company_name()}'s sales pipeline.
{tc.company_description()}

Contact: {first_name} at {company}
Property type: {property_type}
Days in pipeline: {days_in_pipeline}
Automated touches sent: {touches_sent}
Exit reason: {reason}
{estimate_info}

FULL CONVERSATION TIMELINE:
{timeline if timeline else "(No messages found)"}

Analyze this exit and output JSON with these fields:
- exit_reason_category: (one of: "competitor_chosen", "budget", "timing", "no_response", "bad_fit", "other")
- what_went_wrong: (list of 2-4 specific issues — messaging timing, tone, content, follow-up gaps, etc.)
- what_went_right: (list of 1-3 things that worked well despite the exit)
- turning_point: (the specific moment or message where we lost them — or "no engagement" if they never responded)
- their_objections: (list of specific objections or concerns they expressed, or ["none expressed"])
- how_we_handled_objections: (how each objection was addressed, or "not addressed")
- pipeline_recommendation: (one specific, actionable change to messaging, timing, or approach that could prevent similar exits)
- recommendation_type: (one of: "messaging_tone", "timing", "channel", "follow_up_cadence", "value_proposition", "personalization", "other")

Output ONLY valid JSON, no markdown formatting."""

    try:
        response = tracked_create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
            pipeline="sales",
            client_id=tc.client_id(),
            api_key=config.ANTHROPIC_API_KEY(),
        )
        analysis_text = response.content[0].text.strip()

        if analysis_text.startswith("```"):
            analysis_text = analysis_text.split("\n", 1)[1].rsplit("```", 1)[0]
        analysis = json.loads(analysis_text)

    except Exception as e:
        log.error("Exit analysis failed for %s: %s", contact_id, e)
        analysis = {
            "exit_reason_category": "other",
            "what_went_wrong": [str(e)],
            "pipeline_recommendation": "Manual review needed — analysis failed",
            "recommendation_type": "other",
        }

    # Enrich with metadata
    analysis["contact_id"] = contact_id
    analysis["contact_name"] = f"{first_name} {contact.get('lastName', '')}".strip()
    analysis["company"] = company
    analysis["property_type"] = property_type
    analysis["touches_sent"] = touches_sent
    analysis["days_in_pipeline"] = days_in_pipeline
    analysis["exit_reason"] = reason
    analysis["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    analysis["message_count"] = len(messages)

    # Append to log
    with open(config.EXIT_ANALYSIS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(analysis) + "\n")

    log.info("Exit analysis logged for %s (%s at %s)", contact_id, first_name, company)
    return analysis


# ---------------------------------------------------------------------------
# Win analysis (runs when a deal is won — called alongside existing win/loss)
# ---------------------------------------------------------------------------

def run_win_analysis(ghl_client, contact_id: str, reason: str = "") -> dict:
    """
    Pull full conversation history for a won deal, run Claude analysis
    to understand what went right, and log to exit_analysis_log.jsonl.
    """
    state = load_state()
    entry = get_contact(state, contact_id)

    messages = ghl_client.get_full_conversation_history(contact_id)
    contact = ghl_client.get_contact(contact_id)

    first_name = contact.get("firstName", "")
    company = contact.get("companyName", "") or (entry.get("organization", "") if entry else "")
    property_type = entry.get("property_type", "other") if entry else "other"
    touches_sent = entry.get("touches_sent", 0) if entry else 0

    timeline = _build_timeline(messages)
    days_in_pipeline = _calc_days(entry)
    estimate_info = _get_estimate_info(ghl_client, entry)

    prompt = f"""Analyze why this prospect CHOSE {tc.company_name()}'s {tc.company_industry()} services.
{tc.company_description()}

Contact: {first_name} at {company}
Property type: {property_type}
Days in pipeline: {days_in_pipeline}
Automated touches sent: {touches_sent}
Sam's notes: {reason if reason else "N/A"}
{estimate_info}

FULL CONVERSATION TIMELINE:
{timeline if timeline else "(No messages found)"}

Analyze this win and output JSON with these fields:
- win_factors: (list of 3-5 specific things that worked — messaging, timing, personalization, value prop, etc.)
- key_message_that_worked: (the single most effective message or phrase we used)
- turning_point: (the specific moment or message where the deal tipped in our favor)
- objections_overcome: (list of objections raised and how they were handled)
- speed_to_close: (was the cadence right? too slow? too fast? what timing worked?)
- pipeline_recommendation: (one specific, actionable recommendation — either "keep doing X" or "do more of X across all contacts")
- recommendation_type: (one of: "messaging_tone", "timing", "channel", "follow_up_cadence", "value_proposition", "personalization", "other")
- replicable_pattern: (a pattern from this win that can be applied to other prospects — be specific)

Output ONLY valid JSON, no markdown formatting."""

    try:
        response = tracked_create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
            pipeline="sales",
            client_id=tc.client_id(),
            api_key=config.ANTHROPIC_API_KEY(),
        )
        analysis_text = response.content[0].text.strip()

        if analysis_text.startswith("```"):
            analysis_text = analysis_text.split("\n", 1)[1].rsplit("```", 1)[0]
        analysis = json.loads(analysis_text)

    except Exception as e:
        log.error("Win analysis failed for %s: %s", contact_id, e)
        analysis = {
            "win_factors": [str(e)],
            "pipeline_recommendation": "Manual review needed — analysis failed",
            "recommendation_type": "other",
        }

    analysis["outcome"] = "won"
    analysis["contact_id"] = contact_id
    analysis["contact_name"] = f"{first_name} {contact.get('lastName', '')}".strip()
    analysis["company"] = company
    analysis["property_type"] = property_type
    analysis["touches_sent"] = touches_sent
    analysis["days_in_pipeline"] = days_in_pipeline
    analysis["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    analysis["message_count"] = len(messages)

    with open(config.EXIT_ANALYSIS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(analysis) + "\n")

    log.info("Win analysis logged for %s (%s at %s)", contact_id, first_name, company)
    return analysis


# ---------------------------------------------------------------------------
# Weekly review email
# ---------------------------------------------------------------------------

def send_weekly_review():
    """
    Collect all exit and win analyses from the past week, synthesize
    recommendations, and email Sam a weekly pipeline review digest.
    """
    # Load analyses from the past 7 days
    exits, wins = _load_recent_analyses(days=7)

    if not exits and not wins:
        log.info("Weekly review: no exits or wins to report")
        return

    # Build synthesis prompt for overall recommendations
    recommendations = _synthesize_recommendations(exits, wins)

    # Build HTML email
    html = _build_weekly_html(exits, wins, recommendations)

    # Send
    sender = config.GMAIL_SENDER()
    recipient = config.DIGEST_TO_EMAIL()
    password = config.GMAIL_APP_PASSWORD()

    now_str = datetime.now().strftime("%b %d, %Y")
    subject = f"Weekly Pipeline Review — {now_str}"
    if exits:
        subject += f" | {len(exits)} exit(s)"
    if wins:
        subject += f" | {len(wins)} win(s)"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(sender, password)
            srv.sendmail(sender, recipient, msg.as_string())
        log.info("Weekly review sent to %s (%d exits, %d wins)", recipient, len(exits), len(wins))
    except smtplib.SMTPException as e:
        log.error("Failed to send weekly review: %s", e)
        raise

    # Update last sent timestamp
    review_state = {"last_sent": datetime.now(timezone.utc).isoformat()}
    with open(config.WEEKLY_REVIEW_FILE, "w", encoding="utf-8") as f:
        json.dump(review_state, f)


def should_send_weekly_review() -> bool:
    """Check if it's been 7+ days since last weekly review."""
    if not config.WEEKLY_REVIEW_FILE.exists():
        # Check if there's any data to review
        return config.EXIT_ANALYSIS_FILE.exists()

    try:
        with open(config.WEEKLY_REVIEW_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
        last_sent = state.get("last_sent")
        if not last_sent:
            return True
        days_since = (datetime.now(timezone.utc) - _parse_iso(last_sent)).total_seconds() / 86400
        return days_since >= 7
    except Exception:
        return True


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_timeline(messages: list) -> str:
    """Format messages into a readable timeline."""
    timeline = ""
    for msg in messages:
        direction = "→ SENT" if msg.get("direction") == "outbound" else "← RECEIVED"
        msg_type = msg.get("type", "").upper()
        timestamp = msg.get("timestamp", "")[:19]
        subject = msg.get("subject", "")
        body = msg.get("body", "")[:500]
        timeline += f"\n[{timestamp}] {direction} ({msg_type})"
        if subject:
            timeline += f" Subject: {subject}"
        timeline += f"\n{body}\n"
    return timeline


def _calc_days(entry: dict | None) -> str:
    """Calculate days in pipeline from state entry."""
    if not entry:
        return "unknown"
    start = entry.get("discovered_at") or entry.get("proposal_sent_at", "")
    if not start:
        return "unknown"
    start_dt = _parse_iso(start)
    days = int((datetime.now(timezone.utc) - start_dt).total_seconds() / 86400)
    return f"{days} days"


def _get_estimate_info(ghl_client, entry: dict | None) -> str:
    """Pull estimate details if available."""
    if not entry:
        return ""
    estimate_id = entry.get("estimate_id", "")
    if not estimate_id:
        return ""
    try:
        est_data = ghl_client.get_estimate(estimate_id)
        estimate = est_data.get("estimate", est_data)
        return f"Estimate value: ${estimate.get('total', 0):.2f}, Status: {estimate.get('status', 'unknown')}"
    except Exception:
        return ""


def _load_recent_analyses(days: int = 7) -> tuple[list, list]:
    """Load exit and win analyses from the past N days."""
    exits = []
    wins = []

    if not config.EXIT_ANALYSIS_FILE.exists():
        return exits, wins

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    try:
        with open(config.EXIT_ANALYSIS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    analyzed_at = record.get("analyzed_at", "")
                    if analyzed_at and _parse_iso(analyzed_at) >= cutoff:
                        if record.get("outcome") == "won":
                            wins.append(record)
                        else:
                            exits.append(record)
                except json.JSONDecodeError:
                    continue
    except Exception as e:
        log.error("Error loading analyses: %s", e)

    return exits, wins


def _synthesize_recommendations(exits: list, wins: list) -> dict:
    """Use Claude to synthesize individual analyses into overall recommendations."""
    exit_summaries = []
    for e in exits:
        exit_summaries.append(
            f"- {e.get('contact_name', '?')} ({e.get('company', '?')}): "
            f"Category: {e.get('exit_reason_category', '?')}. "
            f"What went wrong: {', '.join(e.get('what_went_wrong', []))}. "
            f"Recommendation: {e.get('pipeline_recommendation', 'N/A')}"
        )

    win_summaries = []
    for w in wins:
        win_summaries.append(
            f"- {w.get('contact_name', '?')} ({w.get('company', '?')}): "
            f"Win factors: {', '.join(w.get('win_factors', []))}. "
            f"Replicable pattern: {w.get('replicable_pattern', 'N/A')}. "
            f"Recommendation: {w.get('pipeline_recommendation', 'N/A')}"
        )

    prompt = f"""You are a sales operations analyst reviewing {tc.company_name()}'s pipeline performance this week.
{tc.company_description()}

EXITS THIS WEEK ({len(exits)}):
{chr(10).join(exit_summaries) if exit_summaries else "None"}

WINS THIS WEEK ({len(wins)}):
{chr(10).join(win_summaries) if win_summaries else "None"}

Based on these outcomes, provide a JSON response with:
- overall_assessment: (2-3 sentence summary of pipeline health this week)
- exit_patterns: (list of common themes across exits — what's the systemic issue, not just individual cases?)
- win_patterns: (list of common themes across wins — what's consistently working?)
- top_recommendation_exit: (the single highest-impact change to reduce exits — be very specific about what to change in the messaging, timing, or approach)
- top_recommendation_win: (the single highest-impact thing to double down on from wins — be specific about how to replicate this across more contacts)
- pipeline_changes: (list of 1-3 specific, implementable changes to the automated pipeline — e.g., "change Day 7 SMS from calendar CTA to value-add", "add a Day 5 check-in for commercial properties")
- risk_areas: (any emerging concerns — e.g., "high exit rate for retail properties", "Day 14 emails getting no engagement")

Output ONLY valid JSON, no markdown formatting."""

    try:
        response = tracked_create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
            pipeline="sales",
            client_id=tc.client_id(),
            api_key=config.ANTHROPIC_API_KEY(),
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0]
        return json.loads(text)
    except Exception as e:
        log.error("Recommendation synthesis failed: %s", e)
        return {
            "overall_assessment": "Analysis failed — manual review needed.",
            "pipeline_changes": [],
        }


def _build_weekly_html(exits: list, wins: list, recommendations: dict) -> str:
    """Build the weekly review HTML email."""

    # --- Header ---
    html = """
    <div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 700px; margin: 0 auto; color: #1a1a1a;">
    <div style="background: #1a2b4a; color: white; padding: 20px 24px; border-radius: 8px 8px 0 0;">
        <h1 style="margin: 0; font-size: 22px;">Weekly Pipeline Review</h1>
        <p style="margin: 4px 0 0; opacity: 0.8; font-size: 14px;">""" + datetime.now().strftime("%B %d, %Y") + """</p>
    </div>
    <div style="padding: 24px; background: #f9fafb; border: 1px solid #e5e7eb; border-top: none; border-radius: 0 0 8px 8px;">
    """

    # --- Overall Assessment ---
    assessment = recommendations.get("overall_assessment", "No assessment available.")
    html += f"""
    <div style="background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px; border-left: 4px solid #1a2b4a;">
        <h2 style="margin: 0 0 8px; font-size: 16px; color: #1a2b4a;">Overall Assessment</h2>
        <p style="margin: 0; font-size: 14px; line-height: 1.5;">{assessment}</p>
    </div>
    """

    # --- Scorecard ---
    html += f"""
    <div style="display: flex; gap: 12px; margin-bottom: 16px;">
        <div style="flex: 1; background: #fee2e2; padding: 12px; border-radius: 6px; text-align: center;">
            <div style="font-size: 28px; font-weight: bold; color: #dc2626;">{len(exits)}</div>
            <div style="font-size: 12px; color: #991b1b;">Exits</div>
        </div>
        <div style="flex: 1; background: #dcfce7; padding: 12px; border-radius: 6px; text-align: center;">
            <div style="font-size: 28px; font-weight: bold; color: #16a34a;">{len(wins)}</div>
            <div style="font-size: 12px; color: #166534;">Wins</div>
        </div>
    </div>
    """

    # --- Exits Section ---
    if exits:
        html += """
        <div style="background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px;">
            <h2 style="margin: 0 0 12px; font-size: 16px; color: #dc2626;">Lost Leads</h2>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                <tr style="background: #f9fafb;">
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Contact</th>
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Reason</th>
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Days</th>
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">What Went Wrong</th>
                </tr>
        """
        for e in exits:
            wrong = "<br>".join(f"• {w}" for w in e.get("what_went_wrong", [])[:3])
            html += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6;">
                        <strong>{e.get('contact_name', '?')}</strong><br>
                        <span style="color: #6b7280; font-size: 12px;">{e.get('company', '')}</span>
                    </td>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6;">{e.get('exit_reason_category', '?')}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6;">{e.get('days_in_pipeline', '?')}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6; font-size: 12px;">{wrong}</td>
                </tr>
            """
        html += "</table></div>"

        # Exit patterns
        patterns = recommendations.get("exit_patterns", [])
        if patterns:
            html += """<div style="background: #fff7ed; padding: 16px; border-radius: 6px; margin-bottom: 16px; border-left: 4px solid #f97316;">
                <h3 style="margin: 0 0 8px; font-size: 14px; color: #c2410c;">Exit Patterns</h3>
                <ul style="margin: 0; padding-left: 20px; font-size: 13px; line-height: 1.6;">"""
            for p in patterns:
                html += f"<li>{p}</li>"
            html += "</ul></div>"

    # --- Wins Section ---
    if wins:
        html += """
        <div style="background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px;">
            <h2 style="margin: 0 0 12px; font-size: 16px; color: #16a34a;">Won Deals</h2>
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                <tr style="background: #f9fafb;">
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Contact</th>
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Days</th>
                    <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">What Worked</th>
                </tr>
        """
        for w in wins:
            factors = "<br>".join(f"• {f}" for f in w.get("win_factors", [])[:3])
            html += f"""
                <tr>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6;">
                        <strong>{w.get('contact_name', '?')}</strong><br>
                        <span style="color: #6b7280; font-size: 12px;">{w.get('company', '')}</span>
                    </td>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6;">{w.get('days_in_pipeline', '?')}</td>
                    <td style="padding: 8px; border-bottom: 1px solid #f3f4f6; font-size: 12px;">{factors}</td>
                </tr>
            """
        html += "</table></div>"

        # Win patterns
        patterns = recommendations.get("win_patterns", [])
        if patterns:
            html += """<div style="background: #f0fdf4; padding: 16px; border-radius: 6px; margin-bottom: 16px; border-left: 4px solid #16a34a;">
                <h3 style="margin: 0 0 8px; font-size: 14px; color: #166534;">Win Patterns</h3>
                <ul style="margin: 0; padding-left: 20px; font-size: 13px; line-height: 1.6;">"""
            for p in patterns:
                html += f"<li>{p}</li>"
            html += "</ul></div>"

    # --- Recommendations ---
    html += """<div style="background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px; border: 2px solid #1a2b4a;">
        <h2 style="margin: 0 0 12px; font-size: 16px; color: #1a2b4a;">Recommendations (Awaiting Your Approval)</h2>"""

    rec_exit = recommendations.get("top_recommendation_exit", "")
    rec_win = recommendations.get("top_recommendation_win", "")
    changes = recommendations.get("pipeline_changes", [])
    risks = recommendations.get("risk_areas", [])

    if rec_exit:
        html += f"""<div style="margin-bottom: 12px;">
            <strong style="color: #dc2626;">To reduce exits:</strong>
            <p style="margin: 4px 0 0; font-size: 13px; line-height: 1.5;">{rec_exit}</p>
        </div>"""

    if rec_win:
        html += f"""<div style="margin-bottom: 12px;">
            <strong style="color: #16a34a;">To replicate wins:</strong>
            <p style="margin: 4px 0 0; font-size: 13px; line-height: 1.5;">{rec_win}</p>
        </div>"""

    if changes:
        html += """<div style="margin-bottom: 12px;">
            <strong>Proposed pipeline changes:</strong>
            <ol style="margin: 4px 0 0; padding-left: 20px; font-size: 13px; line-height: 1.6;">"""
        for c in changes:
            html += f"<li>{c}</li>"
        html += "</ol></div>"

    if risks:
        html += """<div>
            <strong style="color: #f97316;">Risk areas:</strong>
            <ul style="margin: 4px 0 0; padding-left: 20px; font-size: 13px; line-height: 1.6;">"""
        for r in risks:
            html += f"<li>{r}</li>"
        html += "</ul></div>"

    html += """
        <div style="margin-top: 16px; padding: 12px; background: #f0f4ff; border-radius: 6px;">
            <p style="margin: 0; font-size: 13px; color: #1a2b4a;">
                <strong>Reply to this email</strong> to approve, edit, or reject these recommendations.
                No changes will be made until you respond.
            </p>
        </div>
    </div>
    """

    html += "</div></div>"
    return html
