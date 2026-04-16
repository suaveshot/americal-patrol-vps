"""
Email Assistant (Larry) V2 -- Main Entry Point
Checks americalpatrol@gmail.com for client emails, drafts responses,
escalates uncertain ones to Sam, processes Sam's feedback, tracks edits,
and sends daily digest.

Usage:
    python email_assistant/email_monitor.py
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is on path for imports
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from dotenv import load_dotenv
load_dotenv(PROJECT_DIR / ".env")

from email_assistant.config import (
    STATE_FILE,
    LOG_FILE,
    SEARCH_WINDOW_HOURS,
    SIGNATURE,
    SAM_EMAIL,
    URGENCY_PATTERN,
    CHECKIN_INACTIVE_DAYS,
    CHECKIN_MAX_DRAFTS,
    SENTIMENT_ALERT_THRESHOLD,
    is_client_email,
)
from email_assistant.gmail_client import (
    get_gmail_service,
    fetch_unread_emails,
    fetch_thread_messages,
    fetch_sam_replies,
    create_reply_draft,
    send_escalation_email,
    send_html_email,
    send_reply_in_thread,
)
from email_assistant.classifier import analyze_and_draft
from email_assistant.escalation_tracker import (
    record_escalation,
    find_escalation_for_reply,
    resolve_escalation,
    prune_old_escalations,
)
from email_assistant.feedback_parser import parse_sam_response
from email_assistant.digest import (
    check_escalation_aging,
    send_daily_digest,
    send_weekly_report,
)
from email_assistant.learning_tracker import (
    record_draft,
    check_for_edits,
    update_style_guide,
)
from email_assistant.client_tracker import (
    record_interaction,
    get_client_context,
    get_inactive_clients,
    get_sentiment_trend,
)

# Event bus for cross-pipeline integration
try:
    from shared_utils.event_bus import publish_event
except ImportError:
    publish_event = None


# -- Logging ------------------------------------------------------------------
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# -- State management ---------------------------------------------------------
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            log("WARNING: Corrupt state file -- starting fresh")
    return {
        "version": 2,
        "processed_ids": {},
        "pending_escalations": {},
        "draft_log": {},
        "last_run": None,
        "stats": {
            "total_processed": 0,
            "total_drafted": 0,
            "total_escalated": 0,
            "total_skipped": 0,
            "total_feedback_processed": 0,
        },
    }


def save_state(state):
    state["last_run"] = datetime.now().isoformat()
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(str(tmp), str(STATE_FILE))


def prune_old_ids(state, days=7):
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    old_ids = state.get("processed_ids", {})
    state["processed_ids"] = {
        mid: ts for mid, ts in old_ids.items() if ts > cutoff
    }


# -- Phase 1: Process Sam's feedback on escalations --------------------------
def process_feedback(service, state):
    """Check if Sam has replied to any escalation emails and act on it."""
    replies = fetch_sam_replies(service, SAM_EMAIL, hours=SEARCH_WINDOW_HOURS)
    if not replies:
        return

    log(f"Checking {len(replies)} potential feedback replies from Sam")

    for reply in replies:
        reply_id = reply["id"]

        # Don't re-process
        if reply_id in state.get("processed_ids", {}):
            continue

        esc_key = find_escalation_for_reply(state, reply)
        if not esc_key:
            continue

        esc = state["pending_escalations"][esc_key]
        original_email = esc.get("original_email", {})
        proposed = esc.get("proposed_response", "")

        result = parse_sam_response(reply.get("body", ""))
        action = result["action"]
        log(f"  Feedback for '{original_email.get('subject', '?')}': {action}")

        if action == "send_proposed":
            try:
                create_reply_draft(service, original_email, proposed, SIGNATURE)
                resolve_escalation(state, esc_key, "drafted_approved")
                log(f"  -> Draft created (approved by Sam)")
            except Exception as e:
                log(f"  -> ERROR sending proposed: {e}")
                continue

        elif action == "send_custom":
            custom_body = result.get("custom_body", "")
            try:
                create_reply_draft(service, original_email, custom_body, SIGNATURE)
                resolve_escalation(state, esc_key, "sent_custom", custom_body[:200])
                log(f"  -> Draft created with Sam's edits")
            except Exception as e:
                log(f"  -> ERROR creating custom draft: {e}")
                continue

        elif action == "skip":
            resolve_escalation(state, esc_key, "skipped")
            log(f"  -> Skipped per Sam's instruction")

        elif action == "draft":
            if proposed:
                try:
                    create_reply_draft(service, original_email, proposed, SIGNATURE)
                    resolve_escalation(state, esc_key, "drafted", result.get("reason", ""))
                    log(f"  -> Ambiguous reply -- draft created for review")
                except Exception as e:
                    log(f"  -> ERROR creating draft: {e}")
                    continue
            else:
                resolve_escalation(state, esc_key, "drafted", "no proposed response")
                log(f"  -> Ambiguous reply, no proposed response to draft")

        # Mark the feedback reply as processed
        state["processed_ids"][reply_id] = datetime.now().isoformat()
        state["stats"]["total_feedback_processed"] = state["stats"].get("total_feedback_processed", 0) + 1


# -- Priority scoring --------------------------------------------------------
def _score_email_priority(email, is_known_client, is_urgent):
    """
    Score an email for processing priority. Higher = process first.
    Factors: urgency, known client, days since last contact, sentiment trend.
    """
    score = 0

    if is_urgent:
        score += 100
    if is_known_client:
        score += 50

    # Boost clients we haven't heard from in a while (re-engagement signal)
    sender = (email.get("from") or "").lower()
    domain = sender.split("@")[-1].rstrip(">").strip()
    from email_assistant.client_tracker import load_clients
    clients = load_clients()
    key = domain if domain not in ("gmail.com", "yahoo.com", "hotmail.com", "outlook.com") else sender
    entry = clients.get(key)
    if entry and entry.get("last_contact"):
        try:
            last = datetime.fromisoformat(entry["last_contact"])
            days_silent = (datetime.now() - last).days
            if days_silent > 14:
                score += min(days_silent, 60)  # Cap at 60 bonus
        except (ValueError, TypeError):
            pass

    # Boost if declining sentiment (needs extra attention)
    if entry:
        avg_sent, trend = get_sentiment_trend(key)
        if trend == "declining":
            score += 30

    return score


# -- Phase 2: Process new incoming emails -------------------------------------
def process_new_emails(service, state):
    """Fetch, prioritize, and classify new emails."""
    try:
        emails = fetch_unread_emails(service, hours=SEARCH_WINDOW_HOURS)
    except Exception as e:
        log(f"ERROR fetching emails: {e}")
        return

    log(f"Found {len(emails)} unread email(s) in last {SEARCH_WINDOW_HOURS}h")

    # Pre-filter and score for priority processing
    actionable = []
    skipped = 0

    for email in emails:
        email_id = email["id"]
        if email_id in state.get("processed_ids", {}):
            continue

        passes_filter, is_known_client = is_client_email(email)
        if not passes_filter:
            log(f"  Filtered out (noise): {email.get('subject', '?')}")
            state["processed_ids"][email_id] = datetime.now().isoformat()
            state["stats"]["total_skipped"] += 1
            skipped += 1
            continue

        subject = email.get("subject", "(no subject)")
        is_urgent = bool(
            URGENCY_PATTERN.search(subject) or
            URGENCY_PATTERN.search(email.get("body", "")[:500])
        )

        priority = _score_email_priority(email, is_known_client, is_urgent)
        actionable.append((priority, email, is_known_client, is_urgent))

    # Sort by priority (highest first)
    actionable.sort(key=lambda x: -x[0])

    if actionable:
        log(f"Priority queue: {len(actionable)} email(s) to process")

    drafted = 0
    escalated = 0
    skip_count = 0

    for priority, email, is_known_client, is_urgent in actionable:
        email_id = email["id"]
        sender = email.get("from", "unknown")
        subject = email.get("subject", "(no subject)")
        receive_time = datetime.now()

        log(f"Processing [P{priority}]: {subject} (from: {sender})")

        if is_urgent:
            log(f"  -> URGENT email detected")

        # Note attachments
        attachments = email.get("attachments", [])
        if attachments:
            att_summary = ", ".join(
                f"{a['name']} ({a['type']}, {a['size']})" for a in attachments
            )
            log(f"  -> Attachments: {att_summary}")

        # Fetch thread context
        thread_context = fetch_thread_messages(service, email["thread_id"])

        # Analyze with Claude
        try:
            result = analyze_and_draft(
                email,
                is_known_client=is_known_client,
                thread_context=thread_context,
            )
        except Exception as e:
            log(f"  -> Classifier error: {e} -- skipping (will retry next run)")
            continue

        action = result.get("action", "skip")
        confidence = result.get("confidence", 0)
        category = result.get("category", "unknown")
        reasoning = result.get("reasoning", "")
        sentiment = result.get("sentiment")

        log(f"  -> Action: {action} | Confidence: {confidence:.2f} | Category: {category} | Sentiment: {sentiment}")
        log(f"  -> Reasoning: {reasoning}")

        # Compute response time (time from email fetch to draft creation)
        response_time_sec = (datetime.now() - receive_time).total_seconds()

        # Record client interaction with sentiment and response time
        record_interaction(
            email, category, action,
            sentiment=sentiment,
            response_time_sec=response_time_sec,
        )

        # Publish lead inquiries to event bus
        if publish_event and category in ("service_inquiry", "new_lead", "quote_request"):
            try:
                publish_event("email_assistant", "lead_inquiry", {
                    "sender": sender,
                    "subject": subject,
                    "category": category,
                    "confidence": confidence,
                    "email_id": email_id,
                })
                log(f"  -> Lead inquiry published to event bus")
            except Exception as e:
                log(f"  -> Event bus publish failed: {e}")

        if action == "draft_response":
            try:
                draft = create_reply_draft(
                    service,
                    original_email=email,
                    reply_body=result.get("draft_body", ""),
                    signature=SIGNATURE,
                )
                draft_id = draft.get("id", "unknown")
                log(f"  -> Draft created (ID: {draft_id})")
                drafted += 1
                state["stats"]["total_drafted"] += 1

                # Record draft for edit tracking
                record_draft(
                    state, email_id,
                    result.get("draft_body", ""),
                    sender, subject,
                )
            except Exception as e:
                log(f"  -> ERROR creating draft: {e}")
                continue

        elif action == "escalate":
            try:
                prefix = "[Larry] URGENT:" if is_urgent else "[Larry] Need guidance:"
                esc_subject = f"{prefix} {subject}"
                esc_body = _build_escalation_body(email, result)
                sent = send_escalation_email(service, SAM_EMAIL, esc_subject, esc_body)
                esc_msg_id = sent.get("id", email_id)
                log(f"  -> Escalation email sent to {SAM_EMAIL}")

                # Track the escalation
                record_escalation(state, esc_msg_id, email, result)
                state["pending_escalations"][esc_msg_id]["escalation_thread_id"] = sent.get("threadId", "")

                escalated += 1
                state["stats"]["total_escalated"] += 1
            except Exception as e:
                log(f"  -> ERROR sending escalation: {e}")
                continue

        else:
            skip_reason = result.get("skip_reason", "no reason")
            log(f"  -> Skipped (reason: {skip_reason})")
            skip_count += 1
            state["stats"]["total_skipped"] += 1

        # Mark processed
        state["processed_ids"][email_id] = datetime.now().isoformat()
        state["stats"]["total_processed"] += 1

    log(f"New emails: Drafted: {drafted} | Escalated: {escalated} | Skipped: {skipped + skip_count}")


# -- Phase 3: Housekeeping ---------------------------------------------------
def run_housekeeping(service, state):
    """Aging reminders, digest, edit learning, escalation pruning, check-ins, weekly report."""
    check_escalation_aging(service, state, send_escalation_email, log)
    send_daily_digest(service, state, send_html_email, log)
    send_weekly_report(service, state, send_html_email, log)
    check_for_edits(service, state, log)

    learning_dir = Path(__file__).resolve().parent / "learning"
    edit_count = len(list(learning_dir.glob("edit_*.json"))) if learning_dir.exists() else 0
    if edit_count >= 3 and edit_count % 10 == 0:
        update_style_guide(log)

    prune_old_escalations(state)

    # Proactive check-in drafts for inactive clients
    try:
        _generate_checkin_drafts(service, state)
    except Exception as e:
        log(f"ERROR generating check-in drafts: {e}")


# -- Proactive check-in drafts -----------------------------------------------
def _generate_checkin_drafts(service, state):
    """Create check-in draft emails for clients with no recent contact."""
    inactive = get_inactive_clients(days_threshold=CHECKIN_INACTIVE_DAYS)
    if not inactive:
        return

    # Only run once per day
    last_checkin = state.get("checkin_last_run", "")
    if last_checkin:
        try:
            if datetime.fromisoformat(last_checkin).date() == datetime.now().date():
                return
        except ValueError:
            pass

    log(f"Found {len(inactive)} inactive client(s) (>{CHECKIN_INACTIVE_DAYS} days)")
    created = 0

    for key, entry in inactive[:CHECKIN_MAX_DRAFTS]:
        last_contact = entry.get("last_contact", "")[:10]
        last_subject = entry.get("last_subject", "")
        contact_count = entry.get("contact_count", 0)

        # Build a simple check-in email
        # Use the key as a rough display name (domain or email)
        display = key.split("@")[0] if "@" in key else key.replace(".com", "").title()

        checkin_body = (
            f"Hi,\n\n"
            f"Just checking in to make sure everything is going well with your "
            f"security patrol coverage. We haven't heard from you in a little while "
            f"and wanted to make sure all is good on your end.\n\n"
            f"If there's anything we can do to improve our service or if you have "
            f"any questions, please don't hesitate to reach out.\n\n"
            f"Looking forward to hearing from you."
        )

        # Create as a draft (not in any thread -- new conversation)
        try:
            import base64
            from email.mime.text import MIMEText as _MIMEText

            full_body = f"{checkin_body}\n\n{SIGNATURE}"
            msg = _MIMEText(full_body, "plain")
            # Use key as the To address if it looks like an email, otherwise skip
            if "@" in key:
                to_addr = key
            else:
                # Domain key -- we can't send to a domain, skip
                log(f"  Skipping check-in for {key} (no specific email on file)")
                continue

            msg["To"] = to_addr
            msg["Subject"] = "Checking In -- Americal Patrol"
            raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")

            draft = service.users().drafts().create(
                userId="me",
                body={"message": {"raw": raw}},
            ).execute()
            draft_id = draft.get("id", "?")
            log(f"  Check-in draft created for {key} (last contact: {last_contact}, ID: {draft_id})")
            created += 1
        except Exception as e:
            log(f"  ERROR creating check-in draft for {key}: {e}")

    state["checkin_last_run"] = datetime.now().isoformat()
    if created:
        log(f"Created {created} proactive check-in draft(s)")


# -- Escalation body builder -------------------------------------------------
def _build_escalation_body(email, result):
    lines = [
        "Hi Sam,",
        "",
        "I received a client email that I'm not 100% sure how to respond to.",
        "Here's a summary:",
        "",
        "--- ORIGINAL EMAIL ---",
        f"From: {email.get('from', 'unknown')}",
        f"Subject: {email.get('subject', '(no subject)')}",
        f"Date: {email.get('date', 'unknown')}",
    ]

    # Note attachments if present
    attachments = email.get("attachments", [])
    if attachments:
        att_list = ", ".join(f"{a['name']} ({a['type']}, {a['size']})" for a in attachments)
        lines.append(f"Attachments ({len(attachments)}): {att_list}")

    lines.extend([
        "",
        email.get("body", "")[:2000],
        "",
        "--- MY ANALYSIS ---",
        f"Category: {result.get('category', 'unknown')}",
        f"Confidence: {result.get('confidence', 0):.0%}",
        f"Sentiment: {result.get('sentiment', 'N/A')}",
        "",
        result.get("escalation_summary", "(no summary)"),
        "",
    ])

    draft_body = result.get("draft_body", "").strip()
    if draft_body:
        lines.extend([
            "--- PROPOSED RESPONSE ---",
            draft_body,
            "",
        ])

    lines.extend([
        "--- WHAT I NEED ---",
        "Please reply to this email with:",
        "  1 = send my proposed response as-is",
        "  2 = type your edits (I'll create a draft for you to review)",
        "  3 = skip (you'll handle it directly)",
        "",
        "Thanks,",
        "Larry",
    ])

    return "\n".join(lines)


# -- Main entry point --------------------------------------------------------
def run():
    # Ensure data dir exists (persistent Docker volume at /app/data)
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)

    log("=" * 60)
    log("Email Assistant (Larry) V2 -- Starting")
    log("=" * 60)

    try:
        service = get_gmail_service()
    except Exception as e:
        log(f"FATAL: Could not connect to Gmail: {e}")
        return False

    state = load_state()
    prune_old_ids(state)

    # Ensure state has all required keys (upgrade from V1)
    state.setdefault("pending_escalations", {})
    state.setdefault("draft_log", {})
    state["stats"].setdefault("total_feedback_processed", 0)

    # Phase 1: Process feedback from Sam
    try:
        process_feedback(service, state)
    except Exception as e:
        log(f"ERROR in feedback processing: {e}")

    # Phase 2: Process new incoming emails
    try:
        process_new_emails(service, state)
    except Exception as e:
        log(f"ERROR processing new emails: {e}")

    # Phase 3: Housekeeping (aging, digest, learning)
    try:
        run_housekeeping(service, state)
    except Exception as e:
        log(f"ERROR in housekeeping: {e}")

    # Save state
    save_state(state)

    # Report health to watchdog
    try:
        from shared_utils.health_reporter import report_status
        stats = state.get("stats", {})
        report_status(
            "email_assistant",
            "ok",
            f"Processed: {stats.get('total_processed', 0)} | "
            f"Drafted: {stats.get('total_drafted', 0)} | "
            f"Escalated: {stats.get('total_escalated', 0)} | "
            f"Feedback: {stats.get('total_feedback_processed', 0)}",
            metrics=stats,
        )
    except Exception:
        pass

    log("=" * 60)
    log("Done")
    log("=" * 60)
    return True


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
