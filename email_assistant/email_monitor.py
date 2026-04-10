"""
Email Assistant (Larry) — Main Entry Point
Checks americalpatrol@gmail.com for client emails, drafts responses,
and escalates uncertain ones to Sam.

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
    is_client_email,
)
from email_assistant.gmail_client import (
    get_gmail_service,
    fetch_unread_emails,
    create_reply_draft,
    send_escalation_email,
)
from email_assistant.classifier import analyze_and_draft

# Event bus for cross-pipeline integration
try:
    from shared_utils.event_bus import publish_event
except ImportError:
    publish_event = None


# ── Logging ──────────────────────────────────────────────────────────────────
def log(msg):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# ── State management ─────────────────────────────────────────────────────────
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            log("WARNING: Corrupt state file — starting fresh")
    return {
        "version": 1,
        "processed_ids": {},
        "last_run": None,
        "stats": {
            "total_processed": 0,
            "total_drafted": 0,
            "total_escalated": 0,
            "total_skipped": 0,
        },
    }


def save_state(state):
    state["last_run"] = datetime.now().isoformat()
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(str(tmp), str(STATE_FILE))


def prune_old_ids(state, days=7):
    """Remove processed IDs older than `days` to prevent unbounded growth."""
    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    old_ids = state["processed_ids"]
    state["processed_ids"] = {
        mid: ts for mid, ts in old_ids.items() if ts > cutoff
    }


# ── Main ─────────────────────────────────────────────────────────────────────
def run():
    log("=" * 60)
    log("Email Assistant (Larry) — Starting")
    log("=" * 60)

    # Connect to Gmail
    try:
        service = get_gmail_service()
    except Exception as e:
        log(f"FATAL: Could not connect to Gmail: {e}")
        return False

    # Load state
    state = load_state()
    prune_old_ids(state)

    # Fetch unread emails
    try:
        emails = fetch_unread_emails(service, hours=SEARCH_WINDOW_HOURS)
    except Exception as e:
        log(f"ERROR fetching emails: {e}")
        save_state(state)
        return False

    log(f"Found {len(emails)} unread email(s) in last {SEARCH_WINDOW_HOURS}h")

    drafted = 0
    escalated = 0
    skipped = 0

    for email in emails:
        email_id = email["id"]

        # Skip already processed
        if email_id in state["processed_ids"]:
            continue

        sender = email.get("from", "unknown")
        subject = email.get("subject", "(no subject)")
        log(f"Processing: {subject} (from: {sender})")

        # Filter noise
        if not is_client_email(email):
            log(f"  -> Filtered out (noise)")
            state["processed_ids"][email_id] = datetime.now().isoformat()
            state["stats"]["total_skipped"] += 1
            skipped += 1
            continue

        # Analyze with Claude
        try:
            result = analyze_and_draft(email)
        except Exception as e:
            log(f"  -> Classifier error: {e} — skipping (will retry next run)")
            continue

        action = result.get("action", "skip")
        confidence = result.get("confidence", 0)
        category = result.get("category", "unknown")
        reasoning = result.get("reasoning", "")

        log(f"  -> Action: {action} | Confidence: {confidence:.2f} | Category: {category}")
        log(f"  -> Reasoning: {reasoning}")

        # Publish lead inquiries to event bus for sales pipeline ingestion
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
            # Create reply draft in Gmail
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
            except Exception as e:
                log(f"  -> ERROR creating draft: {e}")
                # Don't mark as processed — retry next run
                continue

        elif action == "escalate":
            # Send escalation email directly to Sam
            try:
                esc_subject = f"[Larry] Need guidance: {subject}"
                esc_body = _build_escalation_body(email, result)
                send_escalation_email(service, SAM_EMAIL, esc_subject, esc_body)
                log(f"  -> Escalation email sent to {SAM_EMAIL}")
                escalated += 1
                state["stats"]["total_escalated"] += 1
            except Exception as e:
                log(f"  -> ERROR sending escalation: {e}")
                continue

        else:
            log(f"  -> Skipped")
            skipped += 1
            state["stats"]["total_skipped"] += 1

        # Mark processed
        state["processed_ids"][email_id] = datetime.now().isoformat()
        state["stats"]["total_processed"] += 1

    # Save state
    save_state(state)

    log(f"Done. Drafted: {drafted} | Escalated: {escalated} | Skipped: {skipped}")
    log("=" * 60)
    return True


def _build_escalation_body(email, result):
    """Build the escalation email body for Sam."""
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
        "",
        email.get("body", "")[:2000],
        "",
        "--- MY ANALYSIS ---",
        f"Category: {result.get('category', 'unknown')}",
        f"Confidence: {result.get('confidence', 0):.0%}",
        "",
        result.get("escalation_summary", "(no summary)"),
        "",
    ]

    # Include proposed response if one was drafted
    draft_body = result.get("draft_body", "").strip()
    if draft_body:
        lines.extend([
            "--- PROPOSED RESPONSE ---",
            draft_body,
            "",
        ])

    lines.extend([
        "--- WHAT I NEED ---",
        "Please let me know:",
        "1. Should I send this response as-is?",
        "2. Should I modify it? (reply with edits)",
        "3. Should I skip this one? (you'll handle it directly)",
        "",
        "Thanks,",
        "Larry",
    ])

    return "\n".join(lines)


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
