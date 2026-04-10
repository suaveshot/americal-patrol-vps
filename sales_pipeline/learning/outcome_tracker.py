"""
Sales Pipeline — Learning: Outcome Tracker
Records every message sent with its attributes, then evaluates
outcomes (reply vs no-reply) after a 7-day window.

Data stored in outcome_log.jsonl — one JSON object per line, append-only.
"""

import json
import logging
import re
from datetime import datetime, timezone

from sales_pipeline.config import OUTCOME_LOG_FILE
from sales_pipeline.state import _parse_iso

log = logging.getLogger(__name__)

EVALUATION_WINDOW_DAYS = 7
COLD_START_THRESHOLD = 30


# ---------------------------------------------------------------------------
# Message attribute classifiers
# ---------------------------------------------------------------------------

def _classify_subject_style(subject: str) -> str:
    """Classify subject line into style categories."""
    if not subject:
        return "none"
    subject = subject.strip().lower()
    if subject.endswith("?"):
        return "question"
    if len(subject.split()) <= 3:
        return "short"
    if re.match(r"^(hi|hey)\s+\w+", subject):
        return "name_greeting"
    return "descriptive"


def _classify_cta_type(body: str) -> str:
    """Classify the call-to-action type from message body."""
    body_lower = body.lower()
    if "schedule" in body_lower or "calendar" in body_lower or "book" in body_lower:
        return "calendar"
    if "?" in body:
        return "question"
    if "reply" in body_lower or "let me know" in body_lower:
        return "soft_ask"
    return "statement"


def _classify_opening(body: str) -> str:
    """Classify the opening style of the message."""
    first_line = body.strip().split("\n")[0].lower() if body else ""
    if any(w in first_line for w in ["noticed", "saw", "recently", "just"]):
        return "timeline_hook"
    if any(w in first_line for w in ["struggle", "challenge", "problem", "risk"]):
        return "pain_point"
    if any(w in first_line for w in ["wanted to", "reaching out", "following up"]):
        return "intro"
    return "other"


def _word_count_bucket(body: str) -> str:
    """Bucket word count for analysis."""
    wc = len(body.split()) if body else 0
    if wc <= 50:
        return "under_50"
    if wc <= 80:
        return "50_80"
    if wc <= 125:
        return "80_125"
    return "over_125"


# ---------------------------------------------------------------------------
# Record outcomes
# ---------------------------------------------------------------------------

def record_outcome(
    contact_id: str,
    channel: str,
    touch_number: int,
    phase: str,
    subject: str = "",
    body: str = "",
    property_type: str = "other",
    enrichment_used: bool = False,
    variant: str = "",
) -> None:
    """Append an outcome record to the JSONL log."""
    now = datetime.now(timezone.utc)
    record = {
        "contact_id": contact_id,
        "sent_at": now.isoformat(),
        "send_day": now.strftime("%A"),
        "channel": channel,
        "touch_number": touch_number,
        "phase": phase,
        "subject": subject,
        "subject_style": _classify_subject_style(subject),
        "body_preview": (body or "")[:500],
        "word_count": len(body.split()) if body else 0,
        "word_count_bucket": _word_count_bucket(body),
        "cta_type": _classify_cta_type(body),
        "opening_style": _classify_opening(body),
        "property_type": property_type,
        "enrichment_used": enrichment_used,
        "variant": variant,
        "outcome": "pending",
        "evaluated_at": None,
    }

    with open(OUTCOME_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    log.info("Outcome recorded for %s (touch %d, %s)", contact_id, touch_number, channel)


# ---------------------------------------------------------------------------
# Evaluate pending outcomes
# ---------------------------------------------------------------------------

def evaluate_pending_outcomes(ghl_client) -> dict:
    """
    Check outcomes that are 7+ days old and mark as replied or no_reply.
    Returns {evaluated: int, replied: int, no_reply: int}.
    """
    if not OUTCOME_LOG_FILE.exists():
        return {"evaluated": 0, "replied": 0, "no_reply": 0}

    now = datetime.now(timezone.utc)
    lines = OUTCOME_LOG_FILE.read_text(encoding="utf-8").strip().split("\n")
    updated_lines = []
    stats = {"evaluated": 0, "replied": 0, "no_reply": 0}

    for line in lines:
        if not line.strip():
            updated_lines.append(line)
            continue

        record = json.loads(line)

        if record.get("outcome") != "pending":
            updated_lines.append(line)
            continue

        sent_at = record.get("sent_at", "")
        if not sent_at:
            updated_lines.append(line)
            continue

        sent_dt = _parse_iso(sent_at)
        days_elapsed = (now - sent_dt).total_seconds() / 86400

        if days_elapsed < EVALUATION_WINDOW_DAYS:
            updated_lines.append(line)
            continue

        # Check GHL for inbound reply since send time
        contact_id = record["contact_id"]
        replied = False
        try:
            conversations = ghl_client.search_conversations(contact_id)
            for conv in conversations:
                conv_id = conv.get("id")
                if not conv_id:
                    continue
                messages = ghl_client.get_conversation_messages(conv_id)
                for msg in messages:
                    if msg.get("direction") != "inbound":
                        continue
                    msg_ts = msg.get("dateAdded", "")
                    if msg_ts:
                        msg_dt = _parse_iso(msg_ts)
                        if msg_dt > sent_dt:
                            replied = True
                            break
                if replied:
                    break
        except Exception as e:
            log.warning("Error checking reply for %s: %s", contact_id, e)
            updated_lines.append(line)
            continue

        record["outcome"] = "replied" if replied else "no_reply"
        record["evaluated_at"] = now.isoformat()
        updated_lines.append(json.dumps(record))

        stats["evaluated"] += 1
        if replied:
            stats["replied"] += 1
        else:
            stats["no_reply"] += 1

    # Atomic rewrite
    tmp = OUTCOME_LOG_FILE.with_suffix(".tmp")
    tmp.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
    tmp.replace(OUTCOME_LOG_FILE)

    if stats["evaluated"]:
        log.info(
            "Evaluated %d outcomes: %d replied, %d no_reply",
            stats["evaluated"], stats["replied"], stats["no_reply"],
        )

    return stats


# ---------------------------------------------------------------------------
# Data access for analyzer
# ---------------------------------------------------------------------------

def load_all_outcomes() -> list:
    """Load all outcome records from the JSONL log."""
    if not OUTCOME_LOG_FILE.exists():
        return []
    results = []
    for line in OUTCOME_LOG_FILE.read_text(encoding="utf-8").strip().split("\n"):
        if line.strip():
            results.append(json.loads(line))
    return results


def get_finalized_outcomes() -> list:
    """Return only outcomes that have been evaluated (not pending)."""
    return [r for r in load_all_outcomes() if r.get("outcome") != "pending"]


def get_outcome_count() -> int:
    """Count total finalized outcomes."""
    return len(get_finalized_outcomes())


def is_cold_start() -> bool:
    """True if we don't have enough data for analysis yet."""
    return get_outcome_count() < COLD_START_THRESHOLD
