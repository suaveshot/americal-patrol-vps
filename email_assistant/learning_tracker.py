"""
Email Assistant (Larry) -- Learning Tracker
Tracks Sam's edits to Larry's drafts and builds a style guide over time.
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

LEARNING_DIR = Path(__file__).resolve().parent / "learning"
STYLE_GUIDE_FILE = LEARNING_DIR / "style_guide.json"


def record_draft(state, email_id, draft_body, recipient, subject):
    """Record that Larry created a draft, so we can later check if Sam edited it."""
    if "draft_log" not in state:
        state["draft_log"] = {}
    state["draft_log"][email_id] = {
        "draft_body": draft_body,
        "recipient": recipient,
        "subject": subject,
        "created_at": datetime.now().isoformat(),
        "checked": False,
    }


def check_for_edits(service, state, log_fn):
    """Check if Sam sent any of Larry's drafts with modifications."""
    from email_assistant.gmail_client import find_sent_version_of_draft

    draft_log = state.get("draft_log", {})
    if not draft_log:
        return

    edits_found = 0
    for email_id, entry in list(draft_log.items()):
        if entry.get("checked"):
            continue
        try:
            created = datetime.fromisoformat(entry["created_at"])
            if datetime.now() - created < timedelta(hours=1):
                continue
        except (ValueError, KeyError):
            continue
        if datetime.now() - created > timedelta(days=7):
            entry["checked"] = True
            continue

        sent_body = find_sent_version_of_draft(
            service,
            original_to=entry["recipient"],
            original_subject=entry["subject"],
            draft_created_after=created,
        )
        if sent_body is None:
            continue

        entry["checked"] = True
        original = entry["draft_body"].strip()
        sent = sent_body.strip()

        sig_marker = "Best Regards,"
        if sig_marker in sent:
            sent = sent[:sent.index(sig_marker)].strip()

        if original == sent:
            log_fn(f"  Draft for {entry['recipient']} sent as-is (no edits)")
            continue

        edits_found += 1
        _save_edit_record(entry, original, sent, log_fn)

    if edits_found:
        log_fn(f"  {edits_found} edited draft(s) detected -- learning recorded")

    cutoff = (datetime.now() - timedelta(days=14)).isoformat()
    state["draft_log"] = {
        eid: e for eid, e in draft_log.items()
        if e.get("created_at", "") > cutoff
    }


def _save_edit_record(entry, original, sent_version, log_fn):
    """Save an edit record for later pattern analysis."""
    LEARNING_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filename = LEARNING_DIR / f"edit_{timestamp}.json"
    record = {
        "recipient": entry["recipient"],
        "subject": entry["subject"],
        "original_draft": original,
        "sent_version": sent_version,
        "created_at": entry["created_at"],
        "recorded_at": datetime.now().isoformat(),
    }
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2)
        log_fn(f"  Edit record saved: {filename.name}")
    except Exception as e:
        log_fn(f"  ERROR saving edit record: {e}")


def update_style_guide(log_fn):
    """Analyze accumulated edit records and update the style guide."""
    LEARNING_DIR.mkdir(parents=True, exist_ok=True)
    edits = []
    for f in sorted(LEARNING_DIR.glob("edit_*.json")):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            edits.append(data)
        except (json.JSONDecodeError, OSError):
            continue

    if len(edits) < 3:
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return

    import anthropic

    edit_summaries = []
    for edit in edits[-20:]:
        edit_summaries.append(
            f"Original: {edit['original_draft'][:300]}\n"
            f"Sam's version: {edit['sent_version'][:300]}\n---"
        )

    prompt = (
        "Analyze these before/after pairs where 'Original' is what an AI assistant drafted "
        "and 'Sam's version' is what the human edited it to before sending.\n\n"
        "Identify 3-8 concrete, actionable patterns. Examples of good patterns:\n"
        "- 'Remove phrases like please don't hesitate to reach out'\n"
        "- 'Keep greetings to one short line, not two'\n"
        "- 'Always include a specific next step, not a generic offer'\n\n"
        "Return ONLY a JSON array of pattern strings. No explanation.\n\n"
        + "\n\n".join(edit_summaries)
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3].strip()
        if text.startswith("json"):
            text = text[4:].strip()

        patterns = json.loads(text)
        if not isinstance(patterns, list):
            return

        guide = {
            "patterns": patterns,
            "updated_at": datetime.now().isoformat(),
            "based_on_edits": len(edits),
        }
        with open(STYLE_GUIDE_FILE, "w", encoding="utf-8") as f:
            json.dump(guide, f, indent=2)
        log_fn(f"  Style guide updated with {len(patterns)} patterns from {len(edits)} edits")
    except Exception as e:
        log_fn(f"  ERROR updating style guide: {e}")
