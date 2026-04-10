"""
Call Intelligence — Hourly Call Ingestion
Scans GHL for new call recordings, transcribes, and analyzes them.
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from call_intelligence import config
from call_intelligence.config import (
    DATA_DIR, STATE_FILE, RECORDINGS_DIR, LOG_FILE, load_config,
)
from call_intelligence.db import (
    get_connection, get_call_by_message_id, insert_call,
    insert_transcript, insert_call_scores, insert_call_analysis,
)
from call_intelligence.transcriber import transcribe_audio_bytes, extract_ghl_native_transcript
from call_intelligence.call_analyzer import analyze_call
from sales_pipeline.ghl_client import GHLClient, GHLAPIError
from shared_utils.event_bus import publish_event
from shared_utils.health_reporter import report_status

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("call_intelligence")


# ── State I/O ────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {
        "last_scan_at": None,
        "total_calls_processed": 0,
        "total_calls_failed": 0,
        "last_deal_sync_at": None,
        "backfill_status": "pending",
        "backfill_last_contact_index": 0,
    }


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


# ── GHL Conversation Scanning ────────────────────────────────────

def fetch_recent_conversations(ghl: GHLClient, lookback_hours: int = 2) -> list:
    """
    Get conversations with recent activity. Uses GHL search with date filter.
    Falls back to fetching all contacts' conversations if date filter unsupported.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    try:
        params = {
            "locationId": ghl._location_id,
            "sortBy": "last_message_date",
            "sortOrder": "desc",
            "limit": 50,
        }
        data = ghl._get("/conversations/search", params=params)
        conversations = data.get("conversations", [])

        # Filter to conversations with activity after our cutoff
        recent = []
        for conv in conversations:
            last_msg_date = conv.get("lastMessageDate") or conv.get("dateUpdated", "")
            if last_msg_date:
                try:
                    # Parse the date string, handling both Z and +00:00 formats
                    date_str = last_msg_date.replace("Z", "+00:00")
                    msg_dt = datetime.fromisoformat(date_str)
                    if msg_dt >= cutoff:
                        recent.append(conv)
                except (ValueError, TypeError):
                    recent.append(conv)  # include if date can't be parsed
            else:
                recent.append(conv)  # include if we can't determine date

        log.info("Found %d conversations with recent activity (of %d total)",
                 len(recent), len(conversations))
        return recent

    except GHLAPIError as e:
        log.warning("Conversation search failed (%s), falling back to recent contacts", e)
        return _fallback_recent_conversations(ghl, lookback_hours)


def _fallback_recent_conversations(ghl: GHLClient, lookback_hours: int) -> list:
    """Fallback: fetch recent contacts and get their conversations."""
    try:
        params = {
            "locationId": ghl._location_id,
            "limit": 50,
            "sortBy": "last_activity",
            "sortOrder": "desc",
        }
        data = ghl._get("/contacts/", params)
        contacts = data.get("contacts", [])

        conversations = []
        for contact in contacts[:20]:
            contact_id = contact.get("id")
            if not contact_id:
                continue
            try:
                convs = ghl.search_conversations(contact_id)
                conversations.extend(convs)
                time.sleep(0.3)
            except GHLAPIError:
                continue
        return conversations
    except Exception as e:
        log.error("Fallback conversation fetch failed: %s", e)
        return []


# ── Core Call Processor ──────────────────────────────────────────

def process_call_message(ghl: GHLClient, conn, cfg: dict, msg: dict,
                         contact_id: str, conversation_id: str) -> bool:
    """
    Process a single call message. Transcribes, analyzes, stores.
    Returns True if a new call was processed, False if skipped/failed.
    """
    msg_id = msg.get("id", "")
    meta = msg.get("meta", {}).get("call", {})
    duration = meta.get("duration", 0) or 0
    call_status = meta.get("status", "")

    # Skip short or incomplete calls
    min_duration = cfg.get("min_call_duration_seconds", 10)
    if duration < min_duration:
        return False
    if call_status != "completed":
        return False

    # Dedup check
    if get_call_by_message_id(conn, msg_id):
        return False

    direction = msg.get("direction", "outbound")
    call_timestamp = msg.get("dateAdded", datetime.now(timezone.utc).isoformat())
    caller_phone = msg.get("phone", msg.get("from", ""))
    now = datetime.now(timezone.utc).isoformat()

    # Resolve contact name
    contact_name = ""
    company_name = ""
    try:
        contact = ghl.get_contact(contact_id)
        contact_name = f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
        company_name = contact.get("companyName", "")
    except Exception:
        pass

    # Insert call record
    call_id = insert_call(
        conn,
        ghl_message_id=msg_id,
        ghl_contact_id=contact_id,
        ghl_conversation_id=conversation_id,
        ghl_opportunity_id=None,
        direction=direction,
        duration_seconds=duration,
        call_status=call_status,
        caller_phone=caller_phone,
        contact_name=contact_name or "Unknown",
        company_name=company_name,
        call_timestamp=call_timestamp,
        recording_path=None,
        created_at=now,
    )

    if not call_id:
        return False  # INSERT OR IGNORE hit a dupe

    # Transcribe and analyze
    transcript_result = extract_ghl_native_transcript(msg)
    analysis = None

    try:
        if not transcript_result:
            try:
                audio = ghl.download_call_recording(msg_id)
                if audio:
                    whisper_model = cfg.get("whisper_model", "base")
                    transcript_result = transcribe_audio_bytes(audio, whisper_model)

                    # Save recording
                    RECORDINGS_DIR.mkdir(parents=True, exist_ok=True)
                    rec_path = RECORDINGS_DIR / f"{msg_id}.wav"
                    rec_path.write_bytes(audio)
                    conn.execute(
                        "UPDATE calls SET recording_path = ? WHERE id = ?",
                        (str(rec_path), call_id),
                    )
                else:
                    log.info("No recording available for call %s", msg_id)
            except Exception as e:
                log.warning("Recording download failed for %s: %s", msg_id, e)

        # Store transcript
        if transcript_result and transcript_result.text:
            insert_transcript(
                conn,
                call_id=call_id,
                full_transcript=transcript_result.text,
                source=transcript_result.source,
                word_count=transcript_result.word_count,
                transcribed_at=now,
            )

            # Analyze with Claude
            claude_model = cfg.get("claude_model", "claude-sonnet-4-6")
            analysis = analyze_call(
                transcript=transcript_result.text,
                contact_name=contact_name,
                company_name=company_name,
                direction=direction,
                duration_seconds=duration,
                model_name=claude_model,
            )

            if analysis:
                scores = analysis.get("scores", {})
                insert_call_scores(conn, call_id=call_id, scores=scores, scored_at=now)
                insert_call_analysis(conn, call_id=call_id, analysis=analysis, analyzed_at=now)
        else:
            log.info("No transcript available for call %s, metadata only", msg_id)

    finally:
        # Always commit the call record, even if analysis failed
        conn.commit()

    # Publish event (outside try/finally so commit is guaranteed first)
    try:
        publish_event("call_intelligence", "call_processed", {
            "call_id": call_id,
            "contact_id": contact_id,
            "contact_name": contact_name,
            "direction": direction,
            "duration": duration,
            "has_transcript": bool(transcript_result and transcript_result.text),
            "composite_score": (
                analysis.get("scores", {}).get("composite_score")
                if analysis else None
            ),
        })
    except Exception as e:
        log.warning("Event publish failed for call %s: %s", msg_id, e)

    log.info("Processed call %s: %s %s %ds score=%s",
             msg_id, contact_name, direction, duration,
             analysis.get("scores", {}).get("composite_score") if analysis else "N/A")
    return True


# ── Recording Cleanup ────────────────────────────────────────────

def cleanup_old_recordings(retention_days: int = 90) -> int:
    """Delete recordings older than retention period. Returns count deleted."""
    if not RECORDINGS_DIR.exists():
        return 0
    cutoff = datetime.now() - timedelta(days=retention_days)
    deleted = 0
    for f in RECORDINGS_DIR.glob("*.wav"):
        if datetime.fromtimestamp(f.stat().st_mtime) < cutoff:
            f.unlink()
            deleted += 1
    if deleted:
        log.info("Cleaned up %d recordings older than %d days", deleted, retention_days)
    return deleted


# ── Main Entry Point ─────────────────────────────────────────────

def run():
    log.info("=== Call Intelligence Ingestion Starting ===")

    config.validate_config()
    cfg = load_config()
    state = load_state()

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ghl = GHLClient()
    conn = get_connection()

    lookback = cfg.get("scan_lookback_hours", 2)
    processed = 0
    failed = 0
    skipped = 0

    try:
        conversations = fetch_recent_conversations(ghl, lookback)
        log.info("Scanning %d conversations for calls", len(conversations))

        for conv in conversations:
            conv_id = conv.get("id")
            contact_id = conv.get("contactId", "")
            if not conv_id:
                continue

            try:
                messages = ghl.get_conversation_messages(conv_id)
            except GHLAPIError as e:
                log.warning("Failed to fetch messages for conversation %s: %s", conv_id, e)
                failed += 1
                continue

            for msg in messages:
                if msg.get("messageType") != "TYPE_CALL":
                    continue

                try:
                    if process_call_message(ghl, conn, cfg, msg, contact_id, conv_id):
                        processed += 1
                    else:
                        skipped += 1
                except Exception as e:
                    log.error("Failed to process call %s: %s", msg.get("id"), e)
                    failed += 1

            time.sleep(0.3)

        # Cleanup old recordings
        retention = cfg.get("recordings_retention_days", 90)
        cleanup_old_recordings(retention)

    except Exception as e:
        log.error("Ingestion run failed: %s", e)
        report_status("call_intelligence", "error", str(e))
        raise
    finally:
        conn.close()

    # Update state
    state["last_scan_at"] = datetime.now(timezone.utc).isoformat()
    state["total_calls_processed"] = state.get("total_calls_processed", 0) + processed
    state["total_calls_failed"] = state.get("total_calls_failed", 0) + failed
    save_state(state)

    status_msg = f"Processed {processed}, skipped {skipped}, failed {failed}"
    log.info("=== Ingestion complete: %s ===", status_msg)
    report_status("call_intelligence", "ok", status_msg, metrics={
        "calls_processed": processed,
        "calls_skipped": skipped,
        "calls_failed": failed,
    })


if __name__ == "__main__":
    run()
