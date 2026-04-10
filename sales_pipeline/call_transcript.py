"""
Sales Pipeline — Call Transcript Retrieval & Storage

Downloads call recordings from GHL, transcribes with faster-whisper,
summarizes with Claude, and stores transcripts for use in follow-ups.
"""

import json
import logging
import os
import re
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from sales_pipeline.config import TRANSCRIPTS_FILE, ANTHROPIC_API_KEY

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Transcript storage I/O
# ---------------------------------------------------------------------------

def load_transcripts(path=None) -> dict:
    """Load call transcripts from JSON file."""
    file = Path(path) if path is not None else TRANSCRIPTS_FILE
    if not file.exists():
        return {"version": 1, "contacts": {}}
    try:
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)
        data.setdefault("version", 1)
        data.setdefault("contacts", {})
        return data
    except (json.JSONDecodeError, OSError):
        return {"version": 1, "contacts": {}}


def save_transcripts(data: dict, path=None) -> None:
    """Atomically write transcripts dict to JSON file."""
    file = Path(path) if path is not None else TRANSCRIPTS_FILE
    tmp = file.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, file)


# ---------------------------------------------------------------------------
# Transcript access
# ---------------------------------------------------------------------------

def get_contact_transcripts(transcripts: dict, contact_id: str) -> list:
    """Return sorted list of call records for a contact."""
    entry = transcripts.get("contacts", {}).get(contact_id, {})
    calls = entry.get("calls", [])
    return sorted(calls, key=lambda c: c.get("timestamp", ""))


def get_contact_call_context(transcripts: dict, contact_id: str,
                             since_iso: str = None) -> str:
    """
    Build a formatted string of call summaries for injecting into Claude prompts.
    Optionally filtered to calls after since_iso.
    """
    calls = get_contact_transcripts(transcripts, contact_id)
    if since_iso:
        calls = [c for c in calls if c.get("timestamp", "") >= since_iso]
    if not calls:
        return ""

    lines = []
    for call in calls:
        direction = "SAM CALLED THEM" if call.get("direction") == "outbound" else "THEY CALLED US"
        duration = call.get("duration_seconds", 0)
        ts = call.get("timestamp", "")[:16].replace("T", " ")
        summary = call.get("summary") or call.get("transcript", "")[:300]
        lines.append(f"[PHONE CALL {ts} {duration}s {direction}]: {summary}")

    return (
        "\nPhone call history (reference what was discussed/promised during these calls — "
        "maintain continuity with the conversation):\n"
        + "\n".join(lines)
    )


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------

def _transcribe_audio(audio_data: bytes) -> str:
    """Transcribe WAV audio bytes. Tries faster-whisper, falls back to SpeechRecognition."""
    try:
        return _transcribe_with_whisper(audio_data)
    except Exception as e:
        log.warning("faster-whisper failed (%s), trying SpeechRecognition fallback", e)
        try:
            return _transcribe_with_speech_recognition(audio_data)
        except Exception as e2:
            log.error("All transcription methods failed: %s", e2)
            return ""


def _transcribe_with_whisper(audio_data: bytes) -> str:
    """Transcribe using faster-whisper local model."""
    from faster_whisper import WhisperModel

    # Save to temp WAV file
    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp.write(audio_data)
        tmp_path = tmp.name

    try:
        model = WhisperModel("base", device="cpu", compute_type="int8")
        segments, info = model.transcribe(tmp_path, language="en")
        text = " ".join(seg.text.strip() for seg in segments)
        log.info("Whisper transcription complete (%.0fs audio, detected lang=%s)",
                 info.duration, info.language)
        return text.strip()
    finally:
        os.unlink(tmp_path)


def _transcribe_with_speech_recognition(audio_data: bytes) -> str:
    """Fallback transcription using SpeechRecognition + Google free API."""
    import speech_recognition as sr

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp.write(audio_data)
        tmp_path = tmp.name

    try:
        recognizer = sr.Recognizer()
        with sr.AudioFile(tmp_path) as source:
            audio = recognizer.record(source)
        return recognizer.recognize_google(audio)
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# Claude summarization
# ---------------------------------------------------------------------------

def _summarize_transcript(transcript: str, contact_name: str,
                          direction: str) -> str:
    """Generate a 2-3 sentence summary of a call transcript using Claude."""
    if not transcript or len(transcript.split()) < 5:
        return transcript

    try:
        import anthropic
    except ImportError:
        log.warning("anthropic not installed, skipping summary")
        return transcript[:400]

    api_key = ANTHROPIC_API_KEY()
    if not api_key:
        return transcript[:400]

    dir_label = "outbound call TO" if direction == "outbound" else "inbound call FROM"

    prompt = (
        f"You are reviewing a transcript of an {dir_label} {contact_name}, "
        "a prospective security services client of Americal Patrol.\n\n"
        "Write a concise 2-3 sentence summary focusing on:\n"
        "1. What was discussed (their security needs, concerns)\n"
        "2. What was promised or agreed to (next steps, site visits, quotes)\n"
        "3. Any objections, questions, or specific requests they raised\n\n"
        "Be specific — include names, locations, and details mentioned.\n\n"
        f"Transcript:\n{transcript[:3000]}"
    )

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        return response.content[0].text.strip()
    except Exception as e:
        log.warning("Claude summarization failed: %s", e)
        return transcript[:400]


# ---------------------------------------------------------------------------
# Process calls for a contact
# ---------------------------------------------------------------------------

def process_new_calls(ghl_client, contact_id: str, transcripts: dict) -> int:
    """
    Fetch conversation messages for a contact, find new TYPE_CALL messages,
    download recordings, transcribe, and store. Returns count of new transcripts.
    """
    # Get existing message IDs to skip
    entry = transcripts.get("contacts", {}).get(contact_id, {})
    existing_ids = {c["message_id"] for c in entry.get("calls", [])}

    # Fetch all conversations for the contact
    conversations = ghl_client.search_conversations(contact_id)
    new_count = 0

    for conv in conversations:
        conv_id = conv.get("id")
        if not conv_id:
            continue

        messages = ghl_client.get_conversation_messages(conv_id)

        for msg in messages:
            msg_id = msg.get("id", "")
            msg_type = msg.get("messageType", "")

            # Only process completed calls
            if msg_type != "TYPE_CALL":
                continue
            meta = msg.get("meta", {}).get("call", {})
            if meta.get("status") != "completed" or (meta.get("duration", 0) or 0) < 5:
                continue
            if msg_id in existing_ids:
                continue

            direction = msg.get("direction", "outbound")
            duration = meta.get("duration", 0)
            timestamp = msg.get("dateAdded", "")

            log.info("Processing call %s (%s, %ds) for contact %s",
                     msg_id, direction, duration, contact_id)

            # Try to get transcript from message body first (GHL auto-transcription)
            body = (msg.get("body") or "").strip()
            if body and len(body.split()) > 20:
                transcript_text = body
                source = "ghl_transcript"
                log.info("Using GHL auto-transcript for call %s", msg_id)
            else:
                # Download and transcribe the recording
                audio = ghl_client.download_call_recording(msg_id)
                if not audio:
                    log.warning("No recording available for call %s, skipping", msg_id)
                    continue
                transcript_text = _transcribe_audio(audio)
                source = "whisper_transcription"
                if not transcript_text:
                    log.warning("Transcription returned empty for call %s", msg_id)
                    continue

            # Get contact name for summary
            try:
                contact = ghl_client.get_contact(contact_id)
                contact_name = (
                    f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
                    or "the prospect"
                )
            except Exception:
                contact_name = "the prospect"

            # Generate summary
            summary = _summarize_transcript(transcript_text, contact_name, direction)

            # Store
            call_record = {
                "message_id": msg_id,
                "timestamp": timestamp,
                "direction": direction,
                "duration_seconds": duration,
                "transcript": transcript_text[:5000],
                "summary": summary,
                "source": source,
                "transcribed_at": datetime.now(timezone.utc).isoformat(),
            }

            if contact_id not in transcripts["contacts"]:
                transcripts["contacts"][contact_id] = {"calls": []}
            transcripts["contacts"][contact_id]["calls"].append(call_record)
            transcripts["contacts"][contact_id]["last_processed_at"] = (
                datetime.now(timezone.utc).isoformat()
            )
            existing_ids.add(msg_id)
            new_count += 1
            log.info("Transcribed call %s: %s", msg_id, summary[:100])

    return new_count


def process_all_active_contacts(ghl_client) -> dict:
    """
    Process new calls for all active contacts in pipeline state.
    Returns stats dict.
    """
    from sales_pipeline.state import load_state

    state = load_state()
    transcripts = load_transcripts()
    contacts = state.get("contacts", {})

    total_processed = 0
    contacts_with_calls = 0

    # Only process active contacts (not completed/won/lost)
    active_stages = {
        "discovered", "cold_drafted", "cold_sent",
        "cold_follow_up_1", "cold_follow_up_2", "cold_follow_up_3", "cold_follow_up_4",
        "engaged", "proposal_sent",
        "post_proposal_1", "post_proposal_2", "post_proposal_3", "post_proposal_4",
        "negotiating", "nurture_monthly",
    }

    for contact_id, info in contacts.items():
        stage = info.get("stage", "")
        if stage not in active_stages:
            continue

        try:
            count = process_new_calls(ghl_client, contact_id, transcripts)
            if count > 0:
                total_processed += count
                contacts_with_calls += 1
        except Exception as e:
            log.warning("Failed to process calls for %s: %s", contact_id, e)

    if total_processed > 0:
        save_transcripts(transcripts)
        log.info("Transcribed %d new calls across %d contacts",
                 total_processed, contacts_with_calls)

    return {
        "processed": total_processed,
        "contacts_with_calls": contacts_with_calls,
    }


# ---------------------------------------------------------------------------
# Webhook-triggered: process contacts tagged "pending-transcription"
# ---------------------------------------------------------------------------

TRANSCRIPTION_TAG = "pending-transcription"


def process_tagged_contacts(ghl_client, transcripts: dict) -> int:
    """
    Find GHL contacts tagged 'pending-transcription' (set by GHL workflow
    via n8n webhook), transcribe their calls, and remove the tag.
    Returns count of calls transcribed.
    """
    # Search for contacts with the tag
    try:
        params = {
            "locationId": ghl_client._location_id,
            "query": "",
            "limit": 20,
        }
        data = ghl_client._get("/contacts/", params)
        all_contacts = data.get("contacts", [])
    except Exception as e:
        log.warning("Failed to fetch contacts for tag check: %s", e)
        return 0

    total = 0
    for contact in all_contacts:
        tags = contact.get("tags", [])
        if TRANSCRIPTION_TAG not in tags:
            continue

        contact_id = contact.get("id", "")
        contact_name = contact.get("contactName", contact_id)
        log.info("Found tagged contact for transcription: %s (%s)", contact_name, contact_id)

        try:
            count = process_new_calls(ghl_client, contact_id, transcripts)
            total += count

            # Remove the tag after processing
            updated_tags = [t for t in tags if t != TRANSCRIPTION_TAG]
            ghl_client.update_contact(contact_id, {"tags": updated_tags})
            log.info("Removed '%s' tag from %s", TRANSCRIPTION_TAG, contact_name)
        except Exception as e:
            log.warning("Failed to process tagged contact %s: %s", contact_id, e)

    return total
