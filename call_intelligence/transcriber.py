"""
Call Intelligence — Audio Transcription
Transcribes call recordings using faster-whisper with SpeechRecognition fallback.
"""

import logging
import os
import tempfile
from collections import namedtuple

log = logging.getLogger(__name__)

TranscriptResult = namedtuple("TranscriptResult", ["text", "source", "word_count"])


def transcribe_audio_bytes(audio_data: bytes, model_name: str = "base") -> TranscriptResult:
    """Transcribe WAV audio bytes. Returns TranscriptResult(text, source, word_count)."""
    try:
        text = _transcribe_with_whisper(audio_data, model_name)
        if text:
            return TranscriptResult(text, "whisper", len(text.split()))
    except Exception as e:
        log.warning("faster-whisper failed (%s), trying SpeechRecognition fallback", e)

    try:
        text = _transcribe_with_speech_recognition(audio_data)
        if text:
            return TranscriptResult(text, "whisper_fallback", len(text.split()))
    except Exception as e:
        log.error("All transcription methods failed: %s", e)

    return TranscriptResult("", "unavailable", 0)


def extract_ghl_native_transcript(message: dict) -> TranscriptResult | None:
    """
    Extract transcript from GHL message body if it looks like real text.
    Returns None if body is empty or too short.
    """
    body = (message.get("body") or "").strip()
    if not body or len(body.split()) < 20:
        return None
    return TranscriptResult(body, "ghl_native", len(body.split()))


def _transcribe_with_whisper(audio_data: bytes, model_name: str = "base") -> str:
    """Transcribe using faster-whisper local model."""
    from faster_whisper import WhisperModel

    with tempfile.NamedTemporaryFile(suffix=".wav", delete=False) as tmp:
        tmp.write(audio_data)
        tmp_path = tmp.name

    try:
        model = WhisperModel(model_name, device="cpu", compute_type="int8")
        segments, info = model.transcribe(tmp_path, language="en")
        text = " ".join(seg.text.strip() for seg in segments)
        log.info("Whisper transcription complete (%.0fs audio, lang=%s)",
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
