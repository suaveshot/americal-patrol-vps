"""
Sales Pipeline — Automatic Call Transcription

Standalone script for Task Scheduler. Runs every 15 minutes to:
1. Check for GHL contacts tagged "pending-transcription" (webhook-triggered)
2. Scan active pipeline contacts for new unprocessed calls
3. Download recordings, transcribe with Whisper, summarize with Claude
4. Store transcripts in call_transcripts.json for follow-up use

Usage:
    python -m sales_pipeline.transcribe_calls
"""

import logging
import sys
import time

from sales_pipeline import config
from sales_pipeline.call_transcript import (
    load_transcripts, save_transcripts,
    process_new_calls, process_tagged_contacts,
)

# Set up logging to match other pipeline scripts
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(config.LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("transcribe_calls")


def main():
    config.validate_config()

    from sales_pipeline.ghl_client import GHLClient
    ghl = GHLClient()

    transcripts = load_transcripts()
    total = 0

    # Phase 1: Process webhook-triggered contacts (tagged "pending-transcription")
    try:
        tagged_count = process_tagged_contacts(ghl, transcripts)
        total += tagged_count
        if tagged_count:
            log.info("Processed %d tagged contacts", tagged_count)
    except Exception as e:
        log.warning("Tagged contact processing failed: %s", e)

    # Phase 2: Scan active pipeline contacts for any missed calls
    try:
        from sales_pipeline.state import load_state
        state = load_state()
        contacts = state.get("contacts", {})

        active_stages = {
            "discovered", "cold_drafted", "cold_sent",
            "cold_follow_up_1", "cold_follow_up_2", "cold_follow_up_3", "cold_follow_up_4",
            "engaged", "proposal_sent",
            "post_proposal_1", "post_proposal_2", "post_proposal_3", "post_proposal_4",
            "negotiating", "nurture_monthly",
        }

        for contact_id, info in contacts.items():
            if info.get("stage", "") not in active_stages:
                continue
            try:
                count = process_new_calls(ghl, contact_id, transcripts)
                total += count
            except Exception as e:
                log.warning("Failed to process calls for %s: %s", contact_id, e)
            # Small delay between contacts to avoid GHL rate limits
            time.sleep(0.5)

    except Exception as e:
        log.warning("Active contact scan failed: %s", e)

    if total > 0:
        save_transcripts(transcripts)
        log.info("Total: transcribed %d new calls", total)

    # Report health to watchdog
    try:
        from shared_utils.health_reporter import report_status
        report_status(
            "transcribe_calls",
            "ok",
            f"Transcribed {total} new calls" if total else "No new calls",
            metrics={"transcribed": total},
        )
    except Exception:
        pass


if __name__ == "__main__":
    main()
