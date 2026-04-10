"""
Call Intelligence — Historical Backfill
One-time import of all historical calls from GHL.
Resumable: saves progress every 10 contacts.
"""

import argparse
import logging
import time
from datetime import datetime, timezone

from call_intelligence import config
from call_intelligence.config import LOG_FILE, DATA_DIR, load_config, save_config
from call_intelligence.db import get_connection
from call_intelligence.run_ingestion import (
    process_call_message, load_state, save_state,
)
from sales_pipeline.ghl_client import GHLClient, GHLAPIError

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("call_intelligence.backfill")


def run_backfill(ghl: GHLClient, conn, cfg: dict, state: dict,
                 dry_run: bool = False, max_contacts: int = None) -> dict:
    """
    Backfill all historical calls. Resumable via state file.
    Returns stats dict.
    """
    resume_from = state.get("backfill_last_contact_index", 0)

    log.info("Fetching all GHL contacts...")
    contacts = ghl.get_contacts(page_size=100)
    total_contacts = len(contacts)
    log.info("Found %d contacts, resuming from index %d", total_contacts, resume_from)

    if max_contacts:
        end = min(resume_from + max_contacts, total_contacts)
    else:
        end = total_contacts

    processed = 0
    failed = 0
    skipped = 0

    for i in range(resume_from, end):
        contact = contacts[i]
        contact_id = contact.get("id", "")
        contact_name = (
            f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip()
            or contact_id[:8]
        )

        log.info("[%d/%d] Processing contact: %s", i + 1, total_contacts, contact_name)

        try:
            conversations = ghl.search_conversations(contact_id)
        except GHLAPIError as e:
            log.warning("Failed to get conversations for %s: %s", contact_name, e)
            failed += 1
            time.sleep(0.5)
            continue

        for conv in conversations:
            conv_id = conv.get("id")
            if not conv_id:
                continue

            try:
                messages = ghl.get_conversation_messages(conv_id)
            except GHLAPIError:
                continue

            for msg in messages:
                if msg.get("messageType") != "TYPE_CALL":
                    continue

                if dry_run:
                    meta = msg.get("meta", {}).get("call", {})
                    log.info("  [DRY RUN] Would process call %s (%ds, %s)",
                             msg.get("id", "?"),
                             meta.get("duration", 0),
                             msg.get("direction", "?"))
                    skipped += 1
                    continue

                try:
                    if process_call_message(ghl, conn, cfg, msg, contact_id, conv_id):
                        processed += 1
                    else:
                        skipped += 1
                except Exception as e:
                    log.error("Failed to process call %s: %s", msg.get("id"), e)
                    failed += 1

        # Save progress every 10 contacts
        if not dry_run and (i + 1) % 10 == 0:
            state["backfill_last_contact_index"] = i + 1
            state["backfill_status"] = "in_progress"
            save_state(state)
            log.info("Progress saved: %d/%d contacts", i + 1, total_contacts)

        time.sleep(0.5)

    return {
        "contacts_scanned": end - resume_from,
        "calls_processed": processed,
        "calls_skipped": skipped,
        "calls_failed": failed,
    }


def run():
    parser = argparse.ArgumentParser(description="Backfill historical calls from GHL")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be processed without writing to DB")
    parser.add_argument("--max-contacts", type=int, default=None,
                        help="Limit to N contacts (for testing)")
    args = parser.parse_args()

    log.info("=== Call Intelligence Backfill Starting %s===",
             "(DRY RUN) " if args.dry_run else "")

    config.validate_config()
    cfg = load_config()
    state = load_state()

    if state.get("backfill_status") == "complete":
        log.info("Backfill already complete, nothing to do.")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    ghl = GHLClient()
    conn = get_connection()

    try:
        result = run_backfill(ghl, conn, cfg, state,
                              dry_run=args.dry_run,
                              max_contacts=args.max_contacts)
    except Exception as e:
        log.error("Backfill failed: %s", e)
        raise
    finally:
        conn.close()

    if not args.dry_run:
        state["backfill_status"] = "complete"
        state["backfill_last_contact_index"] = 0
        save_state(state)

        cfg["backfill_complete"] = True
        save_config(cfg)

    log.info("=== Backfill complete: %s ===", result)


if __name__ == "__main__":
    run()
