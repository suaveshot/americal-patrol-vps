"""
Sales Pipeline — Cold Outreach: Draft Builder
Builds draft objects from cold leads and writes them to pipeline_drafts.json.
Deduplicates by contact_id. Atomic write.

Now integrates:
  - Enrichment data from prospect_db
  - HTML signature from templates/signature.py
  - Unsubscribe footer from templates/unsubscribe.py
  - A/B variant generation for initial touches
  - SMS opt-out text
"""

import json
import logging
import os
from datetime import datetime, timezone

from sales_pipeline.config import DRAFTS_FILE
from sales_pipeline.cold_outreach.channel_detector import detect_channel
from sales_pipeline.cold_outreach.form_data_parser import parse_contact
from sales_pipeline.cold_outreach.message_generator import (
    generate_message, generate_ab_variants, MessageGenerationError,
)
from sales_pipeline.enrichment.prospect_db import find_prospect_match
from sales_pipeline.templates.signature import (
    wrap_email_body, build_sms_signature, build_plain_text_signature,
)
from sales_pipeline.templates.unsubscribe import (
    wrap_email_with_unsubscribe, build_sms_opt_out_text,
    build_plain_text_unsubscribe,
)

log = logging.getLogger(__name__)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_existing_drafts() -> dict:
    """Load existing drafts as {contact_id: draft_dict}."""
    if not DRAFTS_FILE.exists():
        return {}
    try:
        with open(DRAFTS_FILE, "r", encoding="utf-8") as f:
            drafts = json.load(f)
        if isinstance(drafts, list):
            return {d["contact_id"]: d for d in drafts if "contact_id" in d}
        return {}
    except (json.JSONDecodeError, OSError):
        return {}


def _save_drafts(drafts_by_id: dict) -> None:
    """Atomically save drafts list to JSON."""
    drafts_list = list(drafts_by_id.values())
    tmp = DRAFTS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(drafts_list, f, indent=2)
    os.replace(tmp, DRAFTS_FILE)


def build_drafts(
    cold_leads: list,
    ghl_client=None,
    ab_testing: bool = True,
) -> list:
    """
    For each cold lead:
      1. Detect channel (requires GHL conversations — pass ghl_client)
      2. Parse form data
      3. Check enrichment database for company research
      4. Generate message via Claude (with A/B variants for email)
      5. Wrap email with signature + unsubscribe footer
      6. Build draft object with status='pending'
    Returns list of new drafts. Existing drafts with same contact_id are preserved.
    """
    existing = _load_existing_drafts()
    new_drafts = []
    skipped = 0

    for lead in cold_leads:
        contact_id = lead.get("id", "")
        if contact_id in existing:
            log.debug(f"Contact {contact_id} already has a draft, skipping")
            skipped += 1
            continue

        # Detect channel
        conversations = []
        if ghl_client:
            try:
                conversations = ghl_client.search_conversations(contact_id)
            except Exception:
                conversations = []
        channel = detect_channel(conversations)

        # Parse form fields
        parsed = parse_contact(lead)
        parsed["channel"] = channel

        # Check enrichment
        enrichment_matched = False
        enrichment_company = ""
        prospect = find_prospect_match(
            parsed["organization"], parsed.get("property_city", "")
        )
        if prospect:
            enrichment_matched = True
            enrichment_company = prospect.get("company", "")

        # Generate message(s)
        try:
            # Determine send priority based on day of week
            # Tuesday = high (first touches), Wed/Thu = normal (follow-ups)
            send_day = datetime.now(timezone.utc).strftime("%A")
            priority = "high" if send_day == "Tuesday" else "normal"

            if channel == "email" and ab_testing:
                variants = generate_ab_variants(parsed, channel)
                subject_a, body_a = variants[0]
                # Cold outreach: plain text (no HTML wrapping)
                plain_a = body_a + "\n\n" + build_plain_text_signature()
                plain_a += "\n" + build_plain_text_unsubscribe(contact_id)

                if len(variants) > 1:
                    subject_b, body_b = variants[1]
                    plain_b = body_b + "\n\n" + build_plain_text_signature()
                    plain_b += "\n" + build_plain_text_unsubscribe(contact_id)
                else:
                    subject_b, plain_b, body_b = "", "", ""

                draft = {
                    "contact_id":       contact_id,
                    "name":             f"{parsed['first_name']} {parsed['last_name']}".strip(),
                    "organization":     parsed["organization"],
                    "property_type":    parsed["property_type"],
                    "channel":          channel,
                    "email":            parsed["email"],
                    "phone":            parsed["phone"],
                    "subject":          subject_a,
                    "message":          plain_a,
                    "message_plain":    body_a,
                    "plain_text_mode":  True,
                    # Variant B (reviewer picks)
                    "variant_b_subject": subject_b,
                    "variant_b_message": plain_b,
                    "variant_b_plain":  body_b if len(variants) > 1 else "",
                    "selected_variant": "a",  # Default to A, reviewer can change to "b"
                    "days_cold":        parsed["days_since_contact"],
                    "priority":         priority,
                    "status":           "pending",
                    "generated_at":     _now_iso(),
                    "sent_at":          None,
                    "enrichment_matched": enrichment_matched,
                    "enrichment_company": enrichment_company,
                }
            else:
                subject, body = generate_message(parsed, channel)

                if channel == "email":
                    # Cold outreach: plain text
                    message = body + "\n\n" + build_plain_text_signature()
                    message += "\n" + build_plain_text_unsubscribe(contact_id)
                else:
                    # SMS: opt-out only (no signature on texts)
                    message = body + "\n" + build_sms_opt_out_text()

                draft = {
                    "contact_id":       contact_id,
                    "name":             f"{parsed['first_name']} {parsed['last_name']}".strip(),
                    "organization":     parsed["organization"],
                    "property_type":    parsed["property_type"],
                    "channel":          channel,
                    "email":            parsed["email"],
                    "phone":            parsed["phone"],
                    "subject":          subject,
                    "message":          message,
                    "message_plain":    body,
                    "plain_text_mode":  channel == "email",
                    "days_cold":        parsed["days_since_contact"],
                    "priority":         priority,
                    "status":           "pending",
                    "generated_at":     _now_iso(),
                    "sent_at":          None,
                    "enrichment_matched": enrichment_matched,
                    "enrichment_company": enrichment_company,
                }

        except MessageGenerationError as e:
            log.error(f"Message generation failed for {contact_id}: {e}")
            continue

        existing[contact_id] = draft
        new_drafts.append(draft)

    _save_drafts(existing)
    log.info(
        f"Draft builder: {len(new_drafts)} new drafts generated, "
        f"{skipped} skipped (already drafted), {len(existing)} total in file"
    )
    return new_drafts
