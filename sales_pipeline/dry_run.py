"""
Sales Pipeline — Dry Run
Generates EXACTLY what the pipeline would send tomorrow, without sending anything.
Outputs all messages to dry_run_output.txt for Sam's review.
"""

import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sales_pipeline import config
from sales_pipeline.state import (
    load_state, get_due_contacts, get_nurture_due_contacts,
    get_contact as get_state_contact, _parse_iso,
    TOUCH_SCHEDULE, MAX_TOUCHES,
)
from sales_pipeline.follow_up.follow_up_engine import (
    POST_PROPOSAL_MATRIX, cold_touch_matrix,
    SUBJECT_TEMPLATES, PROMPT_TEMPLATES, POST_PROPOSAL_PROMPT_TEMPLATES,
    NURTURE_ROTATION, SMS_MAX_LENGTH,
    detect_channel_path, get_touch_content,
    _build_system_prompt, _call_claude, _build_contact_context,
    FOLLOW_UP_SYSTEM_PROMPT,
)
from sales_pipeline.templates.signature import (
    wrap_email_body, build_sms_signature, build_plain_text_signature,
)
from sales_pipeline.templates.unsubscribe import (
    wrap_email_with_unsubscribe, build_sms_opt_out_text,
    build_plain_text_unsubscribe,
)

config.validate_config()

OUTPUT_FILE = Path(__file__).parent / "dry_run_output.txt"


def divider(title: str) -> str:
    return f"\n{'='*70}\n  {title}\n{'='*70}\n"


def sub_divider(title: str) -> str:
    return f"\n{'-'*50}\n  {title}\n{'-'*50}\n"


def main():
    from sales_pipeline.ghl_client import GHLClient
    ghl = GHLClient()

    state = load_state()
    now = datetime.now(timezone.utc)
    output_lines = []

    def out(text: str = ""):
        output_lines.append(text)
        print(text)

    out(divider("SALES PIPELINE DRY RUN — " + now.strftime("%Y-%m-%d %H:%M UTC")))
    out("This shows EXACTLY what the --daily run would send.\n")

    # ─────────────────────────────────────────────────────────
    # 1. POST-PROPOSAL FOLLOW-UPS (auto-send)
    # ─────────────────────────────────────────────────────────
    out(divider("SECTION 1: POST-PROPOSAL FOLLOW-UPS (AUTO-SEND)"))
    out("These send AUTOMATICALLY — no human review gate.\n")

    post_due = get_due_contacts(state, phase="post_proposal")

    if not post_due:
        out("  (none due)\n")
    else:
        for item in post_due:
            cid = item["contact_id"]
            touch_num = item["touch_number"]
            entry = get_state_contact(state, cid)

            first_name = entry.get("first_name", "?")
            last_name = entry.get("last_name", "")
            org = entry.get("organization", "")
            email = entry.get("email", "")
            phone = entry.get("phone", "")
            prop_type = entry.get("property_type", "other")
            proposal_sent = entry.get("proposal_sent_at", "")

            # Detect path
            path = entry.get("path")
            if not path and proposal_sent:
                try:
                    path = detect_channel_path(ghl, cid, proposal_sent)
                except Exception:
                    path = "B"
            if not path:
                path = "B"

            touch_info = POST_PROPOSAL_MATRIX[path][touch_num]
            channel = touch_info["channel"]

            days_since = (now - _parse_iso(proposal_sent)).total_seconds() / 86400 if proposal_sent else 0

            out(sub_divider(f"TO: {first_name} {last_name}" + (f" ({org})" if org else "")))
            out(f"  Contact ID:    {cid}")
            out(f"  Email:         {email}")
            out(f"  Phone:         {phone}")
            out(f"  Property:      {prop_type}")
            out(f"  Proposal sent: {proposal_sent[:10] if proposal_sent else 'N/A'} ({days_since:.0f} days ago)")
            out(f"  Path:          {path} ({'email-engaged' if path == 'A' else 'email-silent'})")
            out(f"  Touch:         #{touch_num} of {MAX_TOUCHES}")
            out(f"  Channel:       {channel.upper()}")
            out(f"  Touch type:    {touch_info['type']}")
            out()

            # Generate the actual message
            try:
                contact = ghl.get_contact(cid)
                subject, body = get_touch_content(
                    touch_info=touch_info,
                    contact=contact,
                    property_type=prop_type,
                    phase="post_proposal",
                    state_entry=entry,
                    ghl_client=ghl,
                    contact_id=cid,
                )

                if channel == "sms":
                    out(f"  --- SMS MESSAGE ---")
                    out(f"  {body}")
                    out(f"  {build_sms_opt_out_text().strip()}")
                    out(f"  --- END SMS ({len(body)} chars) ---")
                else:
                    out(f"  Subject: {subject}")
                    out(f"  --- EMAIL BODY (plain text) ---")
                    out(f"  {body}")
                    out(f"  --- SIGNATURE ---")
                    out(f"  Sam Alarcon")
                    out(f"  Vice President, Americal Patrol, Inc.")
                    out(f"  (805) 515-3834 | americalpatrol.com")
                    out(f"  [Schedule a Security Assessment] button")
                    out(f"  --- FOOTER ---")
                    out(f"  Americal Patrol, Inc. - Oxnard, CA")
                    out(f"  Not interested? Unsubscribe — no hard feelings.")
                    out(f"  --- END EMAIL ---")
            except Exception as e:
                out(f"  ERROR generating message: {e}")

            out()

    # ─────────────────────────────────────────────────────────
    # 2. COLD OUTREACH FOLLOW-UPS (drafts only)
    # ─────────────────────────────────────────────────────────
    out(divider("SECTION 2: COLD OUTREACH FOLLOW-UPS (DRAFTS — need your approval)"))
    out("These generate DRAFTS only — nothing sends until you approve.\n")

    cold_due = get_due_contacts(state, phase="cold_outreach")

    if not cold_due:
        out("  (none due)\n")
    else:
        for item in cold_due:
            cid = item["contact_id"]
            touch_num = item["touch_number"]
            entry = get_state_contact(state, cid)

            first_name = entry.get("first_name", "?")
            last_name = entry.get("last_name", "")
            org = entry.get("organization", "")
            email = entry.get("email", "")
            phone = entry.get("phone", "")
            prop_type = entry.get("property_type", "other")
            initial_channel = entry.get("channel", "email")
            first_outreach = entry.get("first_outreach_at", "")

            matrix = cold_touch_matrix(initial_channel)
            touch_info = matrix[touch_num]
            channel = touch_info["channel"]

            days_since = (now - _parse_iso(first_outreach)).total_seconds() / 86400 if first_outreach else 0

            out(sub_divider(f"TO: {first_name} {last_name}" + (f" ({org})" if org else "")))
            out(f"  Contact ID:      {cid}")
            out(f"  Email:           {email}")
            out(f"  Phone:           {phone}")
            out(f"  Property:        {prop_type}")
            out(f"  First outreach:  {first_outreach[:10] if first_outreach else 'N/A'} ({days_since:.0f} days ago)")
            out(f"  Touch:           #{touch_num} of {MAX_TOUCHES}")
            out(f"  Channel:         {channel.upper()}")
            out(f"  Touch type:      {touch_info['type']}")
            out()

            # Generate the actual message
            try:
                contact = ghl.get_contact(cid)
                subject, body = get_touch_content(
                    touch_info=touch_info,
                    contact=contact,
                    property_type=prop_type,
                    phase="cold_outreach",
                    state_entry=entry,
                    ghl_client=ghl,
                    contact_id=cid,
                )

                if channel == "sms":
                    out(f"  --- SMS MESSAGE ---")
                    out(f"  {body}")
                    out(f"  {build_sms_opt_out_text().strip()}")
                    out(f"  --- END SMS ({len(body)} chars) ---")
                else:
                    out(f"  Subject: {subject}")
                    out(f"  --- EMAIL BODY (plain text) ---")
                    out(f"  {body}")
                    out(f"  --- SIGNATURE ---")
                    out(f"  Sam Alarcon")
                    out(f"  Vice President, Americal Patrol, Inc.")
                    out(f"  (805) 515-3834 | americalpatrol.com")
                    out(f"  [Schedule a Security Assessment] button")
                    out(f"  --- FOOTER ---")
                    out(f"  Americal Patrol, Inc. - Oxnard, CA")
                    out(f"  Not interested? Unsubscribe — no hard feelings.")
                    out(f"  --- END EMAIL ---")
            except Exception as e:
                out(f"  ERROR generating message: {e}")

            out()

    # ─────────────────────────────────────────────────────────
    # 3. MONTHLY NURTURE (auto-send after 30 days)
    # ─────────────────────────────────────────────────────────
    out(divider("SECTION 3: MONTHLY NURTURE (AUTO-SEND — 30-day interval)"))

    nurture_due = get_nurture_due_contacts(state)

    if not nurture_due:
        # Show when nurture contacts WILL be due
        nurture_contacts = [
            (cid, e) for cid, e in state["contacts"].items()
            if e.get("phase") == "nurture" and not e.get("completed")
        ]
        if nurture_contacts:
            earliest_ref = None
            for cid, e in nurture_contacts:
                ref = e.get("last_touch_at") or e.get("nurture_started_at")
                if ref:
                    ref_dt = _parse_iso(ref)
                    if earliest_ref is None or ref_dt < earliest_ref:
                        earliest_ref = ref_dt
            if earliest_ref:
                from datetime import timedelta
                due_date = earliest_ref + timedelta(days=30)
                out(f"  (none due yet — {len(nurture_contacts)} contacts in nurture)")
                out(f"  First nurture emails due: ~{due_date.strftime('%Y-%m-%d')}")
            else:
                out(f"  (none due — {len(nurture_contacts)} contacts in nurture)")
        else:
            out("  (no contacts in nurture)\n")
    else:
        out(f"  {len(nurture_due)} nurture contacts due:\n")
        for item in nurture_due:
            cid = item["contact_id"]
            entry = get_state_contact(state, cid)
            first_name = entry.get("first_name", "?")
            org = entry.get("organization", "")
            out(f"  - {first_name} ({org})")

    out()

    # ─────────────────────────────────────────────────────────
    # 4. NEW COLD LEAD DISCOVERY
    # ─────────────────────────────────────────────────────────
    out(divider("SECTION 4: NEW COLD LEAD DISCOVERY"))
    out("The --daily run also scans GHL for new cold leads (>180 days inactive).")
    out("Any new leads found get DRAFT messages generated for your review.")
    out("(Skipping actual GHL scan in dry run — this would find ~20 new leads)")
    out()

    # ─────────────────────────────────────────────────────────
    # 5. TUESDAY AUTO-SEND CHECK
    # ─────────────────────────────────────────────────────────
    tomorrow = "Wednesday"  # April 8
    out(divider("SECTION 5: TUESDAY AUTO-SEND"))
    out(f"  Tomorrow is {tomorrow}.")
    out(f"  Approved drafts auto-send on TUESDAYS only.")
    out(f"  → No approved drafts will auto-send tomorrow.\n")

    # ─────────────────────────────────────────────────────────
    # Summary
    # ─────────────────────────────────────────────────────────
    out(divider("SUMMARY"))
    out(f"  Post-proposal auto-sends:  {len(post_due)}")
    out(f"  Cold follow-up drafts:     {len(cold_due)}")
    out(f"  Nurture auto-sends:        {len(nurture_due)}")
    out(f"  Tuesday approved sends:    0 (not Tuesday)")
    out()

    # Write to file
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    out(f"\nFull output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
