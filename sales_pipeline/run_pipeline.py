"""
Sales Pipeline --- Unified Entry Point

Usage:
    python -m sales_pipeline.run_pipeline --daily
    python -m sales_pipeline.run_pipeline --generate
    python -m sales_pipeline.run_pipeline --send
    python -m sales_pipeline.run_pipeline --proposal proposal_input.json
    python -m sales_pipeline.run_pipeline --migrate

--daily:     Run follow-ups (both phases) + send digest (Task Scheduler, 8 AM Tu-Th)
--generate:  Fetch cold leads, generate draft messages, email digest for review
--send:      Send approved drafts from pipeline_drafts.json via GHL
--proposal:  Generate a proposal from a JSON input file
--migrate:   One-time migration from old state files
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from sales_pipeline import config
from sales_pipeline.config import LOG_FILE, DRAFTS_FILE
from sales_pipeline.templates.signature import wrap_email_body
from sales_pipeline.templates.unsubscribe import wrap_email_with_unsubscribe
import tenant_context as tc
from sales_pipeline.ghl_client import GHLClient

# Event bus for cross-pipeline integration
try:
    from shared_utils.event_bus import publish_event
except ImportError:
    publish_event = None

# Configure logging before other imports
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(name)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# --daily
# ---------------------------------------------------------------------------

def run_daily():
    """Daily run: evaluate outcomes, check replies, send follow-ups, run learning, send digest."""
    from sales_pipeline.follow_up.follow_up_engine import run_follow_ups
    from sales_pipeline.digest import send_digest

    config.validate_config()
    ghl = GHLClient()

    log.info("=== Sales Pipeline Daily Run ===")

    # Ingest voice leads from event bus (auto-add to pipeline state)
    if publish_event:  # event bus module is available
        try:
            from shared_utils.event_bus import read_events_since
            from sales_pipeline.state import load_state, save_state, add_contact, get_contact
            voice_events = read_events_since("voice_agent", "lead_captured", days=7)
            state = load_state()
            voice_added = 0
            for evt in voice_events:
                cid = evt.get("contact_id", "")
                if cid and not get_contact(state, cid):
                    add_contact(
                        state, cid,
                        stage="discovered",
                        channel="email",
                        first_name=evt.get("first_name", ""),
                        organization=evt.get("company", ""),
                        property_type="other",
                        email="",
                        phone="",
                    )
                    voice_added += 1
            if voice_added:
                save_state(state)
                log.info("Ingested %d voice leads into sales pipeline", voice_added)
        except Exception as e:
            log.warning("Voice lead ingestion failed: %s", e)

    # Ingest email lead inquiries from event bus
    if publish_event:
        try:
            from shared_utils.event_bus import read_events_since as _read_events
            email_events = _read_events("email_assistant", "lead_inquiry", days=7)
            if email_events:
                log.info("Found %d email lead inquiries in event bus", len(email_events))
        except Exception as e:
            log.warning("Email lead ingestion failed: %s", e)

    # Evaluate pending learning outcomes (7+ days old)
    try:
        from sales_pipeline.learning.outcome_tracker import evaluate_pending_outcomes
        eval_stats = evaluate_pending_outcomes(ghl)
        if eval_stats.get("evaluated"):
            log.info("Learning: evaluated %d outcomes", eval_stats["evaluated"])
    except Exception as e:
        log.warning("Learning outcome evaluation failed: %s", e)

    # Run unified follow-up engine (handles cold, post-proposal, and nurture)
    log.info("Running follow-up engine...")
    summary = run_follow_ups(ghl)
    log.info(
        "Follow-ups: %d sent, %d replied, %d nurture, %d cold drafts, %d errors",
        len(summary.get("sent", [])),
        len(summary.get("replied", [])),
        len(summary.get("nurture_sent", [])),
        len(summary.get("cold_drafts_generated", [])),
        len(summary.get("errors", [])),
    )

    # Run learning analysis (weekly or when enough data)
    try:
        from sales_pipeline.learning.outcome_tracker import get_outcome_count
        from sales_pipeline.learning.learning_analyzer import run_analysis, load_insights
        insights = load_insights()
        last_analysis = insights.get("updated_at")
        outcome_count = get_outcome_count()

        # Run analysis weekly or on first threshold
        should_analyze = False
        if outcome_count >= 30:
            if not last_analysis:
                should_analyze = True
            else:
                from sales_pipeline.state import _parse_iso
                days_since = (datetime.now(timezone.utc) - _parse_iso(last_analysis)).total_seconds() / 86400
                if days_since >= 7:
                    should_analyze = True

        if should_analyze:
            log.info("Running learning analysis (%d outcomes)...", outcome_count)
            run_analysis()
    except Exception as e:
        log.warning("Learning analysis failed: %s", e)

    # Generate cold outreach drafts (auto-discovery)
    log.info("Checking for cold leads...")
    try:
        from sales_pipeline.cold_outreach.lead_filter import get_cold_leads
        from sales_pipeline.cold_outreach.draft_builder import build_drafts
        from sales_pipeline.state import load_state, save_state, add_contact, mark_drafted

        state = load_state()
        contacts = ghl.get_contacts()
        threshold = config.THRESHOLD_DAYS()
        cap = config.DAILY_CAP()
        cold_leads = get_cold_leads(contacts, threshold_days=threshold, state=state, daily_cap=cap)

        if cold_leads:
            new_drafts = build_drafts(cold_leads, ghl_client=ghl)
            for draft in new_drafts:
                cid = draft.get("contact_id")
                if cid and not state["contacts"].get(cid):
                    add_contact(
                        state, cid,
                        stage="discovered",
                        channel=draft.get("channel", "email"),
                        first_name=draft.get("name", "").split()[0] if draft.get("name") else "",
                        organization=draft.get("organization", ""),
                        property_type=draft.get("property_type", "other"),
                        email=draft.get("email", ""),
                        phone=draft.get("phone", ""),
                        enrichment_matched=draft.get("enrichment_matched", False),
                        enrichment_company=draft.get("enrichment_company", ""),
                    )
                if cid:
                    mark_drafted(state, cid)
            save_state(state)
            log.info("Cold outreach: %d new drafts generated", len(new_drafts))
        else:
            log.info("Cold outreach: no new cold leads found")
    except Exception as e:
        log.error("Cold outreach generation failed: %s", e)

    # Auto-send approved drafts on Tuesdays
    today = datetime.now().strftime("%A")
    if today == "Tuesday":
        try:
            if DRAFTS_FILE.exists():
                with open(DRAFTS_FILE, "r", encoding="utf-8") as f:
                    drafts = json.load(f)
                approved = [d for d in drafts if d.get("status") == "approved"]
                if approved:
                    log.info("Tuesday: auto-sending %d approved drafts...", len(approved))
                    run_send()
        except Exception as e:
            log.error("Tuesday auto-send failed: %s", e)

    # Send unified digest
    log.info("Sending daily digest...")
    send_digest(ghl, summary)

    # Send weekly pipeline review (exits + wins analysis) if due
    try:
        from sales_pipeline.learning.exit_analyzer import send_weekly_review, should_send_weekly_review
        if should_send_weekly_review():
            log.info("Sending weekly pipeline review...")
            send_weekly_review()
    except Exception as e:
        log.warning("Weekly review failed: %s", e)

    # Publish to event bus for dashboard/weekly update
    if publish_event:
        try:
            publish_event("sales_pipeline", "daily_complete", {
                "follow_ups_sent": len(summary.get("sent", [])),
                "replies_detected": len(summary.get("replied", [])),
                "completed": len(summary.get("completed", [])),
                "cold_drafts_generated": len(summary.get("cold_drafts_generated", [])),
                "nurture_sent": len(summary.get("nurture_sent", [])),
                "errors": len(summary.get("errors", [])),
            })
        except Exception as e:
            log.warning("Event bus publish failed: %s", e)

    # Report health to watchdog. Without this the watchdog can only infer
    # status from log-tail regex, which missed Kyle's per-contact
    # AttributeErrors for 10 days because they didn't hit the ERROR pattern.
    try:
        from shared_utils.health_reporter import report_status
        errors = summary.get("errors", [])
        status = "error" if errors else "ok"
        detail = (
            f"{len(summary.get('sent', []))} sent, "
            f"{len(summary.get('replied', []))} replied, "
            f"{len(summary.get('nurture_sent', []))} nurture, "
            f"{len(summary.get('cold_drafts_generated', []))} cold drafts, "
            f"{len(errors)} errors"
        )
        report_status("sales_pipeline", status, detail, metrics={
            "follow_ups_sent": len(summary.get("sent", [])),
            "replies_detected": len(summary.get("replied", [])),
            "nurture_sent": len(summary.get("nurture_sent", [])),
            "cold_drafts_generated": len(summary.get("cold_drafts_generated", [])),
            "completed": len(summary.get("completed", [])),
            "errors": len(errors),
            "error_samples": [str(e)[:200] for e in errors[:5]],
        })
    except Exception as e:
        log.warning("health_reporter call failed: %s", e)

    log.info("=== Daily run complete ===")


# ---------------------------------------------------------------------------
# --generate
# ---------------------------------------------------------------------------

def run_generate():
    """Generate cold outreach drafts: fetch leads, filter, generate messages, email digest."""
    from sales_pipeline.cold_outreach.lead_filter import get_cold_leads
    from sales_pipeline.cold_outreach.draft_builder import build_drafts
    from sales_pipeline.state import load_state, save_state, add_contact, mark_drafted
    from sales_pipeline.digest import send_digest

    config.validate_config()
    ghl = GHLClient()

    log.info("=== Cold Outreach Draft Generation ===")

    state = load_state()

    # Fetch all contacts from CRM
    log.info("Fetching contacts from CRM...")
    contacts = ghl.get_contacts()
    log.info("Fetched %d contacts total", len(contacts))

    # Filter cold leads
    threshold = config.THRESHOLD_DAYS()
    cap = config.DAILY_CAP()
    cold_leads = get_cold_leads(contacts, threshold_days=threshold, state=state, daily_cap=cap)
    log.info("Cold leads to process: %d", len(cold_leads))

    if not cold_leads:
        log.info("No new cold leads found.")
        save_state(state)
        print("No new cold leads found.")
        return

    # Build drafts (channel detection + form parsing + Claude message generation)
    new_drafts = build_drafts(cold_leads, ghl_client=ghl)
    log.info("New drafts generated: %d", len(new_drafts))

    # Update state for each new draft
    for draft in new_drafts:
        cid = draft.get("contact_id")
        if cid and not state["contacts"].get(cid):
            add_contact(
                state, cid,
                stage="discovered",
                channel=draft.get("channel", "email"),
                first_name=draft.get("name", "").split()[0] if draft.get("name") else "",
                organization=draft.get("organization", ""),
                property_type=draft.get("property_type", "other"),
                email=draft.get("email", ""),
                phone=draft.get("phone", ""),
                enrichment_matched=draft.get("enrichment_matched", False),
                enrichment_company=draft.get("enrichment_company", ""),
            )
        if cid:
            mark_drafted(state, cid)

    save_state(state)

    # Send digest with draft info
    empty_summary = {"sent": [], "replied": [], "completed": [], "cold_drafts_generated": [], "errors": []}
    send_digest(ghl, empty_summary)

    # Publish to event bus
    if publish_event:
        try:
            publish_event("sales_pipeline", "cold_drafts_generated", {
                "draft_count": len(new_drafts),
                "contacts": [d.get("name", "") for d in new_drafts],
            })
        except Exception as e:
            log.warning("Event bus publish failed: %s", e)

    log.info("=== Draft generation complete: %d new drafts ===", len(new_drafts))
    print(f"\n{len(new_drafts)} draft(s) written to pipeline_drafts.json")
    print("Review your digest email, approve contacts in the JSON, then run:")
    print("  python -m sales_pipeline.run_pipeline --send")


# ---------------------------------------------------------------------------
# --send
# ---------------------------------------------------------------------------

def run_send():
    """Send approved drafts from pipeline_drafts.json."""
    from sales_pipeline.ghl_client import GHLAPIError
    from sales_pipeline.state import load_state, save_state, mark_outreached

    config.validate_config()

    log.info("=== Send Approved Drafts ===")

    # Load drafts
    if not DRAFTS_FILE.exists():
        print("No drafts file found. Run --generate first.")
        return

    try:
        with open(DRAFTS_FILE, "r", encoding="utf-8") as f:
            drafts = json.load(f)
    except (json.JSONDecodeError, OSError):
        print("Could not read drafts file.")
        return

    if not isinstance(drafts, list):
        print("Invalid drafts file format.")
        return

    approved = [d for d in drafts if d.get("status") == "approved"]
    log.info("Drafts total: %d, approved: %d", len(drafts), len(approved))

    if not approved:
        print(
            "No approved drafts found.\n"
            "Open pipeline_drafts.json and set status='approved' for contacts to reach."
        )
        return

    ghl = GHLClient()
    state = load_state()
    sent_count = 0
    error_count = 0

    for draft in drafts:
        if draft.get("status") != "approved":
            continue

        contact_id = draft["contact_id"]
        channel = draft.get("channel", "email")
        name = draft.get("name", contact_id)

        # For A/B variants, use selected variant
        selected = draft.get("selected_variant", "a")
        if selected == "b" and draft.get("variant_b_message"):
            subject = draft.get("variant_b_subject", "")
            message = draft.get("variant_b_message", "")
            plain_body = draft.get("variant_b_plain", message)
        else:
            subject = draft.get("subject", "")
            message = draft.get("message", "")
            plain_body = draft.get("message_plain", message)

        try:
            if channel == "sms":
                ghl.send_sms(contact_id, message)
            else:
                # Convert plain text to HTML for proper rendering
                html_body = wrap_email_body(plain_body, include_signature=True)
                html_body = wrap_email_with_unsubscribe(html_body, contact_id)
                ghl.send_email(contact_id, subject, html_body)

            draft["status"] = "sent"
            draft["sent_at"] = datetime.now(timezone.utc).isoformat()
            mark_outreached(state, contact_id, channel)
            sent_count += 1
            log.info("Sent %s to %s (%s)", channel, name, contact_id)

            # Record outcome for learning
            try:
                from sales_pipeline.learning.outcome_tracker import record_outcome
                record_outcome(
                    contact_id=contact_id, channel=channel,
                    touch_number=0, phase="cold_outreach",
                    subject=subject, body=draft.get("message_plain", message),
                    property_type=draft.get("property_type", "other"),
                    enrichment_used=draft.get("enrichment_matched", False),
                    variant=draft.get("selected_variant", ""),
                )
            except Exception:
                pass

        except GHLAPIError as e:
            draft["status"] = "error"
            error_count += 1
            log.error("GHL error sending to %s (%s): %s", name, contact_id, e)
        except Exception as e:
            draft["status"] = "error"
            error_count += 1
            log.error("Error sending to %s (%s): %s", name, contact_id, e)

    # Atomic save drafts
    tmp = DRAFTS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(drafts, f, indent=2)
    os.replace(tmp, DRAFTS_FILE)

    save_state(state)

    # Publish to event bus
    if publish_event:
        try:
            publish_event("sales_pipeline", "drafts_sent", {
                "sent_count": sent_count,
                "error_count": error_count,
            })
        except Exception as e:
            log.warning("Event bus publish failed: %s", e)

    log.info("=== Send complete: sent=%d, errors=%d ===", sent_count, error_count)
    print(f"\nDone. Sent: {sent_count}, Errors: {error_count}")
    if error_count:
        print("Check automation.log for error details.")


# ---------------------------------------------------------------------------
# --proposal
# ---------------------------------------------------------------------------

def run_proposal(input_file: str):
    """Generate and send an estimate/proposal from a JSON input file."""
    from sales_pipeline.proposal.proposal_generator import (
        EstimateInput, create_and_send_estimate, resolve_preset,
    )
    from sales_pipeline.state import load_state, save_state, add_contact, mark_proposal_sent

    config.validate_config()

    input_path = Path(input_file)
    if not input_path.exists():
        log.error("Input file not found: %s", input_path)
        sys.exit(1)

    with open(input_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    resolved = resolve_preset(data)
    inp = EstimateInput(**resolved)
    log.info("Creating estimate for contact %s...", inp.contact_id)

    ghl = GHLClient()
    result = create_and_send_estimate(ghl, inp)
    log.info("Estimate %s created ($%.2f)", result["estimate_id"], result["total"])

    # Add to unified state for post-proposal follow-up
    state = load_state()
    if not state["contacts"].get(inp.contact_id):
        add_contact(
            state, inp.contact_id,
            phase="post_proposal",
            property_type=inp.property_type,
        )
    mark_proposal_sent(
        state, inp.contact_id,
        estimate_id=result.get("estimate_id", ""),
        opportunity_id=inp.opportunity_id or "",
    )
    save_state(state)
    log.info("Contact %s added to post-proposal follow-up sequence", inp.contact_id)

    # Publish to event bus
    if publish_event:
        try:
            publish_event("sales_pipeline", "proposal_created", {
                "contact_id": inp.contact_id,
                "estimate_id": result.get("estimate_id", ""),
                "total_amount": result.get("total", 0),
                "property_type": inp.property_type,
            })
        except Exception as e:
            log.warning("Event bus publish failed: %s", e)

    print(f"\nEstimate created: {result['estimate_id']}")
    print(f"Total: ${result['total']:.2f}")
    print(f"Status: {result['status']}")
    print(f"Branded proposal: {result['proposal_pdf']}")
    print(f"Contact {inp.contact_id} added to follow-up sequence.")


# ---------------------------------------------------------------------------
# --won / --lost
# ---------------------------------------------------------------------------

def _run_win_loss_analysis(ghl, contact_id: str, outcome: str, reason: str = ""):
    """Pull full conversation history and run Claude win/loss analysis."""
    from shared_utils.usage_tracker import tracked_create
    from sales_pipeline.state import load_state, get_contact

    state = load_state()
    entry = get_contact(state, contact_id)

    # Pull full conversation history
    log.info("Pulling conversation history for %s...", contact_id)
    messages = ghl.get_full_conversation_history(contact_id)
    log.info("Found %d messages in conversation history", len(messages))

    # Pull contact details
    contact = ghl.get_contact(contact_id)
    first_name = contact.get("firstName", "")
    company = contact.get("companyName", "")
    property_type = entry.get("property_type", "other") if entry else "other"

    # Pull estimate details
    estimate_info = ""
    estimate_id = entry.get("estimate_id", "") if entry else ""
    if estimate_id:
        try:
            est_data = ghl.get_estimate(estimate_id)
            estimate = est_data.get("estimate", est_data)
            estimate_info = f"Estimate value: ${estimate.get('total', 0):.2f}, Status: {estimate.get('status', 'unknown')}"
        except Exception:
            pass

    # Build conversation timeline
    timeline = ""
    for msg in messages:
        direction = "→ SENT" if msg.get("direction") == "outbound" else "← RECEIVED"
        msg_type = msg.get("type", "").upper()
        timestamp = msg.get("timestamp", "")[:19]
        subject = msg.get("subject", "")
        body = msg.get("body", "")[:500]
        timeline += f"\n[{timestamp}] {direction} ({msg_type})"
        if subject:
            timeline += f" Subject: {subject}"
        timeline += f"\n{body}\n"

    # Calculate days in pipeline
    days_in_pipeline = ""
    if entry:
        start = entry.get("discovered_at") or entry.get("proposal_sent_at", "")
        if start:
            from sales_pipeline.state import _parse_iso
            start_dt = _parse_iso(start)
            days = int((datetime.now(timezone.utc) - start_dt).total_seconds() / 86400)
            days_in_pipeline = f"{days} days"

    prompt = f"""Analyze this {'won' if outcome == 'won' else 'lost'} deal for {tc.company_name()} ({tc.company_industry()} company).

Contact: {first_name} at {company}
Property type: {property_type}
{estimate_info}
Days in pipeline: {days_in_pipeline}
{"Sam's notes: " + reason if reason else ""}

FULL CONVERSATION TIMELINE:
{timeline if timeline else "(No messages found)"}

Analyze this deal and output JSON with these fields:
- outcome: "{outcome}"
- deal_value: (number or null)
- property_type: "{property_type}"
- days_to_close: (number or null)
- turning_point: (what moment shifted the deal — a specific message, objection handled, or timing)
- winning_patterns: (list of 2-4 specific things that worked in our messaging/approach)
- losing_patterns: (list of 2-4 things that didn't work or could improve)
- objections_raised: (list of specific objections the prospect raised)
- objections_handled: (list showing how each objection was addressed, or "not addressed")
- key_message_that_worked: (the single most effective message or phrase we used)
- recommendation: (one actionable takeaway for future deals with similar prospects)

Output ONLY valid JSON, no markdown formatting."""

    try:
        response = tracked_create(
            model="claude-sonnet-4-6",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}],
            pipeline="sales",
            client_id=tc.client_id(),
            api_key=config.ANTHROPIC_API_KEY(),
        )
        analysis_text = response.content[0].text.strip()

        # Parse JSON (strip markdown if Claude wrapped it)
        if analysis_text.startswith("```"):
            analysis_text = analysis_text.split("\n", 1)[1].rsplit("```", 1)[0]
        analysis = json.loads(analysis_text)

    except Exception as e:
        log.error("Win/loss analysis failed: %s", e)
        analysis = {
            "outcome": outcome,
            "property_type": property_type,
            "error": str(e),
            "recommendation": "Manual review needed — Claude analysis failed",
        }

    # Write to win/loss log
    analysis["contact_id"] = contact_id
    analysis["analyzed_at"] = datetime.now(timezone.utc).isoformat()
    analysis["reason"] = reason
    analysis["message_count"] = len(messages)

    win_loss_file = config.WIN_LOSS_LOG_FILE
    with open(win_loss_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(analysis) + "\n")

    return analysis


def run_won(contact_id: str, reason: str = ""):
    """Mark a deal as won, update CRM, run win analysis."""
    from sales_pipeline.state import load_state, save_state, mark_won, get_contact

    config.validate_config()
    ghl = GHLClient()

    state = load_state()
    entry = get_contact(state, contact_id)
    if not entry:
        print(f"Contact {contact_id} not found in pipeline state.")
        return

    mark_won(state, contact_id)
    save_state(state)

    # Update GHL opportunity
    opp_id = entry.get("opportunity_id")
    if opp_id:
        try:
            ghl.update_opportunity(opp_id, {"status": "won"})
            log.info("GHL opportunity %s marked as won", opp_id)
        except Exception as e:
            log.warning("Could not update GHL opportunity: %s", e)

    # Run win analysis (existing win/loss log)
    log.info("Running win analysis for %s...", contact_id)
    analysis = _run_win_loss_analysis(ghl, contact_id, "won", reason)

    # Run detailed win analysis for weekly review digest
    try:
        from sales_pipeline.learning.exit_analyzer import run_win_analysis
        run_win_analysis(ghl, contact_id, reason)
    except Exception as e:
        log.warning("Detailed win analysis failed: %s", e)

    name = entry.get("first_name", "") or contact_id[:12]
    org = entry.get("organization", "")

    # Publish to event bus
    if publish_event:
        try:
            publish_event("sales_pipeline", "deal_won", {
                "contact_id": contact_id,
                "name": name,
                "organization": org,
                "property_type": entry.get("property_type", "other"),
            })
        except Exception as e:
            log.warning("Event bus publish failed: %s", e)

    # Queue offline conversion for Google Ads (if GCLID available)
    try:
        import sys as _sys
        _sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
        from google_ads_automation.tracking.gclid_store import get_gclid
        from google_ads_automation.offline_conversion_uploader import queue_conversion

        gclid = get_gclid(contact_id)

        # Fallback: check GHL contact's gclid custom field
        if not gclid:
            try:
                gclid = ghl.get_contact_custom_field(contact_id, "gclid")
            except Exception:
                pass

        if gclid:
            # Pull contract value from GHL opportunity/estimate
            contract_value = None
            opp_id = entry.get("opportunity_id")
            if opp_id:
                try:
                    opps = ghl.search_opportunities(config.PIPELINE_ID())
                    for opp in opps:
                        if opp.get("id") == opp_id:
                            contract_value = opp.get("monetaryValue", opp.get("value"))
                            if contract_value:
                                contract_value = float(contract_value)
                            break
                except Exception as e:
                    log.warning("Could not fetch opportunity value: %s", e)

            if contract_value and contract_value > 0:
                from datetime import datetime as dt
                queue_conversion(contact_id, gclid, dt.now().isoformat(), contract_value)
                log.info("Queued offline conversion: %s, gclid=%s, value=$%.2f",
                        contact_id, gclid[:20], contract_value)
                print(f"  Offline conversion queued: ${contract_value:.2f} (GCLID: {gclid[:20]}...)")
            else:
                log.warning("GCLID found for %s but no contract value in GHL — skipping offline conversion", contact_id)
                print(f"  GCLID found but no contract value — offline conversion not queued")
        else:
            log.info("No GCLID for %s — lead may not be from Google Ads", contact_id)
    except ImportError:
        log.debug("Google Ads tracking modules not available — skipping offline conversion")
    except Exception as e:
        log.warning("Offline conversion queue failed: %s", e)

    print(f"\nDeal WON: {name}" + (f" ({org})" if org else ""))
    if analysis.get("turning_point"):
        print(f"Turning point: {analysis['turning_point']}")
    if analysis.get("key_message_that_worked"):
        print(f"Key message: {analysis['key_message_that_worked']}")
    print(f"Analysis saved to win_loss_log.jsonl")


def run_lost(contact_id: str, reason: str = ""):
    """Mark a deal as lost, transition to nurture, run loss analysis."""
    from sales_pipeline.state import load_state, save_state, mark_lost, get_contact

    config.validate_config()
    ghl = GHLClient()

    state = load_state()
    entry = get_contact(state, contact_id)
    if not entry:
        print(f"Contact {contact_id} not found in pipeline state.")
        return

    mark_lost(state, contact_id)
    save_state(state)

    # Update GHL opportunity
    opp_id = entry.get("opportunity_id")
    if opp_id:
        try:
            ghl.update_opportunity(opp_id, {"status": "lost"})
            log.info("GHL opportunity %s marked as lost", opp_id)
        except Exception as e:
            log.warning("Could not update GHL opportunity: %s", e)

    # Run loss analysis
    log.info("Running loss analysis for %s...", contact_id)
    analysis = _run_win_loss_analysis(ghl, contact_id, "lost", reason)

    name = entry.get("first_name", "") or contact_id[:12]
    org = entry.get("organization", "")

    # Publish to event bus
    if publish_event:
        try:
            publish_event("sales_pipeline", "deal_lost", {
                "contact_id": contact_id,
                "name": name,
                "organization": org,
                "property_type": entry.get("property_type", "other"),
            })
        except Exception as e:
            log.warning("Event bus publish failed: %s", e)

    print(f"\nDeal LOST: {name}" + (f" ({org})" if org else ""))
    if analysis.get("losing_patterns"):
        print(f"Patterns: {', '.join(analysis['losing_patterns'][:3])}")
    if analysis.get("recommendation"):
        print(f"Recommendation: {analysis['recommendation']}")
    print(f"Contact moved to monthly nurture.")
    print(f"Analysis saved to win_loss_log.jsonl")


# ---------------------------------------------------------------------------
# --migrate
# ---------------------------------------------------------------------------

def run_migrate():
    """One-time migration: merge old cold_outreach_state.json and follow_up_state.json."""
    from sales_pipeline.state import (
        load_state, save_state, merge_states,
    )

    project_dir = Path(__file__).resolve().parent.parent

    cold_state_path = project_dir / "cold_outreach_automation" / "cold_outreach_state.json"
    fu_state_path = project_dir / "sales_autopilot" / "follow_up_state.json"

    cold_state = {}
    fu_state = {}

    if cold_state_path.exists():
        with open(cold_state_path, "r", encoding="utf-8") as f:
            cold_state = json.load(f)
        log.info("Loaded cold outreach state: %d contacts", len(cold_state.get("contacts", {})))
    else:
        log.info("No old cold outreach state file found at %s", cold_state_path)

    if fu_state_path.exists():
        with open(fu_state_path, "r", encoding="utf-8") as f:
            fu_state = json.load(f)
        log.info("Loaded follow-up state: %d contacts", len(fu_state.get("contacts", {})))
    else:
        log.info("No old follow-up state file found at %s", fu_state_path)

    if not cold_state and not fu_state:
        print("No old state files found to migrate.")
        return

    # Check if unified state already has data
    existing = load_state()
    if existing["contacts"]:
        print(f"Warning: unified state already has {len(existing['contacts'])} contacts.")
        answer = input("Overwrite with migrated data? (y/N): ").strip().lower()
        if answer != "y":
            print("Migration cancelled.")
            return

    merged = merge_states(cold_state, fu_state)
    save_state(merged)

    total = len(merged["contacts"])
    print(f"\nMigration complete. {total} contacts merged into pipeline_state.json")
    log.info("Migration complete: %d contacts merged", total)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not tc.is_active():
        log.info("Client account paused -- skipping pipeline run")
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description="Sales Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m sales_pipeline.run_pipeline --daily            Daily follow-ups + digest
  python -m sales_pipeline.run_pipeline --generate         Generate cold outreach drafts
  python -m sales_pipeline.run_pipeline --send             Send approved drafts
  python -m sales_pipeline.run_pipeline --proposal X       Generate proposal from JSON
  python -m sales_pipeline.run_pipeline --won CONTACT_ID   Mark deal as won
  python -m sales_pipeline.run_pipeline --lost CONTACT_ID  Mark deal as lost
  python -m sales_pipeline.run_pipeline --migrate          Migrate old state files
        """,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daily", action="store_true",
                       help="Run daily follow-ups and send digest")
    group.add_argument("--generate", action="store_true",
                       help="Generate cold outreach drafts for review")
    group.add_argument("--send", action="store_true",
                       help="Send approved drafts via GHL")
    group.add_argument("--proposal", type=str, metavar="INPUT_JSON",
                       help="Generate a proposal from JSON input file")
    group.add_argument("--won", type=str, metavar="CONTACT_ID",
                       help="Mark deal as won and run win analysis")
    group.add_argument("--lost", type=str, metavar="CONTACT_ID",
                       help="Mark deal as lost and move to nurture")
    group.add_argument("--migrate", action="store_true",
                       help="One-time migration from old state files")
    parser.add_argument("--reason", type=str, default="",
                        help="Optional reason for --won or --lost")
    args = parser.parse_args()

    try:
        if args.daily:
            run_daily()
        elif args.generate:
            run_generate()
        elif args.send:
            run_send()
        elif args.proposal:
            run_proposal(args.proposal)
        elif args.won:
            run_won(args.won, args.reason)
        elif args.lost:
            run_lost(args.lost, args.reason)
        elif args.migrate:
            run_migrate()
    except Exception as e:
        log.exception("Pipeline failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
