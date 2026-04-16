"""
Win-Back Pipeline Orchestrator

Scans CRM for inactive customers and sends re-engagement messages.

Usage:
    python -m win_back.run_winback              # Normal weekly run
    python -m win_back.run_winback --dry-run     # Show eligible without sending
    python -m win_back.run_winback --force CID   # Force send to a specific contact

Schedule: Weekly (configurable day) via Docker cron
"""

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import tenant_context as tc
from shared_utils.event_bus import publish_event
from shared_utils.health_reporter import report_status
from shared_utils.usage_tracker import check_budget, log_usage
from providers import get_crm, get_email, get_sms

from win_back.config import get_config, load_state, save_state, LOG_FILE
from win_back.inactivity_scanner import scan_inactive
from win_back.message_generator import generate_message
from win_back.campaign_tracker import record_send, increment_daily_ai_count

log = logging.getLogger("win_back")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            logging.FileHandler(LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def _send_message(contact: dict, body: str, subject: str | None,
                  channel: str, config: dict) -> bool:
    """Send via email or SMS using provider factory. Returns True on success."""
    try:
        if channel == "email" and contact.get("email"):
            email_provider = get_email()
            result = email_provider.send_email(
                to=contact["email"],
                subject=subject or f"We miss you at {tc.company_name()}!",
                html_body=f"<p>{body}</p>",
                from_email=tc.sender_email(),
                from_name=tc.sender_name(),
            )
            log_usage("win_back", "email", {"cost_usd": 0.0001}, client_id=tc.client_id())
            return result.get("success", False)

        elif channel == "sms" and contact.get("phone"):
            sms_provider = get_sms()
            result = sms_provider.send_sms(to=contact["phone"], message=body)
            log_usage("win_back", "ghl_sms", {"cost_usd": 0.0079, "segments": 1},
                      client_id=tc.client_id())
            return result.get("success", False)

        else:
            log.warning("No %s for contact %s", channel, contact.get("name", "?"))
            return False

    except Exception as e:
        log.error("Failed to send %s to %s: %s", channel, contact.get("name", "?"), e)
        return False


def run_dry_run():
    """Show eligible contacts without sending anything."""
    log.info("=== Win-Back -- DRY RUN ===")
    config = get_config()
    crm = get_crm()
    state = load_state()

    eligible = scan_inactive(
        crm,
        inactivity_days=config["inactivity_days"],
        exclude_tags=config["exclude_tags"],
        campaign_state=state.get("campaigns", {}),
        cooldown_days=config["cooldown_days"],
        max_results=config["max_per_run"],
    )

    if not eligible:
        log.info("No inactive contacts found.")
        return 0

    log.info("%d contact(s) eligible for win-back:", len(eligible))
    for c in eligible:
        log.info("  %s (%s) -- last activity: %s",
                 c.get("name", "?"), c.get("email", c.get("phone", "?")),
                 c.get("updated_at", "?"))
    return len(eligible)


def run_normal():
    """Normal run: scan + send messages."""
    config = get_config()
    log.info("=== Win-Back -- %s MODE ===", config["mode"].upper())

    # Budget check
    budget = check_budget(tc.client_id())
    if not budget["ok"]:
        log.error("KILL SWITCH: Budget exceeded ($%.2f/$%.2f). Halting.",
                  budget["used_usd"], budget["limit_usd"])
        report_status("win_back", "killed", f"Budget exceeded: ${budget['used_usd']:.2f}")
        publish_event("win_back", "kill_switch_triggered", budget)
        return 0

    if budget["pct"] >= 80:
        log.warning("BUDGET WARNING: %.0f%% used ($%.2f/$%.2f)",
                    budget["pct"], budget["used_usd"], budget["limit_usd"])

    crm = get_crm()
    state = load_state()

    eligible = scan_inactive(
        crm,
        inactivity_days=config["inactivity_days"],
        exclude_tags=config["exclude_tags"],
        campaign_state=state.get("campaigns", {}),
        cooldown_days=config["cooldown_days"],
        max_results=config["max_per_run"],
    )

    if not eligible:
        log.info("No inactive contacts found.")
        report_status("win_back", "ok", "No eligible contacts", metrics={"sent": 0})
        publish_event("win_back", "run_complete", {"eligible": 0, "sent": 0})
        return 0

    sent_count = 0
    errors = []

    for contact in eligible:
        if config["mode"] == "ai_personalized":
            daily_count = increment_daily_ai_count()
            if daily_count > config["max_ai_messages_per_day"]:
                log.warning("AI daily limit reached (%d). Stopping.", config["max_ai_messages_per_day"])
                break

        for channel in config["channels"]:
            try:
                msg = generate_message(
                    contact, config, channel=channel,
                    company_name=tc.company_name(),
                    company_phone=tc.company_phone(),
                    company_website=tc.company_website_url(),
                    client_id=tc.client_id(),
                )

                success = _send_message(contact, msg["body"], msg["subject"], channel, config)

                if success:
                    record_send(contact["id"], contact.get("name", ""), msg["mode"], channel)
                    sent_count += 1
                    log.info("Sent %s to %s via %s", msg["mode"], contact.get("name", "?"), channel)
                else:
                    errors.append(f"{contact.get('name', '?')}/{channel}: send failed")

            except Exception as e:
                log.error("Error processing %s/%s: %s", contact.get("name", "?"), channel, e)
                errors.append(f"{contact.get('name', '?')}/{channel}: {e}")

    detail = f"{sent_count} win-back message(s) sent"
    if errors:
        detail += f", {len(errors)} error(s)"
        status = "warning" if sent_count > 0 else "error"
    else:
        status = "ok"

    report_status("win_back", status, detail, metrics={
        "sent": sent_count, "eligible": len(eligible), "errors": len(errors),
        "mode": config["mode"],
    })

    publish_event("win_back", "messages_sent", {
        "count": sent_count,
        "mode": config["mode"],
        "contacts": [{"id": c["id"], "name": c.get("name", "")} for c in eligible[:sent_count]],
    })

    log.info("=== Win-Back complete: %s ===", detail)
    return sent_count


def main():
    if not tc.is_active():
        log.info("Client account paused -- skipping pipeline run")
        sys.exit(0)

    if not tc.is_pipeline_enabled("win_back"):
        log.info("Win-back pipeline not enabled -- skipping")
        sys.exit(0)

    if not tc.win_back_enabled():
        log.info("Win-back disabled in config -- skipping")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Win-Back Pipeline")
    parser.add_argument("--dry-run", action="store_true", help="Show eligible without sending")
    parser.add_argument("--force", metavar="CONTACT_ID", help="Force send to a specific contact")
    args = parser.parse_args()

    setup_logging()

    try:
        if args.dry_run:
            run_dry_run()
        else:
            run_normal()
    except Exception as e:
        log.exception("Win-Back pipeline failed: %s", e)
        report_status("win_back", "error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
