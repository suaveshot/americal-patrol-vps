"""
Review Engine Orchestrator

Checks client eligibility and sends/drafts review request emails + SMS.

Usage:
    python run_reviews.py              # Normal monthly run
    python run_reviews.py --check      # Dry run -- show eligible clients without sending
    python run_reviews.py --force GID  # Force send to a specific group_id (bypass checks)
    python run_reviews.py --onboarding # Check for new clients (14-30 days old) and send review requests
    python run_reviews.py --respond    # Generate AI responses to unresponded reviews
    python run_reviews.py --competitors # Run competitor review monitoring

Schedule: 1st of every month at 10:00 AM
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Ensure project root is on path for shared_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import tenant_context as tc

from review_engine.config import (
    LOG_FILE, DRAFT_MODE, SEND_SMS, ONBOARDING_MIN_DAYS, ONBOARDING_MAX_DAYS,
    load_state, save_state, load_clients,
)
from review_engine.eligibility_checker import check_eligibility, check_onboarding_eligibility
from review_engine.request_sender import send_review_request, send_sms_review_request, send_new_review_alert
from review_engine.gbp_review_checker import check_for_new_reviews
from review_engine.review_responder import generate_response, should_respond, is_negative
from review_engine.competitor_monitor import run_monitor
from shared_utils.event_bus import publish_event
from shared_utils.health_reporter import report_status

log = logging.getLogger("review_engine")


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


def _check_new_reviews():
    """Check GBP for new reviews and alert Sam if any found."""
    try:
        state = load_state()
        new_reviews = check_for_new_reviews(state)
        if new_reviews:
            log.info("Detected %d new Google review(s)!", len(new_reviews))
            for r in new_reviews:
                log.info("  %s star -- %s", r.get("star_rating", "?"), r.get("reviewer_name", "?"))
            send_new_review_alert(new_reviews)
            publish_event("reviews", "new_reviews_detected", {
                "count": len(new_reviews),
                "reviews": [{"name": r["reviewer_name"], "stars": r["star_rating"]} for r in new_reviews],
            })
        else:
            log.info("No new Google reviews since last check.")
        save_state(state)
    except Exception as e:
        log.warning("New review check failed (non-fatal): %s", e)


def run_check_only():
    """Dry run: show eligibility without sending anything."""
    log.info("=== Review Engine -- CHECK MODE (dry run) ===")
    eligible, state, report = check_eligibility(dry_run=True)
    save_state(state)  # Save updated permanent exclusions from GBP scan

    if not eligible:
        log.info("No clients eligible for review requests right now.")
    else:
        log.info("\n%d client(s) eligible for review requests:", len(eligible))
        for e in eligible:
            gid = e["group"]["group_id"]
            props = ", ".join(a["name"] for a in e["group"]["accounts"])
            recipients = ", ".join(e["group"]["recipients"])
            log.info("  %s (%s) -- %s", gid, props, e["reason"])
            log.info("    -> Would email: %s", recipients)

    return len(eligible)


def run_force(group_id):
    """Force-send a review request to a specific group, bypassing eligibility checks."""
    log.info("=== Review Engine -- FORCE MODE for '%s' ===", group_id)
    clients = load_clients()
    state = load_state()

    group = next((g for g in clients if g["group_id"] == group_id), None)
    if not group:
        log.error("Group '%s' not found in clients.json", group_id)
        return 0

    result = send_review_request(group, clean_days=0)
    log.info("Force %s: %s", result["mode"], result["subject"])

    # Update state
    if "requests" not in state:
        state["requests"] = {}
    state["requests"][group_id] = {
        "last_asked": datetime.now().isoformat(),
        "times_asked": state.get("requests", {}).get(group_id, {}).get("times_asked", 0) + 1,
        "last_result": result,
    }
    save_state(state)
    return 1


def run_normal():
    """Normal run: check eligibility and send/draft requests."""
    mode_label = "DRAFT" if DRAFT_MODE else "SEND"
    log.info("=== Review Engine -- %s MODE ===", mode_label)

    # Check for new Google reviews first (alerts Sam to respond)
    _check_new_reviews()

    eligible, state, report = check_eligibility()

    if not eligible:
        log.info("No clients eligible for review requests this run.")
        save_state(state)
        report_status("reviews", "ok", "No eligible clients", metrics={"sent": 0})
        publish_event("reviews", "run_complete", {
            "eligible": 0, "sent": 0, "mode": mode_label,
        })
        return 0

    sent_count = 0
    errors = []

    for entry in eligible:
        group = entry["group"]
        gid = group["group_id"]
        clean_days = entry["clean_days"]

        try:
            result = send_review_request(group, clean_days)
            sent_count += 1

            # Also send SMS if enabled
            if SEND_SMS:
                try:
                    sms_result = send_sms_review_request(group, clean_days)
                    if sms_result:
                        log.info("SMS review request sent to %s", gid)
                except Exception as sms_err:
                    log.warning("SMS failed for %s (email still sent): %s", gid, sms_err)

            # Update state
            if "requests" not in state:
                state["requests"] = {}
            state["requests"][gid] = {
                "last_asked": datetime.now().isoformat(),
                "times_asked": state.get("requests", {}).get(gid, {}).get("times_asked", 0) + 1,
                "last_result": result,
            }
        except Exception as e:
            log.error("Failed to send review request to %s: %s", gid, e)
            errors.append(f"{gid}: {e}")

    save_state(state)

    # Report health
    detail = f"{sent_count} review request(s) {mode_label.lower()}ed"
    if errors:
        detail += f", {len(errors)} error(s)"
        status = "warning" if sent_count > 0 else "error"
    else:
        status = "ok"

    report_status("reviews", status, detail, metrics={
        "sent": sent_count,
        "eligible": len(eligible),
        "errors": len(errors),
        "mode": mode_label,
    })

    publish_event("reviews", "run_complete", {
        "eligible": len(eligible),
        "sent": sent_count,
        "mode": mode_label,
        "groups_sent": [e["group"]["group_id"] for e in eligible[:sent_count]],
    })

    log.info("=== Review Engine complete: %s ===", detail)
    return sent_count


def run_onboarding():
    """
    Check for new clients (first patrol 14-30 days ago) and send review requests.
    """
    mode_label = "DRAFT" if DRAFT_MODE else "SEND"
    log.info("=== Review Engine -- ONBOARDING MODE (%s) ===", mode_label)

    _check_new_reviews()

    eligible, state = check_onboarding_eligibility()

    if not eligible:
        log.info("No new clients eligible for onboarding review requests.")
        return 0

    sent_count = 0
    for entry in eligible:
        group = entry["group"]
        gid = group["group_id"]
        days_active = entry["days_active"]

        try:
            result = send_review_request(group, clean_days=days_active)
            sent_count += 1

            if SEND_SMS:
                try:
                    send_sms_review_request(group, clean_days=days_active)
                except Exception as sms_err:
                    log.warning("SMS failed for %s: %s", gid, sms_err)

            if "requests" not in state:
                state["requests"] = {}
            state["requests"][gid] = {
                "last_asked": datetime.now().isoformat(),
                "times_asked": state.get("requests", {}).get(gid, {}).get("times_asked", 0) + 1,
                "last_result": result,
                "trigger": "onboarding",
            }
            log.info("Onboarding review request %s for %s (%d days active)",
                     mode_label.lower(), gid, days_active)
        except Exception as e:
            log.error("Failed onboarding review request to %s: %s", gid, e)

    save_state(state)

    publish_event("reviews", "onboarding_run_complete", {
        "eligible": len(eligible),
        "sent": sent_count,
        "mode": mode_label,
        "groups_sent": [e["group"]["group_id"] for e in eligible[:sent_count]],
    })

    log.info("=== Onboarding review complete: %d request(s) %sed ===", sent_count, mode_label.lower())
    return sent_count


def run_respond():
    """Generate and post/draft AI responses to unresponded reviews."""
    config = tc.get_review_engine_config()
    mode_label = "AUTO-POST" if config.get("auto_respond", False) else "DRAFT"
    log.info("=== Review Engine -- RESPOND MODE (%s) ===", mode_label)

    from shared_utils.usage_tracker import check_budget
    budget = check_budget(tc.client_id())
    if not budget["ok"]:
        log.error("KILL SWITCH: Budget exceeded. Halting.")
        report_status("reviews", "killed", f"Budget exceeded: ${budget['used_usd']:.2f}")
        return 0

    from providers import get_reviews, get_sms
    review_provider = get_reviews()

    reviews = review_provider.get_reviews()
    respond_to = config.get("respond_to_stars", [1, 2, 3, 4, 5])
    neg_threshold = config.get("negative_star_threshold", 2)
    max_per_day = config.get("limits", {}).get("max_responses_per_day", 20)

    responded_count = 0

    for review in reviews:
        if responded_count >= max_per_day:
            log.warning("Daily response limit reached (%d). Stopping.", max_per_day)
            break

        if not should_respond(review, respond_to):
            continue

        response_text = generate_response(
            review=review,
            company_name=tc.company_name(),
            company_phone=tc.company_phone(),
            tone=config.get("response_tone", "professional_warm"),
            company_description=tc.company_description(),
            client_id=tc.client_id(),
        )

        if config.get("auto_respond", False):
            result = review_provider.post_response(review["id"], response_text)
            if result.get("success"):
                responded_count += 1
                log.info("Posted response to %s (%d-star): %.50s...",
                         review["reviewer_name"], review["star_rating"], response_text)
        else:
            log.info("DRAFT response to %s (%d-star): %s",
                     review["reviewer_name"], review["star_rating"], response_text)
            responded_count += 1

        if is_negative(review, neg_threshold) and config.get("negative_review_alert", True):
            try:
                sms = get_sms()
                alert_msg = (
                    f"NEGATIVE REVIEW ALERT: {review['star_rating']}-star from "
                    f"{review['reviewer_name']}: {review['text'][:100]}"
                )
                sms.send_sms(tc.owner_phone(), alert_msg)
                log.info("Negative review alert sent to owner")
            except Exception as e:
                log.warning("Failed to send negative review alert: %s", e)

        publish_event("reviews", "review_responded", {
            "review_id": review["id"],
            "reviewer_name": review["reviewer_name"],
            "star_rating": review["star_rating"],
            "mode": mode_label,
        })

    report_status("reviews", "ok", f"{responded_count} review response(s) {mode_label.lower()}",
                  metrics={"responded": responded_count, "mode": mode_label})

    log.info("=== Respond complete: %d response(s) ===", responded_count)
    return responded_count


def run_competitors():
    """Run competitor review monitoring."""
    log.info("=== Review Engine -- COMPETITOR MONITOR ===")
    result = run_monitor()
    log.info("Competitor monitor result: %s", result.get("status", "?"))
    return result


def main():
    if not tc.is_active():
        log.info("Client account paused -- skipping pipeline run")
        sys.exit(0)

    parser = argparse.ArgumentParser(description="Review Engine")
    parser.add_argument("--check", action="store_true", help="Dry run -- show eligible without sending")
    parser.add_argument("--force", metavar="GROUP_ID", help="Force send to a specific group_id")
    parser.add_argument("--onboarding", action="store_true",
                        help="Check for new clients (14-30 days) and send review requests")
    parser.add_argument("--respond", action="store_true",
                        help="Generate AI responses to unresponded reviews")
    parser.add_argument("--competitors", action="store_true",
                        help="Run competitor review monitoring")
    args = parser.parse_args()

    setup_logging()

    try:
        if args.check:
            run_check_only()
        elif args.force:
            run_force(args.force)
        elif args.onboarding:
            run_onboarding()
        elif args.respond:
            run_respond()
        elif args.competitors:
            run_competitors()
        else:
            # Monthly run
            run_normal()
    except Exception as e:
        log.exception("Review Engine failed: %s", e)
        report_status("reviews", "error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
