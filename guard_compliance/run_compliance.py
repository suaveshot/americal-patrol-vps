# guard_compliance/run_compliance.py
"""
Americal Patrol — Guard Compliance & Licensing Tracker
Entry point / orchestrator.

Usage:
    python -m guard_compliance.run_compliance              # Normal daily run
    python -m guard_compliance.run_compliance --check      # Dry run (no notifications)
    python -m guard_compliance.run_compliance --report     # Force compliance report
    python -m guard_compliance.run_compliance --discover   # Log all Connecteam field names
    python -m guard_compliance.run_compliance --refresh-bsis  # Force re-download DCA data
"""

import argparse
import json
import logging
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from guard_compliance import config
from guard_compliance.connecteam_client import (
    get_all_users, discover_custom_fields, extract_officer_data,
)
from guard_compliance.bsis_verifier import (
    download_bsis_data, load_bsis_data, verify_officer,
)
from guard_compliance.compliance_engine import (
    calculate_status, get_pending_notifications, check_bsis_verification,
    update_notification_history, reset_notification_history, detect_renewals,
    load_state, save_state,
)
from guard_compliance.notification_sender import (
    send_sam_alerts, send_officer_notifications,
)
from guard_compliance.report_generator import generate_and_send_report

# ── Logging ───────────────────────────────────────────────────────────────────

log = logging.getLogger("guard_compliance")


def setup_logging():
    log.setLevel(logging.INFO)

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S"))
    log.addHandler(ch)

    # File handler
    fh = logging.FileHandler(config.LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s",
                                      datefmt="%Y-%m-%d %H:%M:%S"))
    log.addHandler(fh)


def load_pipeline_config() -> dict:
    """Load compliance_config.json."""
    try:
        with open(config.CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        log.error(f"Cannot load config: {e}")
        sys.exit(1)


# ── Commands ──────────────────────────────────────────────────────────────────

def cmd_discover():
    """Log all Connecteam custom field names for initial setup."""
    log.info("=== DISCOVER MODE — Scanning Connecteam custom fields ===")

    api_key = config.CONNECTEAM_API_KEY()
    if not api_key or "REPLACE" in api_key:
        log.error("CONNECTEAM_API_KEY is not set. Get your API key from:")
        log.error("  Connecteam → Settings → API Keys → Add API key")
        log.error("  Then add it to .env: CONNECTEAM_API_KEY=your-key-here")
        return

    try:
        users = get_all_users(api_key)
    except Exception as e:
        log.error(f"Failed to connect to Connecteam API: {e}")
        log.error("Check that your CONNECTEAM_API_KEY is correct and your plan supports API access (Expert+)")
        return

    log.info(f"Fetched {len(users)} users from Connecteam")

    field_names = discover_custom_fields(users)

    log.info(f"\n{'='*60}")
    log.info(f"DISCOVERED {len(field_names)} CUSTOM FIELDS:")
    log.info(f"{'='*60}")
    for name in sorted(field_names):
        log.info(f"  - {name}")
    log.info(f"{'='*60}")
    log.info("Update compliance_config.json 'connecteam_field_mappings' with the correct field names.")

    # Also save to state for reference
    state = load_state(config.STATE_FILE)
    state["discovered_fields"] = sorted(field_names)
    save_state(state, config.STATE_FILE)


def cmd_check(pipeline_config: dict):
    """Dry run — show compliance status without sending notifications."""
    log.info("=== CHECK MODE — Dry run (no notifications) ===")

    api_key = config.CONNECTEAM_API_KEY()
    field_mappings = pipeline_config.get("connecteam_field_mappings", {})

    # Fetch users
    users = get_all_users(api_key)
    log.info(f"Fetched {len(users)} active users from Connecteam")

    # Parse officer data
    officers = {}
    for user in users:
        officer = extract_officer_data(user, field_mappings)
        oid = officer["connecteam_id"]
        if oid:
            officers[oid] = officer

    log.info(f"Parsed {len(officers)} officers with credential data")

    # BSIS verification (if data available)
    bsis_config = pipeline_config.get("bsis_verification", {})
    bsis_data = {}
    if bsis_config.get("enabled", True):
        csv_path = download_bsis_data(config.BSIS_DIR, force=False)
        if csv_path:
            bsis_data = load_bsis_data(csv_path)

    # Calculate and display status
    thresholds = pipeline_config.get("alert_thresholds_days", {})
    log.info(f"\n{'='*70}")
    log.info(f"{'Officer':<25} {'Guard Card':<15} {'Status':<12} {'Days':>6}  {'BSIS':>10}")
    log.info(f"{'-'*70}")

    for oid, officer in sorted(officers.items(), key=lambda x: x[1]["name"]):
        gc_num = officer["credentials"].get("guard_card_number", "N/A")
        gc_expiry = officer["credentials"].get("guard_card_expiry")
        status, days = calculate_status(gc_expiry, thresholds)
        days_str = str(days) if days is not None else "N/A"

        # BSIS check
        bsis_result = verify_officer(
            gc_num or "", officer["name"], gc_expiry, bsis_data
        ) if bsis_data else {"verified": None}
        bsis_str = "OK" if bsis_result["verified"] else (
            "FAIL" if bsis_result["verified"] is False else "N/A"
        )

        log.info(f"  {officer['name']:<23} {gc_num or 'N/A':<15} {status:<12} {days_str:>6}  {bsis_str:>10}")

    log.info(f"{'='*70}")


def cmd_report(pipeline_config: dict):
    """Force-generate and send compliance report."""
    log.info("=== REPORT MODE — Generating compliance report ===")

    api_key = config.CONNECTEAM_API_KEY()
    field_mappings = pipeline_config.get("connecteam_field_mappings", {})
    test_mode = pipeline_config.get("test_mode", True) or config.is_test_mode()

    # Fetch and parse
    users = get_all_users(api_key)
    officers = {}
    for user in users:
        officer = extract_officer_data(user, field_mappings)
        oid = officer["connecteam_id"]
        if oid:
            officers[oid] = officer

    # BSIS verification
    bsis_results = {}
    bsis_config = pipeline_config.get("bsis_verification", {})
    if bsis_config.get("enabled", True):
        csv_path = download_bsis_data(config.BSIS_DIR, force=False)
        if csv_path:
            bsis_lookup = load_bsis_data(csv_path)
            for oid, officer in officers.items():
                gc_num = officer["credentials"].get("guard_card_number", "")
                gc_expiry = officer["credentials"].get("guard_card_expiry")
                bsis_results[oid] = verify_officer(gc_num, officer["name"], gc_expiry, bsis_lookup)

    # Build state-like structure for report
    state = {"officers": officers}
    sent = generate_and_send_report(state, bsis_results, test_mode=test_mode)
    log.info(f"Report {'sent' if sent else 'FAILED to send'}")


def cmd_daily(pipeline_config: dict):
    """Normal daily compliance check run."""
    log.info("=== DAILY COMPLIANCE CHECK ===")

    api_key = config.CONNECTEAM_API_KEY()
    field_mappings = pipeline_config.get("connecteam_field_mappings", {})
    thresholds = pipeline_config.get("alert_thresholds_days", {})
    test_mode = pipeline_config.get("test_mode", True) or config.is_test_mode()

    if test_mode:
        log.info("TEST MODE is ON — all notifications go to Sam")

    # ── Step 1: Fetch users from Connecteam ──────────────────────────────
    try:
        users = get_all_users(api_key)
        log.info(f"Fetched {len(users)} active users from Connecteam")
    except Exception as e:
        log.error(f"Failed to fetch users from Connecteam: {e}")
        log.info("Falling back to cached state for expiry checks...")
        users = []

    # ── Step 2: Parse officer data ───────────────────────────────────────
    new_officers = {}
    for user in users:
        officer = extract_officer_data(user, field_mappings)
        oid = officer["connecteam_id"]
        if oid:
            new_officers[oid] = officer

    # Log discovered fields on every run
    if users:
        fields = discover_custom_fields(users)
        log.info(f"Connecteam custom fields found: {sorted(fields)}")

    # ── Step 3: Load previous state ──────────────────────────────────────
    state = load_state(config.STATE_FILE)

    # ── Step 4: Detect renewals (reset notification history) ─────────────
    if new_officers:
        renewals = detect_renewals(state, new_officers)
        for oid, cred_type in renewals:
            reset_notification_history(state, oid, cred_type)

    # ── Step 5: BSIS verification ────────────────────────────────────────
    bsis_results = {}
    bsis_config = pipeline_config.get("bsis_verification", {})
    bsis_alerts = []

    if bsis_config.get("enabled", True):
        try:
            force_refresh = False  # Set True via --refresh-bsis flag
            csv_path = download_bsis_data(config.BSIS_DIR, force=force_refresh)
            if csv_path:
                bsis_lookup = load_bsis_data(csv_path)
                for oid, officer in new_officers.items():
                    gc_num = officer["credentials"].get("guard_card_number", "")
                    gc_expiry = officer["credentials"].get("guard_card_expiry")
                    result = verify_officer(gc_num, officer["name"], gc_expiry, bsis_lookup)
                    bsis_results[oid] = result

                    # Check for BSIS issues
                    officer_with_state = {**officer, **state.get("officers", {}).get(oid, {})}
                    alerts = check_bsis_verification(officer_with_state, result)
                    bsis_alerts.extend(alerts)
        except Exception as e:
            log.warning(f"BSIS verification failed (non-fatal): {e}")

    # ── Step 6: Update state with new officer data ───────────────────────
    if new_officers:
        for oid, officer in new_officers.items():
            existing = state.get("officers", {}).get(oid, {})
            state.setdefault("officers", {})[oid] = {
                **existing,
                "name": officer["name"],
                "email": officer["email"],
                "phone": officer["phone"],
                "status": officer["status"],
                "credentials": officer["credentials"],
                "bsis_verification": bsis_results.get(oid, {}),
                "last_synced": datetime.now().isoformat(),
            }

        if users:
            state["discovered_fields"] = sorted(discover_custom_fields(users))

    # ── Step 7: Calculate compliance and pending notifications ────────────
    all_pending = []
    officer_pending = defaultdict(list)  # officer_id → [alerts]
    sam_alerts_by_tier = defaultdict(list)

    for oid, officer_data in state.get("officers", {}).items():
        # Merge stored notifications with officer data
        check_data = {**officer_data}
        pending = get_pending_notifications(check_data, pipeline_config)

        for p in pending:
            all_pending.append(p)
            officer_pending[oid].append(p)
            sam_alerts_by_tier[p["tier"]].append(p)

    # Add BSIS alerts to Sam's queue
    for alert in bsis_alerts:
        sam_alerts_by_tier[alert["tier"]].append(alert)

    log.info(f"Officers checked: {len(state.get('officers', {}))} | "
             f"Pending notifications: {len(all_pending)} | "
             f"BSIS alerts: {len(bsis_alerts)}")

    # ── Step 8: Send notifications ───────────────────────────────────────
    sam_sent = 0
    officer_sent = 0

    if sam_alerts_by_tier:
        sam_sent = send_sam_alerts(dict(sam_alerts_by_tier), test_mode=test_mode)
        log.info(f"Sam alerts sent: {sam_sent}")

    # Only send officer notifications for reminder/urgent/critical/expired tiers
    officer_notify_tiers = {"reminder", "urgent", "critical", "expired"}
    officer_to_notify = {}
    for oid, alerts in officer_pending.items():
        notify_alerts = [a for a in alerts if a["tier"] in officer_notify_tiers]
        if notify_alerts:
            officer_to_notify[oid] = notify_alerts

    if officer_to_notify:
        officer_sent = send_officer_notifications(officer_to_notify, test_mode=test_mode)
        log.info(f"Officer notifications sent: {officer_sent}")

    # ── Step 9: Update notification history ──────────────────────────────
    for p in all_pending:
        officer = p.get("officer", {})
        oid = officer.get("connecteam_id", "")
        if oid:
            update_notification_history(state, oid, p["credential_type"], p["tier"])

    # ── Step 10: Generate weekly report (if Monday or new BSIS issue) ────
    today = datetime.now()
    report_day = pipeline_config.get("schedule", {}).get("report_day", "Monday")
    is_report_day = today.strftime("%A") == report_day
    has_new_bsis_issues = bool(bsis_alerts)

    if is_report_day or has_new_bsis_issues:
        reason = "report day" if is_report_day else "new BSIS issues detected"
        log.info(f"Generating compliance report ({reason})")
        generate_and_send_report(state, bsis_results, test_mode=test_mode)

    # ── Step 11: Save state ──────────────────────────────────────────────
    save_state(state, config.STATE_FILE)

    # ── Step 12: Publish event + report health ───────────────────────────
    total_officers = len(state.get("officers", {}))
    compliant_count = 0
    for oid, data in state.get("officers", {}).items():
        gc_expiry = data.get("credentials", {}).get("guard_card_expiry")
        if isinstance(gc_expiry, dict):
            gc_expiry = gc_expiry.get("expiry")
        status, _ = calculate_status(gc_expiry, thresholds)
        if status == "valid":
            compliant_count += 1

    try:
        from shared_utils.event_bus import publish_event
        publish_event("guard_compliance", "compliance_check", {
            "officers_checked": total_officers,
            "fully_compliant": compliant_count,
            "alerts_sent": sam_sent + officer_sent,
            "bsis_issues": len(bsis_alerts),
        })
    except Exception as e:
        log.warning(f"Failed to publish event: {e}")

    try:
        from shared_utils.health_reporter import report_status
        detail = (f"{compliant_count}/{total_officers} compliant, "
                  f"{sam_sent + officer_sent} alerts sent")
        status = "ok" if not bsis_alerts and compliant_count == total_officers else "warning"
        report_status("guard_compliance", status, detail,
                      metrics={
                          "officers_checked": total_officers,
                          "compliant": compliant_count,
                          "alerts_sent": sam_sent + officer_sent,
                          "bsis_issues": len(bsis_alerts),
                      })
    except Exception as e:
        log.warning(f"Failed to report health: {e}")

    log.info(f"=== DONE — {compliant_count}/{total_officers} compliant ===")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    setup_logging()

    parser = argparse.ArgumentParser(description="Guard Compliance & Licensing Tracker")
    parser.add_argument("--check", action="store_true", help="Dry run — show status, no notifications")
    parser.add_argument("--report", action="store_true", help="Force compliance report")
    parser.add_argument("--discover", action="store_true", help="Log all Connecteam custom field names")
    parser.add_argument("--refresh-bsis", action="store_true", help="Force re-download DCA BSIS data")
    args = parser.parse_args()

    log.info(f"Guard Compliance Tracker starting at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        config.validate_config()
    except EnvironmentError as e:
        log.error(str(e))
        sys.exit(1)

    if args.discover:
        cmd_discover()
        return

    pipeline_config = load_pipeline_config()

    if args.refresh_bsis:
        log.info("Forcing BSIS data refresh...")
        csv_path = download_bsis_data(config.BSIS_DIR, force=True)
        if csv_path:
            bsis_data = load_bsis_data(csv_path)
            log.info(f"BSIS data refreshed: {len(bsis_data)} records loaded")
        else:
            log.error("Failed to download BSIS data")
        if not args.check and not args.report:
            return

    if args.check:
        cmd_check(pipeline_config)
    elif args.report:
        cmd_report(pipeline_config)
    else:
        cmd_daily(pipeline_config)


if __name__ == "__main__":
    main()
