"""
Americal Patrol - Morning Report Automation
Runs the full pipeline:
  1. Fetch structured data from Connecteam API
  2. Generate branded PDFs from API data
  3. Compose email body via Claude API
  4. Create Gmail draft with branded PDF(s) attached
"""

import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from collections import defaultdict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared_utils.event_bus import publish_event

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / '.env')

from email_fetcher import get_gmail_service
from pdf_analyzer  import load_clients
from connecteam_api import fetch_daily_reports, fetch_vehicle_inspections
from draft_composer    import compose_email_body, build_draft
from supervisor_report import send_supervisor_report, check_and_send_quality_report

LOG_FILE = Path(__file__).parent / 'automation.log'


def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def run():
    log("=" * 60)
    log("Americal Patrol Morning Report Automation - Starting")
    log("=" * 60)

    # ── Step 1: Connect to Gmail (for sending only) ────────────────
    log("Connecting to Gmail for email delivery...")
    try:
        service = get_gmail_service()
    except Exception as e:
        log(f"ERROR: Gmail connection failed: {e}")
        return False

    # ── Step 2: Fetch structured data from Connecteam API ────────
    log("Fetching report data from Connecteam API...")
    clients_groups = load_clients()
    report_date = datetime.now().date()

    try:
        api_data = fetch_daily_reports(clients_groups, report_date)
    except Exception as e:
        log(f"CRITICAL: Connecteam API fetch failed: {e}")
        traceback.print_exc()
        return False

    if not api_data:
        log("No report data returned from Connecteam API. Nothing to do.")
        return True

    # Build grouped dict from API results
    group_lookup = {g['group_id']: g for g in clients_groups}
    grouped = {}
    for gid, reports_data in api_data.items():
        if gid in group_lookup:
            grouped[gid] = {
                'group': group_lookup[gid],
                'pdfs': [],
                'reports_data': reports_data,
            }
            log(f"  {gid}: {len(reports_data)} report(s) from API")

    if not grouped:
        log("No API data matched to client groups.")
        return False

    # ── Step 3: Fetch vehicle inspections from API ───────────────
    log("Fetching vehicle inspection data from Connecteam API...")
    try:
        api_inspections = fetch_vehicle_inspections(report_date)
        if api_inspections:
            log(f"  Got {len(api_inspections)} vehicle inspection(s)")
        else:
            log("  No vehicle inspections found")
    except Exception as e:
        log(f"WARNING: Vehicle inspection fetch failed: {e}")
        traceback.print_exc()
        api_inspections = []

    # ── Step 4: Generate branded PDFs ────────────────────────────
    log("Generating branded PDFs...")
    REPORTS_FOLDER = Path(__file__).parent.parent / "Americal Patrol Morning Reports"
    REPORTS_FOLDER.mkdir(exist_ok=True)

    try:
        from branded_pdf import generate_branded_pdf
    except ImportError as e:
        log(f"ERROR: branded_pdf module not available: {e}")
        return False

    for gid, data in grouped.items():
        group = data['group']
        reports_data = data['reports_data']

        # Skip branded PDF generation for incident-only accounts with no incidents
        if group.get('incident_only') and not any(
            rd and rd.get('has_incidents') for rd in reports_data
        ):
            account_names = ', '.join(a['name'] for a in group['accounts'])
            log(f"  SKIP PDF: {account_names} — incident-only, no incidents today.")
            data['pdfs'] = []
            continue

        client_name = (
            group['accounts'][0]['name']
            if group.get('accounts') else ""
        )
        branded_pdfs = []

        for rd in reports_data:
            prop_name = rd.get('property', 'report').replace(' ', '_')
            rtype = rd.get('report_type', '')

            # Incident-only accounts: skip DARs/vehicle DARs — only brand actual incident reports
            if group.get('incident_only') and rtype != 'incident':
                log(f"  SKIP PDF: {prop_name} ({rtype}) — incident-only account, not an incident report.")
                continue

            placeholder_path = REPORTS_FOLDER / f"api_{prop_name}.pdf"

            try:
                branded = generate_branded_pdf(placeholder_path, rd, client_name=client_name)
            except Exception as e:
                log(f"WARNING: Branding failed for {prop_name}: {e}")
                branded = None

            if branded:
                branded_pdfs.append(branded)
                log(f"  Branded: {branded.name}")

        data['pdfs'] = branded_pdfs

    # ── Step 5: Wait until 7:00 AM to send ─────────────────────────────
    import time as _time
    SEND_AT_HOUR = int(os.environ.get("MORNING_SEND_HOUR", "7"))
    SEND_AT_MIN = int(os.environ.get("MORNING_SEND_MINUTE", "0"))
    now = datetime.now()
    target_time = now.replace(hour=SEND_AT_HOUR, minute=SEND_AT_MIN, second=0, microsecond=0)
    if now < target_time:
        wait_seconds = (target_time - now).total_seconds()
        log(f"Prep complete. Waiting until {SEND_AT_HOUR}:{SEND_AT_MIN:02d} AM to send ({int(wait_seconds)}s)...")
        _time.sleep(wait_seconds)
        log(f"It's {datetime.now().strftime('%I:%M %p')} — sending emails now.")

    # ── Step 6: Compose and send emails ──────────────────────────────────
    log(f"Processing emails for {len(grouped)} client group(s)...")
    drafts_created = 0

    for gid, data in grouped.items():
        group        = data['group']
        pdfs         = data['pdfs']
        reports_data = data['reports_data']
        account_names = ', '.join(a['name'] for a in group['accounts'])

        log(f"Composing email for: {account_names}")

        # Skip incident-only accounts when there are no incidents today
        if group.get('incident_only') and not any(
            rd and rd.get('has_incidents') for rd in reports_data
        ):
            log(f"SKIP: {account_names} — incident-only account, no incidents today.")
            continue

        try:
            subject, email_body = compose_email_body(group, reports_data, pdfs)
        except Exception as e:
            log(f"ERROR: Could not compose email for {gid}: {e}")
            traceback.print_exc()
            continue

        try:
            build_draft(service, group, reports_data, pdfs, subject, email_body)
            drafts_created += 1
            log(f"DRAFT CREATED: {account_names}")
        except Exception as e:
            log(f"ERROR: Could not create draft for {gid}: {e}")
            traceback.print_exc()
            continue


    # -- Step 7: Supervisor summary draft ----------------------------------------
    log("Creating supervisor summary draft...")
    try:
        date_str = datetime.now().strftime("%B %d, %Y")

        all_pdfs = [p for v in grouped.values() for p in v["pdfs"]]
        all_reports_data = [d for v in grouped.values() for d in v["reports_data"]]

        send_supervisor_report(
            service, date_str, all_reports_data, all_pdfs,
            api_inspections=api_inspections
        )
        log("Supervisor report sent successfully.")
    except Exception as e:
        log(f"ERROR: Supervisor report failed: {e}")
        traceback.print_exc()

    # -- Step 8: Quality check email ---------------------------------------------
    log("Running quality check on incident reports...")
    try:
        check_and_send_quality_report(service, date_str, all_reports_data, all_pdfs)
    except Exception as e:
        log(f"ERROR: Quality check failed: {e}")
        traceback.print_exc()


    # -- Step 9b: Ingest incidents for trend analysis -----------------------------
    try:
        from incident_trends.ingest import ingest_daily
        result = ingest_daily(grouped, date_str)
        log(f"Incident trend data ingested: {result['incidents']} incidents, "
            f"{result['patrols']} patrol days.")
    except Exception as e:
        log(f"WARNING: Trend ingestion failed: {e}")

    # -- Step 10: Publish event to pipeline bus -----------------------------------
    try:
        incident_accounts = []
        total_accounts = 0
        overall_incidents = 0
        for gid, data in grouped.items():
            group = data['group']
            reports_data = data['reports_data']
            acct_names = [a['name'] for a in group.get('accounts', [])]
            total_accounts += len(acct_names)
            has_incidents = any(rd and rd.get('has_incidents') for rd in reports_data)
            if has_incidents:
                incident_accounts.extend(acct_names)
                overall_incidents += sum(
                    1 for rd in reports_data if rd and rd.get('has_incidents')
                )
        publish_event("patrol", "daily_summary", {
            "report_date": date_str,
            "total_accounts": total_accounts,
            "accounts_with_incidents": len(incident_accounts),
            "incident_accounts": incident_accounts,
            "drafts_created": drafts_created,
            "overall_incidents": overall_incidents,
        })
        log("Pipeline event published.")
    except Exception as e:
        log(f"WARNING: Event bus publish failed: {e}")

    # ── Done ──────────────────────────────────────────────────────
    log(f"Done. {drafts_created} email(s) sent to clients.")
    return True


if __name__ == '__main__':
    success = run()
    sys.exit(0 if success else 1)
