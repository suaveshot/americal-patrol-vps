"""
Americal Patrol — QBR Generator Orchestrator

Generates quarterly business review PDFs for all clients (or a specific client)
and delivers them via email.

Usage:
    python run_qbr.py                       # Generate QBRs for ALL clients
    python run_qbr.py --client "Harbor Lights"  # Single client by property name
    python run_qbr.py --client harbor_lights    # Single client by group_id
    python run_qbr.py --check                # Dry run — show what would be generated

Schedule: Quarterly, 1st Monday of Jan/Apr/Jul/Oct, 9:00 AM
"""

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Ensure project root is on path for shared_utils
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import (
    LOG_FILE, load_clients, load_state, save_state, current_quarter,
)
from data_aggregator import aggregate_client_data, get_prior_quarter_data
from trend_analyzer import compute_trends, generate_narrative
from report_generator import render_report, generate_pdf
from email_sender import send_qbr
from shared_utils.event_bus import publish_event
from shared_utils.health_reporter import report_status

log = logging.getLogger("qbr_generator")


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


def _find_group(clients, identifier):
    """Find a client group by group_id or property name (case-insensitive)."""
    identifier_lower = identifier.lower().strip()
    for group in clients:
        if group["group_id"].lower() == identifier_lower:
            return group
        for account in group.get("accounts", []):
            if account["name"].lower() == identifier_lower:
                return group
    return None


def generate_single_qbr(group, quarter_label, quarter_months, quarter_year):
    """
    Generate a complete QBR for one client group.
    Returns (pdf_path, data, trends, narrative) or raises on error.
    """
    gid = group["group_id"]
    props = ", ".join(a["name"] for a in group["accounts"])

    log.info("Generating QBR for %s (%s)...", gid, props)

    # Step 1: Aggregate data
    log.info("  Aggregating patrol data for %s...", quarter_label)
    data = aggregate_client_data(group, quarter_months, quarter_year)

    # Step 2: Get prior quarter for comparison (may be None)
    prior = get_prior_quarter_data(group, quarter_months, quarter_year)

    # Step 3: Compute trends
    trends = compute_trends(data, prior)
    log.info(
        "  %s: %d patrol days, %d incidents, %.1f%% incident-free",
        gid, data["total_patrol_days"], data["total_incidents"],
        trends["incident_free_pct"],
    )

    # Step 4: Generate AI narrative
    log.info("  Generating narrative sections...")
    narrative = generate_narrative(data, trends, prior)

    # Step 5: Render HTML and generate PDF
    log.info("  Rendering report...")
    html = render_report(data, trends, narrative)
    pdf_path = generate_pdf(html, gid, quarter_label)

    log.info("  QBR complete: %s", pdf_path)
    return pdf_path, data, trends, narrative


def run_check_only(clients, quarter_label, quarter_months, quarter_year):
    """Dry run: show what QBRs would be generated."""
    log.info("=== QBR Generator — CHECK MODE (dry run) ===")
    log.info("Quarter: %s", quarter_label)
    log.info("Clients: %d", len(clients))

    for group in clients:
        gid = group["group_id"]
        props = ", ".join(a["name"] for a in group["accounts"])
        recipients = ", ".join(group["recipients"])
        data = aggregate_client_data(group, quarter_months, quarter_year)
        log.info(
            "  %s (%s) — %d patrol days, %d incidents → %s",
            gid, props, data["total_patrol_days"], data["total_incidents"], recipients,
        )


def run_normal(clients, quarter_label, quarter_months, quarter_year):
    """Generate and deliver QBRs for all clients."""
    log.info("=== QBR Generator — %s ===", quarter_label)

    state = load_state()
    generated = 0
    errors = []

    for group in clients:
        gid = group["group_id"]
        try:
            pdf_path, data, trends, narrative = generate_single_qbr(
                group, quarter_label, quarter_months, quarter_year
            )

            # Send email
            result = send_qbr(group, pdf_path, quarter_label)

            # Update state
            state["reports"][gid] = {
                "quarter": quarter_label,
                "generated_at": datetime.now().isoformat(),
                "pdf_path": str(pdf_path),
                "incidents": data["total_incidents"],
                "patrol_days": data["total_patrol_days"],
                "email_result": result,
            }

            generated += 1

        except Exception as e:
            log.error("Failed to generate QBR for %s: %s", gid, e, exc_info=True)
            errors.append(f"{gid}: {e}")

    save_state(state)

    # Report health
    detail = f"{generated} QBR(s) generated for {quarter_label}"
    if errors:
        detail += f", {len(errors)} error(s)"
        status = "warning" if generated > 0 else "error"
    else:
        status = "ok"

    report_status("qbr", status, detail, metrics={
        "generated": generated,
        "total_clients": len(clients),
        "errors": len(errors),
        "quarter": quarter_label,
    })

    publish_event("qbr", "reports_generated", {
        "quarter": quarter_label,
        "generated": generated,
        "total_clients": len(clients),
        "groups": [g["group_id"] for g in clients[:generated]],
    })

    log.info("=== QBR Generator complete: %s ===", detail)
    return generated


def main():
    parser = argparse.ArgumentParser(description="Americal Patrol QBR Generator")
    parser.add_argument("--client", metavar="NAME", help="Generate for a single client (name or group_id)")
    parser.add_argument("--check", action="store_true", help="Dry run — show what would be generated")
    args = parser.parse_args()

    setup_logging()

    quarter_label, quarter_months, quarter_year = current_quarter()
    all_clients = load_clients()

    try:
        if args.client:
            group = _find_group(all_clients, args.client)
            if not group:
                log.error("Client '%s' not found. Available: %s",
                          args.client, ", ".join(g["group_id"] for g in all_clients))
                sys.exit(1)
            clients = [group]
        else:
            clients = all_clients

        if args.check:
            run_check_only(clients, quarter_label, quarter_months, quarter_year)
        else:
            run_normal(clients, quarter_label, quarter_months, quarter_year)

    except Exception as e:
        log.exception("QBR Generator failed: %s", e)
        report_status("qbr", "error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
