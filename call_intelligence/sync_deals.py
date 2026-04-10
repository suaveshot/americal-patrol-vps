"""
Call Intelligence — Daily Deal Sync
Pulls opportunity data from GHL and links calls to deal outcomes.
"""

import logging
from datetime import datetime, timezone

from call_intelligence import config
from call_intelligence.config import LOG_FILE, load_config
from call_intelligence.db import (
    get_connection, upsert_deal, link_calls_to_deal, recalculate_deal_stats,
)
from call_intelligence.run_ingestion import load_state, save_state
from sales_pipeline.ghl_client import GHLClient, GHLAPIError
from shared_utils.health_reporter import report_status

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(str(LOG_FILE), encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("call_intelligence.deals")


def sync_all_opportunities(ghl: GHLClient, conn) -> dict:
    """Sync all opportunities from GHL pipeline to deals table."""
    pipeline_id = config.PIPELINE_ID()
    if not pipeline_id:
        log.warning("GHL_INQUIRIES_PIPELINE_ID not set, skipping deal sync")
        return {"synced": 0, "linked": 0}

    now = datetime.now(timezone.utc).isoformat()
    synced = 0
    linked = 0

    try:
        # Note: GHLClient.search_opportunities() returns one page of results.
        # At current volume (<50 deals), this is sufficient. If the pipeline
        # grows past ~100 opportunities, add pagination to ghl_client.py.
        opportunities = ghl.search_opportunities(pipeline_id)
    except GHLAPIError as e:
        log.error("Failed to fetch opportunities: %s", e)
        return {"synced": 0, "linked": 0, "error": str(e)}

    log.info("Found %d opportunities to sync", len(opportunities))

    for opp in opportunities:
        opp_id = opp.get("id", "")
        if not opp_id:
            continue

        contact_id = opp.get("contact", {}).get("id", opp.get("contactId", ""))
        contact_name = opp.get("contact", {}).get("name", "")
        company_name = opp.get("contact", {}).get("companyName", "")
        monetary = opp.get("monetaryValue", 0) or 0
        stage_name = opp.get("pipelineStage", {}).get("name", opp.get("stageName", ""))
        status = opp.get("status", "open")

        # Map GHL status to our outcome
        if status == "won":
            outcome = "won"
        elif status in ("lost", "abandoned"):
            outcome = "lost"
        else:
            outcome = "open"

        upsert_deal(
            conn,
            ghl_opportunity_id=opp_id,
            ghl_contact_id=contact_id,
            contact_name=contact_name,
            company_name=company_name,
            deal_value=float(monetary),
            deal_type=None,
            pipeline_stage=stage_name,
            outcome=outcome,
            won_at=now if outcome == "won" else None,
            lost_at=now if outcome == "lost" else None,
            loss_reason=opp.get("lostReasonId"),
            synced_at=now,
        )
        synced += 1

        # Link calls to this deal
        rows = link_calls_to_deal(conn, contact_id, opp_id)
        if rows:
            linked += rows
            log.info("Linked %d calls to deal %s (%s)", rows, opp_id, contact_name)

        # Recalculate deal stats
        recalculate_deal_stats(conn, opp_id)

    conn.commit()
    return {"synced": synced, "linked": linked}


def run():
    log.info("=== Call Intelligence Deal Sync Starting ===")

    config.validate_config()
    state = load_state()

    ghl = GHLClient()
    conn = get_connection()

    try:
        result = sync_all_opportunities(ghl, conn)
    except Exception as e:
        log.error("Deal sync failed: %s", e)
        report_status("call_intelligence", "error", f"Deal sync failed: {e}")
        raise
    finally:
        conn.close()

    state["last_deal_sync_at"] = datetime.now(timezone.utc).isoformat()
    save_state(state)

    status_msg = f"Synced {result['synced']} deals, linked {result['linked']} calls"
    log.info("=== Deal sync complete: %s ===", status_msg)
    report_status("call_intelligence", "ok", status_msg, metrics=result)


if __name__ == "__main__":
    run()
