"""
Usage Aggregator
Reads per-client JSONL usage logs, computes monthly totals, pushes to
Airtable Clients table, and checks threshold alerts.

Cron: Daily 11:00 PM (after all pipelines finish)

Usage:
    python -m usage_tracker.aggregate_usage
"""

import json
import logging
import os
import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import tenant_context as tc
from shared_utils.health_reporter import report_status

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

USAGE_LOGS_DIR = _PROJECT_ROOT / "usage_logs"
ALERT_STATE_FILE = Path(__file__).parent / "alert_state.json"


def _load_alert_state() -> dict:
    try:
        with open(ALERT_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_alert_state(state: dict):
    tmp = str(ALERT_STATE_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(ALERT_STATE_FILE))


def aggregate_client_usage(client_id: str, month: str | None = None) -> dict:
    """
    Read a client's JSONL usage log for the given month and return totals.

    Returns:
        {
            "input_tokens": int,
            "output_tokens": int,
            "est_cost_usd": float,
            "call_count": int,
            "by_pipeline": {"blog": {"input": ..., "output": ..., "cost": ...}, ...}
        }
    """
    if month is None:
        month = datetime.now().strftime("%Y%m")

    log_file = USAGE_LOGS_DIR / f"{client_id}_{month}.jsonl"
    if not log_file.exists():
        return {
            "input_tokens": 0,
            "output_tokens": 0,
            "est_cost_usd": 0.0,
            "call_count": 0,
            "by_pipeline": {},
        }

    totals = {
        "input_tokens": 0,
        "output_tokens": 0,
        "est_cost_usd": 0.0,
        "call_count": 0,
        "by_pipeline": {},
    }

    with open(log_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue

            totals["input_tokens"] += record.get("input_tokens", 0)
            totals["output_tokens"] += record.get("output_tokens", 0)
            totals["est_cost_usd"] += record.get("est_cost_usd", 0.0)
            totals["call_count"] += 1

            pipeline = record.get("pipeline", "unknown")
            if pipeline not in totals["by_pipeline"]:
                totals["by_pipeline"][pipeline] = {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "est_cost_usd": 0.0,
                }
            bp = totals["by_pipeline"][pipeline]
            bp["input_tokens"] += record.get("input_tokens", 0)
            bp["output_tokens"] += record.get("output_tokens", 0)
            bp["est_cost_usd"] += record.get("est_cost_usd", 0.0)

    totals["est_cost_usd"] = round(totals["est_cost_usd"], 4)
    for bp in totals["by_pipeline"].values():
        bp["est_cost_usd"] = round(bp["est_cost_usd"], 4)

    return totals


def push_to_airtable(client_id: str, totals: dict):
    """Update the client's Airtable record with usage data."""
    pat = os.getenv("AIRTABLE_PAT", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")
    clients_table_id = os.getenv("AIRTABLE_CLIENTS_TABLE_ID", "")

    if not all([pat, base_id, clients_table_id]):
        log.warning("Airtable credentials not set -- skipping push")
        return

    try:
        from pyairtable import Api
        api = Api(pat)
        table = api.table(base_id, clients_table_id)

        # Find the client record by client_id field
        records = table.all(formula=f"{{client_id}} = '{client_id}'")
        if not records:
            log.warning(f"No Airtable record found for client_id={client_id}")
            return

        record_id = records[0]["id"]

        # Top pipeline by cost
        top_pipeline = ""
        if totals["by_pipeline"]:
            top_pipeline = max(
                totals["by_pipeline"],
                key=lambda p: totals["by_pipeline"][p]["est_cost_usd"],
            )

        fields = {
            "Monthly Tokens (Input)": totals["input_tokens"],
            "Monthly Tokens (Output)": totals["output_tokens"],
            "Est Monthly API Cost": round(totals["est_cost_usd"], 2),
            "Top Pipeline by Usage": top_pipeline,
            "Usage Last Updated": datetime.now().isoformat(),
            "Pipeline Usage Breakdown": json.dumps(totals["by_pipeline"]),
        }

        table.update(record_id, fields)
        log.info(f"Airtable updated for {client_id}: ${totals['est_cost_usd']:.2f}")

    except Exception as e:
        log.error(f"Airtable push failed for {client_id}: {e}")


def check_threshold_alerts(client_id: str, totals: dict, plan_tier: str = ""):
    """Check if the client's usage has crossed alert thresholds."""
    thresholds = tc.usage_thresholds()
    tier_config = thresholds.get(plan_tier.lower().replace(" ", "_").replace("-", "_"), {})
    limit = tier_config.get("monthly_cost_limit_usd", 0)

    if limit <= 0:
        return  # No threshold configured for this tier

    cost = totals["est_cost_usd"]
    alert_state = _load_alert_state()
    month = datetime.now().strftime("%Y%m")
    client_alerts = alert_state.get(client_id, {})
    alert_month = client_alerts.get("month", "")

    # Reset alerts if new month
    if alert_month != month:
        client_alerts = {"month": month}

    if cost >= limit and not client_alerts.get("100_sent"):
        _send_threshold_email(
            client_id, cost, limit, "EXCEEDED",
            f"{client_id} has exceeded their ${limit:.2f} {plan_tier} limit "
            f"(${cost:.2f} this month)"
        )
        client_alerts["100_sent"] = True
    elif cost >= limit * 0.8 and not client_alerts.get("80_sent"):
        _send_threshold_email(
            client_id, cost, limit, "WARNING",
            f"{client_id} is at ${cost:.2f} of their ${limit:.2f} {plan_tier} "
            f"limit ({cost / limit * 100:.0f}%)"
        )
        client_alerts["80_sent"] = True

    alert_state[client_id] = client_alerts
    _save_alert_state(alert_state)


def _send_threshold_email(client_id: str, cost: float, limit: float,
                          level: str, detail: str):
    """Send threshold alert email to Sam."""
    sam_email = os.getenv("WATCHDOG_EMAIL_FROM", "")
    to_email = os.getenv("SALES_DIGEST_TO_EMAIL", sam_email)
    gmail_pw = os.getenv("GMAIL_APP_PASSWORD", "")

    if not all([sam_email, to_email, gmail_pw]):
        log.warning(f"Email not configured -- cannot send threshold alert for {client_id}")
        return

    subject = f"[Usage {level}] {client_id} -- ${cost:.2f} / ${limit:.2f}"
    body = (
        f"Usage Alert: {level}\n\n"
        f"Client: {client_id}\n"
        f"Current month cost: ${cost:.2f}\n"
        f"Tier limit: ${limit:.2f}\n"
        f"Usage: {cost / limit * 100:.0f}%\n\n"
        f"Review the client's pipeline usage in Airtable or the dashboard."
    )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = sam_email
    msg["To"] = to_email

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(sam_email, gmail_pw)
            s.send_message(msg)
        log.info(f"Threshold alert sent: {subject}")
    except Exception as e:
        log.error(f"Failed to send threshold alert: {e}")


def discover_clients() -> list[str]:
    """Find all client IDs from existing usage log files."""
    if not USAGE_LOGS_DIR.exists():
        return []

    month = datetime.now().strftime("%Y%m")
    clients = set()
    for f in USAGE_LOGS_DIR.glob(f"*_{month}.jsonl"):
        # Filename: {client_id}_{YYYYMM}.jsonl
        client_id = f.stem.rsplit("_", 1)[0]
        if client_id:
            clients.add(client_id)
    return sorted(clients)


def run():
    """Main aggregation run."""
    log.info("=== Usage Aggregator starting ===")

    clients = discover_clients()
    if not clients:
        log.info("No usage logs found for this month")
        report_status("usage_tracker", "ok", "No usage logs found")
        return

    log.info(f"Found {len(clients)} client(s) with usage data: {clients}")

    for client_id in clients:
        totals = aggregate_client_usage(client_id)
        log.info(
            f"{client_id}: {totals['call_count']} calls, "
            f"{totals['input_tokens']} in / {totals['output_tokens']} out, "
            f"${totals['est_cost_usd']:.4f}"
        )

        push_to_airtable(client_id, totals)

        # Try to get plan tier from Airtable for threshold check
        # Falls back to empty string (no threshold check)
        plan_tier = _get_client_plan_tier(client_id)
        check_threshold_alerts(client_id, totals, plan_tier)

    report_status(
        "usage_tracker", "ok",
        f"Aggregated {len(clients)} client(s)",
        metrics={"clients": len(clients)},
    )
    log.info("=== Usage Aggregator complete ===")


def _get_client_plan_tier(client_id: str) -> str:
    """Look up client's plan tier from Airtable."""
    pat = os.getenv("AIRTABLE_PAT", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")
    clients_table_id = os.getenv("AIRTABLE_CLIENTS_TABLE_ID", "")

    if not all([pat, base_id, clients_table_id]):
        return ""

    try:
        from pyairtable import Api
        api = Api(pat)
        table = api.table(base_id, clients_table_id)
        records = table.all(formula=f"{{client_id}} = '{client_id}'")
        if records:
            return records[0].get("fields", {}).get("Plan Name", "")
    except Exception:
        pass
    return ""


if __name__ == "__main__":
    run()
