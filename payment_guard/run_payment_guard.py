"""
Payment Guard
Monitors client payment status via Airtable and enforces automated
warning/kill/reactivation cycle for delinquent accounts.

Schedule: Daily 9:05 AM (after n8n QBO sync at 9:00 AM)

Flow:
    Day 10 overdue  -> Warning email #1
    Day 14 overdue  -> Warning email #2 (final notice)
    Day 16 overdue  -> KILL (active=false, notify client + Sam)
    Day 30+ overdue -> Escalation alert to Sam
    Payment received -> AUTO-REACTIVATE (active=true, welcome back email)

Usage:
    python -m payment_guard.run_payment_guard
"""

import json
import logging
import os
import smtplib
import sys
from datetime import datetime, timedelta
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

GUARD_DIR = Path(__file__).parent
GUARD_STATE_FILE = GUARD_DIR / "guard_state.json"
GUARD_CONFIG_FILE = GUARD_DIR / "config.json"
AUDIT_LOG_FILE = GUARD_DIR / "audit_log.jsonl"
TENANT_CONFIG_FILE = _PROJECT_ROOT / "tenant_config.json"

# Americal Patrol can NEVER be paused by this system
NEVER_KILL = ["americal_patrol", "ap", "americal-patrol"]

# Overdue thresholds (days)
WARNING_1_DAY = 10
WARNING_2_DAY = 14
KILL_DAY = 16
ESCALATION_DAY = 30


# ── State management ──────────────────────────────────────────────────

def _load_guard_state() -> dict:
    try:
        with open(GUARD_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_guard_state(state: dict):
    tmp = str(GUARD_STATE_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, str(GUARD_STATE_FILE))


def _load_guard_config() -> dict:
    try:
        with open(GUARD_CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"grace_clients": [], "manual_overrides": {}}


def _audit_log(client_id: str, action: str, reason: str,
               payment_status: str = ""):
    """Append an entry to the permanent audit log."""
    record = {
        "ts": datetime.now().isoformat(),
        "client": client_id,
        "action": action,
        "reason": reason,
        "payment_status": payment_status,
    }
    with open(AUDIT_LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, default=str) + "\n")
    log.info(f"AUDIT: {action} -- {client_id} -- {reason}")


# ── Tenant config manipulation ────────────────────────────────────────

def _set_client_active(active: bool):
    """Flip the 'active' flag in this tenant's config."""
    with open(TENANT_CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)

    config["active"] = active

    tmp = str(TENANT_CONFIG_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, default=str)
    os.replace(tmp, str(TENANT_CONFIG_FILE))

    # Reload tenant_context cache
    tc.reload()


def _get_client_active() -> bool:
    """Read current active status from tenant config."""
    with open(TENANT_CONFIG_FILE, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config.get("active", True)


# ── Airtable integration ─────────────────────────────────────────────

def _get_client_payment_info() -> dict | None:
    """
    Read this client's payment info from Airtable.
    Returns: {"payment_status": str, "invoice_date": str, "email": str,
              "company_name": str, "client_id": str}
    """
    pat = os.getenv("AIRTABLE_PAT", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")
    clients_table_id = os.getenv("AIRTABLE_CLIENTS_TABLE_ID", "")
    client_id = tc.client_id()

    if not all([pat, base_id, clients_table_id]):
        log.error("Airtable credentials not configured")
        return None

    try:
        from pyairtable import Api
        api = Api(pat)
        table = api.table(base_id, clients_table_id)

        records = table.all(formula=f"{{client_id}} = '{client_id}'")
        if not records:
            log.warning(f"No Airtable record for client_id={client_id}")
            return None

        fields = records[0].get("fields", {})
        return {
            "record_id": records[0]["id"],
            "payment_status": fields.get("Payment Status", ""),
            "invoice_date": fields.get("Invoice Date", ""),
            "email": fields.get("Email", ""),
            "company_name": fields.get("Company Name", tc.company_name()),
            "client_id": client_id,
        }
    except Exception as e:
        log.error(f"Failed to read Airtable: {e}")
        return None


def _log_activity_to_airtable(client_id: str, action: str, notes: str):
    """Create an Activity record in Airtable for the client timeline."""
    pat = os.getenv("AIRTABLE_PAT", "")
    base_id = os.getenv("AIRTABLE_BASE_ID", "")
    activities_table_id = os.getenv("AIRTABLE_ACTIVITIES_TABLE_ID", "")

    if not all([pat, base_id, activities_table_id]):
        return

    try:
        from pyairtable import Api
        api = Api(pat)
        table = api.table(base_id, activities_table_id)
        table.create({
            "Type": f"Payment Guard: {action}",
            "Notes": notes,
            "Date": datetime.now().isoformat(),
        })
    except Exception as e:
        log.warning(f"Failed to log activity to Airtable: {e}")


# ── Email sending ─────────────────────────────────────────────────────

def _send_email(to: str, subject: str, body: str):
    """Send an email via Gmail SMTP."""
    from_email = os.getenv("WATCHDOG_EMAIL_FROM", "")
    gmail_pw = os.getenv("GMAIL_APP_PASSWORD", "")

    if not all([from_email, gmail_pw]):
        log.warning(f"Email not configured -- cannot send: {subject}")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_email
    msg["To"] = to

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as s:
            s.login(from_email, gmail_pw)
            s.send_message(msg)
        log.info(f"Email sent: {subject} -> {to}")
    except Exception as e:
        log.error(f"Failed to send email: {e}")


def _send_warning_1(client_info: dict, days_overdue: int):
    """Day 10 warning email to client."""
    kill_date = (datetime.now() + timedelta(days=KILL_DAY - days_overdue)).strftime("%B %d, %Y")
    company = client_info["company_name"]

    subject = f"Payment Reminder -- {tc.company_name()}"
    body = (
        f"Hi {company},\n\n"
        f"This is a friendly reminder that your invoice is {days_overdue} days "
        f"past due. Your automated services with {tc.company_name()} will be "
        f"paused on {kill_date} if payment is not received.\n\n"
        f"If you've already sent payment, please disregard this message.\n\n"
        f"If you have any questions about your invoice, please contact us at "
        f"{tc.owner_email()} or {tc.company_phone()}.\n\n"
        f"Best regards,\n"
        f"{tc.company_name()}"
    )

    _send_email(client_info["email"], subject, body)


def _send_warning_2(client_info: dict, days_overdue: int):
    """Day 14 final warning email to client."""
    company = client_info["company_name"]

    subject = f"Final Notice: Services Pausing Soon -- {tc.company_name()}"
    body = (
        f"Hi {company},\n\n"
        f"This is a final notice that your invoice is {days_overdue} days past due. "
        f"Your automated services will be paused in 2 days unless payment "
        f"is received.\n\n"
        f"Services affected: all automations currently running for your account.\n\n"
        f"To avoid any interruption, please submit payment at your earliest "
        f"convenience. If you have questions, reach out to {tc.owner_email()} "
        f"or {tc.company_phone()}.\n\n"
        f"Best regards,\n"
        f"{tc.company_name()}"
    )

    _send_email(client_info["email"], subject, body)


def _send_paused_email(client_info: dict):
    """Day 16 -- automations paused notification to client."""
    company = client_info["company_name"]

    subject = f"Services Paused -- {tc.company_name()}"
    body = (
        f"Hi {company},\n\n"
        f"Due to an outstanding invoice, your automated services with "
        f"{tc.company_name()} have been paused.\n\n"
        f"Your services will resume automatically once payment is received. "
        f"No data has been lost and your configurations are preserved.\n\n"
        f"To restore service, please submit payment or contact us at "
        f"{tc.owner_email()} or {tc.company_phone()}.\n\n"
        f"Best regards,\n"
        f"{tc.company_name()}"
    )

    _send_email(client_info["email"], subject, body)


def _send_reactivation_email(client_info: dict):
    """Payment received -- welcome back email to client."""
    company = client_info["company_name"]

    subject = f"Services Restored -- {tc.company_name()}"
    body = (
        f"Hi {company},\n\n"
        f"Thank you for your payment. Your automated services with "
        f"{tc.company_name()} have been restored and are running normally.\n\n"
        f"If you have any questions, please don't hesitate to reach out at "
        f"{tc.owner_email()} or {tc.company_phone()}.\n\n"
        f"Best regards,\n"
        f"{tc.company_name()}"
    )

    _send_email(client_info["email"], subject, body)


def _send_sam_notification(client_id: str, action: str, detail: str):
    """Notify Sam about a kill/reactivation/escalation."""
    sam_email = os.getenv("SALES_DIGEST_TO_EMAIL",
                          os.getenv("WATCHDOG_EMAIL_FROM", ""))
    if not sam_email:
        return

    subject = f"[Payment Guard] {action.upper()}: {client_id}"
    body = (
        f"Payment Guard Action\n\n"
        f"Client: {client_id}\n"
        f"Action: {action}\n"
        f"Detail: {detail}\n"
        f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    )

    _send_email(sam_email, subject, body)


def _send_escalation_email(client_id: str, days_overdue: int):
    """30+ days -- escalation alert to Sam only."""
    sam_email = os.getenv("SALES_DIGEST_TO_EMAIL",
                          os.getenv("WATCHDOG_EMAIL_FROM", ""))
    if not sam_email:
        return

    subject = f"[Payment Guard] 30+ DAY OVERDUE: {client_id}"
    body = (
        f"Client {client_id} has been overdue for {days_overdue} days.\n\n"
        f"Their automations have been paused since day 16.\n\n"
        f"Consider:\n"
        f"  - Direct outreach to the client\n"
        f"  - Adjusting payment terms\n"
        f"  - Beginning offboarding process\n\n"
        f"This alert will repeat daily until the situation is resolved."
    )

    _send_email(sam_email, subject, body)


# ── Core logic ────────────────────────────────────────────────────────

def _calculate_days_overdue(invoice_date_str: str) -> int:
    """Calculate days since invoice date. Returns 0 if date is invalid."""
    if not invoice_date_str:
        return 0

    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%fZ",
                "%m/%d/%Y", "%d/%m/%Y"):
        try:
            invoice_date = datetime.strptime(invoice_date_str.split("T")[0]
                                             if "T" in invoice_date_str
                                             else invoice_date_str,
                                             fmt.split("T")[0])
            return (datetime.now() - invoice_date).days
        except ValueError:
            continue

    log.warning(f"Could not parse invoice date: {invoice_date_str}")
    return 0


def run():
    """Main payment guard run."""
    log.info("=== Payment Guard starting ===")

    client_id = tc.client_id()

    # Safety: never kill Americal Patrol
    if client_id.lower() in [n.lower() for n in NEVER_KILL]:
        log.info(f"Client {client_id} is in NEVER_KILL list -- skipping")
        report_status("payment_guard", "ok", f"{client_id} excluded (NEVER_KILL)")
        return

    config = _load_guard_config()
    state = _load_guard_state()
    client_state = state.get(client_id, {})

    # Check manual override
    overrides = config.get("manual_overrides", {})
    if client_id in overrides:
        override = overrides[client_id]
        if override == "paused":
            if _get_client_active():
                _set_client_active(False)
                _audit_log(client_id, "manual_pause", "Manual override: paused")
            log.info(f"{client_id} manually overridden to PAUSED")
            report_status("payment_guard", "ok", f"{client_id} manually paused")
            return
        elif override == "active":
            if not _get_client_active():
                _set_client_active(True)
                _audit_log(client_id, "manual_activate", "Manual override: active")
            log.info(f"{client_id} manually overridden to ACTIVE")
            report_status("payment_guard", "ok", f"{client_id} manually active")
            return

    # Get payment info from Airtable
    client_info = _get_client_payment_info()
    if client_info is None:
        log.error("Could not retrieve payment info -- aborting")
        _send_sam_notification(
            client_id, "error",
            "Payment guard could not read Airtable. Check credentials."
        )
        report_status("payment_guard", "error", "Airtable read failed")
        return

    payment_status = client_info["payment_status"]
    days_overdue = _calculate_days_overdue(client_info["invoice_date"])

    log.info(f"Client: {client_id}, Status: {payment_status}, "
             f"Days overdue: {days_overdue}")

    # Check grace list
    grace_clients = [c.lower() for c in config.get("grace_clients", [])]
    is_grace = client_id.lower() in grace_clients

    # ── Handle payment received (reactivation) ──
    if payment_status in ("Current", "Paid"):
        if client_state.get("status") == "killed":
            # Reactivate
            _set_client_active(True)
            _send_reactivation_email(client_info)
            _send_sam_notification(
                client_id, "reactivated",
                f"Payment received. Automations restored."
            )
            _audit_log(client_id, "reactivate", "Payment received",
                       payment_status)
            _log_activity_to_airtable(
                client_id, "Reactivated",
                "Payment received. Automations restored automatically."
            )
            client_state = {"status": "active", "last_checked": datetime.now().isoformat()}
        else:
            client_state["status"] = "active"
            client_state["last_checked"] = datetime.now().isoformat()
            # Clear any warning state
            for key in ["warning_10_sent", "warning_14_sent"]:
                client_state.pop(key, None)

        # Tamper protection: ensure active=true when paid
        if not _get_client_active():
            _set_client_active(True)
            _audit_log(client_id, "tamper_fix", "Config was inactive but client is paid")

        state[client_id] = client_state
        _save_guard_state(state)
        report_status("payment_guard", "ok", f"{client_id} is current")
        log.info(f"{client_id} is current -- all good")
        return

    # ── Handle overdue statuses ──
    if payment_status not in ("Invoice Sent", "Overdue", "Overdue 30+"):
        log.info(f"Unknown payment status '{payment_status}' -- skipping")
        report_status("payment_guard", "ok",
                       f"{client_id}: unknown status '{payment_status}'")
        return

    # Tamper protection: if client is overdue and was killed but active=true,
    # re-kill (unless on grace list)
    if (client_state.get("status") == "killed"
            and _get_client_active()
            and not is_grace):
        _set_client_active(False)
        _audit_log(client_id, "tamper_rekill",
                   "Config was manually set to active while overdue",
                   payment_status)
        _send_sam_notification(
            client_id, "tamper_detected",
            f"Someone set {client_id} to active while overdue. Re-killed."
        )
        log.warning(f"TAMPER: {client_id} was set active while overdue -- re-killed")

    # Day 30+ escalation
    if days_overdue >= ESCALATION_DAY:
        today = datetime.now().strftime("%Y-%m-%d")
        if client_state.get("last_escalation") != today:
            _send_escalation_email(client_id, days_overdue)
            client_state["last_escalation"] = today
            _audit_log(client_id, "escalation_30",
                       f"{days_overdue} days overdue", payment_status)

    # Day 16+ kill
    if days_overdue >= KILL_DAY and client_state.get("status") != "killed":
        if is_grace:
            log.info(f"{client_id} is on grace list -- skipping kill")
            _audit_log(client_id, "kill_skipped_grace",
                       f"{days_overdue} days overdue but on grace list",
                       payment_status)
        else:
            _set_client_active(False)
            _send_paused_email(client_info)
            _send_sam_notification(
                client_id, "killed",
                f"Automations paused. {days_overdue} days overdue."
            )
            _audit_log(client_id, "kill", f"{days_overdue} days overdue",
                       payment_status)
            _log_activity_to_airtable(
                client_id, "Automations Paused",
                f"Payment {days_overdue} days overdue. Automations paused automatically."
            )
            client_state["status"] = "killed"
            client_state["killed_at"] = datetime.now().isoformat()

    # Day 14 warning
    elif days_overdue >= WARNING_2_DAY and not client_state.get("warning_14_sent"):
        _send_warning_2(client_info, days_overdue)
        _audit_log(client_id, "warning_14",
                   f"{days_overdue} days overdue", payment_status)
        _log_activity_to_airtable(
            client_id, "Final Payment Warning",
            f"Final warning sent. Invoice {days_overdue} days overdue. "
            f"Services will pause in {KILL_DAY - days_overdue} day(s)."
        )
        client_state["warning_14_sent"] = datetime.now().strftime("%Y-%m-%d")

    # Day 10 warning
    elif days_overdue >= WARNING_1_DAY and not client_state.get("warning_10_sent"):
        _send_warning_1(client_info, days_overdue)
        _audit_log(client_id, "warning_10",
                   f"{days_overdue} days overdue", payment_status)
        _log_activity_to_airtable(
            client_id, "Payment Warning",
            f"First warning sent. Invoice {days_overdue} days overdue."
        )
        client_state["warning_10_sent"] = datetime.now().strftime("%Y-%m-%d")

    client_state["days_overdue"] = days_overdue
    client_state["last_checked"] = datetime.now().isoformat()
    state[client_id] = client_state
    _save_guard_state(state)

    status = "warning" if days_overdue >= WARNING_1_DAY else "ok"
    report_status("payment_guard", status,
                   f"{client_id}: {days_overdue} days overdue, "
                   f"status={client_state.get('status', 'active')}")
    log.info("=== Payment Guard complete ===")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        log.exception(f"Payment Guard crashed: {e}")
        # Guard failure alerting -- notify Sam
        try:
            sam_email = os.getenv("SALES_DIGEST_TO_EMAIL",
                                  os.getenv("WATCHDOG_EMAIL_FROM", ""))
            if sam_email:
                _send_email(
                    sam_email,
                    f"[Payment Guard] CRASH: {e}",
                    f"The payment guard script crashed.\n\n"
                    f"Error: {e}\n\n"
                    f"Clients may not be monitored until this is fixed.\n"
                    f"Check the logs and fix immediately."
                )
        except Exception:
            pass
        report_status("payment_guard", "error", f"Crashed: {e}")
        sys.exit(1)
