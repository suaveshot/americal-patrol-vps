"""
Americal Patrol — Self-Healing Watchdog

Schedule: every 60 minutes via Windows Task Scheduler.

What it does each run:
  1. Read every pipeline's log file and health_status.json
  2. Flag errors, overdue runs, expired tokens, Excel locks
  3. Apply safe auto-fixes (stale temp files, orphaned processed_pdfs entries,
     missing directories)
  4. Write watchdog/health_status.json for the dashboard
  5. At 8 PM (once/day): send digest email summarising all pipeline statuses

Setup:
  Add to .env:
    WATCHDOG_EMAIL_FROM=you@gmail.com
    WATCHDOG_EMAIL_TO=you@gmail.com
    GMAIL_APP_PASSWORD=xxxx xxxx xxxx xxxx   (Gmail App Password, not account password)
"""

import json
import logging
import os
import re
import smtplib
import socket
import sys
import tempfile
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent

# Load .env
try:
    from dotenv import load_dotenv
    load_dotenv(BASE / ".env")
except ImportError:
    pass

WATCHDOG_DIR = BASE / "watchdog"
HEALTH_FILE  = WATCHDOG_DIR / "health_status.json"
WATCHDOG_LOG = WATCHDOG_DIR / "watchdog.log"
DIGEST_STATE = WATCHDOG_DIR / "digest_state.json"

WATCHDOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=str(WATCHDOG_LOG),
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Pipeline registry ────────────────────────────────────────────────────────
# interval_hours: how long between expected runs before marking "overdue"
# schedule_days:  0=Mon … 6=Sun, None=always-on
PIPELINES = [
    {
        "id":             "patrol",
        "name":           "Morning Reports",
        "log_file":       BASE / "patrol_automation/automation.log",
        "interval_hours": 36,
        "schedule_days":  list(range(7)),
        "token_files":    [BASE / "patrol_automation/token.json"],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "seo",
        "name":           "SEO Analysis",
        "log_file":       BASE / "seo_automation/automation.log",
        "interval_hours": 192,
        "schedule_days":  [0],
        "token_files":    [BASE / "seo_automation/seo_token.json"],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "blog",
        "name":           "Blog Post",
        "log_file":       BASE / "blog_post_automation/automation.log",
        "interval_hours": 192,
        "schedule_days":  [0],
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "social",
        "name":           "Social Media + GBP",
        "log_file":       BASE / "social_media_automation/automation.log",
        "interval_hours": 96,
        "schedule_days":  [1, 3, 5],
        "token_files":    [
            BASE / "social_media_automation/social_drive_token.json",
            BASE / "gbp_automation/gbp_token.json",
        ],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "gbp_standalone",
        "name":           "GBP Standalone",
        "log_file":       BASE / "gbp_automation/automation.log",
        "interval_hours": 192,
        "schedule_days":  [0],
        "token_files":    [BASE / "gbp_automation/gbp_token.json"],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "ads",
        "name":           "Google Ads",
        "log_file":       BASE / "google_ads_automation/automation.log",
        "interval_hours": 192,
        "schedule_days":  [0],
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
        "first_scheduled": None,  # Not yet scheduled
    },
    {
        "id":             "harbor_lights",
        "name":           "Harbor Lights",
        "log_file":       BASE / "Harbor Lights/harbor_lights.log",
        "interval_hours": 36,
        "schedule_days":  list(range(7)),
        "token_files":    [],
        "temp_files":     [Path(tempfile.gettempdir()) / "hl_temp.xlsx"],
        "excel_files":    [BASE / "Harbor Lights/Harbor Lights Guest Parking UPDATED.xlsx"],
    },
    {
        "id":             "voice",
        "name":           "Voice Agent",
        "log_file":       BASE / "voice_agent/automation.log",
        "interval_hours": 48,
        "schedule_days":  None,   # always-on via Vapi + n8n
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "sales_pipeline",
        "name":           "Sales Pipeline",
        "log_file":       BASE / "sales_pipeline/automation.log",
        "interval_hours": 96,
        "schedule_days":  [0, 1, 2, 3, 4],  # Mon-Fri
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "weekly_update",
        "name":           "Weekly Business Update",
        "log_file":       BASE / "weekly_update/automation.log",
        "interval_hours": 192,
        "schedule_days":  [4],  # Friday only
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "reviews",
        "name":           "Review Engine",
        "log_file":       BASE / "review_engine/automation.log",
        "interval_hours": 2400,  # Quarterly (~100 days)
        "schedule_days":  [0],   # 1st Monday of quarter
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
        "first_scheduled": datetime(2026, 4, 1, 10, 0),  # Next quarterly: Apr 1
    },
    {
        "id":             "qbr",
        "name":           "QBR Generator",
        "log_file":       BASE / "qbr_generator/automation.log",
        "interval_hours": 2400,  # Quarterly (~100 days)
        "schedule_days":  [0],   # 1st Monday of quarter
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
        "first_scheduled": datetime(2026, 4, 1, 10, 0),  # Next quarterly: Apr 1
    },
    {
        "id":             "guard_compliance",
        "name":           "Guard Compliance",
        "log_file":       BASE / "guard_compliance/automation.log",
        "interval_hours": 36,
        "schedule_days":  list(range(7)),   # Daily
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "email_assistant",
        "name":           "Email Assistant",
        "log_file":       BASE / "email_assistant/automation.log",
        "interval_hours": 4,
        "schedule_days":  list(range(7)),   # Hourly, 24/7
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "incident_trends",
        "name":           "Incident Trends",
        "log_file":       BASE / "incident_trends/automation.log",
        "interval_hours": 192,
        "schedule_days":  [0],   # Monday analysis
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
    {
        "id":             "transcribe_calls",
        "name":           "Call Transcription",
        "log_file":       BASE / "sales_pipeline/automation.log",
        "interval_hours": 2,     # Every 15 min, but allow 2h grace
        "schedule_days":  list(range(7)),   # Daily
        "token_files":    [],
        "temp_files":     [],
        "excel_files":    [],
    },
]

LOG_TS_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")
LOG_TS_BARE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s")


# ── Log reading helpers ──────────────────────────────────────────────────────

def _read_log_tail(log_file, max_lines=150):
    if not log_file or not log_file.exists():
        return []
    try:
        with open(log_file, encoding="utf-8", errors="replace") as f:
            return f.readlines()[-max_lines:]
    except OSError:
        return []


def _last_log_ts(lines):
    for line in reversed(lines):
        stripped = line.strip()
        m = LOG_TS_RE.match(stripped) or LOG_TS_BARE_RE.match(stripped)
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
    return None


def _first_error_in_last_run(lines):
    """Return the first ERROR line in the last run block, or None."""
    start = 0
    for i, line in enumerate(lines):
        l = line.lower()
        if "starting" in l or "watchdog run" in l:
            start = i
    for line in lines[start:]:
        up = line.upper()
        if "ERROR:" in up or "TRACEBACK" in up:
            return line.strip()[:200]
    return None


# ── Health file I/O ──────────────────────────────────────────────────────────

def _load_health():
    try:
        with open(HEALTH_FILE, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_health(data):
    tmp = str(HEALTH_FILE) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, str(HEALTH_FILE))


# ── Auto-fix routines ────────────────────────────────────────────────────────

def _fix_stale_temp_files(p):
    """Delete temp xlsx/scratch files left by crashed runs (> 2 hours old)."""
    fixed = []
    for tmp in p["temp_files"]:
        if tmp.exists():
            age = datetime.now() - datetime.fromtimestamp(tmp.stat().st_mtime)
            if age > timedelta(hours=2):
                try:
                    tmp.unlink()
                    fixed.append(f"Deleted stale temp file: {tmp.name}")
                    log.info(f"[autofix] Deleted stale temp file: {tmp}")
                except OSError as e:
                    log.warning(f"[autofix] Cannot delete temp file {tmp}: {e}")
    return fixed


def _fix_stale_processed_log():
    """
    Remove entries from Harbor Lights processed_pdfs.json where the source
    PDF no longer exists.  Safe: only removes entries, never adds them.
    """
    fixed = []
    proc_log   = BASE / "Harbor Lights/processed_pdfs.json"
    morning_dir = BASE / "Americal Patrol Morning Reports"

    if not proc_log.exists():
        return fixed

    try:
        with open(proc_log, encoding="utf-8") as f:
            processed = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        log.warning(f"[autofix] Cannot read processed_pdfs.json: {e}")
        return fixed

    if not isinstance(processed, list):
        return fixed

    # Build set of all Harbor Lights PDFs that still exist on disk
    existing = set()
    if morning_dir.exists():
        for dirpath, _, filenames in os.walk(morning_dir):
            for fname in filenames:
                fl = fname.lower()
                if fl.endswith(".pdf") and "harbor" in fl and "lights" in fl:
                    existing.add(fname)

    stale = [p for p in processed if p not in existing]
    if stale:
        cleaned = [p for p in processed if p in existing]
        tmp = str(proc_log) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2)
        os.replace(tmp, str(proc_log))
        for s in stale:
            fixed.append(f"Removed orphaned processed entry: {s}")
            log.info(f"[autofix] Removed orphaned processed_pdfs entry: {s}")

    return fixed


def _fix_missing_dirs():
    """Ensure required runtime directories exist."""
    fixed = []
    required = [
        BASE / "pipeline_events",
        BASE / "watchdog",
        BASE / "voice_agent/call_logs",
    ]
    for d in required:
        if not d.exists():
            d.mkdir(parents=True, exist_ok=True)
            fixed.append(f"Created directory: {d.name}")
            log.info(f"[autofix] Created missing directory: {d}")
    return fixed


# ── Issue detectors ──────────────────────────────────────────────────────────

def _excel_locked(p):
    """Return warning string if an Excel file has an Office lock file (~$name)."""
    for xlsx in p["excel_files"]:
        lock = xlsx.parent / ("~$" + xlsx.name)
        if lock.exists():
            return f"{xlsx.name} is open in another process — close Excel before next run"
    return None


def _token_looks_expired(token_file):
    """
    Return True only if a token file is genuinely broken — corrupt, unreadable,
    or missing a refresh_token (meaning it cannot auto-renew).

    NOTE: Google access tokens expire hourly but auto-refresh via refresh_token.
    Flagging the expiry field alone is a false positive — only flag when the
    refresh mechanism itself is absent.
    """
    if not token_file.exists():
        return False   # not set up yet, not broken
    try:
        with open(token_file, encoding="utf-8") as f:
            data = json.load(f)
        # If there's a valid refresh_token, google-auth will renew automatically
        if data.get("refresh_token"):
            return False
        # No refresh_token — check if there's at least a valid non-expired access token
        expiry = data.get("expiry") or data.get("token_expiry")
        if expiry and data.get("token"):
            exp_str = expiry.replace("Z", "").split("+")[0]
            if datetime.fromisoformat(exp_str) > datetime.now():
                return False   # access token still valid
        # No refresh_token and no valid access token = broken
        return True
    except (json.JSONDecodeError, OSError, ValueError):
        return True   # corrupt file = effectively broken


# ── Core health check loop ───────────────────────────────────────────────────

def run_health_checks():
    now    = datetime.now()
    health = _load_health()
    health.pop("sales_autopilot", None)  # Renamed to sales_pipeline
    issues = []   # [(pipeline_id, severity, message)]

    # Global fixes that apply regardless of pipeline
    global_fixes = _fix_missing_dirs()

    # Clean up old event bus files (>30 days) — centralized here instead of per-pipeline
    try:
        sys.path.insert(0, str(BASE))
        from shared_utils.event_bus import cleanup_old_events
        deleted = cleanup_old_events(days=30)
        if deleted:
            log.info(f"[event_bus] Cleaned up {deleted} event file(s) older than 30 days")
    except Exception as e:
        log.warning(f"[event_bus] Cleanup failed: {e}")

    for p in PIPELINES:
        pid    = p["id"]
        fixes  = list(global_fixes)   # start with global fixes
        alerts = []

        # ── Per-pipeline fixes ─────────────────────────────────────────────
        fixes += _fix_stale_temp_files(p)
        if pid == "harbor_lights":
            fixes += _fix_stale_processed_log()

        # ── Log analysis ──────────────────────────────────────────────────
        lines    = _read_log_tail(p["log_file"])
        last_ts  = _last_log_ts(lines)
        error_ln = _first_error_in_last_run(lines)

        # ── Overdue / scheduled check ────────────────────────────────────
        overdue = False
        scheduled = False
        if p["interval_hours"]:
            if last_ts:
                age_h = (now - last_ts).total_seconds() / 3600
                if age_h > p["interval_hours"]:
                    overdue = True
                    alerts.append(
                        f"Overdue by {age_h - p['interval_hours']:.1f}h "
                        f"(last run: {last_ts.strftime('%a %b %d %H:%M')})"
                    )
            else:
                first = p.get("first_scheduled")
                if first and first > now:
                    scheduled = True
                    alerts.append(
                        f"First run scheduled: {first.strftime('%a %b %d, %I:%M %p')}"
                    )
                elif first and first <= now:
                    overdue = True
                    alerts.append(
                        f"Overdue — was scheduled for "
                        f"{first.strftime('%a %b %d, %I:%M %p')} but no run recorded"
                    )
                else:
                    scheduled = True
                    alerts.append("Not yet scheduled")

        # ── Token expiry checks ────────────────────────────────────────────
        for tf in p["token_files"]:
            if _token_looks_expired(tf):
                alerts.append(f"Token may be expired: {tf.name} — delete it to re-auth")

        # ── Excel lock check ──────────────────────────────────────────────
        lock_msg = _excel_locked(p)
        if lock_msg:
            alerts.append(lock_msg)

        # ── Determine overall status ──────────────────────────────────────
        if error_ln:
            status = "error"
            issues.append((pid, "error", error_ln))
        elif overdue:
            status = "overdue"
            issues.append((pid, "warning", alerts[0] if alerts else "Overdue"))
        elif scheduled:
            status = "scheduled"
        elif alerts:
            status = "warning"
            for a in alerts:
                issues.append((pid, "warning", a))
        else:
            status = "ok"

        prev = health.get(pid, {})
        health[pid] = {
            "status":      status,
            "detail":      error_ln or (alerts[0] if alerts else ""),
            "last_log_ts": last_ts.isoformat() if last_ts else prev.get("last_log_ts"),
            "last_checked": now.isoformat(),
            "fixes":       fixes,
            "alerts":      alerts,
        }

        note = f"status={status}"
        if last_ts:
            note += f" last_run={last_ts.strftime('%m/%d %H:%M')}"
        if fixes:
            note += f" fixes_applied={len(fixes)}"
        log.info(f"[{pid}] {note}")

    _save_health(health)
    return {"health": health, "issues": issues}


def _port_listening(port):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            return s.connect_ex(("127.0.0.1", port)) == 0
    except OSError:
        return False


# ── Daily digest email ────────────────────────────────────────────────────────

def _already_sent_today():
    try:
        with open(DIGEST_STATE, encoding="utf-8") as f:
            state = json.load(f)
        last = datetime.fromisoformat(state.get("last_digest_sent", "2000-01-01"))
        return last.date() == datetime.now().date()
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return False


def _mark_digest_sent():
    with open(DIGEST_STATE, "w", encoding="utf-8") as f:
        json.dump({"last_digest_sent": datetime.now().isoformat()}, f)


def _build_digest_html(health):
    now   = datetime.now()
    icons = {"ok": "✅", "warning": "⚠️", "error": "❌", "overdue": "🕐",
             "scheduled": "📅", "unknown": "❓"}
    bgs   = {"ok": "#f0fdf4", "warning": "#fffbeb", "error": "#fef2f2",
             "overdue": "#fff7ed", "scheduled": "#eff6ff", "unknown": "#f9fafb"}

    rows = ""
    for p in PIPELINES:
        pid   = p["id"]
        info  = health.get(pid, {})
        s     = info.get("status", "unknown")
        icon  = icons.get(s, "❓")
        detail = info.get("detail", "")
        lr_raw = info.get("last_log_ts") or info.get("last_run", "")
        last_run = ""
        if lr_raw:
            try:
                last_run = datetime.fromisoformat(lr_raw).strftime("%a %b %d %I:%M %p")
            except ValueError:
                last_run = lr_raw

        fix_html = ""
        if info.get("fixes"):
            fix_html = "<br><small style='color:#555'>Auto-fixed: " + \
                       "; ".join(info["fixes"]) + "</small>"

        alert_html = ""
        if info.get("alerts"):
            alert_color = "#2563eb" if s == "scheduled" else "#b91c1c"
            alert_html = f"<br><small style='color:{alert_color}'>" + \
                         "<br>".join(info["alerts"]) + "</small>"

        bg = bgs.get(s, "#f9fafb")
        rows += f"""
        <tr style="background:{bg}">
          <td style="padding:8px 12px;font-weight:bold">{icon} {p['name']}</td>
          <td style="padding:8px 12px;text-transform:uppercase;font-size:12px">{s}</td>
          <td style="padding:8px 12px;font-size:13px">{last_run}</td>
          <td style="padding:8px 12px;font-size:13px">{detail}{fix_html}{alert_html}</td>
        </tr>"""

    ok_count = sum(1 for p in PIPELINES if health.get(p["id"], {}).get("status") == "ok")
    total    = sum(1 for p in PIPELINES if health.get(p["id"], {}).get("status") != "scheduled")

    return f"""<!DOCTYPE html>
<html><body style="font-family:Arial,sans-serif;max-width:820px;margin:0 auto;padding:20px">
<h2 style="margin-bottom:4px">Americal Patrol — Daily Automation Digest</h2>
<p style="color:#6b7280;margin-top:0">{now.strftime('%A, %B %d, %Y')} &mdash; {ok_count}/{total} pipelines healthy</p>
<table style="width:100%;border-collapse:collapse;border:1px solid #e5e7eb">
  <thead>
    <tr style="background:#111827;color:#fff">
      <th style="padding:10px 12px;text-align:left">Pipeline</th>
      <th style="padding:10px 12px;text-align:left">Status</th>
      <th style="padding:10px 12px;text-align:left">Last Run</th>
      <th style="padding:10px 12px;text-align:left">Detail</th>
    </tr>
  </thead>
  <tbody>{rows}</tbody>
</table>
<p style="color:#9ca3af;font-size:12px;margin-top:20px">
  Auto-generated by watchdog.py &middot; Open dashboard: <code>cd dashboard &amp;&amp; python status_report.py</code>
</p>
</body></html>"""


def send_daily_digest(health):
    if _already_sent_today():
        return

    sender    = os.getenv("WATCHDOG_EMAIL_FROM") or os.getenv("SUPERVISOR_EMAIL")
    recipient = os.getenv("WATCHDOG_EMAIL_TO")   or os.getenv("SUPERVISOR_EMAIL")
    password  = os.getenv("GMAIL_APP_PASSWORD")  or os.getenv("EMAIL_PASSWORD")

    if not all([sender, recipient, password]):
        log.warning(
            "[digest] Email credentials not set — add WATCHDOG_EMAIL_FROM, "
            "WATCHDOG_EMAIL_TO, GMAIL_APP_PASSWORD to .env"
        )
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Patrol Automations — Daily Status {datetime.now().strftime('%b %d')}"
    msg["From"]    = sender
    msg["To"]      = recipient
    msg.attach(MIMEText(_build_digest_html(health), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(sender, password)
            srv.sendmail(sender, recipient, msg.as_string())
        _mark_digest_sent()
        log.info(f"[digest] Daily digest sent to {recipient}")
    except Exception as e:
        log.error(f"[digest] Failed to send digest: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Watchdog run starting")

    result = run_health_checks()
    health = result["health"]
    issues = result["issues"]

    if issues:
        log.warning(f"{len(issues)} issue(s) found:")
        for pid, severity, msg in issues:
            log.warning(f"  [{pid}] {severity.upper()}: {msg}")
    else:
        log.info("All pipelines healthy")

    # Send daily digest — any run from 5 PM onward (wider window guards against
    # the task missing the old narrow 8-PM-only slot).
    if datetime.now().hour >= 17:
        send_daily_digest(health)

    # Regenerate dashboard HTML
    try:
        import subprocess
        dashboard_script = BASE / "dashboard" / "status_report.py"
        subprocess.run([sys.executable, str(dashboard_script), "--no-browser"], timeout=30, check=True)
        log.info("Dashboard HTML regenerated")
    except Exception as e:
        log.warning(f"Dashboard regeneration failed: {e}")

    log.info("Watchdog run complete")


if __name__ == "__main__":
    main()
