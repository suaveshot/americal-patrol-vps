"""
Americal Patrol (VPS container) -> WCAS Dashboard heartbeat.

Counterpart to ``Americal Patrol/shared/push_heartbeat.py`` on Sam's Windows
box, but tuned for the container layout:

  - Logs live at ``/var/log/ap-*.log`` (tee'd by the cron lines in
    ``entrypoint.sh``), not ``<pipeline>/automation.log``.
  - State files live under ``/app/data/<pipeline>/*.json`` because
    ``entrypoint.sh`` symlinks the in-image paths into the persistent
    ``ap-data`` volume.
  - Pipeline IDs use the dashboard's canonical catalog IDs (sales_autopilot,
    email_assistant, system_watchdog, reviews) so the rings render directly
    without alias resolution.

Design rules (same as the Windows version):
  - Never crash; exit 0 on any error.
  - Bound the HTTP call to a few seconds.
  - Stay aligned with the existing dashboard heartbeat schema.

Usage from cron:
    python3 -m shared_utils.push_heartbeat                  # push all
    python3 -m shared_utils.push_heartbeat --pipeline sales_autopilot
    python3 -m shared_utils.push_heartbeat --dry-run

Env vars (read from /etc/container_env.sh via cron, or the container env):
    DASHBOARD_URL            - https://dashboard.westcoastautomationsolutions.com
    HEARTBEAT_SHARED_SECRET  - shared secret for /api/heartbeat
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

TENANT_ID = "americal_patrol"
LOG_DIR = Path("/var/log")
DATA_DIR = Path("/app/data")
HEARTBEAT_LOG = LOG_DIR / "ap-heartbeat.log"
HEALTH_FILE = DATA_DIR / "watchdog" / "health_status.json"

# Match what shared_utils/health_reporter.py and the Windows script use.
PATROL_LOG_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")

# pipeline_id (catalog canonical) -> { log files, state file, watchdog key }
#
# `log` may be a list when one cron job tees to multiple files (e.g. sales
# pipeline writes ap-sales.log for --hourly/--daily AND ap-transcribe.log for
# the every-15min transcribe sub-job; the freshest of the two reflects the
# pipeline's true heartbeat).
#
# `health_keys` are the keys the in-container health_reporter.py writes to
# /app/data/watchdog/health_status.json. We prefer that file's status when
# present because it's a self-reported run outcome, falling back to log
# tail heuristics when the pipeline didn't call report_status().
PIPELINES: dict[str, dict] = {
    "sales_autopilot": {
        "name": "Sales Pipeline",
        "log": ["ap-sales.log", "ap-transcribe.log"],
        "state": DATA_DIR / "sales_pipeline" / "pipeline_state.json",
        "health_keys": ("sales_pipeline", "sales_autopilot"),
    },
    "email_assistant": {
        "name": "Email Assistant",
        "log": ["ap-email.log"],
        "state": DATA_DIR / "email_assistant.log",  # email_state.json lives at /app/data/email_state.json
        "health_keys": ("email_assistant",),
    },
    "system_watchdog": {
        "name": "System Watchdog",
        "log": ["ap-watchdog.log"],
        "state": DATA_DIR / "watchdog" / "health_status.json",
        "health_keys": ("watchdog", "system_watchdog"),
    },
    "reviews": {
        "name": "Review Engine",
        "log": ["ap-reviews.log"],
        "state": DATA_DIR / "review_engine" / "review_state.json",
        "health_keys": ("reviews", "review_engine"),
    },
    "social": {
        "name": "Social Media",
        "log": ["ap-social.log"],
        "state": DATA_DIR / "social_media_automation" / "social_state.json",
        "health_keys": ("social", "social_media"),
    },
    "gbp": {
        "name": "Google Business Profile",
        # GBP posts are emitted by social_media_automation/gbp_publisher.py
        # (gbp_automation/CLAUDE.md — standalone run_gbp.py was deprecated
        # 2026-04). Read from the social log so the dashboard's GBP ring
        # reflects actual posting cadence, plus gbp_state.json for rotation.
        "log": ["ap-social.log"],
        "state": DATA_DIR / "gbp_automation" / "gbp_state.json",
        "health_keys": ("gbp",),
    },
    "seo": {
        "name": "SEO Analysis",
        "log": ["ap-seo.log"],
        "state": DATA_DIR / "seo_automation" / "seo_state.json",
        "health_keys": ("seo",),
    },
    "blog": {
        "name": "Blog Post",
        "log": ["ap-blog.log"],
        "state": DATA_DIR / "blog_post_automation" / "blog_state.json",
        "health_keys": ("blog",),
    },
    "weekly_update": {
        "name": "Weekly Update",
        "log": ["ap-weekly.log"],
        "state": DATA_DIR / "weekly_update" / "weekly_state.json",
        "health_keys": ("weekly_update", "weekly_digest"),
    },
    "guard_compliance": {
        "name": "Guard Compliance",
        "log": ["ap-guard.log"],
        "state": DATA_DIR / "guard_compliance" / "compliance_state.json",
        "health_keys": ("guard_compliance",),
    },
    "qbr_generator": {
        "name": "QBR Generator",
        "log": ["ap-qbr.log"],
        "state": DATA_DIR / "qbr_generator" / "qbr_state.json",
        "health_keys": ("qbr_generator", "qbr"),
    },
    # Patrol Automation (Morning Reports) — pipeline_id matches the dashboard
    # catalog's `daily_reports` canonical id (legacy_aliases includes 'patrol').
    # The cron line stays commented in entrypoint.sh until Sam verifies the
    # container produces the same output as the Windows TS task — once that
    # cron is enabled, this heartbeat will pick up /var/log/ap-patrol.log.
    "daily_reports": {
        "name": "Morning Reports",
        "log": ["ap-patrol.log"],
        "state": DATA_DIR / "patrol_automation" / "automation.log",
        "health_keys": ("patrol", "daily_reports"),
    },
    "harbor_lights": {
        "name": "Harbor Lights Parking",
        "log": ["ap-hl.log"],
        "state": DATA_DIR / "harbor_lights" / "processed_pdfs.json",
        "health_keys": ("harbor_lights",),
    },
}


def _log(msg: str) -> None:
    try:
        HEARTBEAT_LOG.parent.mkdir(parents=True, exist_ok=True)
        with HEARTBEAT_LOG.open("a", encoding="utf-8") as f:
            f.write(f"[{datetime.now().isoformat()}] {msg}\n")
    except OSError:
        pass


def _tail(path: Path, max_lines: int = 200) -> list[str]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8", errors="replace") as f:
            return f.readlines()[-max_lines:]
    except OSError:
        return []


def _last_timestamp(lines: list[str]) -> datetime | None:
    for line in reversed(lines):
        m = PATROL_LOG_RE.match(line.strip())
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
    return None


def _has_errors(lines: list[str]) -> bool:
    last_run_start = 0
    for i, line in enumerate(lines):
        if "Starting" in line or "Audit started" in line or "begin" in line.lower():
            last_run_start = i
    for line in lines[last_run_start:]:
        upper = line.upper()
        if "ERROR:" in upper or "TRACEBACK" in upper:
            return True
    return False


def _read_json(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _health_entry(pipeline: dict) -> dict | None:
    """Return self-reported health entry from health_status.json, if any."""
    health = _read_json(HEALTH_FILE)
    if not isinstance(health, dict):
        return None
    for key in pipeline.get("health_keys", ()):
        entry = health.get(key)
        if isinstance(entry, dict):
            return entry
    return None


def _build_payload(pipeline_id: str, pipeline: dict) -> dict:
    log_files = pipeline.get("log") or []
    if isinstance(log_files, str):
        log_files = [log_files]
    log_paths = [LOG_DIR / lf for lf in log_files]

    # Pull the freshest log among multiple, but keep the last 20 lines from
    # whichever was freshest so the dashboard's log tail is coherent.
    freshest_lines: list[str] = []
    last_ts: datetime | None = None
    for p in log_paths:
        lines = _tail(p)
        ts = _last_timestamp(lines)
        if ts and (last_ts is None or ts > last_ts):
            last_ts = ts
            freshest_lines = lines

    log_errors = _has_errors(freshest_lines)
    health = _health_entry(pipeline)

    # Status precedence: self-reported health > log heuristic.
    if health and health.get("status") in ("ok", "warning", "error"):
        status_map = {"ok": "success", "warning": "warning", "error": "error"}
        status = status_map[health["status"]]
        # Health_reporter writes a fresh ISO timestamp; trust it over log scan
        try:
            health_ts = datetime.fromisoformat(health.get("last_run", ""))
            if last_ts is None or health_ts > last_ts:
                last_ts = health_ts
        except (TypeError, ValueError):
            pass
    elif not last_ts:
        status = "unknown"
    elif log_errors:
        status = "error"
    else:
        status = "success"

    summary = ""
    state = _read_json(pipeline.get("state")) if pipeline.get("state") else None
    if isinstance(health, dict) and health.get("detail"):
        summary = str(health["detail"])[:240]
    elif isinstance(state, dict):
        if "runs_completed" in state:
            summary = f"Run #{state['runs_completed']}"
        elif "posts_published" in state:
            summary = f"{state['posts_published']} posts published"
        elif "contacts" in state and isinstance(state["contacts"], dict):
            active = sum(
                1
                for c in state["contacts"].values()
                if isinstance(c, dict)
                and not c.get("completed")
                and c.get("stage") not in ("won", "closed_lost", "sequence_done", "unsubscribed")
            )
            summary = f"{active} active contacts"

    log_tail = "".join(freshest_lines[-20:]).strip() if freshest_lines else ""

    state_summary: dict | None = None
    if isinstance(state, dict):
        state_summary = {k: v for k, v in state.items() if not isinstance(v, (dict, list))}
        for k, v in state.items():
            if isinstance(v, list):
                state_summary[f"{k}_count"] = len(v)
            elif isinstance(v, dict):
                state_summary[f"{k}_count"] = len(v)

    return {
        "tenant_id": TENANT_ID,
        "pipeline_id": pipeline_id,
        "pipeline_name": pipeline["name"],
        "status": status,
        "summary": summary,
        "last_run": last_ts.isoformat() if last_ts else None,
        "pushed_at": datetime.now(timezone.utc).isoformat(),
        "log_tail": log_tail[-4000:],
        "state_summary": state_summary,
    }


def _post(url: str, secret: str, payload: dict, timeout: float) -> tuple[bool, str]:
    data = json.dumps(payload).encode("utf-8")
    req = Request(
        url.rstrip("/") + "/api/heartbeat",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Heartbeat-Secret": secret,
            "X-Tenant-Id": payload.get("tenant_id", TENANT_ID),
            "User-Agent": "ap-vps-heartbeat/1.0",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read(2048).decode("utf-8", errors="replace")
            return resp.status == 200, f"HTTP {resp.status}: {body[:300]}"
    except HTTPError as e:
        return False, f"HTTPError {e.code}: {e.read()[:300].decode('utf-8', errors='replace')}"
    except URLError as e:
        return False, f"URLError: {e.reason}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Push container pipeline heartbeats to WCAS dashboard.")
    parser.add_argument("--pipeline", choices=sorted(PIPELINES.keys()),
                        help="Push only one pipeline; default is all.")
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--dry-run", action="store_true",
                        help="Print payloads, skip POST.")
    args = parser.parse_args()

    url = os.environ.get("DASHBOARD_URL", "").strip()
    secret = os.environ.get("HEARTBEAT_SHARED_SECRET", "").strip()

    targets = [args.pipeline] if args.pipeline else list(PIPELINES.keys())

    if not args.dry_run and (not url or not secret):
        _log(f"skipped {targets}: missing DASHBOARD_URL or HEARTBEAT_SHARED_SECRET")
        return 0

    for pid in targets:
        payload = _build_payload(pid, PIPELINES[pid])
        if args.dry_run:
            print(json.dumps(payload, indent=2, default=str))
            continue
        start = time.monotonic()
        ok, detail = _post(url, secret, payload, args.timeout)
        elapsed = time.monotonic() - start
        _log(f"{pid} status={payload['status']} ok={ok} elapsed={elapsed:.2f}s detail={detail[:200]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
