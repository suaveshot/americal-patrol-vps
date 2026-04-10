"""
Americal Patrol — Pipeline Health Reporter

Each pipeline calls report_status() at the end of its run to write
its outcome to watchdog/health_status.json, which the watchdog and
dashboard can read.

Usage:
    from shared_utils.health_reporter import report_status

    report_status("patrol", "ok", "7 drafts created, 2 incidents",
                  metrics={"drafts": 7, "incidents": 2})

    report_status("seo", "error", "GA4 authentication failed")
"""

import json
import os
from datetime import datetime

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HEALTH_FILE = os.path.join(_PROJECT_ROOT, "watchdog", "health_status.json")


def report_status(pipeline: str, status: str, detail: str = "", metrics: dict = None):
    """
    Write pipeline run outcome to watchdog/health_status.json.

    Args:
        pipeline: Pipeline ID matching data_collector.py (e.g. 'patrol', 'seo')
        status:   'ok', 'warning', or 'error'
        detail:   One-line human-readable summary of what happened or the error
        metrics:  Optional pipeline-specific metrics dict (counts, titles, etc.)
    """
    os.makedirs(os.path.dirname(HEALTH_FILE), exist_ok=True)

    try:
        with open(HEALTH_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        data = {}

    data[pipeline] = {
        "status": status,
        "detail": detail,
        "last_run": datetime.now().isoformat(),
        "metrics": metrics or {},
    }

    # Atomic write — never leave a half-written file
    tmp = HEALTH_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, HEALTH_FILE)
