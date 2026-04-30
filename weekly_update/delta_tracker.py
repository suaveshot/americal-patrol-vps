# weekly_update/delta_tracker.py
"""
Loads prior-week metrics from weekly_state.json, calculates
week-over-week percentage deltas, and saves current snapshot.
"""

import json
import os
from pathlib import Path

STATE_FILE = Path(__file__).resolve().parent / "weekly_state.json"


def load_prior_metrics() -> dict:
    """Load last week's metrics snapshot. Returns empty dict on first run."""
    if not STATE_FILE.exists():
        return {}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("metrics", {})
    except (json.JSONDecodeError, OSError):
        return {}


def save_metrics(metrics: dict) -> None:
    """Atomically write current metrics snapshot to state file."""
    from datetime import datetime

    payload = {
        "last_run": datetime.now().isoformat(),
        "metrics": metrics,
    }
    tmp = STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, STATE_FILE)


def calc_delta(current: float, previous: float) -> dict:
    if previous is None or previous == 0:
        if current > 0:
            return {"direction": "new", "pct": 0.0}
        return {"direction": "flat", "pct": 0.0}

    pct = ((current - previous) / abs(previous)) * 100

    if abs(pct) < 5:
        return {"direction": "flat", "pct": abs(pct)}
    elif pct > 0:
        return {"direction": "up", "pct": abs(pct)}
    else:
        return {"direction": "down", "pct": abs(pct)}


def build_deltas(current: dict, prior: dict) -> dict:
    deltas = {}
    for key, val in current.items():
        prev = prior.get(key)
        deltas[key] = calc_delta(val, prev)
    return deltas
