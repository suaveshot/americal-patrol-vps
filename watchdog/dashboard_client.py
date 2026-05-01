"""
Watchdog -> WCAS Admin Dashboard diagnostic poster.

Multi-tenant by design: same code ships with every client VPS we deploy.
Each client's watchdog posts diagnoses, tagged with their `TENANT_ID`, to
the shared admin dashboard at
`https://dashboard.westcoastautomationsolutions.com/api/diagnostic`.

Sam sees every tenant's failures in the admin view, reviews Larry's diagnosis
+ proposed fix, and applies the fix manually before the client notices
anything broke. The client never sees the diagnostic surface — only their
own healthy pipeline tiles.

Mirrors the heartbeat poster pattern from `shared/push_heartbeat.py`:
  - urllib.request only (no extra deps)
  - reuses DASHBOARD_URL + HEARTBEAT_SHARED_SECRET env vars
  - never crashes the watchdog: any failure logs and returns False
  - hard 8s timeout

Env vars (per-client VPS):
  TENANT_ID                       e.g. "americal_patrol", "client_b". Identifies
                                  which tenant this diagnostic belongs to in the
                                  admin dashboard. Default: "americal_patrol"
                                  for backward compatibility.
  DASHBOARD_URL                   shared admin dashboard URL
  HEARTBEAT_SHARED_SECRET         shared secret for the dashboard ingest endpoint
  DIAGNOSTIC_DRY_RUN              "true" to log payload to file instead of POSTing
                                  (default: true until first-send is approved)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

log = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TENANT_ID = "americal_patrol"
DRY_RUN_LOG = Path(__file__).resolve().parent / "diagnostic_dry_run.log"


def _tenant_id(env: dict[str, str]) -> str:
    return (
        env.get("TENANT_ID")
        or os.environ.get("TENANT_ID")
        or DEFAULT_TENANT_ID
    )


def _load_env() -> dict[str, str]:
    env_path = PROJECT_ROOT / ".env"
    out: dict[str, str] = {}
    if not env_path.exists():
        return out
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            out[k.strip()] = v.strip().strip("'").strip('"')
    except OSError:
        pass
    return out


def _build_payload(diagnosis: dict, error_line: str, tenant_id: str) -> dict:
    return {
        "tenant_id": tenant_id,
        "pipeline_id": diagnosis.get("_pipeline_id"),
        "error_signature": diagnosis.get("_signature"),
        "error_line": error_line,
        "diagnosis": {k: v for k, v in diagnosis.items() if not k.startswith("_")},
        "generated_at": diagnosis.get("_generated_at") or datetime.now(timezone.utc).isoformat(),
    }


def _is_dry_run(env: dict[str, str]) -> bool:
    raw = env.get("DIAGNOSTIC_DRY_RUN") or os.environ.get("DIAGNOSTIC_DRY_RUN", "true")
    return raw.lower() in ("1", "true", "yes")


def _write_dry_run(payload: dict) -> None:
    try:
        with DRY_RUN_LOG.open("a", encoding="utf-8") as f:
            f.write(f"\n=== {datetime.now().isoformat()} ===\n")
            f.write(json.dumps(payload, indent=2, default=str))
            f.write("\n")
    except OSError as e:
        log.warning(f"[diagnostic-post] failed to write dry-run log: {e}")


def post_diagnostic(diagnosis: dict, error_line: str, *, timeout: float = 8.0) -> bool:
    """POST a diagnostic to the WCAS dashboard. Returns True on HTTP 200.

    If DIAGNOSTIC_DRY_RUN is set (default), writes the payload to
    diagnostic_dry_run.log instead so Sam can review the actual diagnostic
    quality before flipping it on.
    """
    env = _load_env()
    payload = _build_payload(diagnosis, error_line, _tenant_id(env))

    if _is_dry_run(env):
        _write_dry_run(payload)
        log.info(
            f"[diagnostic-post] DRY RUN — payload for "
            f"{payload['pipeline_id']}/{payload['error_signature']} written to "
            f"{DRY_RUN_LOG.name} (set DIAGNOSTIC_DRY_RUN=false to send)"
        )
        return True

    url = env.get("DASHBOARD_URL") or os.environ.get("DASHBOARD_URL", "")
    secret = env.get("HEARTBEAT_SHARED_SECRET") or os.environ.get("HEARTBEAT_SHARED_SECRET", "")
    if not url or not secret:
        log.warning("[diagnostic-post] missing DASHBOARD_URL or HEARTBEAT_SHARED_SECRET — skipping")
        return False

    data = json.dumps(payload, default=str).encode("utf-8")
    req = Request(
        url.rstrip("/") + "/api/diagnostic",
        data=data,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "X-Heartbeat-Secret": secret,
            "X-Tenant-Id": payload["tenant_id"],
            "User-Agent": "wcas-diagnostic/1.0",
        },
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            ok = resp.status == 200
            log.info(
                f"[diagnostic-post] {payload['pipeline_id']}/{payload['error_signature']} "
                f"HTTP {resp.status}"
            )
            return ok
    except HTTPError as e:
        log.warning(f"[diagnostic-post] HTTPError {e.code}: {e.read()[:300]!r}")
        return False
    except URLError as e:
        log.warning(f"[diagnostic-post] URLError: {e.reason}")
        return False
    except Exception as e:
        log.warning(f"[diagnostic-post] {type(e).__name__}: {e}")
        return False


def _cli() -> int:
    """Build a sample payload from JSON on stdin and print/post it."""
    import argparse

    parser = argparse.ArgumentParser(description="Test the diagnostic dashboard client.")
    parser.add_argument("--dry-run", action="store_true", help="Print payload, never POST")
    args = parser.parse_args()

    sample = {
        "_pipeline_id": "sales_pipeline",
        "_signature": "test1234",
        "_generated_at": datetime.now(timezone.utc).isoformat(),
        "root_cause": "Test diagnosis — dashboard_client smoke test.",
        "evidence": ["watchdog/dashboard_client.py CLI"],
        "confidence": "low",
        "runbook": ["This is a synthetic payload — ignore."],
    }
    if args.dry_run:
        env = _load_env()
        print(json.dumps(_build_payload(sample, "synthetic test error", _tenant_id(env)), indent=2))
        return 0

    ok = post_diagnostic(sample, "synthetic test error")
    return 0 if ok else 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(_cli())
