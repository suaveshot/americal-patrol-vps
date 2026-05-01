"""
Watchdog Diagnostic Engine — researches pipeline failures and proposes fixes.

When the watchdog detects an ERROR / Traceback in a pipeline log, it calls
`diagnose()` here. This module:

  1. Builds a context bundle (log tail, recent pipeline events, file tree,
     last few git commits touching the pipeline directory).
  2. Asks Claude (claude-sonnet-4-6) to identify the root cause and propose
     an exact before/after code change in a strict JSON shape.
  3. Dedupes by error_signature so the same failure isn't re-diagnosed for
     24 hours (cost guard).

The result is consumed by `dashboard_client.post_diagnostic()` and rendered
on the WCAS dashboard for Sam to review and approve. This module never
applies fixes — propose-only by design.

Env vars:
  ANTHROPIC_API_KEY              required, reuses the project key
  DIAGNOSTIC_ENGINE_ENABLED      "true" to enable; default off so the engine
                                 stays inert until Sam reviews actual output
                                 quality on the first few real failures
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

DEDUPE_WINDOW = timedelta(hours=24)
MAX_LOG_LINES = 80
MAX_FILE_TREE_ENTRIES = 60
MAX_PROMPT_CHARS = 14_000
CLAUDE_MODEL = "claude-sonnet-4-6"

STATE_FILE = Path(__file__).resolve().parent / "diagnostic_state.json"

_NORMALIZE_PATTERNS = [
    (re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}[.,]?\d*"), "<TS>"),
    (re.compile(r"\bline \d+\b"), "line <N>"),
    (re.compile(r"0x[0-9a-fA-F]+"), "<HEX>"),
    (re.compile(r"\b\d{4,}\b"), "<NUM>"),
    (re.compile(r"[A-Z]:\\[^\s'\"]+"), "<PATH>"),
    (re.compile(r"/[^\s'\"]{6,}"), "<PATH>"),
]


def _normalize_error(error_line: str) -> str:
    out = error_line.strip()
    for pattern, repl in _NORMALIZE_PATTERNS:
        out = pattern.sub(repl, out)
    return out


def _error_signature(pipeline_id: str, error_line: str) -> str:
    normalized = _normalize_error(error_line)
    h = hashlib.sha1(f"{pipeline_id}::{normalized}".encode("utf-8")).hexdigest()
    return h[:16]


def _load_state() -> dict:
    try:
        with STATE_FILE.open(encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def _save_state(state: dict) -> None:
    tmp = STATE_FILE.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)


def _recently_diagnosed(signature: str, state: dict) -> bool:
    entry = state.get(signature)
    if not entry:
        return False
    try:
        last = datetime.fromisoformat(entry["last_diagnosed"])
    except (KeyError, ValueError):
        return False
    return datetime.now() - last < DEDUPE_WINDOW


def _record_diagnosis(signature: str, state: dict) -> None:
    now = datetime.now().isoformat()
    entry = state.get(signature, {"first_seen": now, "post_count": 0})
    entry["last_diagnosed"] = now
    entry["post_count"] = entry.get("post_count", 0) + 1
    state[signature] = entry
    _save_state(state)


def _recent_commits(directory: Path, count: int = 3) -> list[str]:
    if not directory.exists():
        return []
    try:
        result = subprocess.run(
            ["git", "log", f"-n{count}", "--oneline", "--", str(directory)],
            capture_output=True,
            text=True,
            timeout=5,
            cwd=str(directory.parent),
        )
        if result.returncode != 0:
            return []
        return [line for line in result.stdout.splitlines() if line.strip()]
    except (OSError, subprocess.TimeoutExpired):
        return []


def _file_tree(directory: Path) -> list[str]:
    if not directory.exists() or not directory.is_dir():
        return []
    entries = []
    for path in sorted(directory.rglob("*.py")):
        try:
            rel = path.relative_to(directory)
        except ValueError:
            continue
        entries.append(str(rel))
        if len(entries) >= MAX_FILE_TREE_ENTRIES:
            break
    return entries


def _recent_pipeline_events(events_dir: Path, pipeline_id: str, hours: int = 24) -> list[dict]:
    if not events_dir.exists():
        return []
    cutoff = datetime.now() - timedelta(hours=hours)
    events: list[dict] = []
    for path in sorted(events_dir.glob("*.json"), reverse=True):
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime)
            if mtime < cutoff:
                break
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        source = data.get("source") or data.get("pipeline") or ""
        if pipeline_id in source.lower() or source.lower() in pipeline_id:
            events.append({"file": path.name, "type": data.get("type"), "data": data})
        if len(events) >= 8:
            break
    return events


def _build_prompt(
    pipeline_id: str,
    error_line: str,
    log_tail: list[str],
    state_summary: dict | None,
    file_tree: list[str],
    recent_events: list[dict],
    recent_commits: list[str],
) -> str:
    log_excerpt = "".join(log_tail[-MAX_LOG_LINES:])
    state_excerpt = json.dumps(state_summary, indent=2, default=str)[:1500] if state_summary else "(none)"
    events_excerpt = json.dumps(recent_events, indent=2, default=str)[:2000] if recent_events else "(none)"
    tree_excerpt = "\n".join(file_tree) if file_tree else "(empty)"
    commits_excerpt = "\n".join(recent_commits) if recent_commits else "(no git history available)"

    prompt = f"""You are Larry, an automation engineer diagnosing a failure in the Americal Patrol pipeline `{pipeline_id}`. Your output goes to Sam, the owner — he will review your diagnosis on a dashboard before applying any fix. Be concrete and specific. No filler.

# Failing pipeline
{pipeline_id}

# Error line (most recent ERROR or Traceback marker in the log)
{error_line}

# Last {MAX_LOG_LINES} log lines
```
{log_excerpt}
```

# Watchdog health snapshot for this pipeline
```json
{state_excerpt}
```

# Recent pipeline events (last 24h, this pipeline only)
```json
{events_excerpt}
```

# Recent git commits touching this pipeline directory
```
{commits_excerpt}
```

# Pipeline source file tree (Python files only, capped)
```
{tree_excerpt}
```

# Your task
Identify the root cause of the failure and propose ONE concrete code change that fixes it. If you cannot identify a code-level fix with reasonable confidence, set confidence="low" and put a manual debugging runbook in the `runbook` field instead of a code change.

Respond with ONLY valid JSON in this exact shape (no prose, no markdown fences):
{{
  "root_cause": "1-2 sentence plain-language explanation of WHY the pipeline is failing",
  "evidence": ["specific log line or file:function reference", "another piece of evidence"],
  "suggested_change": {{
    "file_path": "path/to/file.py relative to the Americal Patrol repo root",
    "function_or_section": "name of the function or section to edit",
    "before": "the EXACT existing code snippet that should be replaced (5-15 lines)",
    "after": "the proposed replacement code",
    "rationale": "why this change fixes the root cause"
  }},
  "confidence": "high" | "medium" | "low",
  "runbook": ["manual step Sam should take if confidence is low or before applying the fix"]
}}

If you genuinely cannot localize the fix to a single file/function, set `suggested_change` to null and rely on `runbook`."""

    if len(prompt) > MAX_PROMPT_CHARS:
        prompt = prompt[:MAX_PROMPT_CHARS] + "\n\n[truncated]"
    return prompt


def _call_claude(prompt: str) -> dict | None:
    try:
        import anthropic
    except ImportError:
        log.warning("[diagnostic] anthropic SDK not installed — skipping")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        log.warning("[diagnostic] ANTHROPIC_API_KEY not set — skipping")
        return None

    try:
        client = anthropic.Anthropic(api_key=api_key)
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )
        text = response.content[0].text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
        result = json.loads(text)
    except json.JSONDecodeError as e:
        log.warning(f"[diagnostic] Could not parse Claude response as JSON: {e}")
        return None
    except anthropic.APIError as e:
        log.warning(f"[diagnostic] Claude APIError: {e}")
        return None
    except Exception as e:
        log.warning(f"[diagnostic] Unexpected error calling Claude: {e}")
        return None

    required = {"root_cause", "evidence", "confidence"}
    if not required.issubset(result.keys()):
        log.warning(f"[diagnostic] Claude response missing required keys: {required - result.keys()}")
        return None
    return result


def diagnose(
    pipeline_id: str,
    error_line: str,
    log_tail: list[str],
    state_summary: dict | None,
    pipeline_source_root: Path,
    *,
    events_dir: Path | None = None,
    force: bool = False,
) -> dict | None:
    """Produce a structured diagnosis for a pipeline failure.

    Returns dict with diagnosis fields plus `_signature` and `_pipeline_id`,
    or None if disabled, deduped, or Claude unavailable.
    """
    if os.environ.get("DIAGNOSTIC_ENGINE_ENABLED", "").lower() not in ("1", "true", "yes"):
        log.info("[diagnostic] disabled (set DIAGNOSTIC_ENGINE_ENABLED=true to turn on)")
        return None

    signature = _error_signature(pipeline_id, error_line)
    state = _load_state()
    if not force and _recently_diagnosed(signature, state):
        log.info(f"[diagnostic] {pipeline_id} signature {signature} already diagnosed within 24h — skipping")
        return None

    file_tree = _file_tree(pipeline_source_root)
    commits = _recent_commits(pipeline_source_root)
    events = _recent_pipeline_events(events_dir or pipeline_source_root.parent / "pipeline_events", pipeline_id)

    prompt = _build_prompt(
        pipeline_id=pipeline_id,
        error_line=error_line,
        log_tail=log_tail,
        state_summary=state_summary,
        file_tree=file_tree,
        recent_events=events,
        recent_commits=commits,
    )

    result = _call_claude(prompt)
    if not result:
        return None

    result["_signature"] = signature
    result["_pipeline_id"] = pipeline_id
    result["_generated_at"] = datetime.now(timezone.utc).isoformat()

    _record_diagnosis(signature, state)
    log.info(
        f"[diagnostic] {pipeline_id} signature={signature} "
        f"confidence={result.get('confidence')} "
        f"file={result.get('suggested_change', {}).get('file_path') if result.get('suggested_change') else 'n/a'}"
    )
    return result
