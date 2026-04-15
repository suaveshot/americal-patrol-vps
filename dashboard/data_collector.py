"""
Americal Patrol — Dashboard Data Collector
Reads state files, logs, event bus data, and watchdog health from all pipelines.
Returns unified status dicts for the status report generator.
"""

import json
import re
import socket
import sys
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from shared_utils.event_bus import read_latest_event, read_events_since

# Log timestamp formats used across pipelines
PATROL_LOG_RE = re.compile(r"^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]")
HL_LOG_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")

PIPELINES = [
    {
        "id": "patrol",
        "name": "Morning Reports",
        "schedule": "Daily 7:00 AM",
        "interval_hours": 36,
        "log_file": "patrol_automation/automation.log",
        "log_format": "patrol",
        "state_file": None,
        "event_pipeline": "patrol",
        "event_type": "daily_summary",
    },
    {
        "id": "seo",
        "name": "SEO Analysis",
        "schedule": "Monday 7:00 AM",
        "interval_hours": 192,  # 8 days
        "log_file": "seo_automation/automation.log",
        "log_format": "patrol",
        "state_file": "seo_automation/seo_state.json",
        "event_pipeline": "seo",
        "event_type": "analysis_results",
    },
    {
        "id": "blog",
        "name": "Blog Post",
        "schedule": "Monday 8:00 AM",
        "interval_hours": 192,
        "log_file": "blog_post_automation/automation.log",
        "log_format": "patrol",
        "state_file": "blog_post_automation/blog_state.json",
        "event_pipeline": "blog",
        "event_type": "post_published",
    },
    {
        "id": "gbp",
        "name": "GBP Post",
        "schedule": "Monday 9:00 AM",
        "interval_hours": 192,
        "log_file": "gbp_automation/automation.log",
        "log_format": "patrol",
        "state_file": "gbp_automation/gbp_state.json",
        "event_pipeline": "gbp",
        "event_type": "post_published",
    },
    {
        "id": "social",
        "name": "Social Media",
        "schedule": "Tue/Thu/Sat 10:00 AM",
        "interval_hours": 96,  # 4 days
        "log_file": "social_media_automation/automation.log",
        "log_format": "patrol",
        "state_file": "social_media_automation/social_state.json",
        "event_pipeline": "social",
        "event_type": "posts_published",
    },
    {
        "id": "voice",
        "name": "Voice Agent",
        "schedule": "Always-on (Vapi + n8n)",
        "interval_hours": 48,
        "log_file": "voice_agent/automation.log",
        "log_format": "patrol",
        "state_file": None,
        "event_pipeline": "voice_agent",
        "event_type": "lead_captured",
    },
    {
        "id": "ads",
        "name": "Google Ads",
        "schedule": "Monday 6:00 AM",
        "interval_hours": 192,
        "log_file": "google_ads_automation/automation.log",
        "log_format": "patrol",
        "state_file": None,
        "event_pipeline": None,
        "event_type": None,
    },
    {
        "id": "harbor_lights",
        "name": "Harbor Lights Parking",
        "schedule": "Daily 7:30 AM",
        "interval_hours": 36,
        "log_file": "Harbor Lights/harbor_lights.log",
        "log_format": "patrol",
        "state_file": None,
        "event_pipeline": None,
        "event_type": None,
    },
    {
        "id": "sales_pipeline",
        "name": "Sales Pipeline",
        "schedule": "Mon-Fri 8:00 AM",
        "interval_hours": 72,
        "log_file": "sales_pipeline/automation.log",
        "log_format": "patrol",
        "state_file": "sales_pipeline/pipeline_state.json",
        "event_pipeline": "sales_pipeline",
        "event_type": "daily_complete",
    },
    {
        "id": "weekly_update",
        "name": "Weekly Update",
        "schedule": "Friday 12:00 PM",
        "interval_hours": 192,
        "log_file": "weekly_update/automation.log",
        "log_format": "patrol",
        "state_file": "weekly_update/weekly_state.json",
        "event_pipeline": "weekly_update",
        "event_type": "digest_sent",
    },
    {
        "id": "reviews",
        "name": "Review Engine",
        "schedule": "Quarterly (1st Mon Jan/Apr/Jul/Oct)",
        "interval_hours": 2400,
        "log_file": "review_engine/automation.log",
        "log_format": "harbor_lights",  # bare timestamp format (no brackets)
        "state_file": "review_engine/review_state.json",
        "event_pipeline": "reviews",
        "event_type": "run_complete",
    },
    {
        "id": "qbr",
        "name": "QBR Generator",
        "schedule": "Quarterly (1st Mon Jan/Apr/Jul/Oct)",
        "interval_hours": 2400,
        "log_file": "qbr_generator/automation.log",
        "log_format": "harbor_lights",  # bare timestamp format (no brackets)
        "state_file": "qbr_generator/qbr_state.json",
        "event_pipeline": "qbr",
        "event_type": "reports_generated",
    },
    {
        "id": "guard_compliance",
        "name": "Guard Compliance",
        "schedule": "Daily 6:00 AM",
        "interval_hours": 36,
        "log_file": "guard_compliance/automation.log",
        "log_format": "patrol",
        "state_file": "guard_compliance/compliance_state.json",
        "event_pipeline": "guard_compliance",
        "event_type": "compliance_check",
    },
    {
        "id": "email_assistant",
        "name": "Email Assistant",
        "schedule": "Hourly, 24/7",
        "interval_hours": 4,
        "log_file": "email_assistant/automation.log",
        "log_format": "patrol",
        "state_file": "email_assistant/email_state.json",
        "event_pipeline": "email_assistant",
        "event_type": "lead_inquiry",
    },
]


def _read_log_tail(log_path: Path, max_lines: int = 100) -> list[str]:
    """Read the last N lines of a log file."""
    if not log_path.exists():
        return []
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        return lines[-max_lines:]
    except OSError:
        return []


def _parse_last_timestamp(lines: list[str], log_format: str) -> datetime | None:
    """Extract the most recent timestamp from log lines."""
    regex = HL_LOG_RE if log_format == "harbor_lights" else PATROL_LOG_RE
    for line in reversed(lines):
        m = regex.match(line.strip())
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
    return None


def _detect_errors_in_last_run(lines: list[str], log_format: str) -> bool:
    """Check if the last run block contains ERROR lines."""
    # Find the start of the last run (look for "Starting" or "Audit started")
    last_run_start = 0
    for i, line in enumerate(lines):
        if "Starting" in line or "Audit started" in line:
            last_run_start = i

    last_run_lines = lines[last_run_start:]
    for line in last_run_lines:
        upper = line.upper()
        if "ERROR:" in upper or "TRACEBACK" in upper:
            return True
    return False


def _format_relative_time(dt: datetime) -> str:
    """Format a datetime as a human-readable relative string."""
    now = datetime.now()
    diff = now - dt

    if diff.total_seconds() < 0:
        return dt.strftime("%b %d, %I:%M %p")

    minutes = diff.total_seconds() / 60
    hours = minutes / 60
    days = diff.days

    if days == 0:
        if dt.date() == now.date():
            return f"{dt.strftime('%I:%M %p')} today"
        else:
            return f"{dt.strftime('%I:%M %p')} yesterday"
    elif days == 1:
        return f"Yesterday {dt.strftime('%I:%M %p')}"
    elif days < 7:
        return f"{dt.strftime('%A %I:%M %p')}"
    else:
        return dt.strftime("%b %d, %I:%M %p")


def _read_state_file(state_path: Path) -> dict | None:
    """Read and parse a pipeline state JSON file."""
    if not state_path or not state_path.exists():
        return None
    try:
        with open(state_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return None


def _check_port(port: int) -> bool:
    """Check if a TCP port is listening (for voice agent status)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            return s.connect_ex(("127.0.0.1", port)) == 0
    except OSError:
        return False


def _get_summary(pipeline: dict, state: dict | None) -> str:
    """Build a one-line summary for the pipeline based on its latest event/state."""
    pid = pipeline["id"]

    # Try event bus first
    if pipeline["event_pipeline"] and pipeline["event_type"]:
        event = read_latest_event(pipeline["event_pipeline"], pipeline["event_type"])
        if event:
            if pid == "patrol":
                sent = event.get("emails_sent", 0)
                drafts = event.get("drafts_created", 0)
                incidents = event.get("accounts_with_incidents", 0)
                return f"{sent} sent, {drafts} drafts, {incidents} incidents"
            elif pid == "seo":
                drops = event.get("traffic_drops", 0)
                gaps = event.get("keyword_gaps", 0)
                rising = event.get("rising_pages", 0)
                return f"{drops} drops, {gaps} gaps, {rising} rising"
            elif pid == "blog":
                title = event.get("title", "")
                if title:
                    short = title[:50] + "..." if len(title) > 50 else title
                    return short
            elif pid == "gbp":
                subj = event.get("post_summary", event.get("topic_subject", ""))
                if subj:
                    short = subj[:50] + "..." if len(subj) > 50 else subj
                    return short
            elif pid == "social":
                posts = event.get("posts", [])
                if posts:
                    platforms = [p.get("platform", "?") for p in posts]
                    return f"Posted to {', '.join(platforms)}"
            elif pid == "weekly_update":
                estimates = event.get("estimates_count", 0)
                deals = event.get("deals_closed_count", 0)
                spend = event.get("ads_spend", 0.0)
                return f"{estimates} estimates, {deals} deals, ${spend:.0f} ad spend"

    # Fallback to state file
    if state:
        if pid == "seo":
            summary = state.get("last_analysis_summary", {})
            drops = summary.get("traffic_drops", 0)
            gaps = summary.get("keyword_gaps", 0)
            return f"{drops} drops, {gaps} gaps (run #{state.get('runs_completed', '?')})"
        elif pid == "blog":
            last = state.get("last_post", {})
            title = last.get("title", "")
            if title:
                short = title[:50] + "..." if len(title) > 50 else title
                return f"Post #{state.get('posts_published', '?')}: {short}"
            return f"{state.get('posts_published', 0)} posts published"
        elif pid == "gbp":
            last = state.get("last_post", {})
            subj = last.get("subject")
            if subj:
                return f"Run #{state.get('runs_completed', '?')}: {subj}"
            return f"{state.get('runs_completed', 0)} runs completed"
        elif pid == "social":
            total = state.get("posts_published", 0)
            imgs = state.get("images_generated", 0)
            return f"{total} posts, {imgs} images generated"

    if pid == "voice":
        if pipeline["event_pipeline"]:
            event = read_latest_event("voice_agent", "lead_captured")
            if event:
                lead = event.get("lead_name", event.get("caller_name", ""))
                return f"Last lead: {lead}" if lead else "Lead captured"
        return "Vapi + n8n webhook"

    if pid == "email_assistant":
        if pipeline["event_pipeline"]:
            event = read_latest_event("email_assistant", "lead_inquiry")
            if event:
                subject = event.get("subject", "")
                if subject:
                    short = subject[:50] + "..." if len(subject) > 50 else subject
                    return short
        return "No inquiries yet"

    if pid == "sales_pipeline":
        if state:
            contacts = state.get("contacts", {})
            active = sum(1 for c in contacts.values()
                         if not c.get("completed") and c.get("stage") not in
                         ("won", "closed_lost", "sequence_done", "unsubscribed"))
            proposals = sum(1 for c in contacts.values()
                           if c.get("phase") == "post_proposal" and not c.get("completed"))
            return f"{active} active contacts, {proposals} proposals pending"

    if pid == "harbor_lights":
        return "Parking audit"

    if pid == "reviews":
        if pipeline["event_pipeline"]:
            event = read_latest_event("reviews", "run_complete")
            if event:
                sent = event.get("sent", 0)
                mode = event.get("mode", "draft")
                return f"{sent} review request(s) ({mode})"
        return "No data yet"

    if pid == "qbr":
        if pipeline["event_pipeline"]:
            event = read_latest_event("qbr", "reports_generated")
            if event:
                gen = event.get("generated", 0)
                quarter = event.get("quarter", "")
                return f"{gen} QBR(s) generated ({quarter})"
        return "No data yet"

    return "No data yet"




def get_watchdog_health() -> dict:
    """
    Read watchdog/health_status.json and return its contents.
    Merges watchdog alerts/fixes into the pipeline status where available.
    Returns empty dict if watchdog hasn't run yet.
    """
    health_file = PROJECT_ROOT / "watchdog" / "health_status.json"
    try:
        with open(health_file, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def get_all_pipelines_status() -> list[dict]:
    """Return status dict for every registered pipeline, enriched with watchdog data."""
    results = []
    now = datetime.now()
    watchdog = get_watchdog_health()

    for p in PIPELINES:
        log_path   = PROJECT_ROOT / p["log_file"] if p["log_file"] else None
        state_path = PROJECT_ROOT / p["state_file"] if p["state_file"] else None

        log_lines = _read_log_tail(log_path) if log_path else []
        last_ts   = _parse_last_timestamp(log_lines, p["log_format"])
        has_errors = _detect_errors_in_last_run(log_lines, p["log_format"]) if log_lines else False

        state = _read_state_file(state_path)
        if state and state.get("last_run"):
            try:
                state_ts = datetime.fromisoformat(state["last_run"])
                if not last_ts or state_ts > last_ts:
                    last_ts = state_ts
            except (ValueError, TypeError):
                pass

        # Prefer watchdog's last_log_ts if it's more recent
        wd = watchdog.get(p["id"], {})
        wd_ts_str = wd.get("last_log_ts") or wd.get("last_run")
        if wd_ts_str:
            try:
                wd_ts = datetime.fromisoformat(wd_ts_str)
                if not last_ts or wd_ts > last_ts:
                    last_ts = wd_ts
            except (ValueError, TypeError):
                pass

        if not last_ts:
            status = "unknown"
        elif has_errors or wd.get("status") == "error":
            status = "error"
        elif p["interval_hours"] and (now - last_ts).total_seconds() > p["interval_hours"] * 3600:
            status = "overdue"
        elif wd.get("status") == "warning":
            status = "warning"
        else:
            status = "success"

        result = {
            "id":               p["id"],
            "name":             p["name"],
            "schedule":         p["schedule"],
            "last_run":         last_ts.isoformat() if last_ts else None,
            "last_run_display": _format_relative_time(last_ts) if last_ts else "Never",
            "status":           status,
            "summary":          _get_summary(p, state),
        }

        # Surface watchdog alerts and auto-fix notes in the dashboard
        if wd.get("alerts"):
            result["alerts"] = wd["alerts"]
        if wd.get("fixes"):
            result["auto_fixes"] = wd["fixes"]

        results.append(result)

    return results


def get_productivity_metrics(days: int = 30) -> dict:
    """Aggregate productivity metrics from event bus over the last N days."""
    metrics = {
        "period_days": days,
        "drafts_created": 0,
        "total_incidents": 0,
        "blog_posts": 0,
        "gbp_posts": 0,
        "social_posts": 0,
        "seo_reports": 0,
        "estimates_sent": 0,
        "weekly_digests": 0,
        "review_requests": 0,
        "qbrs_generated": 0,
    }

    # Patrol events
    patrol_events = read_events_since("patrol", "daily_summary", days=days)
    for e in patrol_events:
        metrics["drafts_created"] += e.get("drafts_created", 0)
        metrics["total_incidents"] += e.get("accounts_with_incidents", 0)

    # SEO events
    seo_events = read_events_since("seo", "analysis_results", days=days)
    metrics["seo_reports"] = len(seo_events)

    # Blog events
    blog_events = read_events_since("blog", "post_published", days=days)
    metrics["blog_posts"] = len(blog_events)

    # GBP events
    gbp_events = read_events_since("gbp", "post_published", days=days)
    metrics["gbp_posts"] = len(gbp_events)

    # Social events
    social_events = read_events_since("social", "posts_published", days=days)
    for e in social_events:
        posts = e.get("posts", [])
        metrics["social_posts"] += len(posts) if posts else 1

    # Weekly update events
    weekly_events = read_events_since("weekly_update", "digest_sent", days=days)
    metrics["weekly_digests"] = len(weekly_events)
    for e in weekly_events:
        metrics["estimates_sent"] += e.get("estimates_count", 0)

    # Review engine events
    review_events = read_events_since("reviews", "run_complete", days=days)
    for e in review_events:
        metrics["review_requests"] += e.get("sent", 0)

    # QBR events
    qbr_events = read_events_since("qbr", "reports_generated", days=days)
    for e in qbr_events:
        metrics["qbrs_generated"] += e.get("generated", 0)

    return metrics
