# weekly_update/run_weekly_update.py
"""
Weekly Business Update — Entry Point

Orchestrates: collect -> delta -> compose -> send -> publish event -> save state.
Scheduled via Task Scheduler every Friday at 12:00 PM.
"""

import smtplib
import sys
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

# Ensure project root is on sys.path
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

from weekly_update.config import (
    GMAIL_APP_PASSWORD,
    GMAIL_SENDER,
    TO_EMAIL,
    LOG_FILE,
    validate_config,
)
from weekly_update.data_collector import collect_all
from weekly_update.delta_tracker import (
    build_deltas,
    load_prior_metrics,
    save_metrics,
)
from weekly_update.email_composer import compose_email
from shared_utils.event_bus import publish_event


# ---------------------------------------------------------------------------
# Logging (same pattern as all pipelines)
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# Flatten metrics for delta tracking
# ---------------------------------------------------------------------------

def _flatten_metrics(data: dict) -> dict:
    """Extract the numeric metrics we track week-over-week."""
    ghl = data["ghl"]
    ads = data["ads"]
    voice = data["voice"]

    return {
        "estimates_sent_count": len(ghl["estimates"]),
        "estimates_total_value": ghl["estimates_total"],
        "deals_closed_count": len(ghl["deals_closed"]),
        "deals_closed_value": ghl["deals_closed_total"],
        "ads_spend": ads.get("spend", 0.0),
        "ads_calls": ads.get("calls", 0),
        "ads_cost_per_lead": ads.get("cost_per_lead", 0.0),
        "voice_total_calls": voice.get("total", 0),
        "voice_intake_calls": voice.get("intake", 0),
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    log("Weekly update starting")

    validate_config()

    # 1. Collect data from all sources
    data = collect_all()
    log(f"GHL: {len(data['ghl']['estimates'])} estimates, "
        f"{len(data['ghl']['deals_closed'])} deals closed")
    log(f"Ads: ${data['ads'].get('spend', 0):.2f} spend, "
        f"{data['ads'].get('calls', 0)} calls")
    log(f"Voice: {data['voice']['total']} total calls")

    # 2. Calculate deltas
    current_metrics = _flatten_metrics(data)
    prior_metrics = load_prior_metrics()
    deltas = build_deltas(current_metrics, prior_metrics)
    log(f"Deltas calculated (prior data: {'yes' if prior_metrics else 'first run'})")

    # 3. Compose email
    subject, html_body = compose_email(data, deltas)
    log(f"Email composed: {subject}")

    # 4. Send via SMTP
    sender = GMAIL_SENDER()
    recipient = TO_EMAIL()
    password = GMAIL_APP_PASSWORD()

    msg = MIMEText(html_body, "html")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = recipient

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(sender, password)
            srv.sendmail(sender, recipient, msg.as_string())
        log(f"Email sent to {recipient}")
    except smtplib.SMTPException as e:
        log(f"ERROR: Failed to send email: {e}")
        return  # Do NOT save state so next run retries with same delta baseline

    # 5. Publish event
    publish_event("weekly_update", "digest_sent", {
        "recipient": recipient,
        "estimates_count": len(data["ghl"]["estimates"]),
        "deals_closed_count": len(data["ghl"]["deals_closed"]),
        "ads_spend": data["ads"].get("spend", 0.0),
    })
    log("Event published to event bus")

    # 6. Save state for next week's deltas
    save_metrics(current_metrics)
    log("State saved. Weekly update complete.")


if __name__ == "__main__":
    main()
