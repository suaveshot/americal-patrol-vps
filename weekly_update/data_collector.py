# weekly_update/data_collector.py
"""
Pull metrics from three data sources:
  1. GHL API  — estimates sent + deals closed this week
  2. Google Ads API — weekly spend, calls, CPL
  3. Voice Agent — call volume from call_logs/ metadata files
"""

import json
import os
import re
import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger(__name__)

PROJECT_DIR = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# 1. GHL — Estimates & Deals
# ---------------------------------------------------------------------------

def collect_ghl_data() -> dict:
    """
    Fetch opportunities from GHL pipeline. Return dict with:
        estimates: list of dicts {name, service, amount}
        deals_closed: list of dicts {name, service, amount}
        estimates_total: float
        deals_closed_total: float
    """
    sys.path.insert(0, str(PROJECT_DIR))
    from sales_autopilot.ghl_sales_client import GHLSalesClient
    from weekly_update.config import PIPELINE_ID, PROPOSAL_SENT_STAGE

    client = GHLSalesClient()
    pipeline_id = PIPELINE_ID()
    cutoff = datetime.now() - timedelta(days=7)

    try:
        all_opps = client.search_opportunities(pipeline_id)
    except Exception as e:
        log.error("GHL API error: %s", e)
        return {
            "estimates": [], "deals_closed": [],
            "estimates_total": 0.0, "deals_closed_total": 0.0,
            "error": str(e),
        }

    estimates = []
    deals_closed = []

    proposal_stage_id = PROPOSAL_SENT_STAGE()

    for opp in all_opps:
        name = opp.get("name", "Unknown")
        amount = float(opp.get("monetaryValue") or 0)
        contact = opp.get("contact") or {}
        company = contact.get("companyName") or name

        # Parse creation date
        date_added = opp.get("dateAdded", "")
        try:
            opp_dt = datetime.fromisoformat(date_added.replace("Z", "+00:00")).replace(tzinfo=None)
        except (ValueError, AttributeError):
            opp_dt = None

        # Estimates sent this week: stage == Proposal Sent AND created in last 7 days
        if (proposal_stage_id
                and opp.get("pipelineStageId") == proposal_stage_id
                and opp_dt and opp_dt >= cutoff):
            estimates.append({
                "name": company,
                "service": name,
                "amount": amount,
            })

        # Deals closed this week: status == "won" AND updated in last 7 days
        date_updated = opp.get("dateUpdated", "")
        try:
            upd_dt = datetime.fromisoformat(date_updated.replace("Z", "+00:00")).replace(tzinfo=None)
        except (ValueError, AttributeError):
            upd_dt = None

        if opp.get("status") == "won" and upd_dt and upd_dt >= cutoff:
            deals_closed.append({
                "name": company,
                "service": name,
                "amount": amount,
            })

    return {
        "estimates": estimates,
        "deals_closed": deals_closed,
        "estimates_total": sum(e["amount"] for e in estimates),
        "deals_closed_total": sum(d["amount"] for d in deals_closed),
    }


# ---------------------------------------------------------------------------
# 2. Google Ads — Weekly Performance
# ---------------------------------------------------------------------------

def collect_ads_data() -> dict:
    """
    Pull campaign-level metrics for the trailing 7 days.
    Returns dict with: spend, calls, cost_per_lead, clicks, impressions
    """
    try:
        from dotenv import load_dotenv
        load_dotenv(PROJECT_DIR / ".env")
    except ImportError:
        pass

    try:
        from google.ads.googleads.client import GoogleAdsClient

        client = GoogleAdsClient.load_from_dict({
            "developer_token":   os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
            "client_id":         os.environ["GOOGLE_CLIENT_ID"],
            "client_secret":     os.environ["GOOGLE_CLIENT_SECRET"],
            "refresh_token":     os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
            "login_customer_id": os.environ["GOOGLE_ADS_LOGIN_CUSTOMER_ID"],
            "use_proto_plus":    True,
        })
        customer_id = os.environ["GOOGLE_ADS_CLIENT_CUSTOMER_ID"]

        ga = client.get_service("GoogleAdsService")

        end = datetime.now().date()
        start = end - timedelta(days=7)

        query = f"""
            SELECT
                campaign.name,
                metrics.impressions,
                metrics.clicks,
                metrics.cost_micros,
                metrics.phone_calls,
                metrics.average_cpc
            FROM campaign
            WHERE segments.date BETWEEN '{start}' AND '{end}'
        """

        totals = {"impressions": 0, "clicks": 0, "spend": 0.0, "calls": 0}
        for row in ga.search(customer_id=customer_id, query=query):
            m = row.metrics
            totals["impressions"] += m.impressions
            totals["clicks"]      += m.clicks
            totals["spend"]       += m.cost_micros / 1_000_000
            totals["calls"]       += m.phone_calls

        totals["cost_per_lead"] = (
            round(totals["spend"] / totals["calls"], 2)
            if totals["calls"] > 0 else 0.0
        )
        totals["spend"] = round(totals["spend"], 2)

        return totals

    except Exception as e:
        log.error("Google Ads API error: %s", e)
        return {
            "spend": 0.0, "calls": 0, "cost_per_lead": 0.0,
            "clicks": 0, "impressions": 0,
            "error": str(e),
        }


# ---------------------------------------------------------------------------
# 3. Voice Agent — Call Volume
# ---------------------------------------------------------------------------

def collect_voice_data() -> dict:
    """
    Scan voice_agent/call_logs/ for metadata files from the past 7 days.
    Returns dict with: total, intake, emergency, dropped, general
    """
    calls_dir = PROJECT_DIR / "voice_agent" / "call_logs"
    cutoff = datetime.now() - timedelta(days=7)
    counts = {"total": 0, "intake": 0, "emergency": 0, "dropped": 0, "general": 0}

    if not calls_dir.exists():
        return counts

    emergency_kws = [
        "emergency", "911", "police", "fire", "shooting",
        "assault", "break-in", "robbery", "injured", "weapon",
    ]

    for meta_file in calls_dir.glob("*_meta.json"):
        if meta_file.name.endswith("_drive.json"):
            continue
        try:
            date_match = re.match(r"(\d{8})_(\d{6})", meta_file.name)
            if not date_match:
                continue
            file_dt = datetime.strptime(
                date_match.group(1) + date_match.group(2), "%Y%m%d%H%M%S"
            )
            if file_dt < cutoff:
                continue

            with open(meta_file, "r", encoding="utf-8") as f:
                meta = json.load(f)

            counts["total"] += 1

            # Classify
            if meta.get("is_intake"):
                counts["intake"] += 1
                continue

            # Check transcript for emergency keywords
            transcript_path = meta_file.with_name(
                meta_file.name.replace("_meta.json", "_transcript.txt")
            )
            if transcript_path.exists():
                try:
                    text = transcript_path.read_text(encoding="utf-8").lower()
                    if any(kw in text for kw in emergency_kws):
                        counts["emergency"] += 1
                        continue
                except Exception:
                    pass

            # Check duration for dropped calls
            duration = meta.get("duration", "")
            try:
                parts = duration.split()
                total_secs = int(parts[0]) * 60 + int(parts[2]) if len(parts) >= 4 else 0
                if total_secs < 30:
                    counts["dropped"] += 1
                    continue
            except Exception:
                pass

            counts["general"] += 1

        except Exception:
            continue

    return counts


# ---------------------------------------------------------------------------
# 4. Event Bus — Cross-Pipeline Activity
# ---------------------------------------------------------------------------

def collect_pipeline_activity() -> dict:
    """
    Read event bus for activity across all pipelines in the past 7 days.
    Returns dict with summaries from each pipeline that published events.
    """
    try:
        sys.path.insert(0, str(PROJECT_DIR))
        from shared_utils.event_bus import read_events_since
    except ImportError:
        log.warning("Event bus not available — skipping pipeline activity")
        return {}

    activity = {}

    # Patrol reports
    patrol_events = read_events_since("patrol", "daily_summary", days=7)
    if patrol_events:
        total_emails = sum(e.get("emails_sent", 0) for e in patrol_events)
        total_incidents = sum(1 for e in patrol_events if e.get("incidents"))
        activity["patrol"] = {
            "reports_sent": len(patrol_events),
            "total_emails": total_emails,
            "days_with_incidents": total_incidents,
        }

    # Blog posts
    blog_events = read_events_since("blog", "post_published", days=7)
    if blog_events:
        activity["blog"] = {
            "posts_published": len(blog_events),
            "titles": [e.get("title", "") for e in blog_events],
        }

    # Social media
    social_events = read_events_since("social", "posts_published", days=7)
    if social_events:
        activity["social"] = {
            "posting_days": len(social_events),
        }

    # SEO analysis
    seo_events = read_events_since("seo", "analysis_results", days=7)
    if seo_events:
        latest = seo_events[0]
        activity["seo"] = {
            "traffic_drops": latest.get("traffic_drops", 0),
            "keyword_gaps": latest.get("keyword_gaps", 0),
            "rising_pages": latest.get("rising_pages", 0),
        }

    # Guard compliance
    compliance_events = read_events_since("guard_compliance", "compliance_check", days=7)
    if compliance_events:
        latest = compliance_events[0]
        activity["guard_compliance"] = {
            "officers_checked": latest.get("officers_checked", 0),
            "fully_compliant": latest.get("fully_compliant", 0),
            "alerts_sent": latest.get("alerts_sent", 0),
        }

    # Sales pipeline
    sales_events = read_events_since("sales_pipeline", "daily_complete", days=7)
    if sales_events:
        total_follow_ups = sum(e.get("follow_ups_sent", 0) for e in sales_events)
        total_replies = sum(e.get("replies_detected", 0) for e in sales_events)
        activity["sales_pipeline"] = {
            "active_days": len(sales_events),
            "follow_ups_sent": total_follow_ups,
            "replies_detected": total_replies,
        }

    # Deals won/lost
    won_events = read_events_since("sales_pipeline", "deal_won", days=7)
    lost_events = read_events_since("sales_pipeline", "deal_lost", days=7)
    if won_events or lost_events:
        activity.setdefault("sales_pipeline", {})
        activity["sales_pipeline"]["deals_won"] = len(won_events)
        activity["sales_pipeline"]["deals_lost"] = len(lost_events)

    # Incident trends
    trend_events = read_events_since("trends", "weekly_analysis", days=7)
    if trend_events:
        latest = trend_events[0]
        activity["incident_trends"] = {
            "properties_analyzed": latest.get("properties_analyzed", 0),
            "alerts_generated": latest.get("alerts_generated", 0),
        }

    return activity


# ---------------------------------------------------------------------------
# Aggregate all sources
# ---------------------------------------------------------------------------

def collect_all() -> dict:
    """Run all collectors and return combined metrics dict."""
    log.info("Collecting GHL data...")
    ghl = collect_ghl_data()

    log.info("Collecting Google Ads data...")
    ads = collect_ads_data()

    log.info("Collecting voice agent data...")
    voice = collect_voice_data()

    log.info("Collecting pipeline activity from event bus...")
    pipeline_activity = collect_pipeline_activity()

    return {"ghl": ghl, "ads": ads, "voice": voice, "pipeline_activity": pipeline_activity}
