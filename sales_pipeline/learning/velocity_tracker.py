"""
Pipeline Velocity Tracker

Computes conversion funnel metrics, average days per stage by property type,
and identifies bottlenecks in the sales pipeline.
"""

import json
import logging
from datetime import datetime, timezone
from collections import defaultdict

from sales_pipeline.config import STATE_FILE
from sales_pipeline.state import _parse_iso

log = logging.getLogger(__name__)

# Ordered funnel stages (discovery → close)
FUNNEL_STAGES = [
    ("discovered", "Discovered"),
    ("cold_sent", "Cold Sent"),
    ("proposal_sent", "Proposal Sent"),
    ("negotiating", "Negotiating"),
    ("won", "Won"),
]

# Stage timestamp fields (used to calculate time between stages)
STAGE_TIMESTAMPS = {
    "discovered": "discovered_at",
    "cold_sent": "first_outreach_at",
    "proposal_sent": "proposal_sent_at",
    "negotiating": "replied_at",
    "won": "won_at",
}


def compute_velocity(state: dict = None) -> dict:
    """
    Compute pipeline velocity metrics from current state.

    Returns:
        {
            "funnel": [{"stage": str, "label": str, "count": int, "conversion_pct": float}],
            "avg_days_by_stage": {stage: {"all": float, by_property_type: float}},
            "bottlenecks": [{"stage": str, "avg_days": float, "contact_count": int}],
            "by_property_type": {type: {"count": int, "avg_days_to_proposal": float, "avg_days_to_close": float}},
            "active_contacts": int,
            "computed_at": str,
        }
    """
    if state is None:
        state = _load_state()

    contacts = state.get("contacts", {})
    now = datetime.now(timezone.utc)

    # --- Funnel counts ---
    stage_counts = defaultdict(int)
    for entry in contacts.values():
        stage = entry.get("stage", "discovered")
        stage_counts[stage] += 1

    # Build funnel with cumulative "reached this stage" counts
    funnel = []
    for stage_key, label in FUNNEL_STAGES:
        # Count contacts that have reached at least this stage
        reached = _count_reached_stage(contacts, stage_key)
        funnel.append({
            "stage": stage_key,
            "label": label,
            "count": reached,
        })

    # Add conversion percentages
    for i, item in enumerate(funnel):
        if i == 0:
            item["conversion_pct"] = 100.0
        else:
            prev = funnel[i - 1]["count"]
            item["conversion_pct"] = round((item["count"] / prev * 100) if prev > 0 else 0, 1)

    # --- Average days between stages ---
    stage_durations = defaultdict(list)  # {transition: [days]}
    property_metrics = defaultdict(lambda: {
        "count": 0,
        "days_to_proposal": [],
        "days_to_close": [],
    })

    for entry in contacts.values():
        prop_type = entry.get("property_type", "other")
        property_metrics[prop_type]["count"] += 1

        # Discovery → Cold Sent
        d1 = entry.get("discovered_at")
        d2 = entry.get("first_outreach_at")
        if d1 and d2:
            days = _days_between(d1, d2)
            stage_durations["discovered_to_cold_sent"].append(days)

        # Cold Sent → Proposal
        d2 = entry.get("first_outreach_at")
        d3 = entry.get("proposal_sent_at")
        if d2 and d3:
            days = _days_between(d2, d3)
            stage_durations["cold_sent_to_proposal"].append(days)

        # Discovery → Proposal (total top-of-funnel time)
        if d1 and d3:
            days = _days_between(d1, d3)
            property_metrics[prop_type]["days_to_proposal"].append(days)

        # Proposal → Reply
        d3 = entry.get("proposal_sent_at")
        d4 = entry.get("replied_at")
        if d3 and d4:
            days = _days_between(d3, d4)
            stage_durations["proposal_to_reply"].append(days)

        # Proposal → Won
        d5 = entry.get("won_at")
        if d3 and d5:
            days = _days_between(d3, d5)
            stage_durations["proposal_to_won"].append(days)

        # Discovery → Won (full cycle)
        if d1 and d5:
            days = _days_between(d1, d5)
            property_metrics[prop_type]["days_to_close"].append(days)
            stage_durations["full_cycle"].append(days)

        # Time stuck (contacts still in pipeline — days since last meaningful timestamp)
        if not entry.get("completed") and not entry.get("won_at") and not entry.get("lost_at"):
            last_ts = (
                entry.get("last_touch_at")
                or entry.get("proposal_sent_at")
                or entry.get("first_outreach_at")
                or entry.get("discovered_at")
            )
            if last_ts:
                days_stuck = (now - _parse_iso(last_ts)).total_seconds() / 86400
                stage = entry.get("stage", "discovered")
                stage_durations[f"stuck_at_{stage}"].append(days_stuck)

    # Compute averages
    avg_days = {}
    for key, durations in stage_durations.items():
        if durations:
            avg_days[key] = round(sum(durations) / len(durations), 1)

    # --- By property type ---
    by_property = {}
    for prop_type, metrics in property_metrics.items():
        by_property[prop_type] = {
            "count": metrics["count"],
            "avg_days_to_proposal": _safe_avg(metrics["days_to_proposal"]),
            "avg_days_to_close": _safe_avg(metrics["days_to_close"]),
        }

    # --- Bottlenecks (stages where contacts are stuck longest) ---
    bottlenecks = []
    for key, avg in sorted(avg_days.items(), key=lambda x: x[1], reverse=True):
        if key.startswith("stuck_at_"):
            stage = key.replace("stuck_at_", "")
            count = len(stage_durations[key])
            if avg >= 7 and count >= 2:  # Only flag if 7+ days and 2+ contacts
                bottlenecks.append({
                    "stage": stage,
                    "avg_days_stuck": avg,
                    "contact_count": count,
                })

    active = sum(
        1 for e in contacts.values()
        if not e.get("completed") and not e.get("won_at") and not e.get("lost_at")
        and e.get("stage") != "unsubscribed"
    )

    return {
        "funnel": funnel,
        "avg_days": avg_days,
        "bottlenecks": bottlenecks[:5],
        "by_property_type": by_property,
        "active_contacts": active,
        "computed_at": now.isoformat(),
    }


def build_velocity_html(velocity: dict) -> str:
    """Build HTML section for the digest email."""
    funnel = velocity.get("funnel", [])
    avg_days = velocity.get("avg_days", {})
    bottlenecks = velocity.get("bottlenecks", [])
    by_property = velocity.get("by_property_type", {})

    html = """
    <div style="background: white; padding: 16px; border-radius: 6px; margin-bottom: 16px;">
        <h2 style="margin: 0 0 12px; font-size: 16px; color: #1a2b4a;">Pipeline Velocity</h2>
    """

    # Funnel visualization
    html += """<table style="width: 100%; border-collapse: collapse; font-size: 13px; margin-bottom: 12px;">
        <tr style="background: #f9fafb;">
            <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Stage</th>
            <th style="text-align: center; padding: 8px; border-bottom: 1px solid #e5e7eb;">Count</th>
            <th style="text-align: center; padding: 8px; border-bottom: 1px solid #e5e7eb;">Conversion</th>
            <th style="text-align: left; padding: 8px; border-bottom: 1px solid #e5e7eb;">Funnel</th>
        </tr>"""

    max_count = funnel[0]["count"] if funnel else 1
    for item in funnel:
        bar_width = int((item["count"] / max(max_count, 1)) * 100)
        conv_display = f"{item['conversion_pct']}%" if item["conversion_pct"] < 100 else ""
        color = "#16a34a" if item["stage"] == "won" else "#3b82f6"
        html += f"""
        <tr>
            <td style="padding: 8px; border-bottom: 1px solid #f3f4f6; font-weight: 500;">{item['label']}</td>
            <td style="padding: 8px; border-bottom: 1px solid #f3f4f6; text-align: center;">{item['count']}</td>
            <td style="padding: 8px; border-bottom: 1px solid #f3f4f6; text-align: center; color: #6b7280;">{conv_display}</td>
            <td style="padding: 8px; border-bottom: 1px solid #f3f4f6;">
                <div style="background: #e5e7eb; border-radius: 4px; height: 16px; width: 100%;">
                    <div style="background: {color}; border-radius: 4px; height: 16px; width: {bar_width}%;"></div>
                </div>
            </td>
        </tr>"""
    html += "</table>"

    # Key metrics
    metrics = []
    if "full_cycle" in avg_days:
        metrics.append(f"Avg days to close: <strong>{avg_days['full_cycle']}</strong>")
    if "proposal_to_reply" in avg_days:
        metrics.append(f"Avg days proposal → reply: <strong>{avg_days['proposal_to_reply']}</strong>")
    if "cold_sent_to_proposal" in avg_days:
        metrics.append(f"Avg days cold → proposal: <strong>{avg_days['cold_sent_to_proposal']}</strong>")

    if metrics:
        html += '<div style="font-size: 13px; color: #374151; margin-bottom: 12px;">'
        html += " &nbsp;|&nbsp; ".join(metrics)
        html += "</div>"

    # By property type
    if by_property:
        html += """<h3 style="margin: 12px 0 8px; font-size: 14px; color: #374151;">By Property Type</h3>
        <table style="width: 100%; border-collapse: collapse; font-size: 12px;">
            <tr style="background: #f9fafb;">
                <th style="text-align: left; padding: 6px;">Type</th>
                <th style="text-align: center; padding: 6px;">Contacts</th>
                <th style="text-align: center; padding: 6px;">Avg Days to Proposal</th>
                <th style="text-align: center; padding: 6px;">Avg Days to Close</th>
            </tr>"""
        for ptype, data in sorted(by_property.items(), key=lambda x: x[1]["count"], reverse=True):
            d2p = data["avg_days_to_proposal"] or "—"
            d2c = data["avg_days_to_close"] or "—"
            html += f"""
            <tr>
                <td style="padding: 6px; border-bottom: 1px solid #f3f4f6; text-transform: capitalize;">{ptype}</td>
                <td style="padding: 6px; border-bottom: 1px solid #f3f4f6; text-align: center;">{data['count']}</td>
                <td style="padding: 6px; border-bottom: 1px solid #f3f4f6; text-align: center;">{d2p}</td>
                <td style="padding: 6px; border-bottom: 1px solid #f3f4f6; text-align: center;">{d2c}</td>
            </tr>"""
        html += "</table>"

    # Bottlenecks
    if bottlenecks:
        html += """<div style="margin-top: 12px; padding: 10px; background: #fef3c7; border-radius: 6px; border-left: 4px solid #f59e0b;">
            <strong style="font-size: 13px; color: #92400e;">Bottlenecks</strong>
            <ul style="margin: 4px 0 0; padding-left: 20px; font-size: 12px; color: #78350f;">"""
        for b in bottlenecks:
            html += f"<li>{b['contact_count']} contacts stuck at <strong>{b['stage']}</strong> for avg {b['avg_days_stuck']} days</li>"
        html += "</ul></div>"

    html += "</div>"
    return html


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_state() -> dict:
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"contacts": {}}


def _days_between(iso1: str, iso2: str) -> float:
    """Days between two ISO timestamps."""
    dt1 = _parse_iso(iso1)
    dt2 = _parse_iso(iso2)
    return round((dt2 - dt1).total_seconds() / 86400, 1)


def _safe_avg(values: list) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 1)


def _count_reached_stage(contacts: dict, target_stage: str) -> int:
    """Count contacts that have reached at least the target stage."""
    # Define stage progression order
    stage_order = {
        "discovered": 0,
        "cold_drafted": 1,
        "cold_sent": 2,
        "proposal_sent": 3,
        "negotiating": 4,
        "won": 5,
    }

    target_rank = stage_order.get(target_stage, 0)
    count = 0

    for entry in contacts.values():
        stage = entry.get("stage", "discovered")
        stage_rank = stage_order.get(stage, 0)

        # Also check timestamps for stages that may have been passed through
        if stage_rank >= target_rank:
            count += 1
        elif target_stage == "cold_sent" and entry.get("first_outreach_at"):
            count += 1
        elif target_stage == "proposal_sent" and entry.get("proposal_sent_at"):
            count += 1
        elif target_stage == "negotiating" and entry.get("replied_at"):
            count += 1
        elif target_stage == "won" and entry.get("won_at"):
            count += 1

    return count
