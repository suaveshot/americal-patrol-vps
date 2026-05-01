"""
Americal Patrol — QBR Trend Analyzer

Computes trend metrics, comparisons, and AI-generated narrative sections
for the quarterly business review.
"""

import logging

import anthropic

log = logging.getLogger("qbr_generator")

DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def compute_trends(current_data, prior_data=None):
    """
    Compute trend analysis from aggregated quarter data.

    Returns dict of computed insights:
    {
        "incident_rate": float (incidents per patrol day),
        "incident_delta": int or None (change from prior quarter),
        "incident_delta_pct": float or None,
        "busiest_day": str or None (day of week with most incidents),
        "quietest_day": str or None,
        "incident_free_pct": float (% of days with no incidents),
        "longest_clean_streak": int,
        "coverage_score": str (e.g., "98%"),
    }
    """
    total = current_data["total_incidents"]
    days = current_data["total_patrol_days"] or 1
    incident_rate = total / days

    incident_free_days = days - total
    incident_free_pct = (incident_free_days / days) * 100 if days > 0 else 100

    # Day-of-week analysis
    dow = current_data["incidents_by_day_of_week"]
    busiest_day = None
    quietest_day = None
    if dow:
        busiest_idx = max(dow, key=dow.get)
        busiest_day = DOW_NAMES[busiest_idx]
        # Quietest = day with patrols but fewest incidents (including 0)
        all_days_with_data = set(range(7))
        min_incidents = min(dow.get(d, 0) for d in all_days_with_data)
        quietest_idx = next(d for d in all_days_with_data if dow.get(d, 0) == min_incidents)
        quietest_day = DOW_NAMES[quietest_idx]

    # Quarter-over-quarter comparison
    incident_delta = None
    incident_delta_pct = None
    if prior_data and prior_data["total_incidents"] is not None:
        prior_total = prior_data["total_incidents"]
        incident_delta = total - prior_total
        if prior_total > 0:
            incident_delta_pct = ((total - prior_total) / prior_total) * 100

    return {
        "incident_rate": round(incident_rate, 3),
        "incident_delta": incident_delta,
        "incident_delta_pct": round(incident_delta_pct, 1) if incident_delta_pct is not None else None,
        "busiest_day": busiest_day,
        "quietest_day": quietest_day,
        "incident_free_pct": round(incident_free_pct, 1),
        "longest_clean_streak": current_data["longest_clean_streak"],
        "coverage_score": f"{min(100, round(incident_free_pct))}%",
    }


def generate_narrative(current_data, trends, prior_data=None):
    """
    Use Claude to generate the narrative sections of the QBR:
    - Executive summary (2-3 sentences)
    - Recommendations (2-4 bullets)
    - Next quarter outlook (1-2 sentences)

    Returns dict with keys: executive_summary, recommendations, outlook
    """
    client = anthropic.Anthropic()
    property_names = ", ".join(current_data["property_names"])
    quarter = current_data["quarter_label"]

    # Build context for Claude
    context_parts = [
        f"Property: {property_names}",
        f"Quarter: {quarter}",
        f"Patrol days: {current_data['total_patrol_days']}",
        f"Total incidents: {current_data['total_incidents']}",
        f"Incident-free days: {trends['incident_free_pct']}%",
        f"Longest clean streak: {trends['longest_clean_streak']} days",
    ]

    if trends["busiest_day"]:
        context_parts.append(f"Most incidents on: {trends['busiest_day']}")

    if trends["incident_delta"] is not None:
        direction = "increase" if trends["incident_delta"] > 0 else "decrease"
        if trends["incident_delta"] == 0:
            direction = "no change"
        context_parts.append(
            f"Quarter-over-quarter: {trends['incident_delta']} ({direction})"
        )

    if current_data["incident_dates"]:
        context_parts.append(f"Incident dates: {', '.join(current_data['incident_dates'][:10])}")

    context = "\n".join(context_parts)

    prompt = f"""You are writing sections of a professional Quarterly Business Review (QBR)
for a security patrol company. The client is a property manager or business owner.

Data:
{context}

Write these three sections in a professional, confident tone. Be factual and data-driven.
Do not editorialize or use superlatives. Use specific numbers from the data.

1. EXECUTIVE SUMMARY (2-3 sentences): Overview of patrol performance this quarter.
Start with the property name and quarter.

2. RECOMMENDATIONS (2-4 bullet points): Actionable suggestions based on patterns.
If the property had zero incidents, focus on maintaining coverage and any value-adds.
If incidents occurred on specific days, suggest targeted coverage changes.

3. NEXT QUARTER OUTLOOK (1-2 sentences): Forward-looking note based on trends.

Format your response exactly as:
EXECUTIVE SUMMARY:
[text]

RECOMMENDATIONS:
- [bullet 1]
- [bullet 2]
- [bullet 3]

OUTLOOK:
[text]"""

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=600,
        messages=[{"role": "user", "content": prompt}],
    )

    response = msg.content[0].text.strip()

    # Parse sections
    sections = {"executive_summary": "", "recommendations": "", "outlook": ""}

    if "EXECUTIVE SUMMARY:" in response:
        parts = response.split("RECOMMENDATIONS:")
        sections["executive_summary"] = parts[0].replace("EXECUTIVE SUMMARY:", "").strip()
        if len(parts) > 1:
            rec_parts = parts[1].split("OUTLOOK:")
            sections["recommendations"] = rec_parts[0].strip()
            if len(rec_parts) > 1:
                sections["outlook"] = rec_parts[1].strip()
    else:
        sections["executive_summary"] = response

    return sections
