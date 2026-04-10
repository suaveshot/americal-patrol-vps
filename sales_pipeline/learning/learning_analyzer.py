"""
Sales Pipeline — Learning: Analyzer
Computes reply rates by attribute, feeds data to Claude for meta-analysis,
and generates a DO/AVOID guidance block written to insights.json.

Also incorporates win/loss analysis data for deal-closing pattern learning.

Cold-start mode: under 30 finalized outcomes, uses built-in best practices only.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone

import anthropic

from sales_pipeline.config import (
    ANTHROPIC_API_KEY,
    INSIGHTS_FILE,
    WIN_LOSS_LOG_FILE,
)
from sales_pipeline.learning.outcome_tracker import (
    get_finalized_outcomes,
    is_cold_start,
)

log = logging.getLogger(__name__)

# Best practices used during cold-start (before 30 outcomes)
COLD_START_GUIDANCE = {
    "reply_optimization": (
        "DO: Keep emails under 80 words for first touch. "
        "Use 2-4 word lowercase subject lines. "
        "Use a single, clear CTA — preferably a question. "
        "Reference something specific about their company. "
        "Send first touches on Tuesday mornings. "
        "Write conversationally, like a text to a business acquaintance.\n"
        "AVOID: HTML-heavy formatting for cold emails. "
        "Multiple CTAs. Subject lines over 6 words. "
        "Mentioning that they previously inquired or went cold. "
        "Generic openers like 'I hope this finds you well'."
    ),
    "deal_closing": "Not enough data yet — focus on getting replies first.",
    "updated_at": None,
    "outcome_count": 0,
    "overall_reply_rate": None,
}


def _compute_reply_rates(outcomes: list) -> dict:
    """Compute reply rates by each attribute dimension."""
    dimensions = [
        "subject_style", "cta_type", "opening_style",
        "word_count_bucket", "channel", "send_day",
        "property_type", "phase", "touch_number",
    ]

    rates = {}
    for dim in dimensions:
        groups = defaultdict(lambda: {"sent": 0, "replied": 0})
        for o in outcomes:
            key = str(o.get(dim, "unknown"))
            groups[key]["sent"] += 1
            if o.get("outcome") == "replied":
                groups[key]["replied"] += 1

        rates[dim] = {}
        for key, counts in groups.items():
            rate = (counts["replied"] / counts["sent"] * 100) if counts["sent"] else 0
            rates[dim][key] = {
                "sent": counts["sent"],
                "replied": counts["replied"],
                "rate": round(rate, 1),
            }

    # Overall
    total = len(outcomes)
    replied = sum(1 for o in outcomes if o.get("outcome") == "replied")
    rates["overall"] = {
        "sent": total,
        "replied": replied,
        "rate": round(replied / total * 100, 1) if total else 0,
    }

    return rates


def _load_win_loss_data() -> list:
    """Load win/loss analysis records."""
    if not WIN_LOSS_LOG_FILE.exists():
        return []
    results = []
    for line in WIN_LOSS_LOG_FILE.read_text(encoding="utf-8").strip().split("\n"):
        if line.strip():
            results.append(json.loads(line))
    return results


def _build_analysis_prompt(rates: dict, win_loss_data: list) -> str:
    """Build the Claude prompt for meta-analysis."""
    prompt = """You are analyzing outreach performance data for Americal Patrol, a security patrol company.
Your job is to identify what's working and what isn't, then generate actionable guidance.

## Reply Rate Data by Dimension

"""
    for dim, values in rates.items():
        if dim == "overall":
            prompt += f"**Overall:** {values['sent']} sent, {values['replied']} replied ({values['rate']}%)\n\n"
            continue
        prompt += f"**{dim.replace('_', ' ').title()}:**\n"
        for key, data in sorted(values.items(), key=lambda x: -x[1]["rate"]):
            prompt += f"  - {key}: {data['sent']} sent, {data['replied']} replied ({data['rate']}%)\n"
        prompt += "\n"

    if win_loss_data:
        prompt += "## Win/Loss Analysis Data\n\n"
        wins = [w for w in win_loss_data if w.get("outcome") == "won"]
        losses = [w for w in win_loss_data if w.get("outcome") == "lost"]

        if wins:
            prompt += f"**Wins ({len(wins)}):**\n"
            for w in wins[-5:]:  # Last 5 wins
                prompt += f"  - {w.get('property_type', '?')}, ${w.get('deal_value', '?')}, "
                prompt += f"closed in {w.get('days_to_close', '?')} days\n"
                if w.get("winning_patterns"):
                    prompt += f"    Patterns: {', '.join(w['winning_patterns'][:3])}\n"
                if w.get("key_message_that_worked"):
                    prompt += f"    Key message: {w['key_message_that_worked'][:100]}\n"

        if losses:
            prompt += f"\n**Losses ({len(losses)}):**\n"
            for w in losses[-5:]:
                prompt += f"  - {w.get('property_type', '?')}, "
                if w.get("losing_patterns"):
                    prompt += f"Patterns: {', '.join(w['losing_patterns'][:3])}\n"
                if w.get("objections_raised"):
                    prompt += f"    Objections: {', '.join(w['objections_raised'])}\n"

    prompt += """
## Instructions

Based on this data, generate a concise guidance block (200-300 words max) with two sections:

**REPLY OPTIMIZATION:**
List specific DO and AVOID rules based on the reply rate data. Be concrete — reference specific subject styles, word counts, CTAs, send days, and channels that performed above or below average. Only include patterns with enough data (3+ sends) to be meaningful.

**DEAL CLOSING:**
If win/loss data exists, list patterns from won deals that should be emphasized and patterns from lost deals to avoid. Reference specific objection handling approaches, messaging styles, or timing patterns that correlated with wins.

If there isn't enough win/loss data yet, say "Collecting data — focus on reply optimization for now."

Output ONLY the guidance text, no JSON or markdown headers. Start directly with "DO:" for each section.
"""
    return prompt


def run_analysis() -> dict:
    """
    Run the full learning analysis cycle.
    Returns the insights dict that was written to insights.json.
    """
    if is_cold_start():
        log.info("Learning system in cold-start mode (%d outcomes, need %d)",
                 len(get_finalized_outcomes()), 30)
        # Write cold-start defaults
        _save_insights(COLD_START_GUIDANCE)
        return COLD_START_GUIDANCE

    outcomes = get_finalized_outcomes()
    rates = _compute_reply_rates(outcomes)
    win_loss_data = _load_win_loss_data()

    prompt = _build_analysis_prompt(rates, win_loss_data)

    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY())
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=800,
            messages=[{"role": "user", "content": prompt}],
        )
        guidance_text = response.content[0].text.strip()
    except Exception as e:
        log.error("Learning analysis Claude call failed: %s", e)
        return _load_insights()

    # Split into sections
    reply_section = guidance_text
    deal_section = "Collecting data — focus on reply optimization for now."

    if "DEAL CLOSING:" in guidance_text:
        parts = guidance_text.split("DEAL CLOSING:", 1)
        reply_section = parts[0].replace("REPLY OPTIMIZATION:", "").strip()
        deal_section = parts[1].strip()
    elif "REPLY OPTIMIZATION:" in guidance_text:
        reply_section = guidance_text.split("REPLY OPTIMIZATION:", 1)[1].strip()

    insights = {
        "reply_optimization": reply_section,
        "deal_closing": deal_section,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "outcome_count": len(outcomes),
        "overall_reply_rate": rates.get("overall", {}).get("rate"),
        "top_patterns": _extract_top_patterns(rates),
    }

    _save_insights(insights)
    log.info("Learning analysis complete: %d outcomes analyzed, %.1f%% reply rate",
             len(outcomes), insights.get("overall_reply_rate", 0))

    return insights


def _extract_top_patterns(rates: dict) -> dict:
    """Extract the highest and lowest performing patterns for digest display."""
    top = {}
    for dim in ["subject_style", "cta_type", "channel", "send_day"]:
        if dim not in rates:
            continue
        sorted_items = sorted(rates[dim].items(), key=lambda x: -x[1]["rate"])
        # Only include patterns with 3+ sends
        meaningful = [(k, v) for k, v in sorted_items if v["sent"] >= 3]
        if meaningful:
            best_k, best_v = meaningful[0]
            top[dim] = {
                "best": best_k,
                "best_rate": best_v["rate"],
                "worst": meaningful[-1][0] if len(meaningful) > 1 else None,
                "worst_rate": meaningful[-1][1]["rate"] if len(meaningful) > 1 else None,
            }
    return top


def _save_insights(insights: dict) -> None:
    """Write insights to JSON file."""
    tmp = INSIGHTS_FILE.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(insights, f, indent=2)
    tmp.replace(INSIGHTS_FILE)


def _load_insights() -> dict:
    """Load current insights, or return cold-start defaults."""
    if not INSIGHTS_FILE.exists():
        return COLD_START_GUIDANCE
    try:
        with open(INSIGHTS_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return COLD_START_GUIDANCE


def load_insights() -> dict:
    """Public accessor — load current insights for prompt injection."""
    return _load_insights()


def get_prompt_guidance() -> str:
    """
    Build the guidance block to inject into Claude system prompts.
    Compact format, designed to add ~300 tokens.
    """
    insights = load_insights()

    sections = []

    reply_opt = insights.get("reply_optimization", "")
    if reply_opt:
        sections.append(f"REPLY OPTIMIZATION (from {insights.get('outcome_count', 0)} sends):\n{reply_opt}")

    deal_closing = insights.get("deal_closing", "")
    if deal_closing and deal_closing != "Not enough data yet — focus on getting replies first.":
        sections.append(f"DEAL CLOSING INTELLIGENCE:\n{deal_closing}")

    if not sections:
        return ""

    return "\n\nLEARNING SYSTEM INSIGHTS — follow these data-driven rules:\n" + "\n\n".join(sections)
