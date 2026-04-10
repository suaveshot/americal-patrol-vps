"""
Call Intelligence — Claude API Call Analysis Engine
Analyzes each call transcript for sales metrics, methodology, and coaching insights.
"""

import json
import logging
import re

import anthropic

from call_intelligence.config import ANTHROPIC_API_KEY

log = logging.getLogger(__name__)

ANALYSIS_PROMPT = """You are an expert sales coach analyzing a call transcript for Americal Patrol, a security patrol company in Oxnard, CA. Sam Alarcon is the salesperson. Analyze this transcript and return ONLY a JSON object with no additional text.

CONTEXT: Sam sells patrol security services (routes, guards, cameras) to HOAs, commercial property managers, warehouses, and retail. His target talk-to-listen ratio is 43%. Competitor names may include Allied, ADT, GardaWorld, Guardsmark, Patrol One, or local patrol companies.

SPEAKER INFERENCE RULES:
- Sam typically: introduces himself and Americal Patrol, asks questions, uses "we" for AP services, discusses pricing/contracts
- The prospect typically: describes their property, answers questions, raises objections, asks about pricing
- When unclear, infer from conversational role (questioner vs. answerer)

TRANSCRIPT:
{transcript}

CONTACT INFO:
Name: {contact_name}
Company: {company_name}
Direction: {direction} (inbound = they called us, outbound = Sam called them)
Duration: {duration_seconds} seconds

Return a JSON object with EXACTLY this structure:

{{
  "call_type": "<discovery|follow_up|proposal|objection_handling|closing|service|general>",
  "methodology_detected": "<spin|sandler|challenger|solution_selling|mixed|none>",

  "scores": {{
    "composite_score": 0,
    "talk_listen_ratio": 0.0,
    "question_count": 0,
    "longest_monologue_seconds": 0,
    "conversation_switches": 0,
    "filler_word_count": 0,
    "filler_words_per_minute": 0.0,
    "next_steps_defined": false,
    "discovery_completeness": 0.0,
    "sentiment_start": 0.0,
    "sentiment_end": 0.0,
    "sentiment_trajectory": "stable"
  }},

  "composite_score_reasoning": "",

  "questions_asked": [],
  "objections_raised": [],
  "objection_responses": [],
  "techniques_used": [],
  "competitor_mentions": [],
  "buying_signals": [],
  "disinterest_signals": [],
  "key_topics": [],
  "outcome_prediction": "needs_followup",
  "coachable_moments": []
}}

Field details:
- talk_listen_ratio: float 0.0-1.0, Sam's share of total words
- question_count: integer, questions Sam asked
- longest_monologue_seconds: estimated from word count at ~140 words/min
- conversation_switches: total speaker changes in the call
- filler_word_count: count of um, uh, like, you know, basically, actually, sort of
- next_steps_defined: true if call ended with a concrete next action
- discovery_completeness: 0.0-1.0, coverage of budget/timeline/authority/need
- sentiment_start/end: -1.0 to 1.0, prospect sentiment
- questions_asked: array of verbatim questions Sam asked
- objection_responses: array of {{"objection": "...", "response": "...", "effectiveness": "strong|adequate|weak"}}
- techniques_used: array from [rapport_building, urgency, social_proof, scarcity, authority, pain_funnel, future_pacing, assumptive_close, trial_close]
- competitor_mentions: array of {{"competitor": "...", "context": "...", "sam_response": "..."}}
- coachable_moments: array of {{"type": "missed_question|weak_objection_response|excessive_talking|filler_words|missed_close|good_technique", "description": "...", "segment_text": "..."}}

Composite score rubric (0-100):
- Talk ratio near 43%: +15 (penalize heavily above 60%)
- 11-14 questions asked: +15
- Next steps defined: +10
- Discovery completeness > 0.7: +15
- Sentiment trajectory improving: +10
- No competitor mentions unaddressed: +5
- Monologue < 150s: +10
- Filler words < 5/min: +10
- Strong objection handling: +10

For calls under 60 seconds or with fewer than 20 words, return minimal analysis with composite_score = 0 and a coachable_moments entry noting "insufficient transcript"."""


def analyze_call(transcript: str, contact_name: str, company_name: str,
                 direction: str, duration_seconds: int,
                 model_name: str = "claude-sonnet-4-6") -> dict | None:
    """
    Analyze a call transcript using Claude API.
    Returns dict with 'scores' and top-level analysis fields, or None on failure.
    """
    api_key = ANTHROPIC_API_KEY()
    if not api_key:
        log.error("ANTHROPIC_API_KEY not set, skipping analysis")
        return None

    if not transcript or len(transcript.split()) < 5:
        log.info("Transcript too short for analysis (%d words)", len(transcript.split()))
        return None

    prompt = ANALYSIS_PROMPT.format(
        transcript=transcript[:8000],
        contact_name=contact_name or "Unknown",
        company_name=company_name or "Unknown",
        direction=direction or "unknown",
        duration_seconds=duration_seconds or 0,
    )

    client = anthropic.Anthropic(api_key=api_key)

    for attempt in range(3):
        try:
            messages = [{"role": "user", "content": prompt}]
            if attempt == 2:
                # Last attempt: force JSON start
                messages.append({"role": "assistant", "content": "{"})

            response = client.messages.create(
                model=model_name,
                max_tokens=3000,
                messages=messages,
            )

            raw = response.content[0].text
            if attempt == 2:
                raw = "{" + raw

            result = _parse_analysis_response(raw)
            log.info("Call analysis complete: score=%.0f, type=%s",
                     result.get("scores", {}).get("composite_score", 0),
                     result.get("call_type", "unknown"))
            return result

        except (json.JSONDecodeError, ValueError) as e:
            log.warning("JSON parse failed (attempt %d/3): %s", attempt + 1, e)
            if attempt < 2:
                continue
            log.error("Analysis JSON parse failed after all retries")
            return None
        except anthropic.RateLimitError:
            wait = 5 * (attempt + 1)
            log.warning("Claude rate limited, waiting %ds (attempt %d/3)", wait, attempt + 1)
            import time
            time.sleep(wait)
            continue
        except (anthropic.APIConnectionError, anthropic.APITimeoutError) as e:
            wait = 3 * (attempt + 1)
            log.warning("Claude API transient error (%s), retrying in %ds", e, wait)
            import time
            time.sleep(wait)
            continue
        except Exception as e:
            log.error("Claude API call failed: %s", e)
            return None

    return None


def _parse_analysis_response(raw_text: str) -> dict:
    """Parse Claude's JSON response. Strips markdown fences if present."""
    text = raw_text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    data = json.loads(text)

    required = ["call_type", "scores", "questions_asked", "outcome_prediction"]
    for key in required:
        if key not in data:
            raise ValueError(f"Missing required key: {key}")

    return data
