"""
Review Responder
Generates AI-powered responses to Google reviews using Claude.
Can auto-post responses or create drafts for approval.
"""

import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_PROJECT_ROOT))

from shared_utils.usage_tracker import tracked_create
from review_engine.response_templates import get_response_prompt

log = logging.getLogger(__name__)


def should_respond(review: dict, respond_to_stars: list[int] = None) -> bool:
    if review.get("responded", False):
        return False
    if respond_to_stars and review.get("star_rating", 0) not in respond_to_stars:
        return False
    return True


def is_negative(review: dict, threshold: int = 2) -> bool:
    return review.get("star_rating", 0) <= threshold


def generate_response(
    review: dict,
    company_name: str,
    company_phone: str,
    tone: str = "professional_warm",
    company_description: str = "",
    client_id: str = "",
) -> str:
    system_prompt = get_response_prompt(
        star_rating=review.get("star_rating", 3),
        reviewer_name=review.get("reviewer_name", "there"),
        review_text=review.get("text", ""),
        company_name=company_name,
        company_phone=company_phone,
        tone=tone,
        company_description=company_description,
    )

    response = tracked_create(
        model="claude-haiku-4-5-20251001",
        max_tokens=300,
        system=system_prompt,
        messages=[{"role": "user", "content": "Write the review response."}],
        pipeline="review_engine",
        client_id=client_id or "unknown",
    )

    return response.content[0].text.strip()
