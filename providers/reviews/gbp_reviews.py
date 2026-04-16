"""
Google Business Profile Review Provider
Fetches reviews and posts responses via GBP API.
Reuses OAuth credentials from gbp_automation/.
"""

import json
import logging
import os
from pathlib import Path

import requests

from providers.base import ReviewProvider

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

_STAR_MAP = {
    "ONE": 1, "TWO": 2, "THREE": 3, "FOUR": 4, "FIVE": 5,
    "STAR_RATING_UNSPECIFIED": 0,
}

GBP_API_BASE = "https://mybusiness.googleapis.com/v4"


class GBPReviewProvider(ReviewProvider):

    def __init__(self, config: dict):
        self.account_id = config.get("account_id", "")
        self.location_id = config.get("location_id", "")
        self._token_path = _PROJECT_ROOT / config.get(
            "token_path", "gbp_automation/gbp_token.json"
        )

    def _get_access_token(self) -> str:
        if not self._token_path.exists():
            raise FileNotFoundError(
                f"GBP token not found at {self._token_path}. "
                "Run gbp_automation auth setup first."
            )
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request
        creds = Credentials.from_authorized_user_file(str(self._token_path))
        if creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(self._token_path, "w") as f:
                f.write(creds.to_json())
        return creds.token

    def _location_path(self) -> str:
        return f"accounts/{self.account_id}/locations/{self.location_id}"

    def get_reviews(self, since: str = "", limit: int = 50) -> list[dict]:
        token = self._get_access_token()
        url = f"{GBP_API_BASE}/{self._location_path()}/reviews"
        params = {"pageSize": min(limit, 50)}

        r = requests.get(url, headers={"Authorization": f"Bearer {token}"}, params=params)

        if r.status_code != 200:
            log.error("GBP reviews fetch failed: %s %s", r.status_code, r.text[:200])
            return []

        data = r.json()
        reviews = []

        for rev in data.get("reviews", []):
            star_str = rev.get("starRating", "STAR_RATING_UNSPECIFIED")
            reply = rev.get("reviewReply")

            review = {
                "id": rev.get("reviewId", ""),
                "reviewer_name": rev.get("reviewer", {}).get("displayName", "Anonymous"),
                "star_rating": _STAR_MAP.get(star_str, 0),
                "text": rev.get("comment", ""),
                "timestamp": rev.get("createTime", ""),
                "responded": reply is not None,
                "response_text": reply.get("comment", "") if reply else None,
            }
            reviews.append(review)

        return reviews

    def post_response(self, review_id: str, response_text: str) -> dict:
        token = self._get_access_token()
        url = f"{GBP_API_BASE}/{self._location_path()}/reviews/{review_id}/reply"

        r = requests.put(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"comment": response_text},
        )

        if r.status_code == 200:
            return {"success": True}
        else:
            log.error("GBP review reply failed: %s %s", r.status_code, r.text[:200])
            return {"success": False, "error": r.text[:200]}
