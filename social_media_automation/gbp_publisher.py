"""
Americal Patrol — GBP Post Publisher (Social Pipeline Integration)
Publishes "What's New" posts to Google Business Profile via the legacy v4 API.

GBP posting was moved into the social media pipeline so all content publishing
runs from a single automation. Auth still uses gbp_automation/auth_setup.py
and gbp_automation/gbp_token.json.
"""

import sys
from pathlib import Path

# Allow importing auth_setup from gbp_automation/
_GBP_DIR = Path(__file__).resolve().parent.parent / "gbp_automation"
if str(_GBP_DIR) not in sys.path:
    sys.path.insert(0, str(_GBP_DIR))

from google.auth.transport.requests import AuthorizedSession
from auth_setup import get_credentials

POSTS_BASE_URL = "https://mybusiness.googleapis.com/v4"


def publish_post(text: str, config: dict, log=None) -> str:
    """
    Publish a What's New post to Google Business Profile.

    Args:
        text: Post body (plain text, max 1500 chars).
        config: Full social_config.json dict.
        log: Optional logging function.

    Returns:
        Post resource name (e.g. 'accounts/.../locations/.../localPosts/...').

    Raises:
        ValueError: If account_id or location_id not configured.
        RuntimeError: If GBP API returns an error.
    """
    gbp_config  = config.get("platforms", {}).get("gbp", {})
    account_id  = gbp_config.get("account_id", "").strip()
    location_id = gbp_config.get("location_id", "").strip()
    cta_url     = gbp_config.get("post_cta_url", "https://americalpatrol.com/contact-us")

    if not account_id or not location_id:
        raise ValueError(
            "GBP account_id or location_id not set in social_config.json. "
            "Run: cd gbp_automation && python account_fetcher.py --list"
        )

    # Enforce the 1500-char GBP limit
    if len(text) > 1500:
        text = text[:1497] + "..."

    payload = {
        "languageCode": "en-US",
        "summary":      text,
        "callToAction": {
            "actionType": "LEARN_MORE",
            "url":        cta_url,
        },
        "topicType": "STANDARD",
    }

    session       = AuthorizedSession(get_credentials())
    location_path = f"{account_id}/{location_id}"
    url           = f"{POSTS_BASE_URL}/{location_path}/localPosts"
    resp          = session.post(url, json=payload)

    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"GBP post publish failed: HTTP {resp.status_code} — {resp.text[:400]}"
        )

    post_name = resp.json().get("name", "")
    if log:
        log(f"GBP post published: {post_name}")
    return post_name
