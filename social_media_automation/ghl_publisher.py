"""
Americal Patrol — GoHighLevel Social Media Publisher
Publishes posts to Facebook, Instagram, and LinkedIn via GHL's Social Media Posting API.

All social accounts must be connected in GHL first.
Run ghl_social_setup.py to discover and configure account IDs.

Usage:
    # Called automatically by run_social.py during the pipeline.
    from ghl_publisher import publish_post, get_post_details
"""

import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

import requests

GHL_BASE_URL = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"


def _headers() -> dict:
    api_key = os.environ.get("GHL_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GHL_API_KEY not set in .env")
    return {
        "Authorization": f"Bearer {api_key}",
        "Version": GHL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _location_id() -> str:
    loc = os.environ.get("GHL_LOCATION_ID", "").strip()
    if not loc:
        raise RuntimeError("GHL_LOCATION_ID not set in .env")
    return loc


def publish_post(platform: str, text: str, image_path: Path | None,
                 config: dict) -> str:
    """
    Publish a social media post through GoHighLevel.

    Args:
        platform: "facebook", "instagram", or "linkedin"
        text: Post text content (including hashtags for Instagram)
        image_path: Path to local image file, or None for text-only
        config: Full social_config.json dict

    Returns:
        GHL post ID string

    Raises:
        RuntimeError on missing config or API failure
    """
    platform_cfg = config.get("platforms", {}).get(platform, {})
    account_id = platform_cfg.get("ghl_account_id", "").strip()

    if not account_id:
        raise RuntimeError(
            f"ghl_account_id not set for {platform} in social_config.json. "
            f"Run: python ghl_social_setup.py"
        )

    payload = {
        "locationId": _location_id(),
        "accountIds": [account_id],
        "type": "post",
        "post": text,
        "status": "published",
    }

    # If there's an image, upload to Drive for a public URL
    if image_path and image_path.exists():
        from drive_photos import upload_for_public_url
        public_url = upload_for_public_url(image_path)
        if public_url:
            payload["mediaUrls"] = [public_url]

    resp = requests.post(
        f"{GHL_BASE_URL}/social-media-posting/post",
        headers=_headers(),
        json=payload,
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f"GHL Social API error {resp.status_code}: {resp.text[:300]}"
        )

    data = resp.json()
    post_id = data.get("id") or data.get("postId") or str(data)
    return post_id


def get_post_details(post_id: str) -> dict:
    """
    Fetch post details (including any available metrics) from GHL.

    Args:
        post_id: The GHL post ID returned by publish_post()

    Returns:
        Dict with post details, or {"error": ...} on failure
    """
    try:
        resp = requests.get(
            f"{GHL_BASE_URL}/social-media-posting/post/{post_id}",
            headers=_headers(),
            params={"locationId": _location_id()},
            timeout=15,
        )
        if resp.status_code == 200:
            return resp.json()
        return {"error": f"GHL API {resp.status_code}: {resp.text[:200]}"}
    except Exception as e:
        return {"error": str(e)}
