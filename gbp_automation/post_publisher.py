"""
Americal Patrol - GBP Post Publisher
Creates "What's New" posts on Google Business Profile.

NOTE: The Posts API uses the legacy v4 endpoint (mybusiness.googleapis.com/v4).
This is intentional — the newer Business Information API does not handle posts.
Both endpoints use the same OAuth token (business.manage scope).
"""

import json
from pathlib import Path

from google.auth.transport.requests import AuthorizedSession

from auth_setup import get_credentials

SCRIPT_DIR     = Path(__file__).parent
CONFIG_FILE    = SCRIPT_DIR / 'gbp_config.json'
POSTS_BASE_URL = 'https://mybusiness.googleapis.com/v4'


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _authed_session() -> AuthorizedSession:
    return AuthorizedSession(get_credentials())


def create_whats_new_post(summary: str, log=None) -> str:
    """
    Publishes a What's New post to GBP.
    Returns the post resource name (e.g. 'accounts/.../locations/.../localPosts/...').
    Raises RuntimeError on API failure.
    """
    config      = _load_config()
    account_id  = config.get('account_id', '').strip()
    location_id = config.get('location_id', '').strip()
    cta_url     = config.get('post_cta_url', 'https://americalpatrol.com/contact-us')

    if not account_id or not location_id:
        raise ValueError('account_id or location_id not set in gbp_config.json')

    # Enforce the 1500-char GBP limit before sending
    if len(summary) > 1500:
        summary = summary[:1497] + '...'

    payload = {
        'languageCode': 'en-US',
        'summary':      summary,
        'callToAction': {
            'actionType': 'LEARN_MORE',
            'url':        cta_url,
        },
        'topicType': 'STANDARD',
    }

    session       = _authed_session()
    location_path = f'{account_id}/{location_id}'
    url           = f'{POSTS_BASE_URL}/{location_path}/localPosts'
    resp          = session.post(url, json=payload)

    if resp.status_code not in (200, 201):
        raise RuntimeError(
            f'GBP post publish failed: HTTP {resp.status_code} — {resp.text[:400]}'
        )

    post_name = resp.json().get('name', '')
    if log:
        log(f'GBP post published: {post_name}')
    return post_name


def list_recent_posts(log=None) -> list:
    """Returns up to 5 recent posts for the listing (used for diagnostics)."""
    config      = _load_config()
    account_id  = config.get('account_id', '').strip()
    location_id = config.get('location_id', '').strip()

    if not account_id or not location_id:
        return []

    session       = _authed_session()
    location_path = f'{account_id}/{location_id}'
    resp          = session.get(
        f'{POSTS_BASE_URL}/{location_path}/localPosts',
        params={'pageSize': 5}
    )

    if resp.status_code != 200:
        if log: log(f'WARNING: Could not fetch recent posts: HTTP {resp.status_code}')
        return []

    return resp.json().get('localPosts', [])
