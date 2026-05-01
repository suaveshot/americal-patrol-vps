"""
Americal Patrol - GoHighLevel Blog Publisher
Publishes generated blog posts to GoHighLevel via the v2 REST API.

Usage:
  # One-time setup — fetch and print your Blog Channel ID and Author ID:
  python ghl_publisher.py --fetch-ids

  # Called automatically by run_blog.py during the weekly pipeline.
"""

import argparse
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'blog_config.json'

GHL_BASE_URL = "https://services.leadconnectorhq.com"
GHL_VERSION  = "2021-04-15"


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _headers(api_key: str) -> dict:
    return {
        "Authorization": f"Bearer {api_key}",
        "Version":       GHL_VERSION,
        "Content-Type":  "application/json",
        "Accept":        "application/json",
    }


def fetch_ghl_ids() -> None:
    """
    One-time setup helper.
    Fetches your GHL User ID via the API and prints step-by-step
    instructions for finding your Blog Channel ID in the dashboard.
    """
    api_key     = os.environ.get('GHL_API_KEY', '')
    location_id = os.environ.get('GHL_LOCATION_ID', '')

    if not api_key:
        print("ERROR: GHL_API_KEY not set in .env file.")
        sys.exit(1)

    hdrs = _headers(api_key)

    # ── Fetch User ID via API ─────────────────────────────────────
    print("\nFetching your GHL User ID...")
    r = requests.get(
        f"{GHL_BASE_URL}/users/search",
        headers=hdrs,
        params={"locationId": location_id}
    )
    if r.status_code == 200:
        data  = r.json()
        users = data if isinstance(data, list) else data.get('users', [])
        if users:
            print("\n=== YOUR GHL USERS ===")
            for u in users:
                print(f"  ID: {u.get('id')}  |  Name: {u.get('name')}  |  Email: {u.get('email')}")
            print(f"\n  --> Copy your User ID above into blog_config.json as \"author_id\"")
        else:
            print(f"  No users returned. Raw response: {data}")
    else:
        print(f"  Could not fetch users automatically ({r.status_code}: {r.text})")
        print("  --> Find your User ID manually: GHL → Settings → My Profile → scroll to bottom for User ID")

    # ── Blog Channel ID — must be found in the dashboard URL ─────
    print("\n=== HOW TO FIND YOUR BLOG CHANNEL ID ===")
    print("  GHL does not expose this via API. Follow these steps:")
    print("  1. Log into GoHighLevel and go to your sub-account")
    print("  2. Click 'Sites' in the left menu")
    print("  3. Click 'Blogs' at the top")
    print("  4. Look at the URL in your browser — it will look like:")
    print("     https://app.gohighlevel.com/location/XXXX/blogs/CHANNEL_ID_HERE/posts")
    print("  5. Copy that CHANNEL_ID_HERE value")
    print("  --> Paste it into blog_config.json as \"ghl_blog_channel_id\"")

    print("\n--- SUMMARY: paste these two values into blog_config.json ---")
    print('  "ghl_blog_channel_id": "<from the URL in step 4 above>",')
    print('  "author_id":           "<from the User ID printed above>",')
    print("")


def publish_post(post: dict) -> str:
    """
    Publish a blog post to GoHighLevel.

    Args:
        post: dict with keys title, slug, meta_description, html_content

    Returns:
        The GHL post ID string on success.

    Raises:
        RuntimeError on API failure.
    """
    api_key         = os.environ.get('GHL_API_KEY', '')
    location_id     = os.environ.get('GHL_LOCATION_ID', '')
    blog_channel_id = os.environ.get('GHL_BLOG_CHANNEL_ID', '')
    author_id       = os.environ.get('GHL_AUTHOR_ID', '')
    category_id     = os.environ.get('GHL_CATEGORY_ID', '')

    for field, val in [
        ('GHL_API_KEY',         api_key),
        ('GHL_LOCATION_ID',     location_id),
        ('GHL_BLOG_CHANNEL_ID', blog_channel_id),
    ]:
        if not val:
            raise RuntimeError(
                f"Environment variable '{field}' is not set. Check your .env file."
            )

    payload = {
        "locationId":  location_id,
        "blogId":      blog_channel_id,
        "title":       post['title'],
        "description": post['meta_description'],
        "rawHTML":     post['html_content'],
        "status":      "PUBLISHED",
    }

    r = requests.post(
        f"{GHL_BASE_URL}/blogs/posts",
        headers=_headers(api_key),
        json=payload,
        timeout=30,
    )

    if r.status_code not in (200, 201):
        raise RuntimeError(
            f"GHL API error {r.status_code}: {r.text}"
        )

    data    = r.json()
    post_id = data.get('id') or data.get('postId') or str(data)
    return post_id


def fetch_blog_settings() -> None:
    """Fetch author IDs and category IDs from GHL blog API."""
    api_key         = os.environ.get('GHL_API_KEY', '')
    location_id     = os.environ.get('GHL_LOCATION_ID', '')
    blog_channel_id = os.environ.get('GHL_BLOG_CHANNEL_ID', '')

    if not api_key or not blog_channel_id:
        print("ERROR: GHL_API_KEY and GHL_BLOG_CHANNEL_ID must be set in .env file.")
        sys.exit(1)

    hdrs = _headers(api_key)

    params = {"locationId": location_id, "limit": 10, "offset": 0}

    # ── Authors ───────────────────────────────────────────────────
    print("\nFetching blog authors...")
    r = requests.get(f"{GHL_BASE_URL}/blogs/authors", headers=hdrs, params=params)
    if r.status_code == 200:
        data    = r.json()
        authors = data if isinstance(data, list) else data.get('authors', data.get('data', []))
        if authors:
            print("\n=== AUTHORS ===")
            for a in authors:
                print(f"  ID: {a.get('id') or a.get('_id')}  |  Name: {a.get('name')}")
            print('  --> Copy your ID into .env as GHL_AUTHOR_ID=...')
        else:
            print(f"  No authors found. Raw: {data}")
    else:
        print(f"  ERROR {r.status_code}: {r.text}")

    # ── Categories ────────────────────────────────────────────────
    print("\nFetching blog categories...")
    r = requests.get(f"{GHL_BASE_URL}/blogs/categories", headers=hdrs, params=params)
    if r.status_code == 200:
        data       = r.json()
        categories = data if isinstance(data, list) else data.get('categories', data.get('data', []))
        if categories:
            print("\n=== CATEGORIES ===")
            for c in categories:
                print(f"  ID: {c.get('id') or c.get('_id')}  |  Label: {c.get('label') or c.get('name')}")
            print('  --> Copy your ID into .env as GHL_CATEGORY_ID=...')
        else:
            print(f"  No categories found. Raw: {data}")
    else:
        print(f"  ERROR {r.status_code}: {r.text}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="GoHighLevel Blog Publisher")
    parser.add_argument('--fetch-ids', action='store_true',
                        help='Fetch User ID (legacy setup helper)')
    parser.add_argument('--fetch-blog-settings', action='store_true',
                        help='Fetch author IDs and category IDs from your blog channel')
    args = parser.parse_args()

    if args.fetch_blog_settings:
        fetch_blog_settings()
    elif args.fetch_ids:
        fetch_ghl_ids()
    else:
        parser.print_help()
