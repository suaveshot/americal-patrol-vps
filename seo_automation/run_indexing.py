"""
Americal Patrol — Google Indexing API Submitter
Reads all URLs from americalpatrol.com/sitemap.xml and submits each one
to the Google Indexing API using the service account credentials.

Usage:
  python run_indexing.py              # submit all URLs from sitemap
  python run_indexing.py --url https://americalpatrol.com/page  # single URL
"""

import argparse
import json
import time
from pathlib import Path
from xml.etree import ElementTree

import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request as GoogleRequest

SCRIPT_DIR       = Path(__file__).parent
SA_KEY_FILE      = SCRIPT_DIR / "indexing_service_account.json"
SITEMAP_URL      = "https://americalpatrol.com/sitemap.xml"
INDEXING_API_URL = "https://indexing.googleapis.com/v3/urlNotifications:publish"
SCOPES           = ["https://www.googleapis.com/auth/indexing"]

# Indexing API allows 200 requests/day per project
BATCH_DELAY_SECONDS = 1


def get_credentials():
    creds = service_account.Credentials.from_service_account_file(
        str(SA_KEY_FILE), scopes=SCOPES
    )
    creds.refresh(GoogleRequest())
    return creds


def get_access_token(creds):
    if not creds.valid:
        creds.refresh(GoogleRequest())
    return creds.token


def fetch_sitemap_urls():
    print(f"Fetching sitemap: {SITEMAP_URL}")
    resp = requests.get(SITEMAP_URL, timeout=15)
    resp.raise_for_status()
    root = ElementTree.fromstring(resp.content)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    urls = [loc.text.strip() for loc in root.findall(".//sm:loc", ns) if loc.text]
    print(f"Found {len(urls)} URLs in sitemap")
    return urls


def submit_url(url, access_token):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    payload = {"url": url, "type": "URL_UPDATED"}
    resp = requests.post(INDEXING_API_URL, headers=headers, json=payload, timeout=15)
    return resp.status_code, resp.json()


def run(urls):
    print(f"\nAuthenticating with service account...")
    creds = get_credentials()
    print(f"Authenticated as: {creds.service_account_email}\n")

    results = {"success": [], "failed": []}

    for i, url in enumerate(urls, 1):
        token = get_access_token(creds)
        status, body = submit_url(url, token)

        if status == 200:
            print(f"[{i}/{len(urls)}] OK  {url}")
            results["success"].append(url)
        else:
            error = body.get("error", {}).get("message", str(body))
            print(f"[{i}/{len(urls)}] ERR {url} — {status}: {error}")
            results["failed"].append({"url": url, "status": status, "error": error})

        if i < len(urls):
            time.sleep(BATCH_DELAY_SECONDS)

    print(f"\n--- Done ---")
    print(f"Submitted: {len(results['success'])} succeeded, {len(results['failed'])} failed")

    if results["failed"]:
        print("\nFailed URLs:")
        for f in results["failed"]:
            print(f"  {f['status']} — {f['url']}: {f['error']}")

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit URLs to Google Indexing API")
    parser.add_argument("--url", help="Submit a single URL instead of the full sitemap")
    args = parser.parse_args()

    if args.url:
        urls = [args.url]
    else:
        urls = fetch_sitemap_urls()

    run(urls)
