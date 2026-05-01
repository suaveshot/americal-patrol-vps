"""
Americal Patrol — GHL Social Media Account Setup
Discovers connected social media accounts in GoHighLevel and prints
the account IDs needed for social_config.json.

Usage:
    python ghl_social_setup.py
"""

import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

import requests

GHL_BASE_URL = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"

PLATFORMS = ["facebook", "instagram", "linkedin"]


def _headers() -> dict:
    api_key = os.environ.get("GHL_API_KEY", "").strip()
    if not api_key:
        print("ERROR: GHL_API_KEY not set in .env")
        sys.exit(1)
    return {
        "Authorization": f"Bearer {api_key}",
        "Version": GHL_VERSION,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def fetch_accounts():
    location_id = os.environ.get("GHL_LOCATION_ID", "").strip()
    if not location_id:
        print("ERROR: GHL_LOCATION_ID not set in .env")
        sys.exit(1)

    hdrs = _headers()

    print("\n=== GHL Social Media — Connected Accounts ===\n")

    found_any = False

    for platform in PLATFORMS:
        url = f"{GHL_BASE_URL}/social-media-posting/{location_id}/oauth/{platform}/accounts"
        resp = requests.get(url, headers=hdrs, timeout=15)

        if resp.status_code == 200:
            data = resp.json()
            accounts = data if isinstance(data, list) else data.get("accounts", data.get("data", []))

            if accounts:
                found_any = True
                print(f"  {platform.upper()}:")
                for acct in accounts:
                    acct_id = acct.get("id") or acct.get("_id") or acct.get("accountId")
                    name = acct.get("name") or acct.get("pageName") or acct.get("username") or "—"
                    print(f"    ID: {acct_id}  |  Name: {name}")
                print()
            else:
                print(f"  {platform.upper()}: No accounts connected.")
                print(f"    -> Connect {platform} in GHL: Settings > Social Planner > Connect Account\n")
        else:
            print(f"  {platform.upper()}: API error {resp.status_code}: {resp.text[:200]}")
            print()

    if found_any:
        print("--- NEXT STEP ---")
        print("Copy each account ID into social_config.json under the matching platform:")
        print()
        print('  "platforms": {')
        print('    "facebook":  { "ghl_account_id": "<paste facebook ID here>", ... },')
        print('    "instagram": { "ghl_account_id": "<paste instagram ID here>", ... },')
        print('    "linkedin":  { "ghl_account_id": "<paste linkedin ID here>", ... }')
        print("  }")
        print()
    else:
        print("No connected accounts found.")
        print("Connect your social accounts in GHL first:")
        print("  GHL > Settings > Social Planner > Connect Account")


if __name__ == "__main__":
    fetch_accounts()
