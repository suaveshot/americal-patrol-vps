"""Fetch connected social accounts from GHL."""
import json
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

import requests

GHL_BASE_URL = "https://services.leadconnectorhq.com"
GHL_VERSION = "2021-07-28"

api_key = os.environ.get("GHL_API_KEY", "").strip()
location_id = os.environ.get("GHL_LOCATION_ID", "").strip()

headers = {
    "Authorization": f"Bearer {api_key}",
    "Version": GHL_VERSION,
    "Accept": "application/json",
}

resp = requests.get(
    f"{GHL_BASE_URL}/social-media-posting/{location_id}/accounts",
    headers=headers,
    timeout=15,
)

data = resp.json()
accounts = data.get("results", {}).get("accounts", [])

print("=== Connected Social Accounts ===\n")
for acct in accounts:
    print(f"  Platform: {acct.get('platform', '?').upper()}")
    print(f"  Name:     {acct.get('name', '?')}")
    print(f"  Type:     {acct.get('type', '?')}")
    print(f"  ID:       {acct.get('id', '?')}")
    print(f"  Expired:  {acct.get('isExpired', '?')}")
    print()

print("--- Paste these into social_config.json ---")
by_platform = {}
for acct in accounts:
    p = acct.get("platform", "")
    by_platform.setdefault(p, []).append(acct)

for p in ["facebook", "instagram", "linkedin", "google"]:
    accts = by_platform.get(p, [])
    if accts:
        for a in accts:
            label = f"{a['name']} ({a['type']})"
            print(f"  {p:12s} -> {a['id']}")
            print(f"               [{label}]")
