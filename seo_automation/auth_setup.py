"""
Americal Patrol - SEO Automation Auth Setup
Single Google account sign-in covering GA4, Search Console, and Gmail send.

Run once: python auth_setup.py
"""

import json
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'

SCOPES = [
    'https://www.googleapis.com/auth/analytics.readonly',
    'https://www.googleapis.com/auth/webmasters.readonly',
    'https://www.googleapis.com/auth/gmail.send',
]


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _build_client_config():
    import os
    return {
        "installed": {
            "client_id": os.environ['GOOGLE_CLIENT_ID'],
            "client_secret": os.environ['GOOGLE_CLIENT_SECRET'],
            "project_id": "americal-patrol-automation",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost"],
        }
    }


def get_credentials() -> Credentials:
    """Returns authorized credentials for GA4, Search Console, and Gmail."""
    config     = _load_config()
    token_path = SCRIPT_DIR / config['token_path']

    creds = None
    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_config(
                _build_client_config(), SCOPES
            )
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as f:
            f.write(creds.to_json())

    return creds


if __name__ == '__main__':
    print("=" * 60)
    print("Americal Patrol SEO Automation - Account Setup")
    print("=" * 60)
    print()
    print("Sign in with your Google account that has access to:")
    print("  • Google Analytics 4")
    print("  • Google Search Console")
    print("  • Gmail (to send SEO reports)")
    print()
    print("A browser window will open now.")
    input("Press Enter to continue...")
    get_credentials()
    print()
    print("✓ Authorization complete! (seo_token.json saved)")
    print("You can now run: python run_seo.py")
