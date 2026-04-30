"""
Harbor Lights — one-time Sheets OAuth setup.

Run on Sam's machine (where browser auth works):

    cd "Harbor Lights"
    python auth_setup.py

This mints `sheets_token.json` with the spreadsheets scope. Then base64
the file into SHEETS_TOKEN_B64 and add to /docker/americal-patrol/.env on
the VPS. The container's entrypoint.sh decodes it on boot.

Reuses the same GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET as the rest of the
AP automations (gbp, patrol, etc.) — Sheets is just another scope on the
same OAuth app.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCRIPT_DIR = Path(__file__).resolve().parent
TOKEN_PATH = SCRIPT_DIR / "sheets_token.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def _client_config() -> dict:
    return {
        "installed": {
            "client_id": os.environ["GOOGLE_CLIENT_ID"],
            "client_secret": os.environ["GOOGLE_CLIENT_SECRET"],
            "project_id": "americal-patrol-automation",
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
            "redirect_uris": ["http://localhost"],
        }
    }


def main() -> None:
    load_dotenv(SCRIPT_DIR.parent / ".env")
    if not os.environ.get("GOOGLE_CLIENT_ID") or not os.environ.get("GOOGLE_CLIENT_SECRET"):
        raise SystemExit("Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in Americal Patrol/.env first.")

    creds: Credentials | None = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_config(_client_config(), SCOPES)
            creds = flow.run_local_server(port=0)
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")

    print(f"sheets_token.json saved at {TOKEN_PATH}")
    print("Next step (PowerShell on Sam's PC):")
    print(f'  certutil -encode "{TOKEN_PATH}" sheets_token_b64.txt')
    print("  # then strip the BEGIN/END lines and join the body, paste as SHEETS_TOKEN_B64 in .env")


if __name__ == "__main__":
    main()
