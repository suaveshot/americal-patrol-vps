import os
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / '.env')

SCRIPT_DIR = Path(__file__).parent

SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',    # read/download emails+attachments
    'https://www.googleapis.com/auth/gmail.compose',     # create drafts
    'https://www.googleapis.com/auth/gmail.send',        # (optional) send directly later
]


def _build_client_config():
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


def get_credentials():
    creds = None
    token_path = SCRIPT_DIR / 'token.json'

    if token_path.exists():
        creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_config(_build_client_config(), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds

if __name__ == '__main__':
    creds = get_credentials()
    print("Authorization successful! token.json created.")
