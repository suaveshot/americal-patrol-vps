"""
send_pdfs.py - Sends all PDF files in a folder as email attachments via Gmail API.

Setup:
  1. Enable Gmail API at https://console.cloud.google.com/
  2. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in .env
  3. pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client
  4. Run once to complete OAuth flow; token.json will be saved for future runs
"""

import os
import base64
import mimetypes
from email.message import EmailMessage
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent / '.env')

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ── Configuration ─────────────────────────────────────────────────────────────

PDF_FOLDER = str(Path(__file__).resolve().parent.parent)  # Folder to scan for PDFs
RECIPIENT   = "recipient@example.com"                             # Destination email address
SENDER      = "me"                                                # "me" = your authenticated account
SUBJECT     = "PDF Files Attached"
BODY        = "Please find the attached PDF file(s)."

# OAuth scope – sending mail only
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]

# Paths
SCRIPT_DIR = Path(__file__).parent
TOKEN_FILE = SCRIPT_DIR / "token.json"

# ── Auth ──────────────────────────────────────────────────────────────────────

def get_gmail_service():
    """Authenticate and return a Gmail API service object."""
    creds = None

    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            client_config = {
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
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)

        TOKEN_FILE.write_text(creds.to_json())
        print(f"Token saved to {TOKEN_FILE}")

    return build("gmail", "v1", credentials=creds)

# ── Email building ─────────────────────────────────────────────────────────────

def build_email(pdf_paths: list[Path]) -> str:
    """Create a MIME email with PDFs attached. Returns base64url-encoded raw string."""
    msg = EmailMessage()
    msg["To"]      = RECIPIENT
    msg["From"]    = SENDER
    msg["Subject"] = SUBJECT
    msg.set_content(BODY)

    for pdf_path in pdf_paths:
        mime_type, _ = mimetypes.guess_type(str(pdf_path))
        main_type, sub_type = (mime_type or "application/octet-stream").split("/", 1)
        msg.add_attachment(
            pdf_path.read_bytes(),
            maintype=main_type,
            subtype=sub_type,
            filename=pdf_path.name,
        )
        print(f"  Attached: {pdf_path.name}")

    return base64.urlsafe_b64encode(msg.as_bytes()).decode()

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    folder = Path(PDF_FOLDER)
    if not folder.is_dir():
        raise NotADirectoryError(f"PDF_FOLDER does not exist: {folder}")

    pdf_files = sorted(folder.glob("*.pdf"))
    if not pdf_files:
        print(f"No PDF files found in {folder}")
        return

    print(f"Found {len(pdf_files)} PDF(s) in {folder}:")

    service = get_gmail_service()
    raw_message = build_email(pdf_files)

    try:
        result = service.users().messages().send(
            userId="me",
            body={"raw": raw_message},
        ).execute()
        print(f"\nEmail sent successfully! Message ID: {result['id']}")
        print(f"To: {RECIPIENT} | Subject: {SUBJECT}")
    except HttpError as e:
        print(f"Gmail API error: {e}")
        raise


if __name__ == "__main__":
    main()
