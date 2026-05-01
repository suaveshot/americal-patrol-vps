"""
Americal Patrol — Google Drive Photo Sync
Syncs real photos from a shared Google Drive folder into the local media/real/ directory.

Don or Sam can drop photos in the Drive folder with descriptive filenames:
  team_photo_march2026.jpg → tags: ["team", "photo"]
  vehicle_night_patrol.jpg → tags: ["vehicle", "night", "patrol"]
  property_harbor_lights.jpg → tags: ["property", "harbor", "lights"]

The pipeline checks this folder before each run and downloads new photos.
Real photos are prioritized over AI-generated images.
"""

import io
import os
import re
from datetime import datetime
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
REAL_DIR     = SCRIPT_DIR / "media" / "real"
TOKEN_PATH   = SCRIPT_DIR / "social_drive_token.json"
PATROL_DIR   = SCRIPT_DIR.parent / "patrol_automation"

DRIVE_SCOPES = [
    "https://www.googleapis.com/auth/drive.readonly",
    "https://www.googleapis.com/auth/drive.file",
]

IMAGE_MIMES = {
    "image/jpeg", "image/png", "image/webp", "image/gif",
}


def _build_client_config():
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


def _get_drive_service():
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    creds = None
    if TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), DRIVE_SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_config(_build_client_config(), DRIVE_SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_PATH, "w") as f:
            f.write(creds.to_json())

    return build("drive", "v3", credentials=creds)


def _tags_from_filename(filename: str) -> list[str]:
    """Extract tags from filename by splitting on underscores and dots."""
    stem = Path(filename).stem
    # Remove date patterns like march2026, 20260315
    stem = re.sub(r'\d{4,}', '', stem)
    stem = re.sub(r'(january|february|march|april|may|june|july|august|september|october|november|december)', '', stem, flags=re.IGNORECASE)
    # Split on underscores, hyphens, spaces
    parts = re.split(r'[_\-\s]+', stem)
    return [p.lower().strip() for p in parts if p.strip() and len(p) > 1]


def sync_photos(log=None) -> list[dict]:
    """
    Download new photos from the Google Drive folder.

    Returns list of dicts: [{"filename": ..., "path": ..., "tags": [...]}]
    """
    folder_id = os.environ.get("SOCIAL_DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        if log:
            log("  SOCIAL_DRIVE_FOLDER_ID not set — skipping Drive photo sync.")
        return []

    REAL_DIR.mkdir(parents=True, exist_ok=True)

    # Get list of already-downloaded files
    existing = {f.name for f in REAL_DIR.iterdir() if f.is_file()}

    try:
        service = _get_drive_service()
    except Exception as e:
        if log:
            log(f"  WARNING: Drive auth failed: {e}")
        return []

    # List image files in the folder
    query = f"'{folder_id}' in parents and trashed=false"
    try:
        results = service.files().list(
            q=query,
            fields="files(id, name, mimeType, modifiedTime)",
            orderBy="modifiedTime desc",
            pageSize=50,
        ).execute()
    except Exception as e:
        if log:
            log(f"  WARNING: Drive list failed: {e}")
        return []

    files = results.get("files", [])
    new_photos = []

    for f in files:
        if f["mimeType"] not in IMAGE_MIMES:
            continue

        if f["name"] in existing:
            continue  # Already downloaded

        # Download the file
        try:
            request = service.files().get_media(fileId=f["id"])
            content = request.execute()

            filepath = REAL_DIR / f["name"]
            with open(filepath, "wb") as out:
                out.write(content)

            tags = _tags_from_filename(f["name"])
            new_photos.append({
                "filename": f["name"],
                "path": str(filepath),
                "tags": tags,
            })

            if log:
                log(f"  Downloaded: {f['name']} (tags: {tags})")

        except Exception as e:
            if log:
                log(f"  WARNING: Failed to download {f['name']}: {e}")

    if log:
        if new_photos:
            log(f"  Synced {len(new_photos)} new photo(s) from Google Drive.")
        else:
            log(f"  No new photos in Drive folder ({len(existing)} already synced).")

    return new_photos


def upload_for_public_url(filepath: Path, log=None) -> str | None:
    """
    Upload an image to Google Drive and make it publicly accessible.
    GHL's Social Media Posting API requires images at a public URL.

    Returns the public URL, or None on failure.
    """
    folder_id = os.environ.get("SOCIAL_DRIVE_FOLDER_ID", "").strip()
    if not folder_id:
        if log:
            log("  SOCIAL_DRIVE_FOLDER_ID not set — cannot upload for Instagram.")
        return None

    try:
        from googleapiclient.http import MediaFileUpload

        service = _get_drive_service()

        media = MediaFileUpload(str(filepath), resumable=True)
        file_metadata = {
            "name": f"ig_{filepath.name}",
            "parents": [folder_id],
        }

        uploaded = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webContentLink",
        ).execute()

        file_id = uploaded["id"]

        # Make it publicly accessible
        service.permissions().create(
            fileId=file_id,
            body={"type": "anyone", "role": "reader"},
        ).execute()

        # Get the direct download link
        public_url = f"https://drive.google.com/uc?id={file_id}&export=download"

        if log:
            log(f"  Uploaded to Drive for Instagram: {public_url}")

        return public_url

    except Exception as e:
        if log:
            log(f"  WARNING: Drive upload failed: {e}")
        return None


# Backwards-compatible alias
upload_for_instagram = upload_for_public_url
