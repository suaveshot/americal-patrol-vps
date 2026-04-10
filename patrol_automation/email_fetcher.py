import os
import re
import html as html_module
import base64
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

# ── Config ────────────────────────────────────────────────────────────────────
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.compose',
    'https://www.googleapis.com/auth/gmail.send',
]

SENDER          = 'noreply@reports.connecteam.com'
SCRIPT_DIR      = Path(__file__).parent
REPORTS_FOLDER  = SCRIPT_DIR.parent / 'Americal Patrol Morning Reports'
MAX_SIZE_MB     = 20


# ── Auth ──────────────────────────────────────────────────────────────────────
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
        with open(token_path, 'w') as f:
            f.write(creds.to_json())

    return creds


def get_gmail_service():
    return build('gmail', 'v1', credentials=get_credentials())


# ── Search ────────────────────────────────────────────────────────────────────
def search_morning_reports(service):
    """
    Return all Connecteam report emails received today.
    Searches without 'has:attachment' so link-based emails (>10 MB reports
    where Connecteam sends a download link instead of attaching the PDF) are
    also included.
    """
    today  = datetime.now().strftime('%Y/%m/%d')
    query  = f'from:{SENDER} after:{today}'

    result   = service.users().messages().list(userId='me', q=query).execute()
    messages = result.get('messages', [])

    print(f"[Fetcher] Found {len(messages)} report email(s) from today ({today})")
    return messages


# ── Helpers ───────────────────────────────────────────────────────────────────
def _flatten_parts(payload):
    parts = []
    if 'parts' in payload:
        for part in payload['parts']:
            parts.extend(_flatten_parts(part))
    else:
        parts.append(payload)
    return parts


def _unique_path(folder, filename):
    path = folder / filename
    stem, suffix = os.path.splitext(filename)
    counter = 1
    while path.exists():
        path = folder / f"{stem} ({counter}){suffix}"
        counter += 1
    return path


def _get_email_html(payload):
    """Extract HTML body from email payload parts."""
    for part in _flatten_parts(payload):
        if part.get('mimeType') == 'text/html':
            data = part['body'].get('data', '')
            if data:
                return base64.urlsafe_b64decode(data).decode('utf-8', errors='replace')
    return ''


def _extract_download_url(html_body):
    """
    Find the Connecteam PDF download URL from a link-based report email.
    Connecteam sends: '...Click here to download the file.' where 'Click here'
    is a hyperlink to the actual PDF.
    """
    decoded = html_module.unescape(html_body)
    # Find all hrefs
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', decoded)
    for url in hrefs:
        url_lower = url.lower()
        # Skip mailto, unsubscribe, and Connecteam UI links — want the download link
        if url.startswith('http') and (
            'download' in url_lower
            or '.pdf' in url_lower
            or 'amazonaws' in url_lower
            or 'storage' in url_lower
            or ('connecteam' in url_lower and 'report' in url_lower)
        ):
            return url
    # Fallback: return the first non-trivial http link that isn't the Connecteam homepage
    for url in hrefs:
        if url.startswith('https') and 'connecteam.com' in url and len(url) > 40:
            return url
    return None


def _filename_from_subject(subject, date_prefix):
    """
    Build a filename from the Connecteam email subject when there's no attachment.
    Subject format: "Americal Patrol Inc.'s daily report for {Report Name}"
    """
    # Strip the common prefix
    for prefix in [
        "Americal Patrol Inc.'s daily report for ",
        "Americal Patrol Inc.'s ",
        "daily report for ",
    ]:
        if subject.startswith(prefix):
            subject = subject[len(prefix):]
            break

    # Clean up into a filename-safe string
    clean = re.sub(r'[^\w\s\-]', '', subject).strip()
    clean = re.sub(r'\s+', '_', clean)
    return f"{date_prefix}_{clean}.pdf"


# ── Download ──────────────────────────────────────────────────────────────────
def download_pdf_attachments(service, messages):
    """
    Download PDF reports from Connecteam emails.
    Handles two cases:
      1. PDF attached directly (reports <= 10 MB)
      2. Download link in email body (reports > 10 MB — Connecteam's behaviour
         for large files)
    """
    REPORTS_FOLDER.mkdir(parents=True, exist_ok=True)
    downloaded = []

    for msg_ref in messages:
        msg     = service.users().messages().get(
            userId='me', id=msg_ref['id'], format='full'
        ).execute()
        headers    = {h['name']: h['value'] for h in msg['payload']['headers']}
        subject    = headers.get('Subject', 'Unknown Report')
        email_date = headers.get('Date', '')
        date_prefix = datetime.now().strftime('%Y%m%d')

        found = False

        # ── Case 1: PDF attachment ─────────────────────────────────────────
        for part in _flatten_parts(msg['payload']):
            fname = part.get('filename', '')
            if not fname.lower().endswith('.pdf'):
                continue

            attach_id = part['body'].get('attachmentId')
            if not attach_id:
                continue

            attachment = service.users().messages().attachments().get(
                userId='me', messageId=msg_ref['id'], id=attach_id
            ).execute()
            data = base64.urlsafe_b64decode(attachment['data'])

            filename = f"{date_prefix}_{fname}"
            filepath = _unique_path(REPORTS_FOLDER, filename)

            with open(filepath, 'wb') as f:
                f.write(data)

            size_mb = len(data) / (1024 * 1024)
            print(f"[Fetcher] Downloaded (attachment): {filepath.name} ({size_mb:.1f} MB)")

            downloaded.append({
                'path':       filepath,
                'subject':    subject,
                'email_date': email_date,
                'size_mb':    size_mb,
            })
            found = True

        # ── Case 2: Download link in email body ────────────────────────────
        if not found:
            html_body    = _get_email_html(msg['payload'])
            download_url = _extract_download_url(html_body)

            if not download_url:
                print(f"[Fetcher] WARNING: No PDF or download link found in: {subject}")
                continue

            filename = _filename_from_subject(subject, date_prefix)
            filepath = _unique_path(REPORTS_FOLDER, filename)

            try:
                req = urllib.request.Request(
                    download_url,
                    headers={'User-Agent': 'Mozilla/5.0'}
                )
                with urllib.request.urlopen(req, timeout=60) as resp:
                    # Try to get filename from Content-Disposition
                    cd = resp.headers.get('Content-Disposition', '')
                    cd_match = re.search(r'filename[^;=\n]*=(?:["\']?)([^;\n"\']+)', cd)
                    if cd_match:
                        orig_name = cd_match.group(1).strip()
                        filename  = f"{date_prefix}_{orig_name}"
                        if not filename.lower().endswith('.pdf'):
                            filename += '.pdf'
                        filepath = _unique_path(REPORTS_FOLDER, filename)

                    data = resp.read()

                with open(filepath, 'wb') as f:
                    f.write(data)

                size_mb = len(data) / (1024 * 1024)
                print(f"[Fetcher] Downloaded (link):       {filepath.name} ({size_mb:.1f} MB)")

                downloaded.append({
                    'path':       filepath,
                    'subject':    subject,
                    'email_date': email_date,
                    'size_mb':    size_mb,
                })

            except Exception as e:
                print(f"[Fetcher] ERROR downloading from link for '{subject}': {e}")

    return downloaded


# ── Compress ──────────────────────────────────────────────────────────────────
def compress_pdf_if_needed(report):
    if report['size_mb'] <= MAX_SIZE_MB:
        return report

    filepath = report['path']
    print(f"[Compressor] {filepath.name} is {report['size_mb']:.1f} MB — compressing...")

    try:
        import fitz
    except ImportError:
        print("[Compressor] PyMuPDF not installed. Run: pip install pymupdf")
        return report

    temp_path = filepath.with_stem(filepath.stem + '_tmp')

    # Pass 1: lossless stream compression (fast, works well for text-heavy PDFs)
    doc = fitz.open(str(filepath))
    doc.save(str(temp_path), garbage=4, deflate=True, deflate_images=True, clean=True)
    doc.close()
    pass1_mb = temp_path.stat().st_size / (1024 * 1024)

    # Pass 2: page re-render as JPEG — necessary for image-heavy patrol PDFs
    # (Connecteam embeds high-res JPEGs; pass 1 achieves < 5% on these)
    if pass1_mb > report['size_mb'] * 0.80:
        temp_path.unlink(missing_ok=True)
        doc     = fitz.open(str(filepath))
        new_doc = fitz.open()
        mat     = fitz.Matrix(1.5, 1.5)   # ~108 DPI — readable quality, large size reduction
        for page in doc:
            pix       = page.get_pixmap(matrix=mat, alpha=False)
            img_bytes = pix.tobytes('jpeg', jpg_quality=82)
            new_page  = new_doc.new_page(width=page.rect.width, height=page.rect.height)
            new_page.insert_image(new_page.rect, stream=img_bytes)
        doc.close()
        new_doc.save(str(temp_path), garbage=4, deflate=True, clean=True)
        new_doc.close()

    new_size_mb = temp_path.stat().st_size / (1024 * 1024)
    print(f"[Compressor] {report['size_mb']:.1f} MB -> {new_size_mb:.1f} MB")

    if new_size_mb < report['size_mb']:
        filepath.unlink()
        temp_path.rename(filepath)
        report['size_mb'] = new_size_mb
    else:
        temp_path.unlink()
        print("[Compressor] Compression didn't reduce size — keeping original.")

    return report


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    service  = get_gmail_service()
    messages = search_morning_reports(service)

    if not messages:
        print("No new patrol report emails found.")
    else:
        reports = download_pdf_attachments(service, messages)
        for i, report in enumerate(reports):
            reports[i] = compress_pdf_if_needed(report)

        print(f"\nDone. {len(reports)} PDF(s) saved to:")
        print(f"  {REPORTS_FOLDER}")
        for r in reports:
            print(f"  - {r['path'].name}  ({r['size_mb']:.1f} MB)")
