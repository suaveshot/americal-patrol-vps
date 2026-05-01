"""
Harbor Lights — Google Sheets storage backend.

Replaces openpyxl/xlsx as the canonical store for guest parking records and
the Dashboard. The pipeline writes here, parking_audit reads here, and Sam
views the live sheet in a browser from any device.

Why this exists:
  - Excel-on-OneDrive required Sam's PC to be running for sync. After the
    AP container migration, the pipeline is on the VPS and Sam's PC can be
    off — we needed a cloud-native data store.
  - Sheets gives us multi-user view, no `_check_excel_lock()` headaches,
    HOA-board sharing if ever wanted, and free hosting.

Auth:
  Re-uses the gbp-style OAuth pattern. Sam runs `python auth_setup.py` once
  locally (with GOOGLE_CLIENT_ID/SECRET set), browser-authorizes, and the
  resulting `sheets_token.json` is base64'd into SHEETS_TOKEN_B64 for the
  VPS container to decode. Scope: spreadsheets (read+write).

Storage layout (in the Sheet identified by HL_SHEET_ID):
  - One worksheet per year ("2026", "2027", ...) — appended to over time.
  - "Dashboard" worksheet — recomputed on every update from year sheets.
    Holds KPIs (row 7) + Top 10 plates (rows 11+) + permit categories.
  - "Summary Dashboard" — alias for "Dashboard" for back-compat with the
    original Excel layout (some legacy reads checked it first).

Column schema (year sheets):
  A: Date (formatted "d-mmm-yy")  — only set on the FIRST row of each date group
  B: Plate (e.g. "9SEN628")
  C: Permit / Status (integer, "TOWED", "WARNING", "EXPIRED", "OUTSIDER")

Status semantics: matches the original Excel. The TOWED/WARNING/EXPIRED/
OUTSIDER cells get colored backgrounds via batch_update so they remain
visually scannable.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, date
from pathlib import Path
from typing import Iterable

log = logging.getLogger(__name__)

# Lazy imports so a missing gspread doesn't break test collection.
_gc = None  # cached gspread client
_sh = None  # cached spreadsheet handle


SCRIPT_DIR = Path(__file__).resolve().parent
TOKEN_PATH = SCRIPT_DIR / "sheets_token.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Status formatting (background color hex, font color hex, bold).
# Mirrors `STATUS_STYLE` in hl_update.py — keep in sync.
STATUS_STYLE: dict[str, tuple[str, str, bool]] = {
    "TOWED":    ("FFC7CE", "9C0006", True),   # red — matches Excel
    "WARNING":  ("FFEB9C", "9C5700", True),   # yellow
    "EXPIRED":  ("F2DCDB", "C00000", True),   # pink
    "OUTSIDER": ("DCE6F1", "1F497D", True),   # blue
}

# Header for newly-created year sheets.
YEAR_HEADER = ["Date", "Plate", "Permit / Status"]


def _build_client_config() -> dict:
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


def _credentials():
    """Load + auto-refresh OAuth credentials from sheets_token.json."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    if not TOKEN_PATH.exists():
        raise RuntimeError(
            f"sheets_token.json not found at {TOKEN_PATH}. "
            "Run `python auth_setup.py` once locally or set SHEETS_TOKEN_B64 "
            "in the container's .env."
        )
    creds = Credentials.from_authorized_user_file(str(TOKEN_PATH), SCOPES)
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        TOKEN_PATH.write_text(creds.to_json(), encoding="utf-8")
    return creds


def _client():
    """Cached gspread client. Builds on first call."""
    global _gc
    if _gc is None:
        import gspread
        _gc = gspread.authorize(_credentials())
    return _gc


def open_sheet(sheet_id: str | None = None):
    """Open the Harbor Lights spreadsheet (cached)."""
    global _sh
    if _sh is not None:
        return _sh
    sheet_id = sheet_id or os.environ.get("HL_SHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("HL_SHEET_ID not set in environment.")
    _sh = _client().open_by_key(sheet_id)
    return _sh


def reset_cache() -> None:
    """Force re-auth + re-open on next call (used by tests)."""
    global _gc, _sh
    _gc = None
    _sh = None


# ── Worksheet operations ─────────────────────────────────────────────────────

def get_or_create_year_sheet(spreadsheet, year: int | str):
    """Return the worksheet for the given year, creating it with a header row
    if it doesn't exist yet."""
    import gspread
    title = str(year)
    try:
        return spreadsheet.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=2000, cols=8)
        ws.append_row(YEAR_HEADER, value_input_option="USER_ENTERED")
        log.info(f"Created new year worksheet '{title}'")
        return ws


def get_last_data_row(ws) -> int:
    """Return the 1-indexed row number of the last non-empty row in column A or B.
    Returns 1 if only header is present.
    """
    col_a = ws.col_values(1)
    col_b = ws.col_values(2)
    return max(
        max((i for i, v in enumerate(col_a, start=1) if v), default=1),
        max((i for i, v in enumerate(col_b, start=1) if v), default=1),
    )


def _row_index_of_last_date(ws) -> int:
    """Find the row of the most recent date entry (column A non-empty)."""
    col_a = ws.col_values(1)
    for i in range(len(col_a), 0, -1):
        if col_a[i - 1]:
            return i
    return 0


def append_data_rows(ws, rows: list[tuple[date | None, str, object]]) -> None:
    """Append rows in (date, plate, permit_or_status) form starting at the
    next free row.

    `date` is None for continuation rows within a date group (matches the
    original Excel where only the first row of each group shows the date).

    Status cells (TOWED/WARNING/EXPIRED/OUTSIDER) get colored backgrounds
    via a single batch_format call to stay under the Sheets API rate limit.
    """
    import re
    if not rows:
        return

    start_row = get_last_data_row(ws) + 2  # one blank line after last block

    # Build the values payload. Dates are written as ISO so Sheets parses
    # them as real dates; we then apply the d-mmm-yy format on column A.
    values = []
    status_cells: list[tuple[int, str]] = []  # (row_index, status_key)
    for offset, (d, plate, permit) in enumerate(rows):
        date_str = d.strftime("%Y-%m-%d") if d else ""
        if isinstance(permit, str) and permit.upper() in STATUS_STYLE:
            permit_val = permit.upper()
            status_cells.append((start_row + offset, permit_val))
        elif isinstance(permit, str):
            digits = re.sub(r"[^0-9]", "", permit)
            permit_val = digits if digits else permit.strip()
        else:
            permit_val = permit if permit is not None else ""
        values.append([date_str, str(plate or ""), permit_val])

    end_row = start_row + len(values) - 1
    range_a1 = f"A{start_row}:C{end_row}"
    ws.update(range_a1, values, value_input_option="USER_ENTERED")

    # Batch the formatting: date number-format on column A + status colors on column C.
    requests = []
    sheet_id = ws.id

    # Date format on column A for the new range.
    requests.append({
        "repeatCell": {
            "range": {
                "sheetId": sheet_id,
                "startRowIndex": start_row - 1,
                "endRowIndex": end_row,
                "startColumnIndex": 0,
                "endColumnIndex": 1,
            },
            "cell": {"userEnteredFormat": {"numberFormat": {"type": "DATE", "pattern": "d-mmm-yy"}}},
            "fields": "userEnteredFormat.numberFormat",
        }
    })

    # Status colors on column C — one request per cell. Sheets batch_update
    # collapses these into one HTTP call.
    for row_idx, status_key in status_cells:
        bg, fg, bold = STATUS_STYLE[status_key]
        requests.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": row_idx - 1,
                    "endRowIndex": row_idx,
                    "startColumnIndex": 2,
                    "endColumnIndex": 3,
                },
                "cell": {"userEnteredFormat": {
                    "backgroundColor": _hex_to_rgb(bg),
                    "textFormat": {"foregroundColor": _hex_to_rgb(fg), "bold": bold},
                    "horizontalAlignment": "CENTER",
                }},
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        })

    if requests:
        ws.spreadsheet.batch_update({"requests": requests})


def _hex_to_rgb(h: str) -> dict:
    h = h.lstrip("#")
    return {
        "red":   int(h[0:2], 16) / 255,
        "green": int(h[2:4], 16) / 255,
        "blue":  int(h[4:6], 16) / 255,
    }


# ── Reads (used by parking_audit + dashboard refresh) ────────────────────────

def read_all_records(spreadsheet) -> list[tuple[datetime, str, str]]:
    """Iterate every year worksheet and return a flat list of
    (date, plate, status) for every data row.

    Continuation rows (where the date cell is blank because the previous
    row in the same group held it) inherit the date from the most recent
    non-blank date — matches the original Excel reader logic.
    """
    out: list[tuple[datetime, str, str]] = []
    for ws in spreadsheet.worksheets():
        if not ws.title.isdigit():
            continue  # skip Dashboard / Summary Dashboard / legacy tabs
        rows = ws.get_all_values()
        current_date: datetime | None = None
        for row in rows[1:]:  # skip header
            date_str = row[0] if len(row) > 0 else ""
            plate    = row[1] if len(row) > 1 else ""
            status   = row[2] if len(row) > 2 else ""
            if date_str:
                try:
                    current_date = datetime.strptime(date_str, "%Y-%m-%d")
                except ValueError:
                    try:
                        current_date = datetime.strptime(date_str, "%m/%d/%Y")
                    except ValueError:
                        # Sheets sometimes returns "1-Jan-26" when display
                        # format is applied — try the d-mmm-yy too.
                        try:
                            current_date = datetime.strptime(date_str, "%d-%b-%y")
                        except ValueError:
                            pass
            if plate and current_date:
                out.append((current_date, plate.strip(), str(status or "").strip()))
    return out


# ── Dashboard recompute ──────────────────────────────────────────────────────

def refresh_dashboard(spreadsheet) -> None:
    """Recompute the Dashboard worksheet from all year sheets.

    Layout matches the original Excel Dashboard:
      Row 1:  title
      Row 7:  KPI strip (Total entries, Unique plates, TOWED count, WARNING count)
      Row 11+: Top-10 most-frequent plates table (rank, plate, count, last seen)
    """
    import gspread
    records = read_all_records(spreadsheet)
    if not records:
        return

    # Aggregate
    total = len(records)
    plate_counts: dict[str, int] = {}
    plate_last_seen: dict[str, datetime] = {}
    towed = warning = 0
    for d, plate, status in records:
        plate_counts[plate] = plate_counts.get(plate, 0) + 1
        if d > plate_last_seen.get(plate, datetime.min):
            plate_last_seen[plate] = d
        sk = status.upper()
        if sk == "TOWED":   towed   += 1
        elif sk == "WARNING": warning += 1

    top_10 = sorted(plate_counts.items(), key=lambda kv: -kv[1])[:10]

    try:
        dash = spreadsheet.worksheet("Dashboard")
    except gspread.WorksheetNotFound:
        dash = spreadsheet.add_worksheet(title="Dashboard", rows=50, cols=8)

    # KPI strip at row 7
    dash.update("A7:D7", [["Total entries", "Unique plates", "TOWED", "WARNING"]],
                value_input_option="USER_ENTERED")
    dash.update("A8:D8", [[total, len(plate_counts), towed, warning]],
                value_input_option="USER_ENTERED")

    # Top-10 table starting row 11
    header = [["Rank", "Plate", "Count", "Last seen"]]
    body = [
        [i + 1, plate, count, plate_last_seen[plate].strftime("%m/%d/%Y")]
        for i, (plate, count) in enumerate(top_10)
    ]
    dash.update("A11:D11", header, value_input_option="USER_ENTERED")
    if body:
        dash.update(f"A12:D{12 + len(body) - 1}", body, value_input_option="USER_ENTERED")
