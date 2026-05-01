#!/usr/bin/env python3
"""
Harbor Lights Guest Parking - Daily Update Script
Reads new Harbor Lights HOA Guest Parking and Incident Report PDFs from the
Morning Reports folder tree, extracts license plates / permit numbers /
warning & tow events, and appends them to the Excel tracker with matching
styling. Also refreshes the Summary Dashboard stats.
"""

import logging
import os
import re
import json
import shutil
import sys
import tempfile
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
from itertools import groupby

import pdfplumber

# Storage backend — Google Sheets via sheets_client. The Excel file at
# Harbor Lights Guest Parking UPDATED.xlsx is now legacy (kept for
# parking_audit's optional Excel-fallback read path; new writes go to Sheets).
sys.path.insert(0, str(Path(__file__).resolve().parent))
from sheets_client import (  # noqa: E402
    open_sheet,
    get_or_create_year_sheet,
    append_data_rows,
    refresh_dashboard,
)

# ── Logging ─────────────────────────────────────────────────────────────────
BASE          = str(Path(__file__).resolve().parent.parent)
LOG_FILE      = os.path.join(BASE, "Harbor Lights", "harbor_lights.log")

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# Echo to stdout as well (for Task Scheduler output capture)
_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(logging.Formatter("%(message)s"))
logging.getLogger().addHandler(_console)

# ── Configuration ─────────────────────────────────────────────────────────────
MORNING_DIR   = os.path.join(BASE, "Americal Patrol Morning Reports")
EXCEL_FILE    = os.path.join(BASE, "Harbor Lights", "Harbor Lights Guest Parking UPDATED.xlsx")
PROCESSED_LOG = os.path.join(BASE, "Harbor Lights", "processed_pdfs.json")
TEMP_FILE     = os.path.join(tempfile.gettempdir(), "hl_temp.xlsx")


# Excel lock check removed: Sheets has no lock-file concept. The legacy
# `~$<filename>.xlsx` lock check that lived here is no longer applicable.


# ── Step 1: Find new PDFs ────────────────────────────────────────────────────
def find_harbor_pdfs(root):
    """Return (basename, fullpath) for every Harbor Lights PDF found recursively."""
    results = []
    for dirpath, _, filenames in os.walk(root):
        for fname in filenames:
            if not fname.lower().endswith(".pdf"):
                continue
            fl = fname.lower()
            if "harbor" in fl and "lights" in fl:
                results.append((fname, os.path.join(dirpath, fname)))
    return results


# ── Branded format detection ────────────────────────────────────────────────
# On 2026-04-02 patrol_automation stopped forwarding Connecteam-native PDFs and
# started generating its own branded PDFs via branded_pdf.py. The new PDFs have
# "GUEST PARKING CHECK" / "INCIDENT REPORT" banners, "Date April 15, 2026"
# instead of "04/15/2026", and "Plates: ..." lines instead of "License Plate #".
BRANDED_MARKER_RE       = re.compile(r"GUEST\s+PARKING\s+CHECK|INCIDENT\s+REPORT", re.IGNORECASE)
BRANDED_DATE_RE         = re.compile(r"Date\s+([A-Z][a-z]+\s+\d{1,2},\s*\d{4})")
BRANDED_PLATES_LINE_RE  = re.compile(r"Plates:\s*(.+)")
BRANDED_PLATE_TOKEN_RE  = re.compile(r"([A-Z0-9]{4,10})(?:/([A-Za-z0-9-]+))?")


def _is_branded(full_text):
    return bool(BRANDED_MARKER_RE.search(full_text[:2000]))


def _branded_date(full_text):
    """Return MM/DD/YYYY string if the PDF has a branded 'Date Month D, YYYY' header."""
    m = BRANDED_DATE_RE.search(full_text)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%B %d, %Y").strftime("%m/%d/%Y")
    except ValueError:
        return None


# ── Step 2: Parse Incident Reports ──────────────────────────────────────────
TOW_RE = re.compile(r"\b(?:tow(?:ed|ing)?|vehicle\s+towed|was\s+towed)\b", re.IGNORECASE)


def clean_plate(raw):
    if not raw:
        return None
    plate = re.sub(r"^LP\s*#\s*", "", raw.strip(), flags=re.IGNORECASE)
    plate = re.sub(r"\s+", "", plate).upper()
    return plate if plate else None


def parse_incident_pdfs(incident_pdfs):
    towed_pairs   = set()
    warning_pairs = set()

    for bn, fpath in incident_pdfs:
        try:
            with pdfplumber.open(fpath) as pdf:
                page_texts = [(page.extract_text() or "") for page in pdf.pages]
            full_text = "\n".join(page_texts)

            # Resolve a PDF-level date once. Branded PDFs put the date in the
            # page-1 meta table only; later pages (photos, ticket scans) have no
            # date and would otherwise be skipped.
            old_m = re.search(r"(\d{2}/\d{2}/\d{4})", full_text)
            pdf_date = old_m.group(1) if old_m else _branded_date(full_text)

            for text in page_texts:
                if re.match(r"^\s*\d+/\d+\s*$", text.strip()):
                    continue
                page_m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
                date_str = page_m.group(1) if page_m else pdf_date
                if not date_str:
                    continue
                is_towed = bool(TOW_RE.search(text))
                plates = []
                for m in re.finditer(
                    r"LP\s*#\s*([A-Z0-9][A-Z0-9 ]{0,12}[A-Z0-9])(?=\W|\Z)",
                    text, re.IGNORECASE
                ):
                    p = clean_plate(m.group(1))
                    if p:
                        plates.append(p)
                for m in re.finditer(
                    r"license\s+plate\s+([A-Z0-9]{5,8})\b", text, re.IGNORECASE
                ):
                    p = clean_plate(m.group(1))
                    if p:
                        plates.append(p)
                for plate in set(plates):
                    if is_towed:
                        towed_pairs.add((date_str, plate))
                    else:
                        warning_pairs.add((date_str, plate))
        except Exception as e:
            log.warning(f"Could not parse incident PDF {bn}: {e}")

    warning_pairs -= towed_pairs   # towed takes priority
    return towed_pairs, warning_pairs


# ── Step 3: Parse Guest Parking PDFs ─────────────────────────────────────────
def clean_permit(raw):
    if raw is None:
        return None
    r = raw.upper().strip()
    if "HANDICAP" in r or "HAND" in r:
        return "HANDICAP"
    if "NO PERMIT" in r or "NOPERM" in r:
        return "NO PERMIT"
    if r in ("N/A", "N-A", "NA"):
        return None
    digits = re.sub(r"[^0-9]", "", r)
    if digits:
        return int(digits)
    return None


LP_HEADER_RE = re.compile(
    r"License\s+Plate\s*[#&]?\s*"
    r"(?:(?:Parking\s+)?(?:Pass|Permit)(?:\s+Number)?\s*[:#]?|"
    r"&\s*Parking\s+Permit(?:\s+Number)?\s*[:#]?)?",
    re.IGNORECASE,
)
BLOCK_END_RE = re.compile(
    r"^\s*(?:pictures?|\d+/\d+|harbor\s+lights|license\s+plate"
    r"|[\w\s]+guest\s+parking|\s*)$",
    re.IGNORECASE,
)


def flush_lp_block(lp_lines):
    """Parse accumulated LP text lines; return [(plate, permit), ...]."""
    results, seen = [], set()
    raw = " ".join(lp_lines)
    raw = re.sub(r"\.\s*\(", " (", raw)
    raw = re.sub(r"LP\s*#\s*", "LP#", raw, flags=re.IGNORECASE)

    for m in re.finditer(r"LP#([A-Z0-9\s]{2,20}?)\s*\(+([^)]+)\)", raw, re.IGNORECASE):
        plate  = clean_plate(m.group(1))
        permit = clean_permit(m.group(2))
        if plate and plate not in seen:
            results.append((plate, permit))
            seen.add(plate)

    for m in re.finditer(r"LP#([A-Z0-9\s]{2,20}?)(?=\s*(?:LP#|\Z))", raw, re.IGNORECASE):
        plate = clean_plate(m.group(1))
        if plate and plate not in seen:
            results.append((plate, None))
            seen.add(plate)

    # New format: PLATE/PERMIT (e.g. "6WAE699/082", "9JFT175/handicap", "7NRR195/N-A")
    for m in re.finditer(r"\b([A-Z0-9]{4,10})/([A-Za-z0-9-]+)", raw):
        plate  = clean_plate(m.group(1))
        permit = clean_permit(m.group(2))
        if not plate:
            continue
        if plate in seen:
            for idx, existing in enumerate(results):
                if existing[0] == plate and existing[1] is None and permit is not None:
                    results[idx] = (plate, permit)
            continue
        results.append((plate, permit))
        seen.add(plate)

    for m in re.finditer(r"\b([A-Z0-9]{5,8})\b", raw, re.IGNORECASE):
        token = m.group(1).upper()
        if (token not in seen
                and not token.startswith("LP")
                and any(c.isdigit() for c in token)
                and any(c.isalpha() for c in token)):
            results.append((token, None))
            seen.add(token)

    for m in re.finditer(r"([A-Z0-9]{5,8})\s*\(+([^)]+)\)", raw, re.IGNORECASE):
        plate  = m.group(1).upper()
        permit = clean_permit(m.group(2))
        if any(c.isdigit() for c in plate) and any(c.isalpha() for c in plate):
            existing = next((i for i, r in enumerate(results) if r[0] == plate), None)
            if existing is not None:
                results[existing] = (plate, permit)
            elif plate not in seen:
                results.append((plate, permit))
                seen.add(plate)
    return results


def _parse_branded_guest_text(full_text):
    """Extract (date_str, [(plate, permit), ...]) from a branded GUEST PARKING CHECK PDF.

    New format (post-2026-04-02 patrol_automation cutover): date appears once
    in the meta table ("Date April 15, 2026") and plate lists are prefixed
    with "Plates: ...". Officers enter plates in several shapes on the same
    day — `LP#PLATE(PERMIT)`, `PLATE/PERMIT`, or bare plates — so delegate
    the actual plate+permit parsing to flush_lp_block(), which already
    handles every format the old PDFs used and filters out non-plate words.
    """
    date_str = _branded_date(full_text)
    if not date_str:
        return None, []
    plate_text_lines = BRANDED_PLATES_LINE_RE.findall(full_text)
    if not plate_text_lines:
        return date_str, []
    return date_str, flush_lp_block(plate_text_lines)


def parse_guest_pdfs(guest_pdfs):
    parking_data = {}

    for bn, fpath in guest_pdfs:
        try:
            with pdfplumber.open(fpath) as pdf:
                full_text = ""
                for page in pdf.pages:
                    full_text += (page.extract_text() or "") + "\n"

            # New branded format (post-2026-04-02). Keep the legacy path below
            # for any pre-cutover PDFs or manual re-parses.
            if _is_branded(full_text):
                date_str, entries = _parse_branded_guest_text(full_text)
                if date_str and entries:
                    parking_data.setdefault(date_str, []).extend(entries)
                elif date_str:
                    log.info(f"Branded guest parking PDF {bn}: no plates on {date_str} (no vehicles found)")
                else:
                    log.warning(f"Branded guest parking PDF {bn}: could not resolve date")
                continue

            current_date = None
            sections = re.split(r"Harbor Lights HOA Guest Parking\n", full_text)
            for section in sections:
                if not section.strip():
                    continue
                date_m = re.search(r"(\d{2}/\d{2}/\d{4})", section)
                if date_m:
                    current_date = date_m.group(1)
                if not current_date:
                    continue

                lines, in_lp_block, lp_lines, entries = section.splitlines(), False, [], []
                for line in lines:
                    if LP_HEADER_RE.search(line):
                        if lp_lines:
                            entries.extend(flush_lp_block(lp_lines))
                            lp_lines = []
                        in_lp_block = True
                        data_part = LP_HEADER_RE.sub("", line).strip()
                        if data_part:
                            lp_lines.append(data_part)
                    elif in_lp_block:
                        if BLOCK_END_RE.match(line):
                            entries.extend(flush_lp_block(lp_lines))
                            lp_lines, in_lp_block = [], False
                        else:
                            stripped = line.strip()
                            if stripped:
                                lp_lines.append(stripped)
                if lp_lines:
                    entries.extend(flush_lp_block(lp_lines))

                for plate, permit in entries:
                    parking_data.setdefault(current_date, []).append((plate, permit))

        except Exception as e:
            log.warning(f"Could not parse guest parking PDF {bn}: {e}")

    return parking_data


# ── Step 4–6: Google Sheets update ──────────────────────────────────────────
# Storage cut over from openpyxl/xlsx → Google Sheets in 2026-04. All formatting,
# year-tab management, and dashboard recompute lives in sheets_client.py now.

def update_sheets(all_rows):
    """Append new rows to the current year's worksheet and recompute the
    Dashboard tab. Returns True on success.

    `all_rows` is a list of (datetime, plate, permit_or_status) tuples,
    sorted ascending by date. The first row of each date-group carries the
    date; subsequent rows in the same group set date=None as a continuation.
    """
    spreadsheet = open_sheet()
    current_year = datetime.now().year
    ws = get_or_create_year_sheet(spreadsheet, current_year)

    # Mark continuation rows (same-date-as-previous) by stripping the date
    # from all but the first row of each date group — matches the original
    # Excel write behavior so the visual reads as grouped blocks.
    flagged: list[tuple] = []
    last_date = None
    for date_obj, plate, permit in all_rows:
        d = date_obj if date_obj != last_date else None
        flagged.append((d, plate, permit))
        last_date = date_obj

    append_data_rows(ws, flagged)
    refresh_dashboard(spreadsheet)
    log.info(f"Sheets updated -> tab '{current_year}' + Dashboard refreshed")
    return True


# Status keys (no formatting — sheets_client owns formatting now). Kept
# at module level for back-compat in case anything imports them.
STATUS_STYLE_KEYS = ("TOWED", "WARNING", "EXPIRED", "OUTSIDER", "HANDICAP", "NO PERMIT")


def _LEGACY_OPENPYXL_REMOVED():
    """All openpyxl-based write/dashboard helpers were removed during the
    Sheets migration. Left as a marker for grep purposes."""
    pass


# Legacy openpyxl write/dashboard helpers were here. Removed during the
# 2026-04 Sheets migration. See sheets_client.py for the replacement, and
# git history (or rebuild_excel.py / verify_rebuild.py) if you need the
# original openpyxl logic for a one-shot Excel re-export.



# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    log.info("Harbor Lights update starting")

    # Report health at end of run (imported here to avoid top-level import failure
    # if shared_utils isn't on sys.path when running standalone)
    try:
        sys.path.insert(0, BASE)
        from shared_utils.health_reporter import report_status
    except ImportError:
        report_status = None

    try:
        # Load processed log
        if os.path.exists(PROCESSED_LOG):
            with open(PROCESSED_LOG) as fp:
                processed = set(json.load(fp))
        else:
            processed = set()

        all_pdfs      = find_harbor_pdfs(MORNING_DIR)
        new_pdfs      = [(bn, fp) for bn, fp in all_pdfs if bn not in processed]
        guest_pdfs    = [(bn, fp) for bn, fp in new_pdfs
                         if "guest_parking" in bn.lower() or "guest parking" in bn.lower()]
        incident_pdfs = [(bn, fp) for bn, fp in new_pdfs
                         if "incident_report" in bn.lower() or "incident report" in bn.lower()]

        log.info(f"New Guest Parking PDFs:   {[bn for bn, _ in guest_pdfs]}")
        log.info(f"New Incident Report PDFs: {[bn for bn, _ in incident_pdfs]}")

        if not new_pdfs:
            log.info("No new PDFs found. Nothing to update.")
            if report_status:
                report_status("harbor_lights", "ok", "No new PDFs to process")
            return

        towed_pairs, warning_pairs = parse_incident_pdfs(incident_pdfs)
        parking_data               = parse_guest_pdfs(guest_pdfs)

        # Silent-failure guard: if we processed guest PDFs but extracted zero
        # plates across all of them, the PDF format has likely drifted. The
        # 2026-04-02 branded-PDF cutover hid exactly this bug for 14 days.
        if guest_pdfs and not parking_data:
            err_msg = (
                f"Guest parking PDFs processed but ZERO plates extracted — "
                f"likely PDF format change. PDFs: {[bn for bn, _ in guest_pdfs]}"
            )
            log.error(err_msg)
            if report_status:
                report_status(
                    "harbor_lights", "error",
                    f"{len(guest_pdfs)} guest parking PDF(s) yielded 0 plates — "
                    "format may have changed",
                )

        log.info(f"Towed: {towed_pairs}")
        log.info(f"Warning: {warning_pairs}")
        log.info("Guest parking data:")
        for d, entries in sorted(parking_data.items()):
            log.info(f"  {d}: {entries}")

        # Build rows
        all_rows  = []
        all_dates = sorted(set(
            list(parking_data.keys())
            + [d for d, _ in towed_pairs]
            + [d for d, _ in warning_pairs]
        ))

        for date_str in all_dates:
            try:
                date_obj = datetime.strptime(date_str, "%m/%d/%Y")
            except ValueError:
                continue
            entries, seen_plates = parking_data.get(date_str, []), set()
            for plate, permit in entries:
                if (date_str, plate) in towed_pairs:
                    permit = "TOWED"
                elif (date_str, plate) in warning_pairs:
                    permit = "WARNING"
                all_rows.append((date_obj, plate, permit))
                seen_plates.add(plate)
            for d, plate in towed_pairs:
                if d == date_str and plate not in seen_plates:
                    all_rows.append((date_obj, plate, "TOWED"))
                    seen_plates.add(plate)
            for d, plate in warning_pairs:
                if d == date_str and plate not in seen_plates:
                    all_rows.append((date_obj, plate, "WARNING"))
                    seen_plates.add(plate)

        all_rows.sort(key=lambda x: x[0])
        towed_count   = sum(1 for r in all_rows if r[2] == "TOWED")
        warning_count = sum(1 for r in all_rows if r[2] == "WARNING")
        log.info(f"Rows to insert: {len(all_rows)}  (TOWED={towed_count}, WARNING={warning_count})")

        # Write Sheets (Excel cut over to Google Sheets 2026-04 — see sheets_client.py)
        update_sheets(all_rows)

        # Update processed log
        newly_processed = sorted(set(list(processed) + [bn for bn, _ in (guest_pdfs + incident_pdfs)]))
        with open(PROCESSED_LOG, "w") as fp:
            json.dump(newly_processed, fp, indent=2)

        log.info(f"Harbor Lights Update complete — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        log.info(f"  Guest PDFs processed  : {len(guest_pdfs)}")
        log.info(f"  Incident PDFs processed: {len(incident_pdfs)}")
        log.info(f"  Rows added to Excel   : {len(all_rows)}")
        log.info(f"    Towed flags         : {towed_count}")
        log.info(f"    Warning flags       : {warning_count}")
        log.info("  Dashboard refreshed   : Yes")

        if report_status:
            report_status(
                "harbor_lights", "ok",
                f"{len(all_rows)} rows added (towed={towed_count}, warning={warning_count})",
                metrics={"rows_added": len(all_rows), "towed": towed_count, "warnings": warning_count},
            )

    except OSError as e:
        # File-system issues: PDF read failure, processed-log write failure, etc.
        log.error(f"ERROR: File access failed: {e}")
        if report_status:
            report_status("harbor_lights", "error", str(e))
        sys.exit(1)

    except Exception as e:
        log.error(f"ERROR: Unexpected failure: {e}", exc_info=True)
        if report_status:
            report_status("harbor_lights", "error", str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
