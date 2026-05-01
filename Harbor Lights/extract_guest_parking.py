#!/usr/bin/env python3
"""
Agent 1 — Extract guest parking data from the cumulative PDF.
Reads shared_config.json, parses the guest parking PDF, and writes
extracted_guest_parking.json: list of {date, plate, permit} records.
"""

import json
import os
import re
import sys

import pdfplumber

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, "shared_config.json")


def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


# ── Cleaning helpers (mirrors hl_update.py) ───────────────────────────────────

def clean_plate(raw):
    if not raw:
        return None
    plate = re.sub(r"^LP\s*#\s*", "", raw.strip(), flags=re.IGNORECASE)
    plate = re.sub(r"\s+", "", plate).upper()
    return plate if plate else None


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


# ── LP block parsing (mirrors hl_update.py flush_lp_block) ────────────────────

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
    results, seen = [], set()
    raw = " ".join(lp_lines)
    raw = re.sub(r"\.\s*\(", " (", raw)
    raw = re.sub(r"LP\s*#\s*", "LP#", raw, flags=re.IGNORECASE)

    for m in re.finditer(r"LP#([A-Z0-9\s]{2,20}?)\s*\(+([^)]+)\)", raw, re.IGNORECASE):
        plate = clean_plate(m.group(1))
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
            # Update permit if we now have one and previously had None
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
        plate = m.group(1).upper()
        permit = clean_permit(m.group(2))
        if any(c.isdigit() for c in plate) and any(c.isalpha() for c in plate):
            existing = next((i for i, r in enumerate(results) if r[0] == plate), None)
            if existing is not None:
                results[existing] = (plate, permit)
            elif plate not in seen:
                results.append((plate, permit))
                seen.add(plate)
    return results


def parse_guest_pdf(fpath):
    """Parse cumulative guest parking PDF. Returns list of {date, plate, permit}."""
    records = []
    print(f"  Opening guest parking PDF: {os.path.basename(fpath)}")
    with pdfplumber.open(fpath) as pdf:
        total = len(pdf.pages)
        print(f"  Total pages: {total}")
        full_text = ""
        for i, page in enumerate(pdf.pages):
            if i % 100 == 0:
                print(f"  Reading page {i+1}/{total}...")
            full_text += (page.extract_text() or "") + "\n"

    print("  PDF read complete. Parsing entries...")
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
            records.append({
                "date": current_date,
                "plate": plate,
                "permit": str(permit) if permit is not None else None,
            })

    return records


def main():
    cfg = load_config()
    pdf_path = os.path.join(cfg["base_dir"], cfg["guest_parking_pdf"])
    out_path = os.path.join(cfg["base_dir"], cfg["guest_parking_output"])

    if not os.path.exists(pdf_path):
        print(f"ERROR: PDF not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[Agent 1] Extracting guest parking data from PDF...")
    records = parse_guest_pdf(pdf_path)
    print(f"[Agent 1] Extracted {len(records)} guest parking entries.")

    with open(out_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"[Agent 1] Saved -> {out_path}")


if __name__ == "__main__":
    main()
