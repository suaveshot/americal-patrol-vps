#!/usr/bin/env python3
"""
Agent 2 — Extract incident data from the cumulative incident report PDF.
Reads shared_config.json, parses the incident PDF, and writes
extracted_incidents.json: list of {date, plate, type} records
where type is "TOWED" or "WARNING".
"""

import json
import os
import re
import sys

import pdfplumber

BASE = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE, "shared_config.json")

TOW_RE = re.compile(r"\b(?:tow(?:ed|ing)?|vehicle\s+towed|was\s+towed)\b", re.IGNORECASE)


def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)


def clean_plate(raw):
    if not raw:
        return None
    plate = re.sub(r"^LP\s*#\s*", "", raw.strip(), flags=re.IGNORECASE)
    plate = re.sub(r"\s+", "", plate).upper()
    return plate if plate else None


def parse_incident_pdf(fpath):
    """Parse cumulative incident PDF. Returns (towed_pairs, warning_pairs) as sets of (date, plate)."""
    towed_pairs = set()
    warning_pairs = set()

    print(f"  Opening incident report PDF: {os.path.basename(fpath)}")
    with pdfplumber.open(fpath) as pdf:
        total = len(pdf.pages)
        print(f"  Total pages: {total}")
        for i, page in enumerate(pdf.pages):
            if i % 100 == 0:
                print(f"  Reading page {i+1}/{total}...")
            text = page.extract_text() or ""
            if re.match(r"^\s*\d+/\d+\s*$", text.strip()):
                continue
            date_m = re.search(r"(\d{2}/\d{2}/\d{4})", text)
            if not date_m:
                continue
            date_str = date_m.group(1)
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

    # Towed takes priority over warnings
    warning_pairs -= towed_pairs
    return towed_pairs, warning_pairs


def main():
    cfg = load_config()
    pdf_path = os.path.join(cfg["base_dir"], cfg["incident_pdf"])
    out_path = os.path.join(cfg["base_dir"], cfg["incidents_output"])

    if not os.path.exists(pdf_path):
        print(f"ERROR: PDF not found: {pdf_path}", file=sys.stderr)
        sys.exit(1)

    print(f"[Agent 2] Extracting incident data from PDF...")
    towed_pairs, warning_pairs = parse_incident_pdf(pdf_path)
    print(f"[Agent 2] Extracted {len(towed_pairs)} towed + {len(warning_pairs)} warning incidents.")

    records = []
    for date_str, plate in sorted(towed_pairs):
        records.append({"date": date_str, "plate": plate, "type": "TOWED"})
    for date_str, plate in sorted(warning_pairs):
        records.append({"date": date_str, "plate": plate, "type": "WARNING"})

    with open(out_path, "w") as f:
        json.dump(records, f, indent=2)
    print(f"[Agent 2] Saved -> {out_path}")


if __name__ == "__main__":
    main()
