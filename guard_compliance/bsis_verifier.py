# guard_compliance/bsis_verifier.py
"""
BSIS License Auto-Verification via DCA public data.

The California Department of Consumer Affairs publishes monthly CSV files
of all BSIS licensees at:
    https://www.dca.ca.gov/consumers/public_info/index.shtml
    → "Security And Investigative Services" folder

This module downloads the CSV monthly, caches it locally, and verifies
each officer's guard card number against state records.
"""

import csv
import io
import logging
import os
import zipfile
from datetime import datetime
from pathlib import Path

import requests

from shared_utils.retry import with_retry

log = logging.getLogger(__name__)

# DCA publishes licensee data here — the exact URL may need updating
# after first run if the CSV structure differs from expected
DCA_DATA_URL = "https://data.ca.gov/dataset/bureau-of-security-and-investigative-services"
# Fallback: direct CSV download (updated monthly)
DCA_CSV_URL = (
    "https://data.ca.gov/dataset/"
    "bureau-of-security-and-investigative-services/"
    "resource/BSIS-licensees.csv"
)

LAST_DOWNLOAD_FILE = "last_download.txt"

# Expected CSV column names (DCA format — may vary, handled defensively)
COL_LICENSE_NUM = "license_number"
COL_STATUS = "status"
COL_EXPIRY = "expiration_date"
COL_FIRST_NAME = "first_name"
COL_LAST_NAME = "last_name"
COL_LICENSE_TYPE = "license_type"

# Alternate column name patterns to try
COLUMN_ALIASES = {
    COL_LICENSE_NUM: ["license_number", "lic_nbr", "license_no", "license #", "lic_num", "registration_number"],
    COL_STATUS: ["status", "lic_status", "license_status"],
    COL_EXPIRY: ["expiration_date", "exp_date", "expiry_date", "expiry", "lic_exp_date"],
    COL_FIRST_NAME: ["first_name", "fname", "first"],
    COL_LAST_NAME: ["last_name", "lname", "last", "name"],
    COL_LICENSE_TYPE: ["license_type", "lic_type", "type"],
}


def _resolve_column(headers: list[str], field: str) -> str | None:
    """Find the actual column name from headers using aliases."""
    headers_lower = [h.lower().strip() for h in headers]
    for alias in COLUMN_ALIASES.get(field, [field]):
        if alias.lower() in headers_lower:
            return headers[headers_lower.index(alias.lower())]
    return None


def _needs_refresh(bsis_dir: Path) -> bool:
    """Check if BSIS data needs to be re-downloaded (first run of month)."""
    marker = bsis_dir / LAST_DOWNLOAD_FILE
    if not marker.exists():
        return True
    try:
        last = datetime.fromisoformat(marker.read_text().strip())
        return last.month != datetime.now().month or last.year != datetime.now().year
    except (ValueError, OSError):
        return True


def _mark_downloaded(bsis_dir: Path):
    """Write download timestamp marker."""
    marker = bsis_dir / LAST_DOWNLOAD_FILE
    marker.write_text(datetime.now().isoformat())


@with_retry(max_attempts=2, base_delay=10, exceptions=(requests.Timeout, requests.ConnectionError))
def download_bsis_data(bsis_dir: Path, force: bool = False) -> Path | None:
    """
    Download DCA BSIS licensee CSV from public data portal.
    Only downloads on first run of each month (or if forced).
    Returns path to the CSV file, or None on failure.
    """
    bsis_dir.mkdir(parents=True, exist_ok=True)

    if not force and not _needs_refresh(bsis_dir):
        existing = list(bsis_dir.glob("*.csv"))
        if existing:
            log.info(f"BSIS data is current (downloaded this month). Using cached: {existing[0].name}")
            return existing[0]

    log.info("Downloading fresh BSIS licensee data from DCA...")

    try:
        resp = requests.get(DCA_CSV_URL, timeout=120, stream=True)
        resp.raise_for_status()

        content_type = resp.headers.get("Content-Type", "")

        if "zip" in content_type or DCA_CSV_URL.endswith(".zip"):
            # Handle ZIP archives
            z = zipfile.ZipFile(io.BytesIO(resp.content))
            csv_names = [n for n in z.namelist() if n.endswith(".csv")]
            if not csv_names:
                log.error("ZIP archive contains no CSV files")
                return None
            csv_name = csv_names[0]
            csv_path = bsis_dir / "bsis_licensees.csv"
            with open(csv_path, "wb") as f:
                f.write(z.read(csv_name))
        else:
            # Direct CSV download
            csv_path = bsis_dir / "bsis_licensees.csv"
            with open(csv_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)

        file_size = csv_path.stat().st_size
        log.info(f"BSIS data downloaded: {csv_path.name} ({file_size:,} bytes)")
        _mark_downloaded(bsis_dir)
        return csv_path

    except requests.HTTPError as e:
        log.warning(f"DCA download returned HTTP {e.response.status_code}. "
                    f"The URL may have changed — check DCA website manually.")
        return None
    except Exception as e:
        log.warning(f"Failed to download BSIS data: {e}")
        return None


def load_bsis_data(csv_path: Path) -> dict:
    """
    Read the cached DCA CSV into a lookup dict keyed by license number.
    Returns: {license_number: {status, expiry, first_name, last_name, license_type}}
    """
    if not csv_path or not csv_path.exists():
        return {}

    lookup = {}

    try:
        with open(csv_path, "r", encoding="utf-8", errors="replace") as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames or []

            # Resolve actual column names
            col_lic = _resolve_column(headers, COL_LICENSE_NUM)
            col_status = _resolve_column(headers, COL_STATUS)
            col_exp = _resolve_column(headers, COL_EXPIRY)
            col_fname = _resolve_column(headers, COL_FIRST_NAME)
            col_lname = _resolve_column(headers, COL_LAST_NAME)
            col_type = _resolve_column(headers, COL_LICENSE_TYPE)

            if not col_lic:
                log.error(f"Cannot find license number column in CSV. Headers: {headers}")
                return {}

            log.info(f"BSIS CSV columns resolved: lic={col_lic}, status={col_status}, "
                     f"exp={col_exp}, name={col_fname}/{col_lname}")

            for row in reader:
                lic_num = (row.get(col_lic, "") or "").strip().upper()
                if not lic_num:
                    continue

                # Strip common prefixes (G, GRD, etc.) for flexible matching
                clean_num = lic_num.lstrip("G").lstrip("RD").lstrip("0")

                lookup[lic_num] = {
                    "status": (row.get(col_status, "") or "").strip(),
                    "expiry": (row.get(col_exp, "") or "").strip(),
                    "first_name": (row.get(col_fname, "") or "").strip() if col_fname else "",
                    "last_name": (row.get(col_lname, "") or "").strip() if col_lname else "",
                    "license_type": (row.get(col_type, "") or "").strip() if col_type else "",
                }
                # Also index by cleaned number for flexible lookup
                if clean_num != lic_num:
                    lookup[clean_num] = lookup[lic_num]

        log.info(f"Loaded {len(lookup)} BSIS license records")

    except Exception as e:
        log.error(f"Error reading BSIS CSV: {e}")

    return lookup


def _normalize_license_num(num: str) -> list[str]:
    """Generate possible lookup keys for a license number."""
    if not num:
        return []
    num = num.strip().upper()
    variants = [num]
    # Try with/without G prefix
    if num.startswith("G"):
        variants.append(num[1:])
    else:
        variants.append(f"G{num}")
    # Try zero-stripped
    stripped = num.lstrip("G").lstrip("0")
    if stripped not in variants:
        variants.append(stripped)
    return variants


def _names_match(officer_name: str, dca_first: str, dca_last: str) -> bool:
    """Fuzzy name matching — checks if officer name contains DCA first+last."""
    if not officer_name or (not dca_first and not dca_last):
        return True  # Can't verify, assume OK

    officer_lower = officer_name.lower().strip()
    dca_full = f"{dca_first} {dca_last}".lower().strip()

    # Exact match
    if officer_lower == dca_full:
        return True

    # Last name match (most reliable — first names may be abbreviated)
    if dca_last and dca_last.lower() in officer_lower:
        return True

    return False


def verify_officer(card_number: str, officer_name: str,
                   connecteam_expiry: str | None, bsis_data: dict) -> dict:
    """
    Verify an officer's guard card against BSIS/DCA records.

    Returns:
        {
            "verified": True/False/None (None = data unavailable),
            "dca_status": "Active" / "Expired" / "Revoked" / etc.,
            "dca_expiry": "2026-08-15" or None,
            "name_match": True/False,
            "issues": ["list of human-readable issues"]
        }
    """
    result = {
        "verified": None,
        "dca_status": None,
        "dca_expiry": None,
        "name_match": True,
        "issues": [],
    }

    if not bsis_data:
        result["issues"].append("BSIS/DCA data not available — skipping verification")
        return result

    if not card_number:
        result["verified"] = False
        result["issues"].append("No guard card number on file")
        return result

    # Look up the license number (try multiple formats)
    record = None
    for variant in _normalize_license_num(card_number):
        if variant in bsis_data:
            record = bsis_data[variant]
            break

    if not record:
        result["verified"] = False
        result["issues"].append(
            f"Guard card #{card_number} not found in BSIS records "
            f"(may be a typo or newly issued card not yet in monthly data)"
        )
        return result

    # Status check
    dca_status = record["status"]
    result["dca_status"] = dca_status

    status_upper = dca_status.upper()
    if "REVOKE" in status_upper:
        result["verified"] = False
        result["issues"].append(f"BSIS shows card #{card_number} as REVOKED")
    elif "SUSPEND" in status_upper:
        result["verified"] = False
        result["issues"].append(f"BSIS shows card #{card_number} as SUSPENDED")
    elif "CANCEL" in status_upper:
        result["verified"] = False
        result["issues"].append(f"BSIS shows card #{card_number} as CANCELLED")
    elif "EXPIRED" in status_upper or "INACTIVE" in status_upper:
        result["verified"] = False
        result["issues"].append(f"BSIS shows card #{card_number} as {dca_status}")
    elif "ACTIVE" in status_upper or "CURRENT" in status_upper:
        result["verified"] = True
    else:
        result["issues"].append(f"Unknown BSIS status: '{dca_status}'")

    # Expiry date cross-check
    from guard_compliance.connecteam_client import parse_date
    dca_expiry = parse_date(record["expiry"])
    result["dca_expiry"] = dca_expiry

    if dca_expiry and connecteam_expiry and dca_expiry != connecteam_expiry:
        result["issues"].append(
            f"Expiry mismatch: Connecteam says {connecteam_expiry}, "
            f"BSIS says {dca_expiry} — Connecteam may be outdated"
        )

    # Name match
    name_ok = _names_match(officer_name, record["first_name"], record["last_name"])
    result["name_match"] = name_ok
    if not name_ok:
        result["issues"].append(
            f"Name mismatch: Connecteam has '{officer_name}', "
            f"BSIS has '{record['first_name']} {record['last_name']}' — "
            f"possible wrong card number"
        )

    return result
