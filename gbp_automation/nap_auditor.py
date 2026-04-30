"""
Americal Patrol - NAP Consistency Auditor
Checks that Name, Address, Phone on each directory listing URL matches
the master NAP stored in gbp_config.json.

Add your listing URLs to gbp_config.json > directory_listings once,
and this module will monitor them on every weekly run.
"""

import json
import re
from pathlib import Path

import requests

SCRIPT_DIR      = Path(__file__).parent
CONFIG_FILE     = SCRIPT_DIR / 'gbp_config.json'
REQUEST_TIMEOUT = 12

# Browser-like headers to reduce bot-blocking
_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
}


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _normalize_phone(phone: str) -> str:
    """Strip all non-digits for comparison."""
    return re.sub(r'\D', '', phone)


def _find_phone_in_text(text: str, correct_digits: str) -> tuple:
    """
    Scans page text for 10-digit phone numbers.
    Returns (found_correct: bool, first_other_phone_found: str|None).
    """
    candidates = re.findall(r'[\d\(\)][(\d\s\-\.\(\))]{7,}[\d\)]', text)
    normalized = [re.sub(r'\D', '', p) for p in candidates]
    normalized = [p for p in normalized if len(p) == 10]

    if correct_digits in normalized:
        return True, None

    for p in normalized:
        return False, p

    return False, None


def _find_name_in_text(text: str, business_name: str) -> bool:
    """Case-insensitive check for the core business name (ignores ', Inc.')."""
    core = business_name.replace(', Inc.', '').replace(',Inc.', '').strip().lower()
    return core in text.lower()


def audit_directories(log=None) -> dict:
    """
    Checks each configured directory listing URL for NAP consistency.
    Returns:
    {
        'master_nap': {...},
        'results':   {platform: {'status': 'ok'|'mismatch'|'unchecked'|'error', 'detail': str}},
        'issues':    [str, ...],
        'unchecked': [str, ...]
    }
    """
    config     = _load_config()
    master_nap = config.get('master_nap', {})
    listings   = config.get('directory_listings', {})

    correct_digits   = _normalize_phone(master_nap.get('phone', ''))
    correct_display  = master_nap.get('phone', '')
    business_name    = master_nap.get('business_name', 'Americal Patrol')

    results   = {}
    issues    = []
    unchecked = []

    for platform, url in listings.items():
        if not url or not url.strip():
            results[platform] = {'status': 'unchecked', 'detail': 'No listing URL configured'}
            unchecked.append(platform)
            continue

        try:
            resp = requests.get(url, headers=_HEADERS, timeout=REQUEST_TIMEOUT)

            if resp.status_code != 200:
                detail = f'HTTP {resp.status_code} — check URL in gbp_config.json'
                results[platform] = {'status': 'error', 'detail': detail}
                issues.append(f'{platform}: could not fetch listing ({detail})')
                continue

            text     = resp.text
            phone_ok, other_phone = _find_phone_in_text(text, correct_digits)
            name_ok  = _find_name_in_text(text, business_name)

            if phone_ok and name_ok:
                results[platform] = {'status': 'ok', 'detail': f'Name and phone match ({correct_display})'}
                if log: log(f'  {platform}: OK')
            else:
                parts = []
                if not name_ok:
                    parts.append(f'"{business_name}" not found on page')
                if not phone_ok:
                    if other_phone:
                        parts.append(f'phone mismatch — found {other_phone}, expected {correct_digits}')
                    else:
                        parts.append('phone number not found on page')
                detail = '; '.join(parts)
                results[platform] = {'status': 'mismatch', 'detail': detail}
                issues.append(f'{platform}: {detail}')
                if log: log(f'  MISMATCH {platform}: {detail}')

        except requests.exceptions.Timeout:
            results[platform] = {'status': 'error', 'detail': 'Request timed out'}
            if log: log(f'  {platform}: timed out')
        except Exception as e:
            results[platform] = {'status': 'error', 'detail': str(e)}
            if log: log(f'  {platform}: error — {e}')

    if log:
        ok_count = sum(1 for r in results.values() if r['status'] == 'ok')
        log(f'NAP audit complete: {ok_count}/{len(listings)} listings OK, '
            f'{len(issues)} issue(s), {len(unchecked)} unchecked')

    return {
        'master_nap': master_nap,
        'results':    results,
        'issues':     issues,
        'unchecked':  unchecked,
    }
