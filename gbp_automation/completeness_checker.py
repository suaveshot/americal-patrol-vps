"""
Americal Patrol - GBP Completeness Checker
Reads the live GBP listing and flags any missing or thin fields.

Returns a scored checklist used by the weekly digest email.
"""

import json
from pathlib import Path

from google.auth.transport.requests import AuthorizedSession

from auth_setup import get_credentials

SCRIPT_DIR    = Path(__file__).parent
CONFIG_FILE   = SCRIPT_DIR / 'gbp_config.json'
BUSI_INFO_URL = 'https://mybusinessbusinessinformation.googleapis.com/v1'

READ_MASK = ','.join([
    'name', 'title', 'phoneNumbers', 'websiteUri', 'regularHours',
    'profile', 'categories', 'storefrontAddress', 'openInfo',
])


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _authed_session() -> AuthorizedSession:
    return AuthorizedSession(get_credentials())


def check_completeness(log=None) -> dict:
    """
    Fetches the live GBP listing and returns a completeness report:
    {
        'score':  int (0-100),
        'issues': [str, ...],
        'fields': {field_name: {'status': 'ok'|'missing'|'thin', 'value': str}},
        'raw':    dict (full API response)
    }
    """
    config      = _load_config()
    account_id  = config.get('account_id', '').strip()
    location_id = config.get('location_id', '').strip()

    if not account_id or not location_id:
        msg = 'account_id or location_id not set in gbp_config.json — skipping completeness check'
        if log: log(f'WARNING: {msg}')
        return {'score': 0, 'issues': [msg], 'fields': {}, 'raw': {}}

    session       = _authed_session()
    location_name = f'{account_id}/{location_id}'
    resp          = session.get(
        f'{BUSI_INFO_URL}/{location_name}',
        params={'readMask': READ_MASK}
    )
    resp.raise_for_status()
    data = resp.json()

    issues = []
    fields = {}

    # Business name
    title = data.get('title', '').strip()
    fields['business_name'] = {'status': 'ok' if title else 'missing', 'value': title}
    if not title:
        issues.append('Business name is missing')

    # Primary phone
    phone = data.get('phoneNumbers', {}).get('primaryPhone', '').strip()
    fields['phone'] = {'status': 'ok' if phone else 'missing', 'value': phone}
    if not phone:
        issues.append('Primary phone number is missing')

    # Website
    website = data.get('websiteUri', '').strip()
    fields['website'] = {'status': 'ok' if website else 'missing', 'value': website}
    if not website:
        issues.append('Website URL is missing')

    # Description
    description = data.get('profile', {}).get('description', '').strip()
    if not description:
        fields['description'] = {'status': 'missing', 'value': ''}
        issues.append('Business description is missing — key completeness and keyword signal')
    elif len(description) < 150:
        fields['description'] = {'status': 'thin', 'value': description}
        issues.append(f'Description is thin ({len(description)} chars) — aim for 250+ chars')
    else:
        fields['description'] = {'status': 'ok', 'value': description[:80] + '...'}

    # Hours
    hours = data.get('regularHours', {}).get('periods', [])
    if not hours:
        fields['hours'] = {'status': 'missing', 'value': None}
        issues.append('Business hours not set')
    else:
        fields['hours'] = {'status': 'ok', 'value': f'{len(hours)} day(s) configured'}

    # Primary category
    primary_cat = data.get('categories', {}).get('primaryCategory', {}).get('displayName', '').strip()
    fields['primary_category'] = {'status': 'ok' if primary_cat else 'missing', 'value': primary_cat}
    if not primary_cat:
        issues.append('Primary business category not set — critical for local search ranking')

    # Additional categories
    add_cats = data.get('categories', {}).get('additionalCategories', [])
    fields['additional_categories'] = {
        'status': 'ok' if add_cats else 'thin',
        'value': ', '.join(c.get('displayName', '') for c in add_cats) or 'None'
    }
    if not add_cats:
        issues.append('No additional categories — add relevant secondary categories to improve reach')

    # Address
    addr     = data.get('storefrontAddress', {})
    has_addr = bool(addr.get('addressLines') or addr.get('locality'))
    fields['address'] = {'status': 'ok' if has_addr else 'missing', 'value': addr.get('locality', '')}
    if not has_addr:
        issues.append('Storefront address is missing')

    # Open status
    open_status = data.get('openInfo', {}).get('status', '')
    fields['open_status'] = {
        'status': 'ok' if open_status == 'OPEN' else 'thin',
        'value': open_status or 'not set'
    }
    if open_status != 'OPEN':
        issues.append(f'Business open status is "{open_status or "not set"}" — verify this is correct')

    # Score: deduct per issue severity
    deductions = {'missing': 12, 'thin': 5}
    score = 100
    for f_data in fields.values():
        score -= deductions.get(f_data['status'], 0)
    score = max(0, score)

    if log:
        log(f'GBP completeness score: {score}/100 — {len(issues)} issue(s) found')
        for issue in issues:
            log(f'  ISSUE: {issue}')

    return {'score': score, 'issues': issues, 'fields': fields, 'raw': data}
