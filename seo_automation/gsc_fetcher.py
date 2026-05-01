"""
Americal Patrol - Google Search Console Fetcher
Pulls keyword (query) and page-level search performance data.
Uses googleapiclient (already installed for Gmail auth).
"""

import json
import os
from datetime import date, timedelta
from pathlib import Path

from googleapiclient.discovery import build

from auth_setup import get_credentials

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_service():
    creds = get_credentials()
    return build('searchconsole', 'v1', credentials=creds)


def _date_strings(days_back_start: int, days_back_end: int):
    today = date.today()
    # GSC data lags ~3 days
    end   = today - timedelta(days=max(days_back_end, 3))
    start = today - timedelta(days=days_back_start + 3)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


def _query_search_analytics(service, site_url: str, start: str, end: str,
                              dimensions: list, row_limit: int = 500) -> list:
    body = {
        'startDate': start,
        'endDate':   end,
        'dimensions': dimensions,
        'rowLimit':   row_limit,
        'dataState':  'all',
    }
    response = service.searchanalytics().query(siteUrl=site_url, body=body).execute()
    return response.get('rows', [])


def fetch_queries(site_url: str, lookback_days: int = 7) -> dict:
    """
    Fetch search query performance for current week and prior week.
    Returns:
      {
        "current":  [{"query": "...", "clicks": N, "impressions": N, "ctr": F, "position": F}, ...],
        "previous": [...]
      }
    """
    service = _get_service()

    cur_start,  cur_end  = _date_strings(lookback_days, 1)
    prev_start, prev_end = _date_strings(lookback_days * 2, lookback_days + 1)

    def _parse(rows):
        result = []
        for row in rows:
            keys = row.get('keys', [])
            result.append({
                'query':       keys[0] if keys else '',
                'clicks':      row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr':         round(row.get('ctr', 0.0), 4),
                'position':    round(row.get('position', 0.0), 1),
            })
        return result

    current_rows  = _query_search_analytics(service, site_url, cur_start,  cur_end,  ['query'])
    previous_rows = _query_search_analytics(service, site_url, prev_start, prev_end, ['query'])

    return {
        'current':        _parse(current_rows),
        'previous':       _parse(previous_rows),
        'current_range':  f"{cur_start} to {cur_end}",
        'previous_range': f"{prev_start} to {prev_end}",
    }


def fetch_pages(site_url: str, lookback_days: int = 7) -> dict:
    """
    Fetch page-level search performance for current week and prior week.
    """
    service = _get_service()

    cur_start,  cur_end  = _date_strings(lookback_days, 1)
    prev_start, prev_end = _date_strings(lookback_days * 2, lookback_days + 1)

    def _parse(rows):
        result = []
        for row in rows:
            keys = row.get('keys', [])
            result.append({
                'page':        keys[0] if keys else '',
                'clicks':      row.get('clicks', 0),
                'impressions': row.get('impressions', 0),
                'ctr':         round(row.get('ctr', 0.0), 4),
                'position':    round(row.get('position', 0.0), 1),
            })
        return result

    current_rows  = _query_search_analytics(service, site_url, cur_start,  cur_end,  ['page'])
    previous_rows = _query_search_analytics(service, site_url, prev_start, prev_end, ['page'])

    return {
        'current':  _parse(current_rows),
        'previous': _parse(previous_rows),
    }


def fetch_core_web_vitals(site_url: str, log=None) -> dict:
    """
    Fetch Core Web Vitals via Chrome UX Report (CrUX) API.
    Requires crux_api_key in seo_config.json (free Google API key with CrUX enabled).
    Returns dict of metric scores or empty dict if not configured.
    """
    config  = _load_config()
    api_key = os.environ.get('CRUX_API_KEY', config.get('crux_api_key', '')).strip()

    if not api_key:
        if log:
            log("Core Web Vitals skipped — no crux_api_key in seo_config.json "
                "(enable Chrome UX Report API in GCP and add your key to enable this)")
        return {}

    # Clean domain from sc-domain: prefix or URL
    domain = site_url.replace('sc-domain:', '').replace('https://', '').replace('http://', '').rstrip('/')

    url  = f"https://chromeuxreport.googleapis.com/v1/records:queryRecord?key={api_key}"
    body = {"origin": f"https://{domain}", "formFactor": "PHONE"}

    try:
        import requests
        resp = requests.post(url, json=body, timeout=10)
        resp.raise_for_status()
        data    = resp.json()
        metrics = data.get('record', {}).get('metrics', {})

        def _extract(metric_name: str) -> dict:
            m = metrics.get(metric_name, {})
            hist = m.get('histogram', [])
            p75  = m.get('percentiles', {}).get('p75')
            good = next((b['density'] for b in hist if b.get('start') == 0), 0)
            return {'p75': p75, 'good_pct': round((good or 0) * 100, 1)}

        result = {
            'lcp':  _extract('largest_contentful_paint'),
            'fid':  _extract('first_input_delay'),
            'cls':  _extract('cumulative_layout_shift'),
            'fcp':  _extract('first_contentful_paint'),
            'ttfb': _extract('experimental_time_to_first_byte'),
        }
        if log:
            lcp = result['lcp'].get('p75', 'N/A')
            log(f"Core Web Vitals: LCP p75={lcp}ms, CLS p75={result['cls'].get('p75', 'N/A')}")
        return result

    except Exception as e:
        if log:
            if '404' in str(e):
                log("Core Web Vitals: not enough Chrome traffic data yet for americalpatrol.com "
                    "(CrUX requires minimum traffic threshold — will populate as site grows)")
            else:
                log(f"Core Web Vitals fetch failed (non-fatal): {e}")
        return {}


def fetch_query_page_combined(site_url: str, lookback_days: int = 7) -> list:
    """
    Fetch queries WITH their associated page URLs (query+page dimension combo).
    Used by page_scorer to get per-page CTR and position from GSC.
    Returns list of {"query": ..., "page": ..., "clicks": N, "impressions": N, "ctr": F, "position": F}
    """
    service = _get_service()
    start, end = _date_strings(lookback_days, 1)
    rows = _query_search_analytics(service, site_url, start, end,
                                   ['query', 'page'], row_limit=1000)
    result = []
    for row in rows:
        keys = row.get('keys', [])
        result.append({
            'query':       keys[0] if len(keys) > 0 else '',
            'page':        keys[1] if len(keys) > 1 else '',
            'clicks':      row.get('clicks', 0),
            'impressions': row.get('impressions', 0),
            'ctr':         round(row.get('ctr', 0.0), 4),
            'position':    round(row.get('position', 0.0), 1),
        })
    return result


def fetch_all(log=None) -> dict:
    """
    Main entry point — fetches all GSC data needed for SEO analysis.
    Returns combined dict ready for seo_analyzer.
    """
    config        = _load_config()
    site_url      = config['gsc_site_url']
    lookback_days = config.get('lookback_days', 7)

    if log:
        log(f"Fetching Search Console data for {site_url} (last {lookback_days} days)...")

    query_data    = fetch_queries(site_url, lookback_days)
    page_data     = fetch_pages(site_url, lookback_days)
    combined_data = fetch_query_page_combined(site_url, lookback_days)
    cwv_data      = fetch_core_web_vitals(site_url, log=log)

    if log:
        log(f"GSC: {len(query_data['current'])} queries, {len(page_data['current'])} pages (current week)")

    return {
        'queries':    query_data,
        'pages':      page_data,
        'combined':   combined_data,
        'core_web_vitals': cwv_data,
    }
