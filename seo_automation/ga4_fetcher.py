"""
Americal Patrol - GA4 Data Fetcher
Pulls page traffic and traffic source data from Google Analytics 4.
Returns structured dicts for current week and prior week.
"""

import json
from datetime import date, timedelta
from pathlib import Path

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)

from auth_setup import get_credentials

SCRIPT_DIR = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_client():
    """Build GA4 client using authorized credentials."""
    creds = get_credentials()
    return BetaAnalyticsDataClient(credentials=creds)


def _date_range_strings(days_back_start: int, days_back_end: int):
    """Return (start_date_str, end_date_str) for an offset window."""
    today = date.today()
    end   = today - timedelta(days=days_back_end)
    start = today - timedelta(days=days_back_start)
    return start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')


def fetch_page_traffic(property_id: str, lookback_days: int = 7) -> dict:
    """
    Fetch top pages by sessions for current week and prior week.
    Returns:
      {
        "current": [{"page": "/path", "title": "...", "sessions": N, "pageviews": N}, ...],
        "previous": [...]
      }
    """
    client = _get_client()

    # Current week: last N days
    cur_start, cur_end = _date_range_strings(lookback_days, 1)
    # Prior week: N*2 days ago to N+1 days ago
    prev_start, prev_end = _date_range_strings(lookback_days * 2, lookback_days + 1)

    def _run(start, end):
        request = RunReportRequest(
            property=property_id,
            dimensions=[
                Dimension(name='pagePath'),
                Dimension(name='pageTitle'),
            ],
            metrics=[
                Metric(name='sessions'),
                Metric(name='screenPageViews'),
            ],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            limit=50,
            order_bys=[
                {'metric': {'metric_name': 'sessions'}, 'desc': True}
            ],
        )
        response = client.run_report(request)
        rows = []
        for row in response.rows:
            rows.append({
                'page':      row.dimension_values[0].value,
                'title':     row.dimension_values[1].value,
                'sessions':  int(row.metric_values[0].value),
                'pageviews': int(row.metric_values[1].value),
            })
        return rows

    return {
        'current':  _run(cur_start, cur_end),
        'previous': _run(prev_start, prev_end),
        'current_range':  f"{cur_start} to {cur_end}",
        'previous_range': f"{prev_start} to {prev_end}",
    }


def fetch_traffic_sources(property_id: str, lookback_days: int = 7) -> list:
    """
    Fetch sessions by source/medium for the current week.
    Returns list of {"source": "...", "medium": "...", "sessions": N}
    """
    client = _get_client()
    start, end = _date_range_strings(lookback_days, 1)

    request = RunReportRequest(
        property=property_id,
        dimensions=[
            Dimension(name='sessionSource'),
            Dimension(name='sessionMedium'),
        ],
        metrics=[Metric(name='sessions')],
        date_ranges=[DateRange(start_date=start, end_date=end)],
        limit=20,
        order_bys=[
            {'metric': {'metric_name': 'sessions'}, 'desc': True}
        ],
    )
    response = client.run_report(request)
    return [
        {
            'source':   row.dimension_values[0].value,
            'medium':   row.dimension_values[1].value,
            'sessions': int(row.metric_values[0].value),
        }
        for row in response.rows
    ]


def fetch_monthly_traffic(property_id: str) -> dict:
    """
    Fetch 30-day rolling traffic vs prior 30 days for month-over-month view.
    Returns:
      {
        "current_30":  [{"page": ..., "sessions": N, "pageviews": N}, ...],
        "previous_30": [...],
        "current_range": "...",
        "previous_range": "...",
        "totals": {"current_sessions": N, "previous_sessions": N, "change_pct": F}
      }
    """
    client = _get_client()

    cur_start,  cur_end  = _date_range_strings(30, 1)
    prev_start, prev_end = _date_range_strings(60, 31)

    def _run(start, end):
        request = RunReportRequest(
            property=property_id,
            dimensions=[Dimension(name='pagePath'), Dimension(name='pageTitle')],
            metrics=[Metric(name='sessions'), Metric(name='screenPageViews')],
            date_ranges=[DateRange(start_date=start, end_date=end)],
            limit=50,
            order_bys=[{'metric': {'metric_name': 'sessions'}, 'desc': True}],
        )
        response = client.run_report(request)
        rows = []
        for row in response.rows:
            rows.append({
                'page':      row.dimension_values[0].value,
                'title':     row.dimension_values[1].value,
                'sessions':  int(row.metric_values[0].value),
                'pageviews': int(row.metric_values[1].value),
            })
        return rows

    cur_pages  = _run(cur_start, cur_end)
    prev_pages = _run(prev_start, prev_end)

    cur_total  = sum(p['sessions'] for p in cur_pages)
    prev_total = sum(p['sessions'] for p in prev_pages)
    change_pct = round(((cur_total - prev_total) / prev_total) * 100, 1) if prev_total else 0

    return {
        'current_30':    cur_pages,
        'previous_30':   prev_pages,
        'current_range': f"{cur_start} to {cur_end}",
        'previous_range': f"{prev_start} to {prev_end}",
        'totals': {
            'current_sessions':  cur_total,
            'previous_sessions': prev_total,
            'change_pct':        change_pct,
        },
    }


def fetch_all(log=None) -> dict:
    """
    Main entry point — fetches all GA4 data needed for SEO analysis.
    Returns combined dict ready for seo_analyzer.
    """
    config = _load_config()
    property_id   = config['ga4_property_id']
    lookback_days = config.get('lookback_days', 7)

    if log:
        log(f"Fetching GA4 data for {property_id} (last {lookback_days} days)...")

    page_data    = fetch_page_traffic(property_id, lookback_days)
    source_data  = fetch_traffic_sources(property_id, lookback_days)
    monthly_data = fetch_monthly_traffic(property_id)

    if log:
        log(f"GA4: {len(page_data['current'])} pages (current), {len(source_data)} sources")
        chg = monthly_data['totals']['change_pct']
        log(f"GA4 30-day: {monthly_data['totals']['current_sessions']:,} sessions "
            f"({'+'if chg>=0 else ''}{chg}% MoM)")

    return {
        'pages':   page_data,
        'sources': source_data,
        'monthly': monthly_data,
    }
