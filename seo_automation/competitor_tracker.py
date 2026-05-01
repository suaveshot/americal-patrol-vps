"""
Americal Patrol - Competitor Tracker
Checks where competitor domains rank vs Americal Patrol for target keywords,
broken out by county (Ventura, LA, Orange).

Uses DataForSEO SERP API (https://dataforseo.com).
Pricing: ~$0.0006 per task (15 keywords/week = ~$0.04/month).

Setup:
  1. Sign up at https://app.dataforseo.com/register
  2. Get your login (email) and password from the dashboard
  3. Add to seo_config.json:
       "dataforseo_login": "your@email.com",
       "dataforseo_password": "your_api_password"
"""

import base64
import json
import os
import time
from pathlib import Path

import requests

SCRIPT_DIR    = Path(__file__).parent
CONFIG_FILE   = SCRIPT_DIR / 'seo_config.json'
OUR_DOMAIN    = 'americalpatrol.com'
DFS_BASE_URL  = 'https://api.dataforseo.com/v3/serp/google/organic/live/advanced'


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _auth_header(login: str, password: str) -> dict:
    token = base64.b64encode(f"{login}:{password}".encode()).decode()
    return {'Authorization': f'Basic {token}', 'Content-Type': 'application/json'}


def _find_rank(items: list, domain: str) -> int | None:
    """Return 1-based rank if domain appears in organic results, else None."""
    for item in items:
        if item.get('type') != 'organic':
            continue
        url = item.get('url', '')
        if domain.lower() in url.lower():
            return item.get('rank_absolute')
    return None


def _search_keyword(keyword: str, login: str, password: str,
                    location_code: int = 2840) -> list:
    """
    Call DataForSEO live SERP API for one keyword.
    location_code 2840 = United States.
    Returns list of result items.
    """
    payload = [{
        'keyword':       keyword,
        'location_code': location_code,
        'language_code': 'en',
        'depth':         20,
        'se_domain':     'google.com',
    }]
    try:
        resp = requests.post(
            DFS_BASE_URL,
            headers=_auth_header(login, password),
            json=payload,
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        tasks = data.get('tasks', [])
        if not tasks or tasks[0].get('status_code') != 20000:
            return []
        result = tasks[0].get('result', [])
        if not result:
            return []
        return result[0].get('items', [])
    except Exception:
        return []


def fetch_competitor_rankings(log=None) -> dict:
    """
    For each county, check rankings for all target keywords.
    Returns:
    {
      "ventura_county": {
        "keyword": {
          "americalpatrol.com": rank or None,
          "competitor1.com":    rank or None,
          ...
        }
      },
      ...
    }
    """
    config   = _load_config()
    login    = os.environ.get('DATAFORSEO_LOGIN', config.get('dataforseo_login', '')).strip()
    password = os.environ.get('DATAFORSEO_PASSWORD', config.get('dataforseo_password', '')).strip()
    counties = config.get('competitors', {})

    if not login or not password:
        if log:
            log("Competitor tracking skipped — dataforseo_login/password not set in seo_config.json "
                "(sign up at dataforseo.com ~$0.04/month and add credentials to enable)")
        return {}

    results = {}

    for county, data in counties.items():
        domains        = [OUR_DOMAIN] + data.get('domains', [])
        keywords       = data.get('target_keywords', [])
        county_results = {}

        if log:
            log(f"Competitor tracking: {county} ({len(keywords)} keywords)...")

        for kw in keywords:
            items = _search_keyword(kw, login, password)
            county_results[kw] = {d: _find_rank(items, d) for d in domains}
            time.sleep(0.5)  # be polite to the API

        results[county] = county_results

    if log:
        log(f"Competitor tracking complete: {len(results)} counties analyzed")

    return results


def summarize_for_report(competitor_data: dict) -> dict:
    """
    Build a condensed summary per county for the email report:
    - Keywords where we outrank competitors
    - Keywords where a competitor outranks us
    - Keywords where we don't appear in top 20
    """
    if not competitor_data:
        return {}

    summary = {}

    for county, keywords in competitor_data.items():
        winning, losing, missing = [], [], []

        for kw, rankings in keywords.items():
            our_rank = rankings.get(OUR_DOMAIN)

            if our_rank is None:
                missing.append({'keyword': kw, 'competitors': {
                    d: r for d, r in rankings.items() if d != OUR_DOMAIN and r
                }})
                continue

            beaten_by = {d: r for d, r in rankings.items()
                         if d != OUR_DOMAIN and r and r < our_rank}
            beating   = {d: r for d, r in rankings.items()
                         if d != OUR_DOMAIN and (r is None or r > our_rank)}

            if beaten_by:
                losing.append({'keyword': kw, 'our_rank': our_rank, 'beaten_by': beaten_by})
            else:
                winning.append({'keyword': kw, 'our_rank': our_rank, 'beating': beating})

        summary[county] = {
            'winning': winning,
            'losing':  losing,
            'missing': missing,
        }

    return summary
