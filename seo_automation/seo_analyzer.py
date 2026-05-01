"""
Americal Patrol - SEO Analyzer
Processes GA4 and Search Console data to find:
  - Traffic drops (WoW alerts)
  - Keyword gaps (page 2+ with real search demand)
  - Low CTR keywords (ranking well but not getting clicked)
  - Rising pages (gaining momentum)
  - Blog topic opportunities (mapped to city/account_type combinations)

IMPORTANT: All GSC query data is filtered FIRST to remove:
  - Job-seeking queries (guard jobs, careers, how to become, etc.)
  - Guard card / licensing queries (renewal, bsis, etc.)
  - Out-of-service-area geographic terms (bakersfield, sacramento, etc.)
"""

import json
import re
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'

# Known cities in the service area (Ventura, LA, Orange counties)
# Used to detect geography in queries without disqualifying them
SERVICE_AREA_CITIES = [
    # Ventura County
    'ventura', 'oxnard', 'camarillo', 'thousand oaks', 'simi valley',
    'moorpark', 'port hueneme', 'santa paula', 'ojai', 'fillmore',
    'newbury park', 'westlake village',
    # LA County
    'los angeles', 'long beach', 'glendale', 'burbank', 'pasadena',
    'torrance', 'santa monica', 'west hollywood', 'culver city',
    'el monte', 'downey', 'inglewood', 'hawthorne', 'carson',
    'santa clarita', 'valencia', 'palmdale', 'lancaster', 'malibu',
    'chatsworth', 'woodland hills', 'encino', 'van nuys', 'north hollywood',
    'calabasas', 'agoura hills', 'westlake', 'sherman oaks',
    # Orange County
    'anaheim', 'santa ana', 'irvine', 'huntington beach', 'garden grove',
    'fullerton', 'costa mesa', 'mission viejo', 'buena park', 'newport beach',
    'lake forest', 'tustin', 'yorba linda', 'rancho santa margarita',
    'laguna niguel', 'aliso viejo', 'laguna hills', 'dana point',
    # County-level / regional
    'ventura county', 'los angeles county', 'orange county', 'la county',
    'socal', 'southern california', 'so cal',
]


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


# ── Keyword Filtering ────────────────────────────────────────────────────────

def filter_keywords(queries: list, log=None) -> tuple[list, list]:
    """
    Filter GSC queries. Returns (clean_queries, dropped_queries).
    Drops any query matching a junk term or out-of-area location.
    """
    config = _load_config()
    filters = config.get('keyword_filters', {})
    junk_terms     = [t.lower() for t in filters.get('junk_terms', [])]
    out_of_area    = [t.lower() for t in filters.get('out_of_area_terms', [])]

    clean   = []
    dropped = []

    for q in queries:
        query_lower = q['query'].lower()

        # Check junk terms
        junk_match = next((t for t in junk_terms if t in query_lower), None)
        if junk_match:
            dropped.append({'query': q['query'], 'reason': f'junk term: "{junk_match}"'})
            continue

        # Check out-of-area terms
        area_match = next((t for t in out_of_area if t in query_lower), None)
        if area_match:
            dropped.append({'query': q['query'], 'reason': f'out-of-area: "{area_match}"'})
            continue

        clean.append(q)

    if log and dropped:
        log(f"Filtered out {len(dropped)} junk/out-of-area queries "
            f"(kept {len(clean)} of {len(queries)} total)")

    return clean, dropped


# ── Traffic Drop Detection ───────────────────────────────────────────────────

def find_traffic_drops(current_pages: list, previous_pages: list,
                        threshold_pct: float = 30.0) -> list:
    """
    Find GA4 pages where sessions dropped > threshold% WoW.
    Returns list of dicts with page, current_sessions, previous_sessions, drop_pct.
    """
    prev_map = {p['page']: p['sessions'] for p in previous_pages}
    drops = []

    for page in current_pages:
        path    = page['page']
        cur_ses = page['sessions']
        prev_ses = prev_map.get(path, 0)

        if prev_ses == 0:
            continue

        drop_pct = ((prev_ses - cur_ses) / prev_ses) * 100
        if drop_pct >= threshold_pct:
            drops.append({
                'page':             path,
                'title':            page.get('title', ''),
                'current_sessions': cur_ses,
                'previous_sessions': prev_ses,
                'drop_pct':         round(drop_pct, 1),
            })

    drops.sort(key=lambda x: x['drop_pct'], reverse=True)
    return drops


# ── Rising Pages ─────────────────────────────────────────────────────────────

def find_rising_pages(current_pages: list, previous_pages: list,
                       threshold_pct: float = 50.0, min_sessions: int = 5) -> list:
    """
    Find pages that gained > threshold% sessions WoW (and have meaningful traffic).
    """
    prev_map = {p['page']: p['sessions'] for p in previous_pages}
    rising = []

    for page in current_pages:
        path    = page['page']
        cur_ses = page['sessions']
        prev_ses = prev_map.get(path, 0)

        if cur_ses < min_sessions:
            continue
        if prev_ses == 0:
            if cur_ses >= min_sessions:
                rising.append({
                    'page':     path,
                    'title':    page.get('title', ''),
                    'current_sessions':  cur_ses,
                    'previous_sessions': 0,
                    'growth_pct': 999,
                })
            continue

        growth_pct = ((cur_ses - prev_ses) / prev_ses) * 100
        if growth_pct >= threshold_pct:
            rising.append({
                'page':     path,
                'title':    page.get('title', ''),
                'current_sessions':  cur_ses,
                'previous_sessions': prev_ses,
                'growth_pct': round(growth_pct, 1),
            })

    rising.sort(key=lambda x: x['growth_pct'], reverse=True)
    return rising[:10]


# ── Keyword Gap Analysis ─────────────────────────────────────────────────────

def find_keyword_gaps(queries: list, min_impressions: int = 50,
                       min_position: float = 10.0) -> list:
    """
    Queries with significant impressions but ranking on page 2+ (position > 10).
    These are "quick wins" — real demand, just needs stronger content to rank higher.
    Input queries must already be filtered (no junk/out-of-area).
    """
    gaps = [
        q for q in queries
        if q['impressions'] >= min_impressions and q['position'] > min_position
    ]
    gaps.sort(key=lambda x: x['impressions'], reverse=True)
    return gaps[:20]


def find_low_ctr_keywords(queries: list, max_position: float = 10.0,
                            max_ctr: float = 0.02) -> list:
    """
    Queries ranking in top 10 but with CTR < 2%.
    Good ranking, but title/meta description is not compelling enough.
    """
    low_ctr = [
        q for q in queries
        if q['position'] <= max_position and q['ctr'] < max_ctr
        and q['impressions'] >= 20
    ]
    low_ctr.sort(key=lambda x: x['impressions'], reverse=True)
    return low_ctr[:10]


# ── Blog Topic Opportunity Mapping ──────────────────────────────────────────

def _extract_city_account(query: str, blog_topics: list) -> tuple[str | None, str | None]:
    """
    Try to match a GSC query to a city + account type from blog_config.
    Returns (city, account_type) or (None, None) if no match.
    """
    query_lower = query.lower()

    # Map account type keywords to blog account_type values
    account_keywords = {
        'HOA':        ['hoa', 'homeowner', 'homeowners association', 'residential community',
                       'gated community', 'condo', 'townhome'],
        'Commercial': ['commercial', 'office', 'business park', 'corporate', 'property manager'],
        'Industrial': ['industrial', 'warehouse', 'manufacturing', 'distribution', 'logistics'],
        'Retail':     ['retail', 'shopping', 'store', 'mall', 'strip mall', 'restaurant'],
    }

    cities = list({t['city'] for t in blog_topics})
    matched_city    = None
    matched_account = None

    for city in cities:
        if city.lower() in query_lower:
            matched_city = city
            break

    for account_type, keywords in account_keywords.items():
        if any(kw in query_lower for kw in keywords):
            matched_account = account_type
            break

    return matched_city, matched_account


def map_keywords_to_blog_topics(keyword_gaps: list, blog_topics: list,
                                  log=None) -> dict:
    """
    Match keyword gaps to blog city/account_type combinations.
    Returns:
      {
        "priority_topics": [{"city": "...", "account_type": "..."}, ...],
        "keyword_intelligence": {
          "City_AccountType": {
            "primary_keyword": "...",
            "secondary_keywords": [...],
            "impressions": N,
            "position": F,
            "clicks": N,
          }
        }
      }
    """
    intelligence = {}  # key: "City_AccountType"

    for q in keyword_gaps:
        city, account_type = _extract_city_account(q['query'], blog_topics)
        if not city or not account_type:
            continue

        key = f"{city}_{account_type}"
        if key not in intelligence:
            # This is the first (highest-impression) query for this topic
            primary_kw = q['query']
            intelligence[key] = {
                'city':              city,
                'account_type':      account_type,
                'primary_keyword':   primary_kw,
                'secondary_keywords': [],
                'impressions':        q['impressions'],
                'position':           q['position'],
                'clicks':             q['clicks'],
            }
        else:
            # Additional queries for the same topic become secondary keywords
            sec_kws = intelligence[key]['secondary_keywords']
            if len(sec_kws) < 5 and q['query'] not in sec_kws:
                sec_kws.append(q['query'])
            # Accumulate impressions
            intelligence[key]['impressions'] += q['impressions']

    # Build priority_topics list sorted by impressions (highest opportunity first)
    sorted_keys = sorted(
        intelligence.keys(),
        key=lambda k: intelligence[k]['impressions'],
        reverse=True
    )

    priority_topics = [
        {'city': intelligence[k]['city'], 'account_type': intelligence[k]['account_type']}
        for k in sorted_keys[:3]
    ]

    # Clean the intelligence dict to keep only the top 3
    kw_intelligence = {k: intelligence[k] for k in sorted_keys[:3]}

    if log:
        log(f"Mapped {len(kw_intelligence)} blog topic opportunities from keyword gaps")
        for k, v in kw_intelligence.items():
            log(f"  → {k}: {v['impressions']} impressions, pos {v['position']}")

    return {
        'priority_topics':     priority_topics,
        'keyword_intelligence': kw_intelligence,
    }


# ── Missing Page Opportunity Scoring ─────────────────────────────────────────

def _find_missing_page_opportunities(clean_queries: list, current_pages: list,
                                      log=None) -> list:
    """
    Find high-impression keywords where:
    1. We rank outside top 20 (position > 20) — likely no dedicated page
    2. The keyword contains a clear service area or service type

    These represent NEW page opportunities (not just improvements to existing pages).
    Returns list sorted by opportunity score descending.
    """
    # Build set of pages that already exist with meaningful traffic
    existing_paths = {p['page'] for p in current_pages if p['sessions'] >= 2}

    opportunities = []
    seen_concepts = set()

    for q in clean_queries:
        if q['impressions'] < 30:
            continue
        if q['position'] <= 20:
            continue  # Already ranking — this is a gap, not a missing page

        query_lower = q['query'].lower()

        # Check if query contains a specific city or service type
        has_city = any(city in query_lower for city in SERVICE_AREA_CITIES)
        has_service = any(kw in query_lower for kw in [
            'security', 'patrol', 'guard', 'surveillance', 'monitoring',
            'hoa', 'commercial', 'industrial', 'retail', 'parking'
        ])

        if not (has_city and has_service):
            continue

        # Deduplicate similar concepts
        concept = query_lower[:20]
        if concept in seen_concepts:
            continue
        seen_concepts.add(concept)

        # Opportunity score: impressions × (position/50) — higher position = bigger gap
        opp_score = round(q['impressions'] * min(q['position'] / 50, 2))

        opportunities.append({
            'query':       q['query'],
            'impressions': q['impressions'],
            'position':    q['position'],
            'clicks':      q['clicks'],
            'opp_score':   opp_score,
            'recommendation': 'new_page',
        })

    opportunities.sort(key=lambda x: x['opp_score'], reverse=True)

    if log and opportunities:
        log(f"Missing page opportunities: {len(opportunities[:5])} identified")

    return opportunities[:8]


# ── Master Analysis Function ─────────────────────────────────────────────────

def analyze(ga4_data: dict, gsc_data: dict, blog_config: dict, log=None) -> dict:
    """
    Run all analyses. Returns a complete analysis report dict.
    """
    config     = _load_config()
    thresholds = config.get('alert_thresholds', {})

    drop_threshold    = thresholds.get('traffic_drop_pct', 30)
    gap_impressions   = thresholds.get('keyword_gap_impressions_min', 50)
    gap_position      = thresholds.get('keyword_gap_position_min', 10)
    rising_threshold  = thresholds.get('rising_page_growth_pct', 50)

    # ── Step 1: Filter GSC queries ────────────────────────────────────
    raw_queries = gsc_data['queries']['current']
    clean_queries, dropped_queries = filter_keywords(raw_queries, log=log)

    # ── Step 2: Traffic drops (GA4) ───────────────────────────────────
    drops = find_traffic_drops(
        ga4_data['pages']['current'],
        ga4_data['pages']['previous'],
        threshold_pct=drop_threshold,
    )
    if log:
        log(f"Traffic drops detected: {len(drops)}")

    # ── Step 3: Rising pages (GA4) ────────────────────────────────────
    rising = find_rising_pages(
        ga4_data['pages']['current'],
        ga4_data['pages']['previous'],
        threshold_pct=rising_threshold,
    )
    if log:
        log(f"Rising pages detected: {len(rising)}")

    # ── Step 4: Keyword gaps (GSC, filtered) ─────────────────────────
    keyword_gaps = find_keyword_gaps(
        clean_queries,
        min_impressions=gap_impressions,
        min_position=gap_position,
    )
    if log:
        log(f"Keyword gaps found: {len(keyword_gaps)}")

    # ── Step 5: Low CTR keywords (GSC, filtered) ─────────────────────
    low_ctr = find_low_ctr_keywords(clean_queries)
    if log:
        log(f"Low CTR keywords found: {len(low_ctr)}")

    # ── Step 6: Map to blog topics ────────────────────────────────────
    blog_topics = blog_config.get('topics', [])
    topic_opportunities = map_keywords_to_blog_topics(keyword_gaps, blog_topics, log=log)

    # ── Step 7: Opportunity scoring (pages we're missing entirely) ───────
    opportunity_pages = _find_missing_page_opportunities(
        clean_queries, ga4_data['pages']['current'], log=log
    )

    return {
        'traffic_drops':         drops,
        'rising_pages':          rising,
        'keyword_gaps':          keyword_gaps,
        'low_ctr_keywords':      low_ctr,
        'dropped_queries':       dropped_queries,
        'priority_topics':       topic_opportunities['priority_topics'],
        'keyword_intelligence':  topic_opportunities['keyword_intelligence'],
        'top_pages':             ga4_data['pages']['current'][:10],
        'traffic_sources':       ga4_data['sources'][:10],
        'date_range':            ga4_data['pages'].get('current_range', ''),
        'monthly':               ga4_data.get('monthly', {}),
        'core_web_vitals':       gsc_data.get('core_web_vitals', {}),
        'opportunity_pages':     opportunity_pages,
        'all_queries':           clean_queries,
        'combined_queries':      gsc_data.get('combined', []),
    }
