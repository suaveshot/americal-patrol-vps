"""
Americal Patrol - Landing Page Performance Scorer
Scores each service page 0-100 based on:
  - CTR score      (30%) — are people clicking when they see us?
  - Position score (40%) — where do we rank on Google?
  - Traffic trend  (30%) — growing, stable, or declining?

Score bands:
  80-100  Healthy — maintain
  60-79   Good — minor improvements needed
  40-59   Fair — needs attention soon
  20-39   Poor — prioritize fixes
  0-19    Critical — urgent action required
"""

from pathlib import Path


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _score_ctr(ctr: float) -> float:
    """CTR → 0-100. Security industry avg ~3-5%."""
    if ctr >= 0.08:  return 100
    if ctr >= 0.05:  return 85
    if ctr >= 0.03:  return 65
    if ctr >= 0.02:  return 45
    if ctr >= 0.01:  return 25
    return 10


def _score_position(pos: float) -> float:
    """Google rank position → 0-100. Top 3 = money."""
    if pos <= 0:    return 0
    if pos <= 1:    return 100
    if pos <= 3:    return 90
    if pos <= 5:    return 75
    if pos <= 10:   return 55
    if pos <= 20:   return 30
    if pos <= 30:   return 15
    return 5


def _score_trend(current: int, previous: int) -> float:
    """WoW traffic trend → 0-100."""
    if previous == 0:
        return 60  # No prior data — neutral
    change_pct = ((current - previous) / previous) * 100
    if change_pct >= 30:   return 100
    if change_pct >= 10:   return 80
    if change_pct >= -5:   return 60   # Stable
    if change_pct >= -20:  return 35
    if change_pct >= -40:  return 15
    return 5


def _label(score: float) -> str:
    if score >= 80: return 'Healthy'
    if score >= 60: return 'Good'
    if score >= 40: return 'Fair'
    if score >= 20: return 'Poor'
    return 'Critical'


def _color(score: float) -> str:
    if score >= 80: return '#2e7d32'
    if score >= 60: return '#558b2f'
    if score >= 40: return '#f57f17'
    if score >= 20: return '#e65100'
    return '#c62828'


# ── Main scoring function ─────────────────────────────────────────────────────

def score_pages(ga4_data: dict, gsc_data: dict) -> list:
    """
    Combine GA4 + GSC data to score every page we have data for.
    Returns list of page score dicts sorted by score ascending (worst first).
    """
    weights = {'ctr': 0.30, 'position': 0.40, 'trend': 0.30}

    # Build lookup maps
    cur_pages  = {p['page']: p for p in ga4_data['pages']['current']}
    prev_pages = {p['page']: p for p in ga4_data['pages']['previous']}

    # GSC page data — aggregate per page
    gsc_pages: dict[str, dict] = {}
    for q in gsc_data['queries']['current']:
        page = q.get('page', '')
        if not page:
            continue
        if page not in gsc_pages:
            gsc_pages[page] = {'impressions': 0, 'clicks': 0, 'position_sum': 0, 'count': 0}
        gsc_pages[page]['impressions'] += q.get('impressions', 0)
        gsc_pages[page]['clicks']      += q.get('clicks', 0)
        gsc_pages[page]['position_sum'] += q.get('position', 0)
        gsc_pages[page]['count']        += 1

    scores = []
    all_pages = set(cur_pages.keys()) | set(gsc_pages.keys())

    for page in all_pages:
        ga4_cur  = cur_pages.get(page, {})
        ga4_prev = prev_pages.get(page, {})
        gsc      = gsc_pages.get(page, {})

        cur_sessions  = ga4_cur.get('sessions', 0)
        prev_sessions = ga4_prev.get('sessions', 0)

        impressions  = gsc.get('impressions', 0)
        clicks       = gsc.get('clicks', 0)
        count        = gsc.get('count', 1)
        avg_position = gsc.get('position_sum', 0) / count if count else 0
        ctr = clicks / impressions if impressions else 0

        # Skip pages with almost no data
        if cur_sessions < 2 and impressions < 5:
            continue

        ctr_score   = _score_ctr(ctr)
        pos_score   = _score_position(avg_position)
        trend_score = _score_trend(cur_sessions, prev_sessions)

        total = round(
            ctr_score   * weights['ctr'] +
            pos_score   * weights['position'] +
            trend_score * weights['trend']
        )

        scores.append({
            'page':             page,
            'title':            ga4_cur.get('title', ''),
            'score':            total,
            'label':            _label(total),
            'color':            _color(total),
            'ctr_score':        round(ctr_score),
            'position_score':   round(pos_score),
            'trend_score':      round(trend_score),
            'ctr_pct':          round(ctr * 100, 1),
            'avg_position':     round(avg_position, 1),
            'sessions':         cur_sessions,
            'prev_sessions':    prev_sessions,
            'impressions':      impressions,
        })

    scores.sort(key=lambda x: x['score'])
    return scores


def build_scorer_html(scores: list) -> str:
    """Build HTML table of page scores for inclusion in the email report."""
    if not scores:
        return ''

    rows = ''
    for p in scores[:12]:
        score = p['score']
        color = p['color']
        label = p['label']
        title = (p['title'] or p['page'])[:50]
        bg    = '#fff' if rows.count('<tr') % 2 == 0 else '#f9f9f9'
        rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;">{title}</td>'
            f'<td style="padding:7px 10px;text-align:center;">'
            f'<span style="background:{color};color:white;padding:2px 8px;'
            f'border-radius:12px;font-weight:bold;font-size:12px;">'
            f'{score}</span></td>'
            f'<td style="padding:7px 10px;text-align:center;color:{color};'
            f'font-weight:bold;">{label}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{p["ctr_pct"]}%</td>'
            f'<td style="padding:7px 10px;text-align:center;">#{p["avg_position"]}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{p["sessions"]:,}</td>'
            f'</tr>'
        )

    return f"""
<h3 style="color:#1a3a5c;font-size:14px;margin:20px 0 8px;">
  🏅 Landing Page Health Scores
</h3>
<p style="font-size:12px;color:#666;margin:0 0 8px;">
  Score = CTR (30%) + Google Position (40%) + Traffic Trend (30%).
  Worst pages shown first.
</p>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#1a3a5c;color:white;">
      <th style="padding:8px 10px;text-align:left;">Page</th>
      <th style="padding:8px 10px;text-align:center;">Score</th>
      <th style="padding:8px 10px;text-align:center;">Health</th>
      <th style="padding:8px 10px;text-align:center;">CTR</th>
      <th style="padding:8px 10px;text-align:center;">Position</th>
      <th style="padding:8px 10px;text-align:center;">Sessions</th>
    </tr>
  </thead>
  <tbody>{rows}</tbody>
</table>"""
