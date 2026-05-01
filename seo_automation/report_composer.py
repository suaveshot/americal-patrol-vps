"""
Americal Patrol - SEO Report Composer
Enhanced with:
  - HTML stats dashboard (real numbers, tables, % changes)
  - Specific per-page fix recommendations
  - Security industry market trends & predictions
  - HTML report archive saved to SEO Reports folder
  - Blog automation integration
"""

import base64
import json
import os
import winreg
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import anthropic
from googleapiclient.discovery import build

from auth_setup import get_credentials
import page_scorer
import competitor_tracker

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'
REPORTS_DIR = SCRIPT_DIR.parent / 'SEO Reports'


# ── Auth helpers ─────────────────────────────────────────────────────────────

def _get_anthropic_api_key() -> str:
    """Read ANTHROPIC_API_KEY from environment, .env file, or Windows registry."""
    key = os.environ.get('ANTHROPIC_API_KEY')
    if key:
        return key
    # Try .env file in project root
    env_file = SCRIPT_DIR.parent / '.env'
    if env_file.exists():
        for line in env_file.read_text(encoding='utf-8', errors='ignore').splitlines():
            line = line.strip()
            if line.startswith('ANTHROPIC_API_KEY='):
                val = line.split('=', 1)[1].strip().strip('"').strip("'")
                if val:
                    return val
    raise RuntimeError(
        "ANTHROPIC_API_KEY not found. Add it to "
        f"{env_file} as: ANTHROPIC_API_KEY=sk-ant-..."
    )


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_gmail_service():
    creds = get_credentials()
    return build('gmail', 'v1', credentials=creds)


# ── Stats Dashboard (built in Python — guaranteed accurate numbers) ───────────

def _pct_change_html(current: float, previous: float) -> str:
    """Return a colored HTML span showing WoW % change."""
    if previous == 0:
        return '<span style="color:#1565c0;">NEW</span>'
    change = ((current - previous) / previous) * 100
    if change >= 0:
        return f'<span style="color:#2e7d32;font-weight:bold;">+{change:.1f}% ↑</span>'
    else:
        return f'<span style="color:#c62828;font-weight:bold;">{change:.1f}% ↓</span>'


def _build_stats_html(analysis: dict) -> str:
    top_pages  = analysis.get('top_pages', [])
    drops      = analysis.get('traffic_drops', [])
    low_ctr    = analysis.get('low_ctr_keywords', [])
    sources    = analysis.get('traffic_sources', [])
    rising     = analysis.get('rising_pages', [])
    date_range = analysis.get('date_range', '')

    # ── Total sessions ────────────────────────────────────────────────
    cur_sessions  = sum(p.get('sessions', 0)  for p in top_pages)
    cur_pageviews = sum(p.get('pageviews', 0) for p in top_pages)
    prev_sessions = sum(
        p.get('previous_sessions', 0)
        for p in drops + rising
        if p.get('previous_sessions')
    )

    # ── Organic traffic (from sources) ───────────────────────────────
    organic = next(
        (s for s in sources if 'google' in s.get('source','').lower()
         or 'organic' in s.get('medium','').lower()),
        None
    )
    organic_sessions = organic.get('sessions', 0) if organic else 0

    # Top-level stat cards
    stat_cards = f"""
<div style="display:flex;gap:12px;margin:16px 0;flex-wrap:wrap;">
  <div style="flex:1;min-width:140px;background:#e8f5e9;border-left:4px solid #2e7d32;padding:12px;border-radius:4px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;">Sessions (This Week)</div>
    <div style="font-size:24px;font-weight:bold;color:#1a3a5c;">{cur_sessions:,}</div>
  </div>
  <div style="flex:1;min-width:140px;background:#e3f2fd;border-left:4px solid #1565c0;padding:12px;border-radius:4px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;">Organic Sessions</div>
    <div style="font-size:24px;font-weight:bold;color:#1a3a5c;">{organic_sessions:,}</div>
  </div>
  <div style="flex:1;min-width:140px;background:#fff3e0;border-left:4px solid #e65100;padding:12px;border-radius:4px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;">Pageviews</div>
    <div style="font-size:24px;font-weight:bold;color:#1a3a5c;">{cur_pageviews:,}</div>
  </div>
  <div style="flex:1;min-width:140px;background:#fce4ec;border-left:4px solid #c62828;padding:12px;border-radius:4px;">
    <div style="font-size:11px;color:#555;text-transform:uppercase;">Traffic Alerts</div>
    <div style="font-size:24px;font-weight:bold;color:#c62828;">{len(drops)}</div>
  </div>
</div>"""

    # Top pages table
    pages_rows = ''
    prev_map = {p['page']: p.get('previous_sessions', 0) for p in drops + rising}
    for p in top_pages[:8]:
        page_label = p['page']
        title      = p.get('title', '') or page_label
        sessions   = p.get('sessions', 0)
        pageviews  = p.get('pageviews', 0)
        prev_s     = prev_map.get(page_label, 0)
        chg        = _pct_change_html(sessions, prev_s) if prev_s else '—'
        bg         = '#fff' if pages_rows.count('<tr') % 2 == 0 else '#f9f9f9'
        pages_rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;max-width:220px;overflow:hidden;'
            f'text-overflow:ellipsis;white-space:nowrap;">{title[:55]}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{sessions:,}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{pageviews:,}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{chg}</td>'
            f'</tr>'
        )

    pages_table = f"""
<h3 style="color:#1a3a5c;font-size:14px;margin:20px 0 8px;">📄 Top Pages — {date_range}</h3>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#1a3a5c;color:white;">
      <th style="padding:8px 10px;text-align:left;">Page</th>
      <th style="padding:8px 10px;text-align:center;">Sessions</th>
      <th style="padding:8px 10px;text-align:center;">Pageviews</th>
      <th style="padding:8px 10px;text-align:center;">WoW Change</th>
    </tr>
  </thead>
  <tbody>{pages_rows}</tbody>
</table>""" if pages_rows else ''

    # Keyword performance table
    all_gsc = analysis.get('keyword_gaps', []) + analysis.get('low_ctr_keywords', [])
    seen, kw_deduped = set(), []
    for q in sorted(all_gsc, key=lambda x: x.get('impressions', 0), reverse=True):
        if q['query'] not in seen:
            seen.add(q['query'])
            kw_deduped.append(q)

    kw_rows = ''
    for q in kw_deduped[:10]:
        ctr_pct = round(q.get('ctr', 0) * 100, 1)
        pos     = round(q.get('position', 0), 1)
        imp     = q.get('impressions', 0)
        clicks  = q.get('clicks', 0)
        ctr_color = '#c62828' if ctr_pct < 2 else ('#e65100' if ctr_pct < 5 else '#2e7d32')
        pos_color = '#2e7d32' if pos <= 5 else ('#e65100' if pos <= 10 else '#c62828')
        bg = '#fff' if kw_rows.count('<tr') % 2 == 0 else '#f9f9f9'
        kw_rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;">{q["query"]}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{imp:,}</td>'
            f'<td style="padding:7px 10px;text-align:center;'
            f'color:{pos_color};font-weight:bold;">#{pos}</td>'
            f'<td style="padding:7px 10px;text-align:center;'
            f'color:{ctr_color};font-weight:bold;">{ctr_pct}%</td>'
            f'<td style="padding:7px 10px;text-align:center;">{clicks:,}</td>'
            f'</tr>'
        )

    kw_table = f"""
<h3 style="color:#1a3a5c;font-size:14px;margin:20px 0 8px;">🔍 Keyword Performance</h3>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#1a3a5c;color:white;">
      <th style="padding:8px 10px;text-align:left;">Search Query</th>
      <th style="padding:8px 10px;text-align:center;">Impressions</th>
      <th style="padding:8px 10px;text-align:center;">Position</th>
      <th style="padding:8px 10px;text-align:center;">CTR</th>
      <th style="padding:8px 10px;text-align:center;">Clicks</th>
    </tr>
  </thead>
  <tbody>{kw_rows}</tbody>
</table>
<p style="font-size:11px;color:#888;margin:4px 0 0;">
  Position color: <span style="color:#2e7d32">■</span> Top 5 &nbsp;
  <span style="color:#e65100">■</span> Top 10 &nbsp;
  <span style="color:#c62828">■</span> Page 2+ &nbsp;&nbsp;
  CTR color: <span style="color:#c62828">■</span> Under 2% &nbsp;
  <span style="color:#2e7d32">■</span> 5%+
</p>""" if kw_rows else ''

    # Traffic alerts table
    drop_rows = ''
    for d in drops[:5]:
        page   = d['page']
        cur_s  = d.get('current_sessions', 0)
        prev_s = d.get('previous_sessions', 0)
        drop   = d.get('drop_pct', 0)
        bg = '#fff' if drop_rows.count('<tr') % 2 == 0 else '#fff5f5'
        drop_rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;">{page}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{prev_s:,}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{cur_s:,}</td>'
            f'<td style="padding:7px 10px;text-align:center;color:#c62828;font-weight:bold;">'
            f'-{drop}% ↓</td>'
            f'</tr>'
        )

    drops_table = f"""
<h3 style="color:#c62828;font-size:14px;margin:20px 0 8px;">⚠️ Traffic Alerts</h3>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#c62828;color:white;">
      <th style="padding:8px 10px;text-align:left;">Page</th>
      <th style="padding:8px 10px;text-align:center;">Last Week</th>
      <th style="padding:8px 10px;text-align:center;">This Week</th>
      <th style="padding:8px 10px;text-align:center;">Drop</th>
    </tr>
  </thead>
  <tbody>{drop_rows}</tbody>
</table>""" if drop_rows else ''

    # Traffic sources table
    src_rows = ''
    for s in sources[:5]:
        src    = s.get('source', '')
        medium = s.get('medium', '')
        sess   = s.get('sessions', 0)
        bg = '#fff' if src_rows.count('<tr') % 2 == 0 else '#f9f9f9'
        src_rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;">{src}</td>'
            f'<td style="padding:7px 10px;">{medium}</td>'
            f'<td style="padding:7px 10px;text-align:center;font-weight:bold;">'
            f'{sess:,}</td>'
            f'</tr>'
        )

    sources_table = f"""
<h3 style="color:#1a3a5c;font-size:14px;margin:20px 0 8px;">📡 Traffic Sources</h3>
<table style="width:100%;border-collapse:collapse;font-size:13px;">
  <thead>
    <tr style="background:#1a3a5c;color:white;">
      <th style="padding:8px 10px;text-align:left;">Source</th>
      <th style="padding:8px 10px;text-align:left;">Medium</th>
      <th style="padding:8px 10px;text-align:center;">Sessions</th>
    </tr>
  </thead>
  <tbody>{src_rows}</tbody>
</table>""" if src_rows else ''

    return f"""
<div style="background:#f5f7fa;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 12px;font-size:16px;">
    📊 Analytics Dashboard — {date_range}
  </h2>
  {stat_cards}
  {pages_table}
  {kw_table}
  {drops_table}
  {sources_table}
</div>"""


# ── Claude Prompt (analysis, specific fixes, market trends) ─────────────────

def _build_claude_prompt(analysis: dict, date_range: str) -> str:
    top_pages     = analysis.get('top_pages', [])[:5]
    drops         = analysis.get('traffic_drops', [])[:5]
    gaps          = analysis.get('keyword_gaps', [])[:8]
    low_ctr       = analysis.get('low_ctr_keywords', [])[:8]
    rising        = analysis.get('rising_pages', [])[:5]
    sources       = analysis.get('traffic_sources', [])[:5]
    priority      = analysis.get('priority_topics', [])
    dropped_count = len(analysis.get('dropped_queries', []))

    def fmt_pages(pages):
        if not pages: return "No data."
        return '\n'.join(
            f"  • {p.get('title', p['page'])} ({p['page']}): "
            f"{p['sessions']} sessions, {p['pageviews']} pageviews"
            for p in pages
        )

    def fmt_drops(d_list):
        if not d_list: return "No significant drops."
        return '\n'.join(
            f"  • {d['page']}: {d['current_sessions']} sessions "
            f"(was {d['previous_sessions']}) — DOWN {d['drop_pct']}%"
            for d in d_list
        )

    def fmt_gaps(g_list):
        if not g_list: return "No keyword gaps identified."
        return '\n'.join(
            f"  • \"{g['query']}\": {g['impressions']} impressions, "
            f"position {g['position']}, {g['clicks']} clicks"
            for g in g_list
        )

    def fmt_low_ctr(l_list):
        if not l_list: return "No low-CTR keywords."
        return '\n'.join(
            f"  • \"{l['query']}\": pos {l['position']}, "
            f"CTR {round(l['ctr']*100,1)}%, {l['impressions']} impressions, "
            f"URL: {l.get('page','unknown')}"
            for l in l_list
        )

    def fmt_rising(r_list):
        if not r_list: return "No notable rising pages."
        lines = []
        for r in r_list:
            growth = 'NEW' if r['growth_pct'] == 999 else f"+{r['growth_pct']}%"
            lines.append(f"  • {r['page']}: {r['current_sessions']} sessions ({growth})")
        return '\n'.join(lines)

    def fmt_sources(s_list):
        if not s_list: return "No source data."
        return '\n'.join(
            f"  • {s['source']} / {s['medium']}: {s['sessions']} sessions"
            for s in s_list
        )

    def fmt_priority(p_list):
        if not p_list: return "No data-driven topics this week — using normal rotation."
        return '\n'.join(
            f"  • {p['account_type']} Security in {p['city']}, CA"
            for p in p_list
        )

    return f"""You are a senior SEO analyst and digital marketing strategist writing a weekly
performance report for Americal Patrol, Inc. — a veteran-owned security patrol company
serving Ventura County, LA County, and Orange County, California since 1986.
Website: americalpatrol.com

Write a professional, data-driven weekly SEO report for company leadership (Don and Sandra).
Be specific, direct, and actionable. These are business owners, not SEO technicians —
translate data into business impact and give them exact things to do.

═══════════════════════════════════════════
DATA FOR {date_range}
(NOTE: {dropped_count} job-seeking/guard-card/out-of-area queries auto-filtered as irrelevant)
═══════════════════════════════════════════

TOP PAGES BY TRAFFIC:
{fmt_pages(top_pages)}

TRAFFIC SOURCES:
{fmt_sources(sources)}

TRAFFIC DROPS (pages that fell significantly WoW):
{fmt_drops(drops)}

KEYWORD GAPS (ranking page 2+ but real demand exists — quick wins):
{fmt_gaps(gaps)}

LOW CTR KEYWORDS (ranking well in top 10, but not getting clicked — need title/meta fixes):
{fmt_low_ctr(low_ctr)}

RISING CONTENT:
{fmt_rising(rising)}

NEXT BLOG TOPICS QUEUED (data-driven):
{fmt_priority(priority)}

═══════════════════════════════════════════
WRITE THE EMAIL WITH THESE EXACT SECTIONS:
═══════════════════════════════════════════

1. <h2>📋 Executive Summary</h2>
   2-3 sentences on the overall health of the website this week. Mention key wins and
   key concerns. Use specific numbers.

2. <h2>🏆 Top Performing Pages</h2>
   Brief commentary on what's driving traffic and why it matters for lead generation.

3. <h2>⚠️ Traffic Alerts</h2>
   For each drop, explain likely causes (algorithm update, seasonal, content aging, etc.)
   and what the business risk is if left unaddressed.

4. <h2>🔧 Specific Page Fixes — Do These This Week</h2>
   For EACH low-CTR keyword, provide:
   - The exact URL of the page to fix
   - The CURRENT problem (e.g., "title tag is too generic")
   - A SPECIFIC recommended title tag rewrite (under 60 chars, include city + keyword)
   - A SPECIFIC recommended meta description rewrite (under 155 chars, include a call to action)

   For EACH traffic drop page, provide 1-2 specific content changes to make on that page.

5. <h2>🎯 Keyword Quick Wins</h2>
   Explain which keyword gaps are the biggest opportunity and WHY (impressions vs position gap).
   Recommend which existing page to strengthen OR whether a new page/blog post is needed.

6. <h2>📈 Rising Content</h2>
   Call out what's gaining traction and recommend how to accelerate it (internal links,
   social sharing, CTA optimization, etc.)

7. <h2>📡 Security Industry Trends & Predictions</h2>
   Based on current trends in the private security industry in Southern California:
   - Identify 2-3 market trends directly relevant to Americal Patrol's service mix
     (HOA patrol, commercial, industrial, retail — Ventura/LA/OC counties)
   - Make data-informed predictions for the next 90 days
   - Recommend 1-2 SEO or content actions to position Americal Patrol ahead of those trends
   Be specific to Southern California market conditions, not generic industry fluff.

8. <h2>✍️ This Week's Blog Strategy</h2>
   Explain which blog topic is queued and how it directly ties to the keyword/traffic data above.

9. <h2>✅ Action Items This Week</h2>
   A numbered list of 5-7 specific, prioritized action items. Each item should be concrete
   (not "improve SEO" — instead "Update the title tag on /hoa-security-camarillo to include
   the exact phrase 'HOA Security Camarillo CA'"). Order by highest ROI first.

FORMATTING RULES:
- Return ONLY the HTML email body (no <html>/<head>/<body> wrappers)
- Use <h2>, <p>, <ul>, <li>, <ol>, <strong>, <em> tags
- For specific fixes, use a <div style="background:#f0f4ff;border-left:3px solid #1565c0;
  padding:10px;margin:8px 0;border-radius:3px;"> block for each page fix
- Write in complete sentences — do NOT copy/paste raw data lists
- Max 900 words total (the stats dashboard is shown separately above this section)"""


# ── Month-over-Month Dashboard ────────────────────────────────────────────────

def _build_mom_html(monthly: dict) -> str:
    if not monthly:
        return ''
    totals   = monthly.get('totals', {})
    cur      = totals.get('current_sessions', 0)
    prev     = totals.get('previous_sessions', 0)
    chg      = totals.get('change_pct', 0)
    chg_html = (f'<span style="color:#2e7d32;font-weight:bold;">+{chg}% ↑</span>'
                if chg >= 0 else
                f'<span style="color:#c62828;font-weight:bold;">{chg}% ↓</span>')
    cur_range  = monthly.get('current_range', '')
    prev_range = monthly.get('previous_range', '')

    top_rows = ''
    prev_map = {p['page']: p['sessions'] for p in monthly.get('previous_30', [])}
    for p in monthly.get('current_30', [])[:6]:
        prev_s = prev_map.get(p['page'], 0)
        chg_p  = round(((p['sessions'] - prev_s) / prev_s) * 100, 1) if prev_s else None
        chg_cell = (f'<span style="color:{"#2e7d32" if chg_p >= 0 else "#c62828"};'
                    f'font-weight:bold;">{"+" if chg_p >= 0 else ""}{chg_p}%</span>'
                    if chg_p is not None else '—')
        bg = '#fff' if top_rows.count('<tr') % 2 == 0 else '#f9f9f9'
        top_rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:6px 10px;font-size:12px;">{p["page"][:55]}</td>'
            f'<td style="padding:6px 10px;text-align:center;">{prev_s:,}</td>'
            f'<td style="padding:6px 10px;text-align:center;">{p["sessions"]:,}</td>'
            f'<td style="padding:6px 10px;text-align:center;">{chg_cell}</td>'
            f'</tr>'
        )

    return f"""
<div style="background:#f0f4ff;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 10px;font-size:15px;">📅 30-Day Month-over-Month</h2>
  <div style="display:flex;gap:16px;margin-bottom:12px;flex-wrap:wrap;">
    <div style="flex:1;min-width:130px;background:#fff;border-left:4px solid #1565c0;
         padding:10px;border-radius:4px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;">This 30 Days</div>
      <div style="font-size:22px;font-weight:bold;color:#1a3a5c;">{cur:,}</div>
      <div style="font-size:11px;color:#888;">{cur_range}</div>
    </div>
    <div style="flex:1;min-width:130px;background:#fff;border-left:4px solid #9e9e9e;
         padding:10px;border-radius:4px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;">Prior 30 Days</div>
      <div style="font-size:22px;font-weight:bold;color:#555;">{prev:,}</div>
      <div style="font-size:11px;color:#888;">{prev_range}</div>
    </div>
    <div style="flex:1;min-width:130px;background:#fff;border-left:4px solid #2e7d32;
         padding:10px;border-radius:4px;">
      <div style="font-size:11px;color:#555;text-transform:uppercase;">MoM Change</div>
      <div style="font-size:22px;font-weight:bold;">{chg_html}</div>
    </div>
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:12px;">
    <thead>
      <tr style="background:#1a3a5c;color:white;">
        <th style="padding:6px 10px;text-align:left;">Page</th>
        <th style="padding:6px 10px;text-align:center;">Prev 30d</th>
        <th style="padding:6px 10px;text-align:center;">This 30d</th>
        <th style="padding:6px 10px;text-align:center;">Change</th>
      </tr>
    </thead>
    <tbody>{top_rows}</tbody>
  </table>
</div>"""


# ── Competitor Dashboard ───────────────────────────────────────────────────────

def _build_competitor_html(competitor_summary: dict) -> str:
    if not competitor_summary:
        return ''

    county_labels = {
        'ventura_county': 'Ventura County',
        'la_county':      'LA County',
        'orange_county':  'Orange County',
    }

    sections = ''
    for county, data in competitor_summary.items():
        label    = county_labels.get(county, county.replace('_', ' ').title())
        winning  = data.get('winning', [])
        losing   = data.get('losing', [])
        missing  = data.get('missing', [])

        win_rows = ''
        for w in winning[:3]:
            comp_str = ', '.join(f"{d} (#{r})" for d, r in w.get('beating', {}).items() if r) or 'All others'
            win_rows += (
                f'<tr><td style="padding:5px 8px;font-size:12px;">{w["keyword"]}</td>'
                f'<td style="padding:5px 8px;text-align:center;color:#2e7d32;font-weight:bold;">#{w["our_rank"]}</td>'
                f'<td style="padding:5px 8px;font-size:11px;color:#666;">{comp_str}</td></tr>'
            )

        lose_rows = ''
        for l in losing[:3]:
            beaten_str = ', '.join(f"{d} (#{r})" for d, r in l.get('beaten_by', {}).items())
            lose_rows += (
                f'<tr><td style="padding:5px 8px;font-size:12px;">{l["keyword"]}</td>'
                f'<td style="padding:5px 8px;text-align:center;color:#c62828;font-weight:bold;">#{l["our_rank"]}</td>'
                f'<td style="padding:5px 8px;font-size:11px;color:#c62828;">{beaten_str}</td></tr>'
            )

        miss_rows = ''
        for m in missing[:2]:
            comp_str = ', '.join(f"{d}(#{r})" for d, r in m.get('competitors', {}).items())
            miss_rows += (
                f'<tr><td style="padding:5px 8px;font-size:12px;" colspan="2">{m["keyword"]}</td>'
                f'<td style="padding:5px 8px;font-size:11px;color:#e65100;">'
                f'We don\'t appear — {comp_str}</td></tr>'
            )

        no_data      = '<tr><td colspan="3" style="padding:5px 8px;color:#999;font-size:11px;">No data yet</td></tr>'
        missing_table = (
            '<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px;">'
            '<thead><tr style="background:#fff3e0;">'
            '<th style="padding:5px 8px;text-align:left;color:#e65100;" colspan="3">'
            '&#10060; Not Ranking (Missing Page)</th></tr></thead>'
            '<tbody>' + miss_rows + '</tbody></table>'
        ) if miss_rows else ''

        sections += (
            '<div style="margin-bottom:16px;">'
            f'<h4 style="color:#1a3a5c;margin:0 0 6px;font-size:13px;">&#128205; {label}</h4>'
            '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
            '<thead><tr style="background:#e8f5e9;">'
            '<th style="padding:5px 8px;text-align:left;color:#2e7d32;">&#9989; Winning Keywords</th>'
            '<th style="padding:5px 8px;text-align:center;color:#2e7d32;">Our Rank</th>'
            '<th style="padding:5px 8px;text-align:left;color:#2e7d32;">Competitors Below Us</th>'
            '</tr></thead>'
            f'<tbody>{win_rows or no_data}</tbody></table>'
            '<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:6px;">'
            '<thead><tr style="background:#ffebee;">'
            '<th style="padding:5px 8px;text-align:left;color:#c62828;">&#9888;&#65039; Losing Keywords</th>'
            '<th style="padding:5px 8px;text-align:center;color:#c62828;">Our Rank</th>'
            '<th style="padding:5px 8px;text-align:left;color:#c62828;">Who\'s Beating Us</th>'
            '</tr></thead>'
            f'<tbody>{lose_rows or no_data}</tbody></table>'
            f'{missing_table}'
            '</div>'
        )

    return f"""
<div style="background:#f9f9f9;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 12px;font-size:15px;">🏁 Competitor Rankings by County</h2>
  {sections}
  <p style="font-size:11px;color:#999;margin:8px 0 0;">
    Powered by DataForSEO — rankings checked weekly for target keywords per county.
  </p>
</div>"""


# ── Core Web Vitals HTML ──────────────────────────────────────────────────────

def _build_cwv_html(cwv: dict) -> str:
    if not cwv:
        return ''

    def _badge(metric: str, p75, good_pct: float) -> str:
        thresholds = {
            'lcp':  (2500, 4000),
            'fid':  (100, 300),
            'cls':  (0.1, 0.25),
            'fcp':  (1800, 3000),
            'ttfb': (800, 1800),
        }
        lo, hi = thresholds.get(metric, (0, 0))
        if p75 is None:
            color, label = '#9e9e9e', 'No Data'
        elif p75 <= lo:
            color, label = '#2e7d32', 'Good'
        elif p75 <= hi:
            color, label = '#f57f17', 'Needs Work'
        else:
            color, label = '#c62828', 'Poor'
        val = f"{p75}ms" if metric != 'cls' else str(p75)
        return (
            f'<td style="padding:7px 10px;text-align:center;">{val}</td>'
            f'<td style="padding:7px 10px;text-align:center;">{good_pct}%</td>'
            f'<td style="padding:7px 10px;text-align:center;">'
            f'<span style="background:{color};color:white;padding:2px 8px;'
            f'border-radius:10px;font-size:11px;">{label}</span></td>'
        )

    rows = ''
    metric_labels = {
        'lcp': ('LCP', 'Largest Contentful Paint — how fast main content loads'),
        'fid': ('FID', 'First Input Delay — responsiveness to first interaction'),
        'cls': ('CLS', 'Cumulative Layout Shift — visual stability'),
        'fcp': ('FCP', 'First Contentful Paint — time to first visible content'),
        'ttfb': ('TTFB', 'Time to First Byte — server response speed'),
    }
    for key, (short, desc) in metric_labels.items():
        data = cwv.get(key, {})
        if not data:
            continue
        bg = '#fff' if rows.count('<tr') % 2 == 0 else '#f9f9f9'
        rows += (
            f'<tr style="background:{bg};" title="{desc}">'
            f'<td style="padding:7px 10px;font-weight:bold;">{short}</td>'
            f'<td style="padding:7px 10px;font-size:11px;color:#666;">{desc}</td>'
            + _badge(key, data.get('p75'), data.get('good_pct', 0)) +
            '</tr>'
        )

    if not rows:
        return ''

    return f"""
<div style="background:#f5f7fa;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 10px;font-size:15px;">⚡ Core Web Vitals (Mobile)</h2>
  <table style="width:100%;border-collapse:collapse;font-size:13px;">
    <thead>
      <tr style="background:#1a3a5c;color:white;">
        <th style="padding:8px 10px;text-align:left;">Metric</th>
        <th style="padding:8px 10px;text-align:left;">Description</th>
        <th style="padding:8px 10px;text-align:center;">p75</th>
        <th style="padding:8px 10px;text-align:center;">% Good</th>
        <th style="padding:8px 10px;text-align:center;">Status</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
</div>"""


# ── Meta Rewrite Export ───────────────────────────────────────────────────────

def _save_meta_rewrite_file(claude_report: str, date_str: str) -> None:
    """Parse Claude's page fix recommendations and save as a ready-to-use text file."""
    REPORTS_DIR.mkdir(exist_ok=True)
    safe_date = date_str.replace(' ', '_').replace(',', '').replace('/', '-')
    path      = REPORTS_DIR / f"Meta_Rewrites_{safe_date}.txt"

    content = (
        f"AMERICAL PATROL — META TAG REWRITES\n"
        f"Generated: {date_str}\n"
        f"{'=' * 60}\n\n"
        f"Copy these directly into your website CMS.\n"
        f"Each block shows: Page URL, new Title tag, new Meta Description.\n\n"
        f"{'=' * 60}\n\n"
        f"[See full SEO Report for context on why these changes are recommended]\n\n"
        f"SOURCE REPORT EXCERPT:\n"
        f"{claude_report[:3000]}\n"
    )
    path.write_text(content, encoding='utf-8')


# ── Save report copy ─────────────────────────────────────────────────────────

def _save_report_copy(full_html: str, date_str: str, log=None) -> None:
    REPORTS_DIR.mkdir(exist_ok=True)
    safe_date = date_str.replace(' ', '_').replace(',', '').replace('/', '-')
    filename  = f"SEO_Report_{safe_date}.html"
    path      = REPORTS_DIR / filename
    path.write_text(full_html, encoding='utf-8')
    if log:
        log(f"Report archived: SEO Reports/{filename}")


# ── Main compose & send ──────────────────────────────────────────────────────

def compose_and_send(analysis: dict, log=None) -> bool:
    config     = _load_config()
    recipients = config.get('recipients', [])
    date_range = analysis.get('date_range', 'this week')

    if not recipients:
        if log: log("ERROR: No recipients configured in seo_config.json")
        return False

    # ── Step 1: Build stats dashboard ─────────────────────────────────
    if log: log("Building analytics dashboard...")
    stats_html = _build_stats_html(analysis)

    # ── Step 2: Page health scores ─────────────────────────────────────
    if log: log("Scoring landing pages...")
    ga4_data = {'pages': {
        'current':  analysis.get('top_pages', []),
        'previous': [],
    }}
    gsc_data_for_scorer = {'queries': {'current': analysis.get('combined_queries', [])}}
    page_scores    = page_scorer.score_pages(ga4_data, gsc_data_for_scorer)
    scorer_html    = page_scorer.build_scorer_html(page_scores)

    # ── Step 3: Month-over-month dashboard ────────────────────────────
    monthly_html = _build_mom_html(analysis.get('monthly', {}))

    # ── Step 4: Core Web Vitals ────────────────────────────────────────
    cwv_html = _build_cwv_html(analysis.get('core_web_vitals', {}))

    # ── Step 5: Competitor rankings ────────────────────────────────────
    if log: log("Fetching competitor rankings by county...")
    comp_data    = competitor_tracker.fetch_competitor_rankings(log=log)
    comp_summary = competitor_tracker.summarize_for_report(comp_data)
    comp_html    = _build_competitor_html(comp_summary)

    # ── Step 6: Generate Claude narrative ─────────────────────────────
    if log: log("Generating SEO analysis, page fixes, and market trends via Claude...")
    analysis['competitor_summary'] = comp_summary
    analysis['page_scores']        = page_scores[:5]
    prompt      = _build_claude_prompt(analysis, date_range)
    client      = anthropic.Anthropic(api_key=_get_anthropic_api_key())
    response    = client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=3500,
        messages=[{'role': 'user', 'content': prompt}]
    )
    report_body = response.content[0].text.strip()

    # ── Step 7: Assemble full HTML ─────────────────────────────────────
    today_str = datetime.now().strftime('%B %d, %Y')
    subject   = f"Americal Patrol SEO Report — Week of {today_str}"

    full_html = f"""<html><body style="font-family:Arial,sans-serif;max-width:800px;margin:0 auto;color:#333;">
<div style="background:#1a3a5c;padding:20px;border-radius:8px 8px 0 0;">
  <h1 style="color:white;margin:0;font-size:22px;">Americal Patrol — Weekly SEO Report</h1>
  <p style="color:#cde;margin:5px 0 0;font-size:14px;">{today_str} &nbsp;|&nbsp; americalpatrol.com</p>
</div>
<div style="padding:24px;background:#fff;border:1px solid #ddd;border-top:none;border-radius:0 0 8px 8px;">
{monthly_html}
{stats_html}
{scorer_html}
{cwv_html}
{comp_html}
<hr style="border:none;border-top:1px solid #e0e0e0;margin:24px 0;">
{report_body}
</div>
<p style="font-size:11px;color:#999;text-align:center;margin-top:16px;">
  Generated automatically by Americal Patrol SEO Automation &nbsp;|&nbsp;
  Archived in SEO Reports folder
</p>
</body></html>"""

    # ── Step 8: Send email ─────────────────────────────────────────────
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['To']      = ', '.join(recipients)
    msg['From']    = 'me'
    msg.attach(MIMEText(
        f"Americal Patrol Weekly SEO Report — {today_str}\n\n"
        "Please open in Gmail or Outlook to view the full formatted report.",
        'plain'
    ))
    msg.attach(MIMEText(full_html, 'html'))

    raw   = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    gmail = _get_gmail_service()
    gmail.users().messages().send(userId='me', body={'raw': raw}).execute()

    if log:
        log(f"SEO report email sent to: {', '.join(recipients)}")
        log(f"Subject: {subject}")

    # ── Step 9: Archive HTML + meta rewrite file ───────────────────────
    _save_report_copy(full_html, today_str, log=log)
    _save_meta_rewrite_file(report_body, today_str)

    return True
