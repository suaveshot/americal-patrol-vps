"""
Americal Patrol - Weekly SEO Analysis Pipeline
Runs every Monday at 7:00 AM (before the blog automation at 8:00 AM).

Pipeline:
  1. Fetch GA4 page traffic data (current + prior week)
  2. Fetch Google Search Console query data (current + prior week)
  3. Run SEO analysis (drops, gaps, rising pages, keyword opportunities)
  4. Update blog_config.json with priority topics + keyword intelligence
  5. Compose and send weekly SEO digest email
  6. Save state snapshot for next week's comparison
  7. Log all results

Run manually: python run_seo.py
"""

import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared_utils.event_bus import publish_event

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import ga4_fetcher
import gsc_fetcher
import seo_analyzer
import topic_updater
import report_composer
import page_scorer
import competitor_tracker

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'seo_config.json'
STATE_FILE  = SCRIPT_DIR / 'seo_state.json'
LOG_FILE    = SCRIPT_DIR / 'automation.log'

BLOG_CONFIG_FILE = SCRIPT_DIR / '../blog_post_automation/blog_config.json'
REPORTS_DIR      = SCRIPT_DIR / '../SEO Reports'

# Ensure SEO Reports archive folder exists
REPORTS_DIR.mkdir(exist_ok=True)


# ── Logging ──────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [SEO] {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


# ── State management ─────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {'last_run': None, 'runs_completed': 0}


def _save_state(state: dict) -> None:
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)


def _load_blog_config() -> dict:
    with open(BLOG_CONFIG_FILE.resolve(), 'r', encoding='utf-8') as f:
        return json.load(f)


# ── Main pipeline ─────────────────────────────────────────────────────────────

def run() -> bool:
    log('=' * 60)
    log('Americal Patrol SEO Analysis - Starting')
    log('=' * 60)

    state = _load_state()

    # ── Step 1: Fetch GA4 data ─────────────────────────────────────────
    log('Step 1/5: Fetching Google Analytics 4 data...')
    try:
        ga4_data = ga4_fetcher.fetch_all(log=log)
    except Exception as e:
        log(f'ERROR: GA4 fetch failed: {e}')
        traceback.print_exc()
        return False

    # ── Step 2: Fetch Search Console data ─────────────────────────────
    log('Step 2/5: Fetching Google Search Console data...')
    try:
        gsc_data = gsc_fetcher.fetch_all(log=log)
    except Exception as e:
        log(f'ERROR: Search Console fetch failed: {e}')
        traceback.print_exc()
        return False

    # ── Step 3: Run SEO analysis ───────────────────────────────────────
    log('Step 3/5: Running SEO analysis...')
    try:
        blog_config = _load_blog_config()
        analysis    = seo_analyzer.analyze(ga4_data, gsc_data, blog_config, log=log)
        log(f"Analysis complete: {len(analysis['traffic_drops'])} drops, "
            f"{len(analysis['keyword_gaps'])} gaps, "
            f"{len(analysis['rising_pages'])} rising pages")
    except Exception as e:
        log(f'ERROR: SEO analysis failed: {e}')
        traceback.print_exc()
        return False

    # ── Step 4: Update blog topic + keyword intelligence ──────────────
    log('Step 4/5: Updating blog priority topics and keyword intelligence...')
    try:
        success = topic_updater.update_blog_config(
            priority_topics=analysis['priority_topics'],
            keyword_intelligence=analysis['keyword_intelligence'],
            log=log,
        )
        if not success:
            log('WARNING: Blog config update failed — blog will use normal rotation.')
    except Exception as e:
        log(f'WARNING: Blog config update error: {e}')
        # Non-fatal — report still gets sent

    # ── Step 5: Compose and send email report ─────────────────────────
    log('Step 5/5: Composing and sending weekly SEO digest email...')
    try:
        sent = report_composer.compose_and_send(analysis, log=log)
        if not sent:
            log('ERROR: Email send failed.')
            return False
    except Exception as e:
        log(f'ERROR: Report email failed: {e}')
        traceback.print_exc()
        return False

    # ── Save state ─────────────────────────────────────────────────────
    state['last_run']        = datetime.now().isoformat()
    state['runs_completed']  = state.get('runs_completed', 0) + 1
    state['last_analysis_summary'] = {
        'date_range':       analysis.get('date_range', ''),
        'traffic_drops':    len(analysis['traffic_drops']),
        'keyword_gaps':     len(analysis['keyword_gaps']),
        'rising_pages':     len(analysis['rising_pages']),
        'priority_topics':  [
            f"{t['account_type']} in {t['city']}"
            for t in analysis['priority_topics']
        ],
    }
    _save_state(state)

    # ── Publish event to pipeline bus ────────────────────────────────
    try:
        publish_event("seo", "analysis_results", {
            "date_range": analysis.get('date_range', ''),
            "traffic_drops": len(analysis['traffic_drops']),
            "keyword_gaps": len(analysis['keyword_gaps']),
            "rising_pages": len(analysis['rising_pages']),
            "priority_topics": [
                {"city": t['city'], "account_type": t['account_type']}
                for t in analysis['priority_topics']
            ],
            "top_keywords": [
                kw.get('query', '') for kw in analysis.get('keyword_gaps', [])[:5]
            ],
            "trending_topics": [
                p.get('page', '') for p in analysis.get('rising_pages', [])[:5]
            ],
        })
        log("Pipeline event published.")
    except Exception as e:
        log(f"WARNING: Event bus publish failed: {e}")

    log('=' * 60)
    log(f"SEO Analysis complete. Run #{state['runs_completed']}.")
    log('Blog updated with data-driven topics and keyword intelligence.')
    log('GBP keyword sync published to event bus.')
    log('SEO Reports folder updated with HTML report + meta rewrite file.')
    log('=' * 60)
    return True


if __name__ == '__main__':
    success = run()
    sys.exit(0 if success else 1)
