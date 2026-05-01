"""
Americal Patrol - Weekly Blog Pipeline
Orchestrates the full blog automation:
  1. Load the next topic (city + account type) from rotation
  2. Generate an SEO-optimized blog post via Claude API
  3. Publish the post to GoHighLevel
  4. Advance the rotation counter for next week
  5. Log the result

Runs every Monday at 8:00 AM via Windows Task Scheduler.
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

from blog_generator import generate_blog_post
from ghl_publisher   import publish_post

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'blog_config.json'
STATE_FILE  = SCRIPT_DIR / 'blog_state.json'
LOG_FILE    = SCRIPT_DIR / 'automation.log'


# ── Logging (matches the existing main.py pattern) ────────────────────────────
def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [BLOG] {msg}"
    print(line)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


# ── State management ──────────────────────────────────────────────────────────
def _load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {"topic_index": 0, "posts_published": 0, "last_run": None}


def _save_state(state: dict) -> None:
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=2)


# ── Main pipeline ─────────────────────────────────────────────────────────────
def run() -> bool:
    log("=" * 60)
    log("Americal Patrol Weekly Blog Automation - Starting")
    log("=" * 60)

    # ── Step 1: Load config and state ─────────────────────────────
    try:
        with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        log("ERROR: blog_config.json not found. Cannot run.")
        return False

    state  = _load_state()
    topics = config.get('topics', [])

    if not topics:
        log("ERROR: No topics found in blog_config.json.")
        return False

    # ── Priority topics from SEO analysis take precedence over rotation ──
    priority_topics = config.get('priority_topics', [])
    if priority_topics:
        topic        = priority_topics.pop(0)
        config['priority_topics'] = priority_topics
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        city         = topic['city']
        account_type = topic['account_type']
        idx          = next(
            (i for i, t in enumerate(topics)
             if t['city'] == city and t['account_type'] == account_type),
            state['topic_index'] % len(topics)
        )
        log(f"This week's topic (SEO priority): {account_type} security in {city}, CA")
    else:
        idx          = state['topic_index'] % len(topics)
        topic        = topics[idx]
        city         = topic['city']
        account_type = topic['account_type']
        log(f"This week's topic: {account_type} security in {city}, CA (topic {idx + 1} of {len(topics)})")

    # ── Step 2: Generate blog post via Claude ─────────────────────
    log("Generating blog post via Claude API...")
    try:
        post = generate_blog_post(city, account_type)
        word_count = len(post['html_content'].split())
        log(f"Blog generated: '{post['title']}' (~{word_count} words)")
    except Exception as e:
        log(f"ERROR: Blog generation failed: {e}")
        traceback.print_exc()
        _notify_failure(f"Blog generation failed for {account_type} / {city}: {e}")
        return False

    # ── Step 3: Publish to GoHighLevel ────────────────────────────
    log("Publishing to GoHighLevel...")
    try:
        ghl_post_id = publish_post(post)
        log(f"Published successfully. GHL Post ID: {ghl_post_id}")
    except Exception as e:
        log(f"ERROR: GHL publish failed: {e}")
        traceback.print_exc()
        _notify_failure(f"GHL publish failed for '{post['title']}': {e}")
        return False

    # ── Step 4: Advance rotation counter ─────────────────────────
    state['topic_index']     = (idx + 1) % len(topics)
    state['posts_published'] = state.get('posts_published', 0) + 1
    state['last_run']        = datetime.now().isoformat()
    state['last_post']       = {
        "title":        post['title'],
        "city":         city,
        "account_type": account_type,
        "slug":         post['slug'],
        "ghl_post_id":  ghl_post_id,
    }
    _save_state(state)

    # ── Publish event to pipeline bus ─────────────────────────────
    try:
        publish_event("blog", "post_published", {
            "title": post['title'],
            "slug": post['slug'],
            "city": city,
            "account_type": account_type,
            "ghl_post_id": ghl_post_id,
        })
        log("Pipeline event published.")
    except Exception as e:
        log(f"WARNING: Event bus publish failed: {e}")

    log(f"Done. Total posts published: {state['posts_published']}")
    log(f"Next week: {topics[state['topic_index']]['account_type']} in {topics[state['topic_index']]['city']}")
    return True


def _notify_failure(message: str) -> None:
    """
    Log a failure notice. In a future update this can send an email
    via the existing Gmail infrastructure (email_fetcher.get_gmail_service).
    """
    log(f"FAILURE NOTICE: {message}")
    log("ACTION REQUIRED: Check automation.log and re-run manually if needed.")


if __name__ == '__main__':
    success = run()
    sys.exit(0 if success else 1)
