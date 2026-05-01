"""
Americal Patrol - Blog Topic & Keyword Updater
Writes SEO analysis results back into blog_post_automation/blog_config.json:
  - priority_topics: topics to write next, ranked by search opportunity
  - keyword_intelligence: real keyword data for each topic to inject into Claude prompts
"""

import json
import sys
from pathlib import Path

SCRIPT_DIR       = Path(__file__).parent
BLOG_CONFIG_FILE = SCRIPT_DIR / '../blog_post_automation/blog_config.json'

sys.path.insert(0, str(SCRIPT_DIR.parent))
try:
    from shared_utils.event_bus import publish_event
    _HAS_EVENT_BUS = True
except Exception:
    _HAS_EVENT_BUS = False


def update_blog_config(priority_topics: list, keyword_intelligence: dict,
                        log=None) -> bool:
    """
    Load blog_config.json, update priority_topics and keyword_intelligence,
    and save it back. Returns True on success.
    """
    try:
        blog_config_path = BLOG_CONFIG_FILE.resolve()

        with open(blog_config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)

        # Merge new priority topics with any existing ones (deduplicate)
        existing_priority = config.get('priority_topics', [])
        existing_keys = {(t['city'], t['account_type']) for t in existing_priority}

        new_additions = [
            t for t in priority_topics
            if (t['city'], t['account_type']) not in existing_keys
        ]
        merged_priority = (new_additions + existing_priority)[:3]

        config['priority_topics'] = merged_priority

        # Merge keyword intelligence (new data overwrites old for same key)
        existing_intel = config.get('keyword_intelligence', {})
        existing_intel.update(keyword_intelligence)

        # Keep only intelligence for the top 3 priority topics
        active_keys = {f"{t['city']}_{t['account_type']}" for t in merged_priority}
        config['keyword_intelligence'] = {
            k: v for k, v in existing_intel.items() if k in active_keys
        }

        with open(blog_config_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)

        if log:
            if merged_priority:
                topics_str = ', '.join(f"{t['account_type']} in {t['city']}" for t in merged_priority)
                log(f"Updated blog priority topics: {topics_str}")
                for key, intel in config['keyword_intelligence'].items():
                    sec = intel.get('secondary_keywords', [])
                    log(f"  Keyword intelligence for {key}: "
                        f"{intel.get('impressions', 0)} impressions, "
                        f"pos {intel.get('position', 'N/A')}, "
                        f"{len(sec)} secondary keywords")
            else:
                log("No new blog topics identified from keyword data this week.")

        # ── Publish top keywords to GBP event bus ─────────────────────
        if _HAS_EVENT_BUS and keyword_intelligence:
            try:
                top_keywords = []
                for intel in list(keyword_intelligence.values())[:3]:
                    top_keywords.append(intel.get('primary_keyword', ''))
                    top_keywords.extend(intel.get('secondary_keywords', [])[:2])
                top_keywords = [k for k in top_keywords if k][:10]

                publish_event('seo', 'gbp_keyword_sync', {
                    'top_keywords':        top_keywords,
                    'priority_topics':     [
                        f"{t['account_type']} Security in {t['city']}"
                        for t in merged_priority
                    ],
                    'keyword_intelligence': keyword_intelligence,
                })
                if log:
                    log(f"GBP keyword sync published: {len(top_keywords)} keywords")
            except Exception as e:
                if log:
                    log(f"WARNING: GBP event publish failed (non-fatal): {e}")

        return True

    except Exception as e:
        if log:
            log(f"ERROR updating blog_config.json: {e}")
        return False
