"""
Americal Patrol — Social Media Automation Pipeline
Generates unique social media posts for Facebook, Instagram, and LinkedIn.

Pipeline:
  1. Plan content (read event bus, check seasonal calendar, pick content types)
  2. Generate unique post per platform via Claude API
  3. Generate or select images (Drive photos first, then AI generation)
  4. Publish to platforms (or email drafts for review in draft_mode)
  5. Publish event to pipeline bus
  6. Advance content rotation

Runs Tuesday, Thursday, Saturday at 10:00 AM via Windows Task Scheduler.

Usage:
    python run_social.py
    python run_social.py --dry-run
    python run_social.py --dry-run --platform facebook
"""

import argparse
import json
import sys
import traceback
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from shared_utils.event_bus import publish_event, cleanup_old_events

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=True)

import content_planner
import content_generator
import media_manager

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / "social_config.json"
STATE_FILE  = SCRIPT_DIR / "social_state.json"
LOG_FILE    = SCRIPT_DIR / "automation.log"


def log(msg: str) -> None:
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [SOCIAL] {msg}"
    print(line.encode("ascii", errors="replace").decode("ascii"))
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def _load_config() -> dict:
    with open(CONFIG_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def _load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {
        "facebook_index": 0, "instagram_index": 0, "linkedin_index": 0,
        "gbp_index": 0, "gbp_last_posted": None,
        "posts_published": 0, "last_run": None, "last_posts": [],
        "images_generated": 0,
    }


def _save_state(state: dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


def run(dry_run: bool = False, platform_filter: str | None = None) -> bool:
    log("=" * 60)
    log(f"Americal Patrol Social Media Automation — Starting{'  [DRY RUN]' if dry_run else ''}")
    log("=" * 60)

    config = _load_config()
    state  = _load_state()

    # ── Step 1: Plan content ─────────────────────────────────────
    log("Step 1: Planning content for each platform...")
    try:
        plans = content_planner.plan_posts(log=log)
    except Exception as e:
        log(f"ERROR: Content planning failed: {e}")
        traceback.print_exc()
        return False

    # ── Step 2: Generate posts ───────────────────────────────────
    log("Step 2: Generating unique posts via Claude API...")
    try:
        posts = content_generator.generate_all_posts(plans, log=log)
    except Exception as e:
        log(f"ERROR: Content generation failed: {e}")
        traceback.print_exc()
        return False

    if not any(posts.values()):
        log("ERROR: No posts were generated for any platform.")
        return False

    # ── Step 3: Handle images ────────────────────────────────────
    log("Step 3: Processing images...")
    image_paths = {}

    for platform, post in posts.items():
        if not post:
            continue

        image_prompt = post.get("image_prompt", "")
        image_tags   = post.get("image_tags", [])

        if not image_prompt:
            log(f"  {platform}: No image needed.")
            continue

        # Try to find a matching existing image first
        existing = media_manager.find_matching_image(image_tags)
        if existing:
            log(f"  {platform}: Reusing existing image: {existing['filename']}")
            media_manager.mark_image_used(existing["filename"])
            image_paths[platform] = Path(existing["path"])
            continue

        # Generate a new image
        if not dry_run:
            try:
                from image_generator import generate_image
                img_bytes, filename = generate_image(image_prompt, log=log)
                img_path = media_manager.save_image(img_bytes, filename, image_tags)
                media_manager.mark_image_used(filename)
                image_paths[platform] = img_path
                state["images_generated"] = state.get("images_generated", 0) + 1
                log(f"  {platform}: New image generated: {filename}")
            except Exception as e:
                log(f"  WARNING: Image generation failed for {platform}: {e}")
        else:
            log(f"  {platform}: [DRY RUN] Would generate image: {image_prompt[:60]}...")

    # ── Step 4: Publish or draft ─────────────────────────────────
    log("Step 4: Publishing posts...")
    published_results = []
    draft_mode = config.get("draft_mode", True)

    for platform, post in posts.items():
        if not post:
            continue

        if platform_filter and platform != platform_filter:
            log(f"  {platform}: Skipped (filter: {platform_filter} only)")
            continue

        image_path = image_paths.get(platform)

        if dry_run:
            log(f"\n  -- {platform.upper()} [DRY RUN] --")
            log(f"  Content type: {plans.get(platform, {}).get('content_type', 'N/A')}")
            log(f"  Post text:\n    {post['post_text'][:200].replace(chr(10), chr(10) + '    ')}...")
            if post.get("image_prompt"):
                log(f"  Image prompt: {post['image_prompt'][:100]}...")
            published_results.append({
                "platform": platform,
                "content_type": plans.get(platform, {}).get("content_type", ""),
                "status": "dry_run",
            })
            continue

        if draft_mode:
            # In draft mode, we'll email the posts for review instead of publishing
            log(f"  {platform}: Draft mode — post queued for email review")
            published_results.append({
                "platform": platform,
                "content_type": plans.get(platform, {}).get("content_type", ""),
                "status": "drafted",
                "post_text": post["post_text"],
                "image_path": str(image_path) if image_path else None,
            })
        else:
            # Auto-publish mode
            try:
                post_id = _publish_to_platform(platform, post, image_path, config)
                log(f"  {platform}: Published! Post ID: {post_id}")
                published_results.append({
                    "platform": platform,
                    "content_type": plans.get(platform, {}).get("content_type", ""),
                    "status": "published",
                    "post_id": post_id,
                })
            except Exception as e:
                log(f"  ERROR: {platform} publish failed: {e}")
                traceback.print_exc()
                published_results.append({
                    "platform": platform,
                    "content_type": plans.get(platform, {}).get("content_type", ""),
                    "status": "failed",
                    "error": str(e),
                })

    # Send draft review email if in draft mode
    if draft_mode and not dry_run and published_results:
        try:
            from draft_emailer import send_draft_review_email
            send_draft_review_email(published_results, plans, log=log)
        except Exception as e:
            log(f"WARNING: Draft review email failed: {e}")
            traceback.print_exc()

    # ── Step 5: Publish event to pipeline bus ────────────────────
    if not dry_run:
        try:
            publish_event("social", "posts_published", {
                "posts": [
                    {
                        "platform": r["platform"],
                        "content_type": r["content_type"],
                        "status": r["status"],
                    }
                    for r in published_results
                ],
            })
            log("Pipeline event published.")
        except Exception as e:
            log(f"WARNING: Event bus publish failed: {e}")

        # Publish GBP-specific event for cross-pipeline topic dedup
        gbp_published = [r for r in published_results
                         if r["platform"] == "gbp" and r["status"] in ("published", "drafted")]
        if gbp_published:
            try:
                gbp_plan = plans.get("gbp", {})
                publish_event("gbp", "post_published", {
                    "post_summary": posts.get("gbp", {}).get("post_text", "")[:200],
                    "topic_type": gbp_plan.get("content_type", ""),
                    "topic_subject": gbp_plan.get("subject", ""),
                })
                log("GBP pipeline event published.")
            except Exception as e:
                log(f"WARNING: GBP event bus publish failed: {e}")

    # ── Step 6: Advance rotation and save state ──────────────────
    if not dry_run:
        # Only advance rotation for platforms that actually had content planned
        _meta_keys = {"seo_context", "seasonal", "blog_event", "gbp_recent_topics"}
        active_platforms = set(plans.keys()) - _meta_keys
        content_planner.advance_rotation(active_platforms=active_platforms, log=log)
        state["posts_published"] = state.get("posts_published", 0) + len(published_results)
        state["last_run"]  = datetime.now().isoformat()
        state["last_posts"] = published_results

        # Track GBP weekly posting
        if any(r["platform"] == "gbp" and r["status"] in ("published", "drafted")
               for r in published_results):
            state["gbp_last_posted"] = datetime.now().isoformat()

        _save_state(state)

        # Periodic cleanup
        cleanup_old_events(days=30)
        cleaned = media_manager.cleanup_old_media(days=30)
        if cleaned:
            log(f"Cleaned up {cleaned} old media file(s).")

    log("=" * 60)
    success_count = sum(1 for r in published_results if r["status"] in ("published", "drafted", "dry_run"))
    log(f"Social Media Automation complete. {success_count}/{len(published_results)} post(s) processed.")
    log("=" * 60)
    return True


def _publish_to_platform(platform: str, post: dict, image_path: Path | None,
                         config: dict) -> str:
    """Publish to the given platform. GBP uses Google API; others use GHL."""
    if platform == "gbp":
        from gbp_publisher import publish_post as gbp_publish
        return gbp_publish(post["post_text"], config)
    else:
        from ghl_publisher import publish_post
        return publish_post(platform, post["post_text"], image_path, config)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Americal Patrol Social Media Automation")
    parser.add_argument("--dry-run", action="store_true",
                        help="Generate content without publishing or sending emails")
    parser.add_argument("--platform", choices=["facebook", "instagram", "linkedin", "gbp"],
                        help="Only process a single platform")
    args = parser.parse_args()

    success = run(dry_run=args.dry_run, platform_filter=args.platform)
    sys.exit(0 if success else 1)
