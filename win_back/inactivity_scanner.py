"""
Win-Back Inactivity Scanner

Queries the CRM for customers whose last activity exceeds the
configured inactivity threshold. Applies exclusion filters and
cooldown checks.
"""

import logging
from datetime import datetime, timedelta

from shared_utils.event_bus import read_events_since

log = logging.getLogger(__name__)


def scan_inactive(
    crm,
    inactivity_days: int = 90,
    exclude_tags: list[str] = None,
    campaign_state: dict = None,
    cooldown_days: int = 90,
    max_results: int = 100,
) -> list[dict]:
    """
    Scan CRM for inactive customers eligible for win-back outreach.

    Args:
        crm:             CRM provider instance (from providers.get_crm())
        inactivity_days: Days since last activity to qualify as inactive
        exclude_tags:    Contact tags that disqualify (e.g., do_not_contact)
        campaign_state:  Dict of contact_id -> {last_sent, outcome} from winback_state.json
        cooldown_days:   Days since last win-back message before re-contacting
        max_results:     Max contacts to return

    Returns:
        List of contact dicts that are eligible for win-back outreach.
    """
    if exclude_tags is None:
        exclude_tags = ["do_not_contact"]
    if campaign_state is None:
        campaign_state = {}

    cutoff = datetime.now() - timedelta(days=inactivity_days)
    cooldown_cutoff = datetime.now() - timedelta(days=cooldown_days)

    all_contacts = crm.list_contacts(limit=500)
    eligible = []

    # Check sales pipeline events to avoid double-contacting
    recent_sales_events = read_events_since("sales_pipeline", "follow_up_sent", days=30)
    recently_followed_up = set()
    for event in recent_sales_events:
        cid = event.get("contact_id", "")
        if cid:
            recently_followed_up.add(cid)

    for contact in all_contacts:
        contact_id = contact.get("id", "")
        tags = [t.lower() for t in contact.get("tags", [])]

        # Skip if tagged for exclusion
        if any(tag in tags for tag in [t.lower() for t in exclude_tags]):
            continue

        # Skip if recently followed up by sales pipeline
        if contact_id in recently_followed_up:
            continue

        # Check last activity date
        updated_at = contact.get("updated_at", "")
        if not updated_at:
            continue

        try:
            last_activity = datetime.fromisoformat(updated_at.replace("Z", "+00:00")).replace(tzinfo=None)
        except (ValueError, TypeError):
            continue

        if last_activity > cutoff:
            continue

        # Check cooldown from previous win-back
        prev_campaign = campaign_state.get(contact_id, {})
        if prev_campaign:
            last_sent = prev_campaign.get("last_sent", "")
            if last_sent:
                try:
                    sent_date = datetime.fromisoformat(last_sent.replace("Z", "+00:00")).replace(tzinfo=None)
                    if sent_date > cooldown_cutoff:
                        continue
                except (ValueError, TypeError):
                    pass

        # Must have email or phone to contact
        if not contact.get("email") and not contact.get("phone"):
            continue

        eligible.append(contact)
        if len(eligible) >= max_results:
            break

    log.info("Found %d inactive contacts eligible for win-back (scanned %d total)",
             len(eligible), len(all_contacts))
    return eligible
