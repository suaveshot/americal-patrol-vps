"""
Approve all pending drafts + generate cold follow-up drafts, then send at 2 PM.
"""
import json
import subprocess
import sys
import os
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sales_pipeline import config
from sales_pipeline.config import DRAFTS_FILE
from sales_pipeline.state import (
    load_state, get_due_contacts, get_contact as get_state_contact,
)
from sales_pipeline.follow_up.follow_up_engine import (
    cold_touch_matrix, get_touch_content, SUBJECT_TEMPLATES,
)
from sales_pipeline.ghl_client import GHLClient

config.validate_config()

# Step 1: Approve all pending drafts in pipeline_drafts.json
print("Step 1: Approving pending drafts...")
with open(DRAFTS_FILE, "r", encoding="utf-8") as f:
    drafts = json.load(f)

approved_count = 0
for d in drafts:
    if d.get("status") == "pending":
        d["status"] = "approved"
        approved_count += 1

print(f"  Approved {approved_count} pending drafts")

# Step 2: Generate cold follow-up drafts and add as approved
print("\nStep 2: Generating cold follow-up drafts...")
ghl = GHLClient()
state = load_state()
cold_due = get_due_contacts(state, phase="cold_outreach")
print(f"  {len(cold_due)} cold contacts due for follow-up")

followup_count = 0
for item in cold_due:
    cid = item["contact_id"]
    touch_number = item["touch_number"]
    entry = get_state_contact(state, cid)

    # Skip if already has a draft in the file
    existing = [d for d in drafts if d.get("contact_id") == cid and d.get("touch_number") == touch_number]
    if existing:
        print(f"  Skipping {cid} - already has a draft")
        continue

    try:
        contact = ghl.get_contact(cid)
        property_type = entry.get("property_type", "other") if entry else "other"
        initial_channel = entry.get("channel", "email") if entry else "email"

        matrix = cold_touch_matrix(initial_channel)
        touch_info = matrix[touch_number]
        channel = touch_info["channel"]

        subject, body = get_touch_content(
            touch_info=touch_info,
            contact=contact,
            property_type=property_type,
            phase="cold_outreach",
            state_entry=entry,
            ghl_client=ghl,
            contact_id=cid,
        )

        first_name = entry.get("first_name", "") if entry else ""
        org = entry.get("organization", "") if entry else ""

        drafts.append({
            "contact_id": cid,
            "name": f"{first_name} {entry.get('last_name', '')}".strip() if entry else "",
            "organization": org,
            "property_type": property_type,
            "channel": channel,
            "email": entry.get("email", "") if entry else "",
            "phone": entry.get("phone", "") if entry else "",
            "subject": subject,
            "message": body,
            "message_plain": body,
            "plain_text_mode": True,
            "selected_variant": "a",
            "status": "approved",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "touch_number": touch_number,
            "is_follow_up": True,
        })
        followup_count += 1
        print(f"  Generated follow-up for {first_name} ({org or 'no org'})")

    except Exception as e:
        print(f"  ERROR for {cid}: {e}")

# Save all drafts
tmp = DRAFTS_FILE.with_suffix(".tmp")
with open(tmp, "w", encoding="utf-8") as f:
    json.dump(drafts, f, indent=2, ensure_ascii=False)
os.replace(tmp, DRAFTS_FILE)

total_to_send = approved_count + followup_count
print(f"\nTotal ready to send: {total_to_send}")
print(f"  New cold outreach: {approved_count}")
print(f"  Cold follow-ups: {followup_count}")

# Step 3: Wait until 2 PM Pacific, then send
target_hour = 14  # 2 PM
now = datetime.now()
if now.hour >= target_hour:
    print(f"\nIt's already past 2 PM ({now.strftime('%I:%M %p')}). Sending now...")
else:
    wait_until = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    wait_secs = (wait_until - now).total_seconds()
    print(f"\nCurrent time: {now.strftime('%I:%M %p')}")
    print(f"Waiting until 2:00 PM ({int(wait_secs // 60)} minutes)...")
    time.sleep(wait_secs)
    print(f"It's 2 PM - sending now!")

# Step 4: Run the send
print("\nSending...")
project_dir = str(Path(__file__).resolve().parent.parent)
subprocess.run(
    [sys.executable, "-m", "sales_pipeline.run_pipeline", "--send"],
    cwd=project_dir,
)

print("\nDone!")
