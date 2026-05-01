#!/usr/bin/env python3
"""
One-off: rewrite today's Harbor Lights guest parking violation draft,
excluding 6HLV373 and 7PSD094 from the Unpermitted Tow Authorization
section. Leaves everything else identical.

Replaces the existing draft (same subject) rather than creating a duplicate.
"""

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from parking_audit import (
    read_parking_records,
    find_violations,
    build_email,
    create_draft,
    get_gmail_service,
    HOA_RECIPIENTS,
    CC_LIST,
)

PLATES_TO_DROP = {"6HLV373", "7PSD094"}

# Existing draft to remove once the replacement is in place.
OLD_DRAFT_ID = "r-1183819636747193831"  # placeholder; discovered at runtime


def main():
    today = date.today()
    records = read_parking_records()
    rule_d, citation = find_violations(records, today)

    print(f"Run date       : {today}")
    print(f"Rule D plates  : {sorted(rule_d)}")
    print(f"Citation plates: {sorted(citation)}")

    dropped = [p for p in PLATES_TO_DROP if p in citation]
    not_found_citation = [p for p in PLATES_TO_DROP if p not in citation]
    in_rule_d = [p for p in PLATES_TO_DROP if p in rule_d]

    print(f"\nDropping from citation section: {dropped}")
    if not_found_citation:
        print(f"Not in citation section (no change needed): {not_found_citation}")
    if in_rule_d:
        print(f"NOTE: these are in Rule D section (not touched): {in_rule_d}")

    citation_filtered = {p: v for p, v in citation.items() if p not in PLATES_TO_DROP}

    subject, body = build_email(rule_d, today, citation_filtered)
    if subject is None:
        print("Nothing to report after filtering. Aborting.")
        return

    service = get_gmail_service()

    # Find and delete the existing draft with the same subject for today.
    drafts_list = service.users().drafts().list(userId="me", maxResults=50).execute()
    to_delete = []
    for d in drafts_list.get("drafts", []):
        msg = service.users().messages().get(
            userId="me", id=d["message"]["id"], format="metadata",
            metadataHeaders=["Subject"],
        ).execute()
        headers = msg.get("payload", {}).get("headers", [])
        subj = next((h["value"] for h in headers if h["name"] == "Subject"), "")
        if subj == subject:
            to_delete.append(d["id"])

    print(f"\nExisting drafts with same subject: {len(to_delete)}")

    new_id = create_draft(service, subject, body, HOA_RECIPIENTS, cc=CC_LIST)
    print(f"New draft created: {new_id}")

    for did in to_delete:
        service.users().drafts().delete(userId="me", id=did).execute()
        print(f"Deleted old draft: {did}")

    print("\nDone.")


if __name__ == "__main__":
    main()
