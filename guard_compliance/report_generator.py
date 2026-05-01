# guard_compliance/report_generator.py
"""
Guard Compliance — Report Generator
Builds the HTML compliance dashboard and sends it.
"""

import logging

from guard_compliance.compliance_engine import calculate_status
from guard_compliance.templates.compliance_report import (
    build_compliance_report_html, build_report_subject,
)
from guard_compliance.notification_sender import send_compliance_report

log = logging.getLogger(__name__)


def build_officer_summaries(state: dict, bsis_results: dict = None) -> list[dict]:
    """
    Transform the compliance state into a list of officer summary dicts
    suitable for the report template.
    """
    officers = state.get("officers", {})
    bsis_results = bsis_results or {}
    summaries = []

    for oid, data in officers.items():
        creds = data.get("credentials", {})
        gc_expiry = creds.get("guard_card_expiry")

        if isinstance(gc_expiry, dict):
            gc_expiry = gc_expiry.get("expiry")

        gc_status, gc_days = calculate_status(gc_expiry)

        bsis = bsis_results.get(oid, {})

        summaries.append({
            "name": data.get("name", "Unknown"),
            "email": data.get("email", ""),
            "phone": data.get("phone", ""),
            "guard_card_status": gc_status,
            "guard_card_days": gc_days,
            "guard_card_expiry": gc_expiry or "N/A",
            "bsis_verified": bsis.get("verified"),
            "bsis_status": bsis.get("dca_status", ""),
        })

    # Sort: expired first, then by days remaining
    summaries.sort(key=lambda x: (
        x["guard_card_days"] if x["guard_card_days"] is not None else 9999
    ))

    return summaries


def generate_and_send_report(state: dict, bsis_results: dict = None,
                             test_mode: bool = True) -> bool:
    """
    Generate the compliance report and email it.
    Returns True if sent successfully.
    """
    summaries = build_officer_summaries(state, bsis_results)
    total = len(summaries)
    compliant = sum(1 for s in summaries if s["guard_card_status"] == "valid")

    html = build_compliance_report_html(summaries, test_mode=test_mode)
    subject = build_report_subject(compliant, total)

    if test_mode:
        subject = f"[TEST] {subject}"

    log.info(f"Compliance report: {compliant}/{total} compliant")
    return send_compliance_report(html, subject, test_mode=test_mode)
