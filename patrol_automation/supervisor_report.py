import re, json, base64
from datetime import datetime, timedelta
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

SCRIPT_DIR     = Path(__file__).parent
SCHEDULE_FILE  = SCRIPT_DIR / "schedule.json"
RECIPIENTS     = ["salarcon@americalpatrol.com", "don@americalpatrol.com"]
GAP_THRESHOLD  = 90
LATE_THRESHOLD = 30


def load_schedule():
    with open(SCHEDULE_FILE) as f:
        return json.load(f)["accounts"]


def get_clock_outs(property_name, report_date, schedule):
    prop_lower = property_name.lower()
    for sn, entries in schedule.items():
        if sn.lower() in prop_lower or prop_lower in sn.lower():
            result = []
            for e in entries:
                h, m = map(int, e["clock_out"].split(":"))
                base = datetime(report_date.year, report_date.month, report_date.day, h, m)
                if e.get("overnight", False):
                    base += timedelta(days=1)
                result.append(base)
            return result
    return []


def parse_vehicle_inspections(pdf_path):
    try:
        import pdfplumber
    except ImportError:
        return []
    inspections = []
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            tl = text.lower()
            if "vehicle" not in tl or "inspection" not in tl:
                continue
            if "post" in tl:
                insp_type = "post"
            elif "pre" in tl:
                insp_type = "pre"
            else:
                continue
            ts = re.search(r"(\d{2}/\d{2}/\d{4}),\s*(\d{1,2}:\d{2}\s*[AP]M)", text)
            timestamp = None
            time_str  = None
            if ts:
                try:
                    timestamp = datetime.strptime(
                        ts.group(1) + " " + ts.group(2).strip(), "%m/%d/%Y %I:%M %p"
                    )
                    time_str = timestamp.strftime("%I:%M %p").lstrip("0")
                except ValueError:
                    pass
            mileage = None
            for line in text.splitlines():
                if re.search(r"mile|odometer", line, re.IGNORECASE):
                    nums = re.findall(r"\b(\d{4,6})\b", line)
                    if nums:
                        mileage = int(nums[0])
                        break
            if mileage is None:
                nums = re.findall(r"\b(\d{4,6})\b", text)
                if nums:
                    mileage = int(nums[0])
            officer = None
            _skip = {'vehicle', 'inspection', 'patrol', 'report', 'pre', 'post', 'daily', 'auto'}
            for line in [l.strip() for l in text.strip().splitlines() if l.strip()]:
                if (re.match(r"^[A-Z][a-z]+(?: [A-Z][a-z]+)+$", line)
                        and not any(w in line.lower() for w in _skip)):
                    officer = line
                    break
            if timestamp:
                inspections.append({
                    "type": insp_type, "officer": officer or "Unknown",
                    "timestamp": timestamp, "time_str": time_str or "Unknown",
                    "mileage": mileage, "pdf_name": pdf_path.stem,
                })
    return inspections


def find_patrol_gaps(all_reports_data, gap_minutes=GAP_THRESHOLD):
    gaps = []
    for report in all_reports_data:
        rounds = sorted(report.get("rounds", []), key=lambda r: r["timestamp"])
        for i in range(1, len(rounds)):
            prev  = rounds[i - 1]
            curr  = rounds[i]
            delta = (curr["timestamp"] - prev["timestamp"]).total_seconds() / 60
            if delta > gap_minutes:
                gaps.append({
                    "property":    report["property"],
                    "officer":     curr["officer"],
                    "from_time":   prev["time_str"],
                    "to_time":     curr["time_str"],
                    "gap_minutes": int(delta),
                })
    return gaps


def find_late_submissions(all_reports_data, schedule, early_minutes=LATE_THRESHOLD):
    """
    Flag properties where the guard's LAST patrol round was submitted more than
    early_minutes before their scheduled clock-out.
    e.g. clock-out 6:00 AM, last report 5:15 AM → 45 min early → flagged.
         clock-out 6:00 AM, last report 5:50 AM → 10 min early → OK.
    """
    early = []
    for report in all_reports_data:
        rounds = report.get("rounds", [])
        if not rounds:
            continue
        prop        = report["property"]
        report_date = rounds[0]["timestamp"].date()
        clock_outs  = get_clock_outs(prop, report_date, schedule)
        if not clock_outs:
            continue

        last_round = max(rounds, key=lambda r: r["timestamp"])

        for co in clock_outs:
            minutes_before = (co - last_round["timestamp"]).total_seconds() / 60
            if minutes_before > early_minutes:
                early.append({
                    "property":      prop,
                    "officer":       last_round["officer"],
                    "last_report":   last_round["time_str"],
                    "clock_out":     co.strftime("%I:%M %p").lstrip("0"),
                    "minutes_early": int(minutes_before),
                })
    return early


def _vehicle_table_html(all_inspections):
    by_officer = {}
    for insp in all_inspections:
        o = insp["officer"]
        if o not in by_officer:
            by_officer[o] = {}
        by_officer[o][insp["type"]] = insp

    if not by_officer:
        return "<p style='color:#666;font-style:italic'>No vehicle inspection data found.</p>"

    rows = []
    for officer in sorted(by_officer):
        data = by_officer[officer]
        pre  = data.get("pre")
        post = data.get("post")
        pre_t  = pre["time_str"]      if pre  else "<span style='color:#e63946'>MISSING</span>"
        pre_m  = str(pre["mileage"])  if pre  and pre["mileage"]  else "N/A"
        post_t = post["time_str"]     if post else "<span style='color:#e63946'>MISSING</span>"
        post_m = str(post["mileage"]) if post and post["mileage"] else "N/A"
        if pre and post and pre.get("mileage") and post.get("mileage"):
            drv    = str(post["mileage"] - pre["mileage"]) + " mi"
            status = "<span style='color:#2d6a4f;font-weight:bold'>OK</span>"
        elif not pre and not post:
            drv    = "N/A"
            status = "<span style='color:#e63946;font-weight:bold'>Both MISSING</span>"
        else:
            drv    = "N/A"
            status = "<span style='color:#e63946;font-weight:bold'>Incomplete</span>"
        rows.append(
            "<tr style='border-bottom:1px solid #eee'>"
            + "<td style='padding:8px'>" + officer + "</td>"
            + "<td style='padding:8px'>" + pre_t   + "</td>"
            + "<td style='padding:8px'>" + pre_m   + "</td>"
            + "<td style='padding:8px'>" + post_t  + "</td>"
            + "<td style='padding:8px'>" + post_m  + "</td>"
            + "<td style='padding:8px'>" + drv     + "</td>"
            + "<td style='padding:8px'>" + status  + "</td>"
            + "</tr>"
        )
    hdr = (
        "<table cellpadding='0' cellspacing='0' style='border-collapse:collapse;"
        "width:100%;font-size:13px'>"
        "<thead><tr style='background:#1a1a2e;color:#fff'>"
        "<th style='padding:8px;text-align:left'>Officer</th>"
        "<th style='padding:8px;text-align:left'>Pre Time</th>"
        "<th style='padding:8px;text-align:left'>Starting Miles</th>"
        "<th style='padding:8px;text-align:left'>Post Time</th>"
        "<th style='padding:8px;text-align:left'>Ending Miles</th>"
        "<th style='padding:8px;text-align:left'>Miles Driven</th>"
        "<th style='padding:8px;text-align:left'>Status</th>"
        "</tr></thead>"
    )
    return hdr + "<tbody>" + "".join(rows) + "</tbody></table>"


def _section(bc, title, body):
    return (
        "<div style='border:1px solid " + bc + ";border-radius:6px;padding:18px;margin-bottom:24px'>"
        "<h2 style='margin:0 0 14px;font-size:16px;color:" + bc + "'>" + title + "</h2>"
        + body + "</div>"
    )


def _simple_table(headers, rows_html):
    th = "".join("<th style='padding:8px;text-align:left'>" + h + "</th>" for h in headers)
    return (
        "<table cellpadding='0' cellspacing='0' style='border-collapse:collapse;"
        "width:100%;font-size:13px'>"
        "<thead><tr style='background:#1a1a2e;color:#fff'>" + th + "</tr></thead>"
        "<tbody>" + rows_html + "</tbody></table>"
    )


def _post_checks_html(post_checks_data):
    if not post_checks_data:
        return "<p style='color:#2d6a4f;font-weight:bold'>No post check reports received.</p>"

    rows = []
    for report in post_checks_data:
        for entry in report.get("post_checks", []):
            notes_cell = entry.get("notes") or "&mdash;"
            post_cell  = entry.get("post")  or "&mdash;"
            rows.append(
                "<tr style='border-bottom:1px solid #eee'>"
                + "<td style='padding:8px;white-space:nowrap'>" + entry["time_str"] + "</td>"
                + "<td style='padding:8px'>" + entry["officer"] + "</td>"
                + "<td style='padding:8px'>" + post_cell + "</td>"
                + "<td style='padding:8px;font-size:12px;color:#555'>" + notes_cell + "</td>"
                + "</tr>"
            )

    if not rows:
        return "<p style='color:#666;font-style:italic'>No entries found in post check reports.</p>"

    return (
        "<div style='overflow-x:auto;-webkit-overflow-scrolling:touch'>"
        + _simple_table(["Time", "Officer", "Post", "Notes"], "".join(rows))
        + "</div>"
    )


def build_html_report(date_str, all_reports_data, all_inspections, gaps, late_submissions, post_checks_data=None):
    incident_reports = [r for r in all_reports_data if r.get("has_incidents")]
    if incident_reports:
        blocks = []
        for r in incident_reports:
            for rnd in r.get("incident_rounds", []):
                notes = "; ".join(rnd.get("incident_notes", [])) or "See attached PDF."
                blocks.append(
                    "<div style='border-left:4px solid #e63946;padding:8px 14px;"
                    "margin-bottom:10px;background:#fff5f5;border-radius:3px'>"
                    "<strong>" + r["property"] + "</strong> &mdash; "
                    + rnd["time_str"] + " &mdash; " + rnd["officer"] + "<br>"
                    + "<span style='color:#555;font-size:13px'>" + notes + "</span></div>"
                )
        incident_html = "".join(blocks)
    else:
        incident_html = (
            "<p style='color:#2d6a4f;font-weight:bold'>"
            "No incidents reported. All patrols completed normally.</p>"
        )

    vehicle_html = "<div style='overflow-x:auto;-webkit-overflow-scrolling:touch'>" + _vehicle_table_html(all_inspections) + "</div>"

    if gaps:
        gr = "".join(
            "<tr style='border-bottom:1px solid #eee'>"
            + "<td style='padding:8px'>" + g["property"]  + "</td>"
            + "<td style='padding:8px'>" + g["officer"]   + "</td>"
            + "<td style='padding:8px'>" + g["from_time"] + "</td>"
            + "<td style='padding:8px'>" + g["to_time"]   + "</td>"
            + "<td style='padding:8px;color:#e07c00;font-weight:bold'>"
            + str(g["gap_minutes"]) + " min</td></tr>"
            for g in gaps
        )
        gap_html = "<div style='overflow-x:auto;-webkit-overflow-scrolling:touch'>" + _simple_table(["Property", "Officer", "From", "To", "Gap"], gr) + "</div>"
    else:
        gap_html = "<p style='color:#2d6a4f;font-weight:bold'>No patrol gaps over 90 minutes detected.</p>"

    if late_submissions:
        lr = "".join(
            "<tr style='border-bottom:1px solid #eee'>"
            + "<td style='padding:8px'>" + ls["property"]    + "</td>"
            + "<td style='padding:8px'>" + ls["officer"]     + "</td>"
            + "<td style='padding:8px'>" + ls["last_report"] + "</td>"
            + "<td style='padding:8px'>" + ls["clock_out"]   + "</td>"
            + "<td style='padding:8px;color:#e07c00;font-weight:bold'>-"
            + str(ls["minutes_early"]) + " min</td></tr>"
            for ls in late_submissions
        )
        late_html = "<div style='overflow-x:auto;-webkit-overflow-scrolling:touch'>" + _simple_table(["Property", "Officer", "Last Report", "Clock-Out", "Early By"], lr) + "</div>"
    else:
        late_html = "<p style='color:#2d6a4f;font-weight:bold'>No early endings detected.</p>"

    patrol_checks     = [r for r in (post_checks_data or []) if r.get("report_type") == "patrol_post_check"]
    supervisor_checks = [r for r in (post_checks_data or []) if r.get("report_type") == "supervisor_checklist"]

    body = (
        _section("#e63946", "Incident Summary", incident_html)
        + _section("#1a1a2e", "Vehicle Inspections", vehicle_html)
        + _section("#e07c00", "Patrol Gaps (&gt;90 Minutes)", gap_html)
        + _section("#e07c00", "Early Endings (&gt;30 Min Before Clock-Out)", late_html)
        + _section("#2563eb", "Patrol Officer Post Checks", _post_checks_html(patrol_checks))
        + _section("#7c3aed", "Supervisor Post Checklist", _post_checks_html(supervisor_checks))
    )

    return (
        "<!DOCTYPE html>\n<html>\n"
        "<head><meta name='viewport' content='width=device-width,initial-scale=1'></head>\n"
        "<body style='font-family:Arial,sans-serif;margin:0;padding:12px;background:#f4f4f4'>\n"
        "<div style='width:100%;max-width:960px;margin:auto;background:#fff;border-radius:8px;"
        "overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)'>\n"
        "  <div style='background:#1a1a2e;color:#fff;padding:16px'>\n"
        "    <h1 style='margin:0;font-size:20px'>Americal Patrol &mdash; Supervisor Report</h1>\n"
        "    <p style='margin:6px 0 0;opacity:0.75;font-size:14px'>" + date_str + "</p>\n"
        "  </div>\n"
        "  <div style='padding:16px'>" + body + "</div>\n"
        "  <div style='background:#f0f0f0;padding:10px 16px;font-size:11px;color:#999'>\n"
        "    Generated automatically by Americal Patrol Report Automation\n"
        "  </div>\n"
        "</div>\n</body>\n</html>"
    )


def send_supervisor_report(service, date_str, all_reports_data, all_pdfs, inspection_pdfs=None, api_inspections=None):
    schedule        = load_schedule()

    # Use API-sourced inspections if provided, otherwise fall back to PDF parsing
    if api_inspections is not None:
        all_inspections = api_inspections
    else:
        from pdf_analyzer import parse_report as _parse_report
        all_inspections = []
        for pdf in (inspection_pdfs or []):
            all_inspections.extend(parse_vehicle_inspections(pdf))
        for pdf in all_pdfs:
            all_inspections.extend(parse_vehicle_inspections(pdf))

    # TODO: Post check data currently not fetched via API. The PDF stem matching
    # below won't find post checks in API-generated branded PDFs. Add a
    # submissions_to_post_check() in connecteam_api.py when this data is needed.
    post_checks_data = []

    gaps             = find_patrol_gaps(all_reports_data)
    late_submissions = find_late_submissions(all_reports_data, schedule)
    html_body        = build_html_report(date_str, all_reports_data, all_inspections, gaps, late_submissions, post_checks_data=post_checks_data)
    subject          = "Supervisor Report - " + date_str

    # HTML-only email — no PDF attachments (files are too large and not needed here;
    # the supervisor report IS the analysis)
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"]      = ", ".join(RECIPIENTS)
    msg.attach(MIMEText(html_body, "html"))

    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(
        userId="me", body={"raw": raw}
    ).execute()
    print("[Supervisor] Report sent to: " + ", ".join(RECIPIENTS))
    return True




QUALITY_RECIPIENT = "salarcon@americalpatrol.com"


def _send_email(service, to, subject, body, html=False):
    """Send an email immediately (not a draft)."""
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["To"]      = to
    msg.attach(MIMEText(body, "html" if html else "plain"))
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    service.users().messages().send(
        userId="me",
        body={"raw": raw}
    ).execute()


def _build_quality_html(date_str, results):
    """
    Render quality check results as HTML cards, one per report file.
    Each entry shows original text and a corrected/translated version for easy copy-paste.
    """
    total_entries = sum(len(r.get("entries", [])) for r in results)
    total_reports = len(results)

    badge_colors = {
        "spelling": ("background:#e07c00;color:#fff", "SPELLING"),
        "grammar":  ("background:#b8860b;color:#fff", "GRAMMAR"),
        "language": ("background:#1e6091;color:#fff", "LANGUAGE"),
    }

    summary_bar = (
        "<div style='background:#f0f4f8;border-radius:6px;padding:10px 14px;"
        "margin-bottom:20px;font-size:13px;color:#333'>"
        "<strong>" + str(total_reports) + " reports reviewed</strong>"
        " &middot; "
        "<strong style='color:" + ("#e07c00" if total_entries else "#2d6a4f") + "'>"
        + str(total_entries) + " entr" + ("ies" if total_entries != 1 else "y") + " with issues"
        "</strong></div>"
    )

    cards = []
    for r in results:
        filename = r.get("filename", "Unknown")
        entries  = r.get("entries", [])

        if not entries:
            continue

        entry_blocks = []
        for entry in entries:
            badges_html = ""
            for itype in entry.get("issue_types", []):
                style, label = badge_colors.get(itype.lower(), ("background:#888;color:#fff", itype.upper()))
                badges_html += (
                    "<span style='" + style + ";padding:2px 6px;border-radius:3px;"
                    "font-size:11px;font-weight:bold;margin-right:6px'>" + label + "</span>"
                )

            summary   = (entry.get("summary") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            original  = (entry.get("original") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            corrected = (entry.get("corrected") or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

            is_translation = "language" in [t.lower() for t in entry.get("issue_types", [])]
            corrected_label = "Translation (copy this)" if is_translation else "Corrected (copy this)"

            entry_blocks.append(
                "<div style='margin-bottom:14px;padding:10px;background:#fafafa;border-radius:4px;"
                "border-left:3px solid #e07c00'>"
                "<div style='margin-bottom:8px'>" + badges_html
                + "<span style='font-size:12px;color:#666;font-style:italic'>" + summary + "</span>"
                "</div>"
                "<div style='margin-bottom:6px'>"
                "<div style='font-size:11px;color:#999;text-transform:uppercase;margin-bottom:2px'>Original:</div>"
                "<div style='font-size:12px;color:#888;line-height:1.5;padding:6px 8px;"
                "background:#f0f0f0;border-radius:3px;white-space:pre-wrap'>" + original + "</div>"
                "</div>"
                "<div>"
                "<div style='font-size:11px;color:#2d6a4f;text-transform:uppercase;font-weight:bold;"
                "margin-bottom:2px'>" + corrected_label + ":</div>"
                "<div style='font-size:13px;color:#1a1a2e;line-height:1.5;padding:8px 10px;"
                "background:#e8f5e9;border:1px solid #c8e6c9;border-radius:4px;"
                "white-space:pre-wrap'>" + corrected + "</div>"
                "</div>"
                "</div>"
            )

        cards.append(
            "<div style='border:1px solid #f8d7da;border-radius:6px;"
            "padding:14px;margin-bottom:14px'>"
            "<div style='font-weight:bold;font-size:13px;color:#1a1a2e;margin-bottom:10px'>"
            "&#128196; " + filename + "</div>"
            + "".join(entry_blocks)
            + "</div>"
        )

    content = summary_bar + "".join(cards)

    return (
        "<!DOCTYPE html><html>"
        "<head><meta name='viewport' content='width=device-width,initial-scale=1'></head>"
        "<body style='font-family:Arial,sans-serif;margin:0;padding:12px;background:#f4f4f4'>"
        "<div style='width:100%;max-width:800px;margin:auto;background:#fff;border-radius:8px;"
        "overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)'>"
        "  <div style='background:#1a1a2e;color:#fff;padding:14px 16px'>"
        "    <h2 style='margin:0;font-size:18px'>Report Quality Check</h2>"
        "    <p style='margin:4px 0 0;opacity:0.75;font-size:13px'>" + date_str + "</p>"
        "  </div>"
        "  <div style='padding:14px 16px'>" + content + "</div>"
        "  <div style='background:#f0f0f0;padding:10px 16px;font-size:11px;color:#999'>"
        "    Generated automatically by Americal Patrol Report Automation"
        "  </div>"
        "</div></body></html>"
    )


def check_and_send_quality_report(service, date_str, all_reports_data, pdf_paths=None):
    """
    Reviews ALL report text (via raw PDF extraction) for spelling/grammar errors
    and non-English content. Sends a direct email (not a draft) to QUALITY_RECIPIENT.
    """
    import os
    try:
        import anthropic
    except ImportError:
        print("[Quality] anthropic not installed. Run: pip install anthropic")
        return
    try:
        import pdfplumber
    except ImportError:
        print("[Quality] pdfplumber not installed. Run: pip install pdfplumber")
        return

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("[Quality] ANTHROPIC_API_KEY not set - skipping quality check")
        return

    subject = "Report Quality Check - " + date_str

    # ── Build text blocks from raw PDF extraction ────────────────────────────
    entries = []

    if pdf_paths:
        for pdf_path in pdf_paths:
            try:
                raw_text = []
                with pdfplumber.open(str(pdf_path)) as pdf:
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            raw_text.append(t)
                full_text = "\n".join(raw_text)
                # Sample up to 8000 chars — enough to capture full entries for correction
                sample = full_text[:8000].strip()
                if sample:
                    entries.append({
                        "filename": pdf_path.name,
                        "text": sample,
                    })
            except Exception as exc:
                print(f"[Quality] Could not extract text from {pdf_path.name}: {exc}")

    # Fallback: use parsed structured data if no PDFs were supplied
    if not entries:
        for report in all_reports_data:
            text_items = []
            for rnd in report.get("rounds", []):
                text_items.extend(rnd.get("incident_notes", []))
                for key, val in rnd.get("checks", {}).items():
                    if val and val.lower() not in ("completed", "unknown", "n/a", ""):
                        text_items.append(key.replace("_", " ").title() + ": " + val)
            if text_items:
                entries.append({
                    "filename": report["property"],
                    "text": " | ".join(text_items),
                })

    if not entries:
        _send_email(
            service, QUALITY_RECIPIENT, subject,
            "Quality check for " + date_str + ":\n\nNo report text found to review."
        )
        print("[Quality] No report text found. Clean report sent.")
        return

    text_block = "\n\n---\n\n".join(
        "File: " + e["filename"] + "\n\n" + e["text"]
        for e in entries
    )

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=16000,
        messages=[{
            "role": "user",
            "content": (
                "You are reviewing security patrol reports written by security guards.\n\n"
                "Return ONLY a valid JSON array. No explanation, no markdown, no preamble.\n\n"
                "Each element represents one report file:\n"
                '{"filename": "exact filename as given", '
                '"entries": [{"original": "exact text of the entry with issues", '
                '"corrected": "complete corrected text", '
                '"issue_types": ["spelling", "grammar", "language"], '
                '"summary": "brief description of fixes"}]}\n\n'
                "RULES:\n"
                "- Only include entries that have spelling errors, grammar errors, or non-English text.\n"
                "- For spelling/grammar errors: 'corrected' = the FULL entry text with ALL errors fixed.\n"
                "- For Spanish or non-English text: 'corrected' = the FULL English translation.\n"
                "- For entries that are in Spanish AND have errors: just provide the English translation.\n"
                "- 'original' must be the exact text from the report so the reader can locate it.\n"
                "- 'issue_types' lists all categories present (e.g. [\"spelling\", \"grammar\"]).\n"
                "- 'summary' is brief, e.g. '3 spelling fixes' or 'Translated from Spanish'.\n"
                "- Use an empty entries array for reports with no problems.\n\n"
                "Reports to review:\n\n" + text_block
            )
        }]
    )

    raw_response = response.content[0].text.strip()

    # Parse JSON — strip any accidental markdown fencing first
    import json, re as _re
    cleaned = _re.sub(r'^```\w*\s*|\s*```$', '', raw_response).strip()
    try:
        results  = json.loads(cleaned)
        html_body = _build_quality_html(date_str, results)
    except (json.JSONDecodeError, ValueError, TypeError):
        # Fallback: render raw text in the old <pre> style so email still delivers
        safe_text = raw_response.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        html_body = (
            "<!DOCTYPE html><html>"
            "<head><meta name='viewport' content='width=device-width,initial-scale=1'></head>"
            "<body style='font-family:Arial,sans-serif;margin:0;padding:12px;background:#f4f4f4'>"
            "<div style='width:100%;max-width:800px;margin:auto;background:#fff;border-radius:8px;"
            "overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,0.1)'>"
            "  <div style='background:#1a1a2e;color:#fff;padding:14px 16px'>"
            "    <h2 style='margin:0;font-size:18px'>Report Quality Check</h2>"
            "    <p style='margin:4px 0 0;opacity:0.75;font-size:13px'>" + date_str + "</p>"
            "  </div>"
            "  <div style='padding:14px 16px'>"
            "    <pre style='white-space:pre-wrap;font-family:Arial,sans-serif;"
            "font-size:13px;line-height:1.7;margin:0'>" + safe_text + "</pre>"
            "  </div>"
            "  <div style='background:#f0f0f0;padding:10px 16px;font-size:11px;color:#999'>"
            "    Generated automatically by Americal Patrol Report Automation"
            "  </div>"
            "</div></body></html>"
        )
        print("[Quality] WARNING: Could not parse JSON from Claude — fell back to plain text rendering.")

    _send_email(service, QUALITY_RECIPIENT, subject, html_body, html=True)
    print("[Quality] Quality report sent to: " + QUALITY_RECIPIENT)

if __name__ == "__main__":
    import sys
    from email_fetcher import get_gmail_service
    from pdf_analyzer import load_clients
    from connecteam_api import fetch_daily_reports, fetch_vehicle_inspections

    service = get_gmail_service()
    clients_groups = load_clients()
    report_date = datetime.now().date()

    api_data = fetch_daily_reports(clients_groups, report_date)
    all_reports_data = [d for reports in api_data.values() for d in reports]
    api_inspections = fetch_vehicle_inspections(report_date)

    date_str = datetime.now().strftime("%B %d, %Y")
    send_supervisor_report(service, date_str, all_reports_data, [], api_inspections=api_inspections)
    print("Done.")
