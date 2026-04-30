import base64
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

import anthropic

SCRIPT_DIR = Path(__file__).parent

BCC_LIST = ["salarcon@americalpatrol.com", "don@americalpatrol.com"]

SIGNATURE = (
    "Best Regards,\n"
    "Larry\n\n"
    "Americal Patrol, Inc.\n"
    "Mailing: 3301 Harbor Blvd., Oxnard, CA 93035\n"
    "VC Office: (805) 844-9433  |  LA & OC Office: (714) 521-0855  |  FAX: (866) 526-8472\n"
    "www.americalpatrol.com"
)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _format_account_name(account):
    """Format account name for client-facing emails (no internal T-x labels)."""
    return account['name']


def _accounts_label(group):
    """e.g. 'Industry Centre T-9 & Baldwin Place'  (used in email body)"""
    return ' & '.join(_format_account_name(a) for a in group['accounts'])


def _subject_property_names(group):
    """
    Property names separated by ' | ' for the subject line.
    e.g. 'Manhattan Plaza' or 'Stadium Plaza | Towers Industrial Park | ...'
    """
    return ' | '.join(sorted(a['name'] for a in group['accounts']))


def _date_from_dar_filenames(pdf_paths):
    """Extract earliest first-date from DAR PDF filenames (fallback when parsing fails)."""
    import re as _re
    fn_dates = []
    for p in pdf_paths:
        # Only look at DAR filenames, skip incident reports
        stem = p.stem
        if 'DAR' not in stem or 'Incident' in stem:
            continue
        matches = _re.findall(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)_(\d{2})_(\d{4})', stem
        )
        if matches:
            mon, day, year = matches[0]  # first date = shift start
            try:
                fn_dates.append(datetime.strptime(f"{mon} {day} {year}", "%b %d %Y"))
            except ValueError:
                pass
    return min(fn_dates) if fn_dates else None


def _report_date(reports_data, pdf_paths=None):
    """Pull MM/DD/YYYY date string — DAR reports only (incident reports excluded).
    DAR reports have a non-empty 'rounds' list; incident reports have rounds=[].
    Falls back to DAR filenames when no DAR could be parsed.
    """
    dates = []
    for rd in reports_data:
        if rd and rd.get('date') and rd.get('rounds'):  # DARs only
            try:
                dates.append(datetime.strptime(rd['date'], '%B %d, %Y'))
            except ValueError:
                pass
    if dates:
        return min(dates).strftime('%m/%d/%Y')
    # Fallback: DAR filenames
    if pdf_paths:
        fn_dt = _date_from_dar_filenames(pdf_paths)
        if fn_dt:
            return fn_dt.strftime('%m/%d/%Y')
    return datetime.now().strftime('%m/%d/%Y')


def _report_date_long(reports_data, pdf_paths=None):
    """Pull long-form date — DAR reports only (incident reports excluded).
    Falls back to DAR filenames when no DAR could be parsed.
    """
    dates = []
    for rd in reports_data:
        if rd and rd.get('date') and rd.get('rounds'):  # DARs only
            try:
                dates.append(datetime.strptime(rd['date'], '%B %d, %Y'))
            except ValueError:
                pass
    if dates:
        dt = min(dates)
        return f"{dt.strftime('%B')} {dt.day}, {dt.year}"
    # Fallback: DAR filenames
    if pdf_paths:
        fn_dt = _date_from_dar_filenames(pdf_paths)
        if fn_dt:
            return f"{fn_dt.strftime('%B')} {fn_dt.day}, {fn_dt.year}"
    now = datetime.now()
    return f"{now.strftime('%B')} {now.day}, {now.year}"


# ── Incident summary via Claude ───────────────────────────────────────────────
def _compose_incident_summary(reports_data):
    """
    Use Claude to write a professional incident summary.
    - One paragraph per property that has incidents (separated by blank line).
    - Each paragraph starts with "Incident Summary — {Property}:" or
      just "Incident Summary:" when there is only one property with incidents.
    - Varied connective language (no repetitive "Additionally").
    """
    client = anthropic.Anthropic()

    # Group incidents by property
    by_property = {}
    for rd in reports_data:
        if not rd or not rd.get('has_incidents') or rd.get('report_type') == 'guest_parking':
            continue
        prop = rd['property']
        if prop not in by_property:
            by_property[prop] = []
        for r in rd['incident_rounds']:
            notes = '; '.join(r['incident_notes']) if r['incident_notes'] else 'Incident noted'
            incident_date = (
                r['timestamp'].strftime('%B %d, %Y')
                if r.get('timestamp') else rd['date']
            )
            by_property[prop].append(
                f"Date: {incident_date} | Time: {r['time_str']} | "
                f"Officer: {r['officer']} | Details: {notes}"
            )

    if not by_property:
        return None

    single_property = len(by_property) == 1

    prompt_parts = [
        "You are writing an incident summary section for a professional security patrol email "
        "sent to a client.",
        "",
        "Rules:",
        "- Be factual, concise, and professional.",
        "- Vary connective language — do not use 'Additionally' more than once per paragraph. "
        "Mix in alternatives like: 'A second incident occurred at...', 'Later that evening...', "
        "'At [time], Officer [name] observed...', 'Shortly after...', "
        "'Officer [name] also reported...', 'At approximately [time]...', etc.",
        "- Do not start consecutive sentences with the same word.",
        "- Write in past tense.",
        "- Do not editorialize or add opinions.",
        "",
    ]

    if single_property:
        prop = list(by_property.keys())[0]
        lines = by_property[prop]
        prompt_parts += [
            "Write ONE paragraph starting with exactly 'Incident Summary:' followed by a space.",
            "Describe all incidents below in chronological order.",
            "",
            f"Incidents at {prop}:",
        ] + lines
    else:
        prompt_parts += [
            "Write one paragraph PER PROPERTY, each starting with exactly "
            "'Incident Summary — [Property Name]:' followed by a space.",
            "Separate paragraphs with a blank line between them.",
            "Describe each property's incidents in chronological order within its paragraph.",
            "",
            "Incidents by property:",
        ]
        for prop, lines in by_property.items():
            prompt_parts.append(f"\n{prop}:")
            prompt_parts.extend(lines)

    msg = client.messages.create(
        model='claude-sonnet-4-6',
        max_tokens=500,
        messages=[{'role': 'user', 'content': '\n'.join(prompt_parts)}]
    )
    return msg.content[0].text.strip()


# ── Email body builder ────────────────────────────────────────────────────────
def compose_email_body(group, reports_data, pdf_paths=None):
    """
    Build the full email body using the Americal Patrol format.
    Returns (subject, body) tuple.

    Subject format:  {Property name(s)} Daily Security Report {Date}
      - Multiple properties separated by ' | '
      - Date comes from the PDF report timestamp (so overnight properties
        show the correct next-calendar-day date automatically)
    """
    label      = _accounts_label(group)        # used in body text (& separator)
    date_str   = _report_date(reports_data, pdf_paths)    # MM/DD/YYYY for body text
    props      = _subject_property_names(group)   # "Prop A | Prop B" for subject
    # Guest parking findings (vehicles on streets) are routine, not security incidents
    has_incidents = any(
        rd and rd.get('has_incidents') and rd.get('report_type') != 'guest_parking'
        for rd in reports_data
    )

    # ── Subject ───────────────────────────────────────────────────
    subject = f"{props} Daily Security Report {date_str}"

    # ── One-time announcement for new branded reports ────────────
    import os
    _announce = os.environ.get("BRANDED_PDF_ANNOUNCE", "").lower() in ("true", "1", "yes")
    _announcement = ""
    if _announce:
        _announcement = (
            "Please note: We have upgraded our reporting format to provide you with "
            "enhanced, professionally branded security reports. These new reports "
            "include executive summaries, detailed patrol documentation, and improved "
            "photo quality for your convenience.\n\n"
            "For incident reports, you will notice a color-coded severity indicator "
            "on the executive summary section:\n"
            "  - Red: Serious incidents (trespassing, theft, vandalism, break-in)\n"
            "  - Orange: Elevated concerns (suspicious activity, disturbance, alarm activation)\n"
            "  - Blue: Low-level matters (parking violations, noise complaints, key/lock issues)\n"
            "  - Green: Routine observations (maintenance issues, lighting, general property checks)\n\n"
            "This system allows you to quickly assess the severity of any reported "
            "incident at a glance.\n\n"
        )

    # ── Body ──────────────────────────────────────────────────────
    if not has_incidents:
        body = (
            f"Good morning,\n\n"
            f"{_announcement}"
            f"Please find attached the Daily Activity Reports for {label} for {date_str}. "
            f"No incidents were reported during this period.\n\n"
            f"{SIGNATURE}"
        )
    else:
        incident_para = _compose_incident_summary(reports_data)

        body = (
            f"Good morning,\n\n"
            f"{_announcement}"
            f"Please find attached the Daily Activity Report and Incident Report "
            f"for {label} for {date_str}.\n\n"
            f"{incident_para}\n\n"
            f"{SIGNATURE}"
        )

    return subject, body


# ── Gmail draft builder ───────────────────────────────────────────────────────
def build_draft(service, group, reports_data, pdf_paths, subject, email_body):
    """Create a Gmail draft with all PDFs attached and BCC to internal team."""
    msg            = MIMEMultipart()
    msg['To']      = ', '.join(group['recipients'])
    msg['Bcc']     = ', '.join(BCC_LIST)
    msg['Subject'] = subject
    msg.attach(MIMEText(email_body, 'plain'))

    for pdf_path in pdf_paths:
        with open(pdf_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=pdf_path.name)
        msg.attach(part)

    raw   = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    draft = service.users().drafts().create(
        userId='me',
        body={'message': {'raw': raw}}
    ).execute()

    print(f"[Composer] Draft created: '{subject}'")
    print(f"           To:  {', '.join(group['recipients'])}")
    print(f"           Bcc: {', '.join(BCC_LIST)}")
    print(f"           Attachments: {len(pdf_paths)} PDF(s)")
    return draft


def send_email(service, group, reports_data, pdf_paths, subject, email_body):
    """Send an email immediately with all PDFs attached and BCC to internal team."""
    msg            = MIMEMultipart()
    msg['To']      = ', '.join(group['recipients'])
    msg['Bcc']     = ', '.join(BCC_LIST)
    msg['Subject'] = subject
    msg.attach(MIMEText(email_body, 'plain'))

    for pdf_path in pdf_paths:
        with open(pdf_path, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=pdf_path.name)
        msg.attach(part)

    raw     = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    message = service.users().messages().send(
        userId='me',
        body={'raw': raw}
    ).execute()

    print(f"[Composer] Email SENT: '{subject}'")
    print(f"           To:  {', '.join(group['recipients'])}")
    print(f"           Bcc: {', '.join(BCC_LIST)}")
    print(f"           Attachments: {len(pdf_paths)} PDF(s)")
    return message


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    from pdf_analyzer import parse_report, match_client
    from email_fetcher import get_gmail_service
    from collections import defaultdict

    folder = SCRIPT_DIR.parent / 'Americal Patrol Morning Reports'
    pdfs   = list(folder.glob('*.pdf'))
    if not pdfs:
        print("No PDFs found.")
        exit(1)

    grouped = defaultdict(lambda: {'group': None, 'pdfs': [], 'reports_data': []})
    for pdf in pdfs:
        grp = match_client(pdf)
        if not grp:
            continue
        gid = grp['group_id']
        grouped[gid]['group'] = grp
        grouped[gid]['pdfs'].append(pdf)
        grouped[gid]['reports_data'].append(parse_report(pdf))

    service = get_gmail_service()
    for gid, data in grouped.items():
        subject, body = compose_email_body(data['group'], data['reports_data'])
        print(f"\nSubject: {subject}")
        print("-" * 50)
        print(body)
        print("-" * 50)
        build_draft(service, data['group'], data['reports_data'], data['pdfs'], subject, body)
