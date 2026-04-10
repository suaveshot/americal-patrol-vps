import re
import json
from pathlib import Path
from datetime import datetime

# ── Client loader ─────────────────────────────────────────────────────────────
SCRIPT_DIR   = Path(__file__).parent
CLIENTS_FILE = SCRIPT_DIR / 'clients.json'

def load_clients():
    with open(CLIENTS_FILE) as f:
        return json.load(f)['groups']

def match_client(pdf_path, subject=''):
    """
    Match a PDF to a client group using the filename and email subject.
    Returns the matching client group dict, or None if no match.
    """
    clients  = load_clients()
    haystack = (pdf_path.stem + ' ' + subject).lower()

    best_group = None
    best_score = 0

    for group in clients:
        score = sum(1 for kw in group['keywords'] if kw.lower() in haystack)
        if score > best_score:
            best_score = score
            best_group = group

    if best_score == 0:
        print(f"[Matcher] WARNING: No client match for '{pdf_path.name}'")
        return None

    names = [a['name'] for a in best_group['accounts']]
    print(f"[Matcher] '{pdf_path.name}' -> {', '.join(names)} (score={best_score})")
    return best_group


# ── PDF parser ────────────────────────────────────────────────────────────────
def parse_report(pdf_path):
    """
    Auto-detect PDF format and parse accordingly.
    Supports:
      - Standard DAR (Daily Activity Report): 'Officer checked for...' format
      - Incident Report: 'Type of Incident:' format
    Returns a dict with report data, or None if unparseable.
    """
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")

    stem_lower = pdf_path.stem.lower()
    if 'post_checklist' in stem_lower or 'supervisor_post' in stem_lower:
        return _parse_post_check_report(pdf_path, 'supervisor_checklist')
    elif 'post_check' in stem_lower:
        return _parse_post_check_report(pdf_path, 'patrol_post_check')
    elif 'incident_report' in stem_lower:
        return _parse_incident_report(pdf_path)
    elif 'guest_parking' in stem_lower:
        return _parse_guest_parking(pdf_path)

    # Peek at first page to detect format
    with pdfplumber.open(str(pdf_path)) as pdf:
        first_text = pdf.pages[0].extract_text() if pdf.pages else ''

    if first_text and 'Type of Incident:' in first_text:
        return _parse_incident_report(pdf_path)
    else:
        return _parse_dar(pdf_path)


def _date_from_filename(pdf_path, fallback_dt):
    """
    Extract the report date from the Connecteam filename.
    Filenames end with  _Mar_10_2026_Mar_10_2026  (start_date_end_date).
    We take the LAST occurrence, which is the report end date.
    Falls back to fallback_dt.strftime('%B %d, %Y') if no pattern found.
    """
    matches = re.findall(
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)_(\d{2})_(\d{4})',
        pdf_path.stem,
    )
    if matches:
        mon, day, year = matches[0]   # first date = shift start date
        try:
            return datetime.strptime(f"{mon} {day} {year}", "%b %d %Y").strftime('%B %d, %Y')
        except ValueError:
            pass
    return fallback_dt.strftime('%B %d, %Y')


def _parse_dar(pdf_path):
    """Parse standard Connecteam DAR (Daily Activity Report) PDFs."""
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")

    rounds        = []
    property_name = pdf_path.stem.split('DAR')[0].strip().rstrip('_').rstrip()

    image_only_pages = 0
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                if page.images:
                    image_only_pages += 1
                continue
            # Accept both standard DARs ("Officer checked") and
            # vehicle patrol DARs ("Any ... found on property?")
            is_standard = 'Officer checked' in text
            is_vehicle  = '?' in text and re.search(
                r'(homeless|unauthorized|lights|gates|property damage|dumping)',
                text, re.IGNORECASE,
            )
            if not is_standard and not is_vehicle:
                continue
            round_data = _parse_round_page(text)
            if round_data:
                rounds.append(round_data)

    # OCR fallback for image-only pages
    if not rounds and image_only_pages > 0:
        from ocr_fallback import is_ocr_available, ocr_pdf_pages
        if is_ocr_available():
            print(f"[Parser] OCR fallback: {pdf_path.name} has {image_only_pages} image-only pages, running Tesseract...")
            ocr_texts = ocr_pdf_pages(pdf_path)
            for page_num, text in ocr_texts.items():
                is_standard = 'Officer checked' in text
                is_vehicle  = '?' in text and re.search(
                    r'(homeless|unauthorized|lights|gates|property damage|dumping)',
                    text, re.IGNORECASE,
                )
                if not is_standard and not is_vehicle:
                    continue
                round_data = _parse_round_page(text)
                if round_data:
                    rounds.append(round_data)
            if rounds:
                print(f"[Parser] OCR extracted {len(rounds)} round(s) from {pdf_path.name}")
            else:
                print(f"[Parser] OCR completed but no parseable rounds found in {pdf_path.name}")
        else:
            print(f"[Parser] WARNING: {pdf_path.name} has {image_only_pages} image-only pages. "
                  f"Install Tesseract OCR for automatic text extraction: winget install tesseract-ocr.tesseract")

    if not rounds:
        return None

    rounds.sort(key=lambda r: r['timestamp'])
    report_date     = _date_from_filename(pdf_path, rounds[0]['timestamp'])
    officers        = list(dict.fromkeys(r['officer'] for r in rounds))
    has_incidents   = any(r['has_incident'] for r in rounds)
    incident_rounds = [r for r in rounds if r['has_incident']]

    return {
        'property':        property_name,
        'date':            report_date,
        'officers':        officers,
        'rounds':          rounds,
        'total_rounds':    len(rounds),
        'first_time':      rounds[0]['time_str'],
        'last_time':       rounds[-1]['time_str'],
        'has_incidents':   has_incidents,
        'incident_rounds': incident_rounds,
    }


def _parse_incident_page_text(text, pdf_path):
    """Parse a single page of text from an incident report.
    Returns (incident_dict, raw_timestamp) or (None, None) if not parseable."""
    lines         = [l.strip() for l in text.strip().splitlines() if l.strip()]
    officer       = None
    timestamp     = None
    time_str      = None
    incident_type = None
    report_text   = None
    address       = None

    for line in lines:
        ts_match = re.search(r'(\d{2}/\d{2}/\d{4}),\s*(\d{1,2}:\d{2}\s*[AP]M)', line)
        if ts_match:
            try:
                timestamp = datetime.strptime(
                    f"{ts_match.group(1)} {ts_match.group(2).strip()}",
                    '%m/%d/%Y %I:%M %p'
                )
                time_str = timestamp.strftime('%I:%M %p').lstrip('0')
            except ValueError:
                pass
            continue

        if line.startswith('Type of Incident:'):
            incident_type = line.split(':', 1)[1].strip()
        elif line.startswith('Report:'):
            report_text = line.split(':', 1)[1].strip()
        elif line.startswith('Address:'):
            addr = line.split(':', 1)[1].strip()
            if addr:
                address = addr
        elif re.match(r'^[A-Z][a-z]+(?: [A-Z][a-z]+)+$', line) and not officer and 'Incident Report' not in line:
            officer = line

    if not timestamp:
        return None, None

    raw_timestamp = timestamp

    # Correct after-midnight timestamps
    if timestamp.hour < 8:
        fn_matches = re.findall(
            r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)_(\d{2})_(\d{4})',
            pdf_path.stem,
        )
        if fn_matches:
            mon, day, yr = fn_matches[-1]
            try:
                fn_end = datetime.strptime(f"{mon} {day} {yr}", "%b %d %Y")
                if fn_end.date() > timestamp.date():
                    timestamp = timestamp.replace(
                        year=fn_end.year, month=fn_end.month, day=fn_end.day
                    )
                    time_str = timestamp.strftime('%I:%M %p').lstrip('0')
            except ValueError:
                pass

    notes = []
    if incident_type:
        notes.append(f"Type of Incident: {incident_type}")
    if address:
        notes.append(f"Address: {address}")
    if report_text:
        notes.append(f"Report: {report_text}")

    return {
        'officer':        officer or 'Unknown Officer',
        'timestamp':      timestamp,
        'time_str':       time_str or 'Unknown',
        'checks':         {},
        'has_incident':   True,
        'incident_notes': notes,
    }, raw_timestamp


def _parse_incident_report(pdf_path):
    """
    Parse Connecteam Incident Report PDFs.
    Each page with 'Type of Incident:' is one incident entry.
    Returns report data with has_incidents=True and all entries in incident_rounds.
    """
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")

    incidents = []

    # Extract property name — strip "Incident Report..." or "_Incident_Report..." suffix
    stem = pdf_path.stem
    for sep in ['_Incident_Report', ' Incident Report']:
        if sep.lower() in stem.lower():
            idx           = stem.lower().index(sep.lower())
            property_name = stem[:idx].replace('_', ' ').strip()
            break
    else:
        property_name = stem.replace('_', ' ').strip()

    raw_timestamps = []   # pre-correction timestamps for report-date fallback
    image_only_pages = 0

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                if page.images:
                    image_only_pages += 1
                continue
            if 'Type of Incident:' not in text:
                continue

            incident, raw_ts = _parse_incident_page_text(text, pdf_path)
            if incident:
                incidents.append(incident)
            if raw_ts:
                raw_timestamps.append(raw_ts)

    # OCR fallback for image-only pages
    if not incidents and image_only_pages > 0:
        from ocr_fallback import is_ocr_available, ocr_pdf_pages
        if is_ocr_available():
            print(f"[Parser] OCR fallback: {pdf_path.name} has {image_only_pages} image-only pages, running Tesseract...")
            ocr_texts = ocr_pdf_pages(pdf_path)
            for page_num, text in ocr_texts.items():
                if 'Type of Incident:' not in text:
                    continue
                incident, raw_ts = _parse_incident_page_text(text, pdf_path)
                if incident:
                    incidents.append(incident)
                if raw_ts:
                    raw_timestamps.append(raw_ts)
            if incidents:
                print(f"[Parser] OCR extracted {len(incidents)} incident(s) from {pdf_path.name}")
            else:
                print(f"[Parser] OCR completed but no parseable incidents found in {pdf_path.name}")
        else:
            print(f"[Parser] WARNING: {pdf_path.name} has {image_only_pages} image-only pages. "
                  f"Install Tesseract OCR for automatic text extraction: winget install tesseract-ocr.tesseract")

    if not incidents:
        return None

    incidents.sort(key=lambda r: r['timestamp'])
    officers    = list(dict.fromkeys(r['officer'] for r in incidents))
    # Use the minimum RAW (pre-correction) timestamp as the fallback date.
    # This ensures Connecteam's shift-start date is used, not the corrected
    # calendar date that after-midnight incidents were bumped to.
    fallback_dt = min(raw_timestamps) if raw_timestamps else incidents[0]['timestamp']
    report_date = _date_from_filename(pdf_path, fallback_dt)

    return {
        'property':        property_name,
        'date':            report_date,
        'officers':        officers,
        'rounds':          [],      # no standard patrol rounds in incident reports
        'total_rounds':    0,
        'first_time':      incidents[0]['time_str'],
        'last_time':       incidents[-1]['time_str'],
        'has_incidents':   True,
        'incident_rounds': incidents,
    }


def _parse_guest_parking(pdf_path):
    """Parse Connecteam Guest Parking check PDFs (e.g. Harbor Lights HOA).
    Format: street-by-street parking checks with license plates for vehicles found."""
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")

    # Extract property name — strip "Guest_Parking..." suffix
    stem = pdf_path.stem
    stem_lower = stem.lower()
    for sep in ['_guest_parking', ' guest parking']:
        if sep in stem_lower:
            idx = stem_lower.index(sep)
            property_name = stem[:idx].replace('_', ' ').strip()
            break
    else:
        property_name = stem.replace('_', ' ').strip()

    officer   = None
    timestamp = None
    time_str  = None
    streets   = []  # list of (street_name, has_vehicles, plates)

    # Try pdfplumber first, fall back to OCR
    page_texts = []
    image_only_pages = 0
    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                page_texts.append(text)
            elif page.images:
                image_only_pages += 1

    if not page_texts and image_only_pages > 0:
        from ocr_fallback import is_ocr_available, ocr_pdf_pages
        if is_ocr_available():
            print(f"[Parser] OCR fallback: {pdf_path.name} has {image_only_pages} image-only pages, running Tesseract...")
            ocr_texts = ocr_pdf_pages(pdf_path)
            page_texts = [text for _, text in sorted(ocr_texts.items())]

    if not page_texts:
        return None

    for text in page_texts:
        lines = [l.strip() for l in text.strip().splitlines() if l.strip()]

        # Extract officer and timestamp from header (first page)
        if not timestamp:
            for line in lines:
                ts_match = re.search(r'(\d{2}/\d{2}/\d{4}),\s*(\d{1,2}:\d{2}\s*[AP]M)', line)
                if ts_match:
                    try:
                        timestamp = datetime.strptime(
                            f"{ts_match.group(1)} {ts_match.group(2).strip()}",
                            '%m/%d/%Y %I:%M %p'
                        )
                        time_str = timestamp.strftime('%I:%M %p').lstrip('0')
                    except ValueError:
                        pass
                elif re.match(r'^[A-Z][a-z]+(?: [A-Z][a-z]+)+$', line) and not officer:
                    if 'Guest Parking' not in line and 'Harbor' not in line:
                        officer = line

        # Extract street checks
        current_street = None
        current_plates = []
        collecting_plates = False

        for line in lines:
            parking_match = re.match(r'(.+?)\s*Guest Parking Vehicles:\s*(Yes|No)', line, re.IGNORECASE)
            if parking_match:
                # Save previous street
                if current_street and collecting_plates:
                    streets.append((current_street, True, list(current_plates)))
                    current_plates = []

                street_name  = parking_match.group(1).strip()
                has_vehicles = parking_match.group(2).lower() == 'yes'
                if has_vehicles:
                    current_street = street_name
                    collecting_plates = True
                else:
                    streets.append((street_name, False, []))
                    current_street = None
                    collecting_plates = False
            elif collecting_plates:
                if line.startswith('License Plate'):
                    # "License Plate & Parking Permit Number: PLATE/PERMIT"
                    after_colon = line.split(':', 1)[-1].strip()
                    if after_colon:
                        current_plates.append(after_colon)
                elif line in ('Pictures', '') or re.match(r'^\d+/\d+$', line):
                    continue
                elif re.match(r'^[A-Z0-9]', line) and '/' in line or re.match(r'^\d+[A-Z]', line):
                    current_plates.append(line.strip())

        # Save last street
        if current_street and collecting_plates:
            streets.append((current_street, True, list(current_plates)))

    if not timestamp:
        return None

    # Build incident rounds from streets with vehicles
    has_incidents   = any(has_v for _, has_v, _ in streets)
    incident_rounds = []
    for street_name, has_vehicles, plates in streets:
        if not has_vehicles:
            continue
        notes = [f"Street: {street_name}"]
        if plates:
            notes.append(f"Plates: {', '.join(plates)}")
        incident_rounds.append({
            'officer':        officer or 'Unknown Officer',
            'timestamp':      timestamp,
            'time_str':       time_str or 'Unknown',
            'checks':         {},
            'has_incident':   True,
            'incident_notes': notes,
        })

    report_date = _date_from_filename(pdf_path, timestamp)

    # Build summary of all streets checked
    all_rounds = []
    for street_name, has_vehicles, plates in streets:
        notes = [f"Street: {street_name}", f"Vehicles: {'Yes' if has_vehicles else 'No'}"]
        if plates:
            notes.append(f"Plates: {', '.join(plates)}")
        all_rounds.append({
            'officer':        officer or 'Unknown Officer',
            'timestamp':      timestamp,
            'time_str':       time_str or 'Unknown',
            'checks':         {'guest_parking': 'Yes' if has_vehicles else 'No'},
            'has_incident':   has_vehicles,
            'incident_notes': notes if has_vehicles else [],
        })

    return {
        'property':        property_name,
        'date':            report_date,
        'officers':        [officer] if officer else ['Unknown Officer'],
        'rounds':          all_rounds,
        'total_rounds':    len(all_rounds),
        'first_time':      time_str or 'Unknown',
        'last_time':       time_str or 'Unknown',
        'has_incidents':   has_incidents,
        'incident_rounds': incident_rounds,
    }


def _parse_post_check_report(pdf_path, report_type):
    """
    Parse Connecteam Post Check / Post Checklist PDFs.
    Extracts per-entry data: timestamp, officer, post name, and notes.
    Returns a report dict with a 'post_checks' key containing the entries.
    """
    try:
        import pdfplumber
    except ImportError:
        raise RuntimeError("pdfplumber not installed. Run: pip install pdfplumber")

    # Derive property name by stripping known suffixes
    stem = pdf_path.stem
    for suffix in ['_Supervisor_Post_Checklist', '_Post_Checklist', '_Post_Check']:
        if suffix.lower() in stem.lower():
            idx = stem.lower().index(suffix.lower())
            stem = stem[:idx]
            break
    property_name = stem.replace('_', ' ').strip()

    # Labels to scan for post name and notes
    _post_labels    = {'post', 'location', 'post name', 'site', 'station'}
    _notes_labels   = {'notes', 'comments', 'observations', 'remarks', 'status', 'report'}
    _ts_re          = re.compile(r'(\d{2}/\d{2}/\d{4}),\s*(\d{1,2}:\d{2}\s*[AP]M)')
    _officer_re     = re.compile(r'^[A-Z][a-z]+(?: [A-Z][a-z]+)+$')
    _labeled_re     = re.compile(r'^([^:]+):\s*(.+)$')
    _skip_words     = {'patrol', 'vehicle', 'inspection', 'americal', 'connecteam', 'daily', 'activity'}

    check_entries = []

    with pdfplumber.open(str(pdf_path)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue

            # Require a timestamp AND at least one post-check keyword on this page
            tl = text.lower()
            if not _ts_re.search(text):
                continue
            if not any(kw in tl for kw in ('post', 'check', 'location', 'station')):
                continue

            lines     = [l.strip() for l in text.strip().splitlines() if l.strip()]
            timestamp = None
            time_str  = None
            officer   = None
            post      = None
            notes_parts = []

            for line in lines:
                ts_match = _ts_re.search(line)
                if ts_match:
                    try:
                        timestamp = datetime.strptime(
                            ts_match.group(1) + ' ' + ts_match.group(2).strip(),
                            '%m/%d/%Y %I:%M %p'
                        )
                        time_str = timestamp.strftime('%I:%M %p').lstrip('0')
                    except ValueError:
                        pass
                    continue

                if _officer_re.match(line) and not officer:
                    if not any(w in line.lower() for w in _skip_words):
                        officer = line
                    continue

                lm = _labeled_re.match(line)
                if lm:
                    label = lm.group(1).strip().lower()
                    value = lm.group(2).strip()
                    if label in _post_labels and not post:
                        post = value
                    elif label in _notes_labels and value:
                        notes_parts.append(value)

            if not timestamp:
                continue

            check_entries.append({
                'officer':     officer or 'Unknown Officer',
                'timestamp':   timestamp,
                'time_str':    time_str or 'Unknown',
                'post':        post,
                'notes':       '; '.join(notes_parts) if notes_parts else None,
                'report_type': report_type,
            })

    if not check_entries:
        return None

    check_entries.sort(key=lambda e: e['timestamp'])
    officers    = list(dict.fromkeys(e['officer'] for e in check_entries))
    fallback_dt = check_entries[0]['timestamp']
    report_date = _date_from_filename(pdf_path, fallback_dt)

    return {
        'property':        property_name,
        'date':            report_date,
        'officers':        officers,
        'rounds':          [],
        'total_rounds':    0,
        'first_time':      check_entries[0]['time_str'],
        'last_time':       check_entries[-1]['time_str'],
        'has_incidents':   False,
        'incident_rounds': [],
        'post_checks':     check_entries,
        'report_type':     report_type,
    }


def _parse_round_page(text):
    """Parse a single patrol round from DAR page text."""
    lines    = [l.strip() for l in text.strip().splitlines() if l.strip()]
    officer  = None
    timestamp = None
    time_str = None
    checks   = {}
    notes    = []

    for line in lines:
        ts_match = re.search(r'(\d{2}/\d{2}/\d{4}),\s*(\d{1,2}:\d{2}\s*[AP]M)', line)
        if ts_match:
            try:
                timestamp = datetime.strptime(
                    f"{ts_match.group(1)} {ts_match.group(2).strip()}",
                    '%m/%d/%Y %I:%M %p'
                )
                time_str = timestamp.strftime('%I:%M %p').lstrip('0')
            except ValueError:
                pass
            continue

        ll = line.lower()
        # Standard DAR format: "Officer checked for X: Completed"
        if 'unwanted persons' in ll or 'unwanted person' in ll:
            checks['unwanted_persons'] = _extract_status(line)
        elif 'illegal dumping' in ll:
            checks['illegal_dumping'] = _extract_status(line)
        elif 'property damage' in ll or ('vandalism' in ll and len(ll) > 15):
            checks['property_damage'] = _extract_status(line)
        # Vehicle patrol DAR format: "Any homeless people found?: No"
        elif ('homeless' in ll or 'unauthorized' in ll) and '?' in ll:
            checks['unwanted_persons'] = _extract_qa_status(line)
        elif ('lights' in ll or 'gates' in ll) and '?' in ll:
            checks['facility_check'] = _extract_qa_status(line)
        elif 'property damage' in ll and '?' in ll:
            checks['property_damage'] = _extract_qa_status(line)

        if re.match(r'^[A-Z][a-z]+(?: [A-Z][a-z]+)+$', line) and not officer:
            officer = line

    check_values = list(checks.values())
    # "Completed" and "No" = normal (no incident); "Yes" on negative questions = incident
    _normal = {'completed', 'no', 'yes'}
    has_incident = any(
        v and v.lower() not in _normal and v is not None
        for v in check_values
    ) or bool(notes)

    incident_notes = [
        f"{k.replace('_', ' ').title()}: {v}"
        for k, v in checks.items()
        if v and v.lower() not in ('completed', 'no', 'yes')
    ]

    if not timestamp:
        return None

    return {
        'officer':        officer or 'Unknown Officer',
        'timestamp':      timestamp,
        'time_str':       time_str or 'Unknown',
        'checks':         checks,
        'has_incident':   has_incident,
        'incident_notes': incident_notes,
    }


def _extract_status(line):
    """Extract the status value from a check line like 'Officer checked for X: Completed'"""
    if ':' in line:
        parts  = line.split(':', 1)
        status = parts[-1].strip().split('\n')[0].strip()
        return status if status else 'Completed'
    if 'Completed' in line:
        return 'Completed'
    return 'Unknown'


def _extract_qa_status(line):
    """Extract Yes/No answer from question-format line like 'Any homeless people?: No'"""
    if '?:' in line:
        answer = line.split('?:')[-1].strip().split('\n')[0].strip()
        return answer if answer else 'No'
    if ':' in line:
        answer = line.split(':')[-1].strip().split('\n')[0].strip()
        return answer if answer else 'No'
    return 'Unknown'


# ── Standalone test ───────────────────────────────────────────────────────────
if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1:
        pdf = Path(sys.argv[1])
    else:
        folder = SCRIPT_DIR.parent / 'Americal Patrol Morning Reports'
        pdfs   = list(folder.glob('*.pdf'))
        if not pdfs:
            print("No PDFs found.")
            sys.exit(1)
        pdf = pdfs[0]

    print(f"Analyzing: {pdf.name}\n")
    data = parse_report(pdf)
    if not data:
        print("Could not parse report.")
        sys.exit(1)

    print(f"Property  : {data['property']}")
    print(f"Date      : {data['date']}")
    print(f"Officers  : {', '.join(data['officers'])}")
    print(f"Rounds    : {data['total_rounds']}  ({data['first_time']} - {data['last_time']})")
    print(f"Incidents : {'YES' if data['has_incidents'] else 'None'}")
    if data['has_incidents']:
        for r in data['incident_rounds']:
            print(f"  {r['time_str']}: {r['incident_notes']}")

    client = match_client(pdf)
    if client:
        print(f"\nClient group : {client['group_id']}")
        print(f"Recipients   : {', '.join(client['recipients'])}")
