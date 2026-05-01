"""
Connecteam API client — fetches structured form submission data.

Replaces PDF parsing for report data. Provides clean officer names,
exact timestamps, check statuses, and direct CDN photo URLs per round.

Auth: OAuth 2.0 client credentials. Credentials in .env file.
"""

import os
import requests
from datetime import datetime, timedelta
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
API_BASE = "https://api.connecteam.com"

_token_cache = {"token": None, "expires_at": 0}


def _load_credentials():
    """Load OAuth credentials from .env file."""
    env_path = SCRIPT_DIR / ".env"
    creds = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                key, val = line.split("=", 1)
                creds[key.strip()] = val.strip()
    client_id = os.environ.get("CONNECTEAM_CLIENT_ID") or creds.get("CONNECTEAM_CLIENT_ID")
    client_secret = os.environ.get("CONNECTEAM_CLIENT_SECRET") or creds.get("CONNECTEAM_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("Missing CONNECTEAM_CLIENT_ID or CONNECTEAM_CLIENT_SECRET in .env")
    return client_id, client_secret


def _get_token():
    """Get a valid OAuth bearer token (cached for 24h)."""
    import time
    now = time.time()
    if _token_cache["token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    client_id, client_secret = _load_credentials()
    resp = requests.post(
        f"{API_BASE}/oauth/v1/token",
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials"},
    )
    resp.raise_for_status()
    data = resp.json()
    _token_cache["token"] = data["access_token"]
    _token_cache["expires_at"] = now + data.get("expires_in", 86400)
    return _token_cache["token"]


def _headers():
    return {"Authorization": f"Bearer {_get_token()}"}


def _get_paginated(url, key, stop_before=None):
    """Fetch all pages of a paginated Connecteam endpoint.

    Args:
        stop_before: Unix timestamp. Stop fetching when submissions are older
                     than this (submissions come newest-first).
    """
    import time as _time
    results = []
    while url:
        for attempt in range(4):
            resp = requests.get(url, headers=_headers())
            if resp.status_code == 429:
                wait = 2 ** attempt
                print(f"[API] Rate limited, waiting {wait}s...", flush=True)
                _time.sleep(wait)
                continue
            break
        resp.raise_for_status()
        data = resp.json()
        if "data" not in data:
            break
        items = data["data"].get(key, [])
        if not items:
            break
        results.extend(items)

        # Early stop: if oldest item on this page is before our cutoff, done
        if stop_before and items:
            oldest_ts = min(i.get("submissionTimestamp", 0) for i in items)
            if oldest_ts < stop_before:
                break

        offset = data.get("paging", {}).get("offset")
        if offset:
            base = url.split("?")[0]
            url = f"{base}?offset={offset}"
            _time.sleep(0.5)  # pace requests to avoid rate limits
        else:
            url = None
    return results


# ── Forms ─────────────────────────────────────────────────────────────────────

def list_forms():
    """Return all forms: [{formId, formName, questions}, ...]"""
    return _get_paginated(f"{API_BASE}/forms/v1/forms", "forms")


def get_form(form_id):
    """Get a single form's structure (questions, types)."""
    resp = requests.get(f"{API_BASE}/forms/v1/forms/{form_id}", headers=_headers())
    resp.raise_for_status()
    return resp.json()["data"]


def get_form_submissions(form_id, since_hours=48):
    """Get recent submissions for a form.

    Args:
        since_hours: Only fetch submissions from the last N hours (default 48).
                     Set to None to fetch all submissions (slow).
    """
    import time as _time
    stop_before = None
    if since_hours:
        stop_before = int(_time.time()) - (since_hours * 3600)
    return _get_paginated(
        f"{API_BASE}/forms/v1/forms/{form_id}/form-submissions",
        "formSubmissions",
        stop_before=stop_before,
    )


def get_form_submission(form_id, submission_id):
    """Get a single submission."""
    resp = requests.get(
        f"{API_BASE}/forms/v1/forms/{form_id}/form-submissions/{submission_id}",
        headers=_headers(),
    )
    resp.raise_for_status()
    return resp.json()["data"]


# ── Users ─────────────────────────────────────────────────────────────────────

_user_cache = {}


def get_user(user_id):
    """Get user info by ID. Cached per session."""
    if user_id in _user_cache:
        return _user_cache[user_id]
    resp = requests.get(
        f"{API_BASE}/users/v1/users",
        headers=_headers(),
        params={"userIds": user_id},
    )
    resp.raise_for_status()
    users = resp.json()["data"].get("users", [])
    user = users[0] if users else {"firstName": "Unknown", "lastName": "Officer"}
    _user_cache[user_id] = user
    return user


def get_officer_name(user_id):
    """Get clean 'First Last' name for an officer."""
    user = get_user(user_id)
    return f"{user['firstName']} {user['lastName']}"


# ── Submissions → Parsed Report Data ─────────────────────────────────────────

def _build_question_map(form):
    """Map questionId → {title, questionType, allAnswers}."""
    qmap = {}
    for q in form.get("questions", []):
        qmap[q["questionId"]] = q
        # Handle nested group questions
        for sub in q.get("questions", []):
            qmap[sub["questionId"]] = sub
    return qmap


def submissions_to_dar(form_id, submissions, form=None):
    """Convert API submissions to the same dict format that pdf_analyzer returns.

    Returns a report dict compatible with branded_pdf and draft_composer.
    """
    if form is None:
        form = get_form(form_id)
    qmap = _build_question_map(form)
    property_name = form["formName"].replace(" DAR", "").strip()

    rounds = []
    for sub in submissions:
        officer = get_officer_name(sub["submittingUserId"])
        ts_unix = sub["submissionTimestamp"]
        tz_name = sub.get("submissionTimezone", "America/Los_Angeles")
        timestamp = datetime.utcfromtimestamp(ts_unix)

        # Adjust for timezone offset (Connecteam timestamps are UTC)
        # For America/Los_Angeles: UTC-7 (PDT) or UTC-8 (PST)
        try:
            import zoneinfo
            from datetime import timezone
            tz = zoneinfo.ZoneInfo(tz_name)
            timestamp = datetime.fromtimestamp(ts_unix, tz=tz).replace(tzinfo=None)
        except Exception:
            timestamp = datetime.utcfromtimestamp(ts_unix) - timedelta(hours=7)

        time_str = timestamp.strftime("%I:%M %p").lstrip("0")
        entry_num = sub.get("entryNum")

        # Parse answers
        checks = {}
        photos = []
        for ans in sub.get("answers", []):
            q = qmap.get(ans["questionId"], {})
            qtitle = q.get("title", "").lower()
            qtype = ans.get("questionType", q.get("questionType", ""))

            if qtype == "task":
                checked = ans.get("isChecked", False)
                status = "Completed" if checked else "Not Completed"
                if "unwanted" in qtitle:
                    checks["unwanted_persons"] = status
                elif "property damage" in qtitle and "vandalism" in qtitle:
                    checks["property_damage"] = status
                    checks["vandalism"] = status
                elif "property damage" in qtitle:
                    checks["property_damage"] = status
                elif "vandalism" in qtitle:
                    checks["vandalism"] = status
                elif "illegal dumping" in qtitle:
                    checks["illegal_dumping"] = status
                elif "homeless" in qtitle or "unauthorized" in qtitle:
                    checks["unwanted_persons"] = status
                elif "lights" in qtitle or "gates" in qtitle:
                    checks["facility_check"] = status
                elif any(kw in qtitle for kw in ("office", "spa", "bathroom", "rv parking", "rv lot")):
                    checks["facility_areas"] = status

            elif qtype == "image":
                for img in ans.get("images", []):
                    photos.append(img["url"])

            elif qtype == "yesNo":
                # Connecteam yesNo answers can arrive in three shapes depending
                # on form/version: selectedIndex (int, current Vehicle Patrol DAR),
                # selectedOptionId (int, legacy), or selectedAnswers[0].text
                # (seen on guest parking). Try all three before giving up.
                selected = ans.get("selectedIndex")
                if selected is None:
                    selected = ans.get("selectedOptionId")
                all_answers = q.get("allAnswers", [])
                answer_text = "Unknown"
                for opt in all_answers:
                    if opt.get("yesNoOptionId") == selected:
                        answer_text = opt["text"]
                        break
                if answer_text == "Unknown":
                    sel_answers = ans.get("selectedAnswers", [])
                    if sel_answers:
                        answer_text = sel_answers[0].get("text", "") or "Unknown"
                if "homeless" in qtitle or "unauthorized" in qtitle:
                    checks["unwanted_persons"] = answer_text
                elif "property damage" in qtitle and "vandalism" in qtitle:
                    checks["property_damage"] = answer_text
                    checks["vandalism"] = answer_text
                elif "property damage" in qtitle:
                    checks["property_damage"] = answer_text
                elif "vandalism" in qtitle:
                    checks["vandalism"] = answer_text
                elif "lights" in qtitle or "gates" in qtitle:
                    checks["facility_check"] = answer_text
                elif "dumping" in qtitle:
                    checks["illegal_dumping"] = answer_text
                elif any(kw in qtitle for kw in ("office", "spa", "bathroom", "rv parking", "rv lot")):
                    checks["facility_areas"] = answer_text

        # "unknown"/"n/a" mean the API couldn't resolve the answer — that's a
        # data-quality issue, not an incident. Filter both out of incident detection
        # AND from the human-readable incident_notes so client reports don't show
        # "Unwanted Persons: Unknown" lines.
        _normal = {"completed", "no", "yes", "unknown", "n/a", ""}
        has_incident = any(
            v and v.lower() not in _normal for v in checks.values()
        )
        incident_notes = [
            f"{k.replace('_', ' ').title()}: {v}"
            for k, v in checks.items()
            if v and v.lower() not in _normal
        ]

        rounds.append({
            "officer": officer,
            "timestamp": timestamp,
            "time_str": time_str,
            "checks": checks,
            "has_incident": has_incident,
            "incident_notes": incident_notes,
            "photos": photos,
            "entry_num": entry_num,
        })

    if not rounds:
        return None

    rounds.sort(key=lambda r: r["timestamp"])
    officers = list(dict.fromkeys(r["officer"] for r in rounds))
    has_incidents = any(r["has_incident"] for r in rounds)
    incident_rounds = [r for r in rounds if r["has_incident"]]
    report_date = rounds[0]["timestamp"].strftime("%B %d, %Y")

    return {
        "property": property_name,
        "date": report_date,
        "officers": officers,
        "rounds": rounds,
        "total_rounds": len(rounds),
        "first_time": rounds[0]["time_str"],
        "last_time": rounds[-1]["time_str"],
        "has_incidents": has_incidents,
        "incident_rounds": incident_rounds,
        "entry_num": rounds[-1].get("entry_num"),
        "source": "api",
    }


def submissions_to_incident(form_id, submissions, form=None):
    """Convert API incident report submissions to the standard report dict."""
    if form is None:
        form = get_form(form_id)
    qmap = _build_question_map(form)
    property_name = form["formName"].replace(" Incident Report", "").strip()

    incidents = []
    for sub in submissions:
        officer = get_officer_name(sub["submittingUserId"])
        ts_unix = sub["submissionTimestamp"]
        tz_name = sub.get("submissionTimezone", "America/Los_Angeles")

        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_name)
            timestamp = datetime.fromtimestamp(ts_unix, tz=tz).replace(tzinfo=None)
        except Exception:
            timestamp = datetime.utcfromtimestamp(ts_unix) - timedelta(hours=7)

        time_str = timestamp.strftime("%I:%M %p").lstrip("0")
        entry_num = sub.get("entryNum")

        incident_type = ""
        report_text = ""
        address = ""
        photos = []

        for ans in sub.get("answers", []):
            q = qmap.get(ans["questionId"], {})
            qtitle = q.get("title", "").lower()
            qtype = ans.get("questionType", q.get("questionType", ""))

            if qtype == "multipleChoice":
                # API uses selectedAnswers (with text) not selectedOptionIds
                selected = [a["text"] for a in ans.get("selectedAnswers", [])]
                if "type" in qtitle and "incident" in qtitle:
                    incident_type = ", ".join(selected)
                elif "address" in qtitle or "location" in qtitle:
                    address = ", ".join(selected)
                elif "suite" in qtitle and selected:
                    address += f" Suite {', '.join(selected)}"

            elif qtype == "openEnded":
                text_val = ans.get("value", "") or ans.get("textAnswer", "")
                if "report" in qtitle or "description" in qtitle or "detail" in qtitle:
                    report_text = text_val
                elif "address" in qtitle or "location" in qtitle:
                    address = text_val

            elif qtype == "image":
                for img in ans.get("images", []):
                    photos.append(img["url"])

        notes = []
        if incident_type:
            notes.append(f"Type of Incident: {incident_type}")
        if address:
            notes.append(f"Address: {address}")
        if report_text:
            notes.append(f"Report: {report_text}")

        incidents.append({
            "officer": officer,
            "timestamp": timestamp,
            "time_str": time_str,
            "checks": {},
            "has_incident": True,
            "incident_notes": notes,
            "photos": photos,
            "entry_num": entry_num,
        })

    if not incidents:
        return None

    incidents.sort(key=lambda r: r["timestamp"])
    officers = list(dict.fromkeys(r["officer"] for r in incidents))
    report_date = incidents[0]["timestamp"].strftime("%B %d, %Y")

    return {
        "property": property_name,
        "date": report_date,
        "officers": officers,
        "rounds": [],
        "total_rounds": 0,
        "first_time": incidents[0]["time_str"],
        "last_time": incidents[-1]["time_str"],
        "has_incidents": True,
        "incident_rounds": incidents,
        "entry_num": incidents[-1].get("entry_num"),
        "source": "api",
    }


def submissions_to_guest_parking(form_id, submissions, form=None):
    """Convert guest parking form submissions to standard report dict."""
    if form is None:
        form = get_form(form_id)
    qmap = _build_question_map(form)
    property_name = form["formName"].replace(" Guest Parking", "").strip()

    all_rounds = []
    incident_rounds = []

    for sub in submissions:
        officer = get_officer_name(sub["submittingUserId"])
        ts_unix = sub["submissionTimestamp"]
        tz_name = sub.get("submissionTimezone", "America/Los_Angeles")

        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_name)
            timestamp = datetime.fromtimestamp(ts_unix, tz=tz).replace(tzinfo=None)
        except Exception:
            timestamp = datetime.utcfromtimestamp(ts_unix) - timedelta(hours=7)

        time_str = timestamp.strftime("%I:%M %p").lstrip("0")
        photos = []

        # First pass: create street rounds from yesNo questions
        # and collect plate text + photos that follow each street
        current_round = None
        for ans in sub.get("answers", []):
            q = qmap.get(ans["questionId"], {})
            qtype = ans.get("questionType", q.get("questionType", ""))
            qtitle = q.get("title", "")

            if qtype == "yesNo":
                # Start a new street round
                street_name = qtitle.replace("Guest Parking Vehicles", "").strip()

                # Try to get explicit yesNo answer
                selected = ans.get("selectedOptionId")
                all_answers = q.get("allAnswers", [])
                answer_text = ""
                for opt in all_answers:
                    if opt.get("yesNoOptionId") == selected:
                        answer_text = opt["text"]
                        break
                if not answer_text:
                    sel_answers = ans.get("selectedAnswers", [])
                    if sel_answers:
                        answer_text = sel_answers[0].get("text", "")

                current_round = {
                    "officer": officer,
                    "timestamp": timestamp,
                    "time_str": time_str,
                    "street_name": street_name,
                    "checks": {"guest_parking": "Unknown"},
                    "has_incident": False,
                    "incident_notes": [],
                    "photos": [],
                    "_yesno_answer": answer_text,  # may be empty
                    "_plates": "",
                }
                all_rounds.append(current_round)

            elif qtype == "openEnded" and "license" in qtitle.lower():
                plates = ans.get("value", "") or ans.get("textAnswer", "")
                if plates and current_round:
                    current_round["_plates"] = plates.strip()

            elif qtype == "image":
                img_urls = [img["url"] for img in ans.get("images", [])]
                if img_urls and current_round:
                    current_round["photos"].extend(img_urls)

        # Second pass: determine vehicle presence per street
        # Priority: explicit yesNo > infer from plates/photos
        for rnd in all_rounds:
            yesno = rnd.pop("_yesno_answer", "")
            plates = rnd.pop("_plates", "")

            if yesno:
                has_vehicles = yesno.lower() == "yes"
            else:
                # API didn't return yesNo selection — infer from data
                has_vehicles = bool(plates) or bool(rnd["photos"])

            rnd["has_incident"] = has_vehicles
            rnd["checks"]["guest_parking"] = "Yes" if has_vehicles else "No"
            if has_vehicles:
                street = rnd["street_name"]
                rnd["incident_notes"] = [f"Street: {street}", f"Vehicles: Yes"]
                if plates:
                    rnd["incident_notes"].append(f"Plates: {plates}")
                incident_rounds.append(rnd)

    if not all_rounds:
        return None

    officers = list(dict.fromkeys(r["officer"] for r in all_rounds))
    has_incidents = any(r["has_incident"] for r in all_rounds)
    report_date = all_rounds[0]["timestamp"].strftime("%B %d, %Y")
    time_str = all_rounds[0]["time_str"]

    return {
        "property": property_name,
        "date": report_date,
        "officers": officers,
        "rounds": all_rounds,
        "total_rounds": len(all_rounds),
        "first_time": time_str,
        "last_time": time_str,
        "has_incidents": has_incidents,
        "incident_rounds": incident_rounds,
        "source": "api",
    }


# ── Date filtering ────────────────────────────────────────────────────────────

def filter_submissions_by_date(submissions, target_date):
    """Filter submissions to those from a specific date (local time).

    target_date: datetime.date or datetime object.
    Returns submissions where the local submission time falls on target_date.
    """
    if hasattr(target_date, "date"):
        target_date = target_date.date()

    filtered = []
    for sub in submissions:
        ts_unix = sub["submissionTimestamp"]
        tz_name = sub.get("submissionTimezone", "America/Los_Angeles")
        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_name)
            local_dt = datetime.fromtimestamp(ts_unix, tz=tz)
        except Exception:
            local_dt = datetime.utcfromtimestamp(ts_unix) - timedelta(hours=7)

        if local_dt.date() == target_date:
            filtered.append(sub)
    return filtered


def _get_property_windows(report_date):
    """Calculate per-property submission windows based on schedule.json.

    report_date is the day the pipeline runs (today).
    Captures the previous day's daytime shifts + overnight shifts ending this morning.

    For 24-hour properties (multiple back-to-back shifts like Stadium Plaza, LAX):
    the window starts at the exact day shift clock_in with NO buffer — this prevents
    overnight entries from the previous report bleeding in. Buffer is only added to
    the end of the last shift.

    For single-shift properties: ±15 min buffer on both ends.

    Returns: dict of property_name_lower -> list of (start, end) datetime tuples
    """
    import json
    schedule = json.load(open(SCRIPT_DIR / "schedule.json"))["accounts"]
    buffer = timedelta(minutes=15)
    yesterday = report_date - timedelta(days=1)
    windows = {}

    for prop_name, entries in schedule.items():
        is_24hr = len(entries) > 1  # back-to-back shifts = 24-hour property

        if is_24hr:
            # 24-hour property: combine into one window with hard start, buffered end
            shifts = []
            for entry in entries:
                ci_h, ci_m = map(int, entry["clock_in"].split(":"))
                co_h, co_m = map(int, entry["clock_out"].split(":"))

                if entry.get("overnight"):
                    start = datetime(yesterday.year, yesterday.month, yesterday.day, ci_h, ci_m)
                    end = datetime(report_date.year, report_date.month, report_date.day, co_h, co_m)
                else:
                    start = datetime(yesterday.year, yesterday.month, yesterday.day, ci_h, ci_m)
                    end = datetime(yesterday.year, yesterday.month, yesterday.day, co_h, co_m)
                shifts.append((start, end))

            # Hard start at day shift clock_in, buffered end at overnight clock_out
            combined_start = min(s[0] for s in shifts)
            combined_end = max(s[1] for s in shifts) + buffer
            windows[prop_name.lower()] = [(combined_start, combined_end)]
        else:
            # Single shift: ±15 min buffer on both ends
            entry = entries[0]
            ci_h, ci_m = map(int, entry["clock_in"].split(":"))
            co_h, co_m = map(int, entry["clock_out"].split(":"))

            if entry.get("overnight"):
                start = datetime(yesterday.year, yesterday.month, yesterday.day, ci_h, ci_m)
                end = datetime(report_date.year, report_date.month, report_date.day, co_h, co_m)
            else:
                start = datetime(yesterday.year, yesterday.month, yesterday.day, ci_h, ci_m)
                end = datetime(yesterday.year, yesterday.month, yesterday.day, co_h, co_m)

            windows[prop_name.lower()] = [(start - buffer, end + buffer)]

    return windows


def _match_form_to_property(form_name, property_windows):
    """Match a Connecteam form name to a property in schedule.json.

    Uses word-boundary matching of property names against the form name.
    Prevents partial matches (e.g., "la" in "lax").
    Returns the property name (lowercase) or None.
    """
    import re
    name_lower = form_name.lower()
    best_prop = None
    best_score = 0
    for prop_name in property_windows:
        words = prop_name.split()
        score = sum(1 for w in words if re.search(r'\b' + re.escape(w) + r'\b', name_lower))
        if score > best_score:
            best_score = score
            best_prop = prop_name
    return best_prop if best_score > 0 else None


def _filter_by_property_windows(submissions, form_name, property_windows):
    """Filter submissions using per-property shift windows.

    Matches the form to a property, then filters submissions to those
    within the property's shift window(s). Falls back to a wide 24h window
    if the form doesn't match any property in schedule.json.
    """
    prop = _match_form_to_property(form_name, property_windows)

    if prop:
        shift_subs = []
        for win_start, win_end in property_windows[prop]:
            shift_subs.extend(filter_submissions_by_shift(submissions, win_start, win_end))
        # Deduplicate submissions that matched multiple overlapping windows
        seen = set()
        deduped = []
        for s in shift_subs:
            key = s.get("entryNum") or id(s)
            if key not in seen:
                seen.add(key)
                deduped.append(s)
        return deduped, prop
    else:
        # Fallback: wide window for forms not in schedule.json
        yesterday = min(w[0] for wins in property_windows.values() for w in wins)
        today_end = max(w[1] for wins in property_windows.values() for w in wins)
        return filter_submissions_by_shift(submissions, yesterday, today_end), None


def fetch_daily_reports(clients_groups, report_date=None):
    """Fetch all report data from Connecteam API for a given date.

    Uses per-property shift windows from schedule.json to filter submissions.
    Captures both daytime and overnight shifts for the previous day.

    Args:
        clients_groups: List of client group dicts from clients.json.
        report_date: date object (default: today).

    Returns:
        Dict mapping group_id -> list of parsed report dicts.
        Each report dict is compatible with branded_pdf and draft_composer.
    """
    import time as _time
    if report_date is None:
        report_date = datetime.now().date()

    print(f"[API] Fetching Connecteam form data for {report_date}...", flush=True)

    # Get per-property shift windows from schedule.json
    property_windows = _get_property_windows(report_date)
    print(f"[API] Loaded shift windows for {len(property_windows)} properties", flush=True)

    # Get all forms and map to client groups
    all_forms = list_forms()
    form_map = {}  # form_id -> {form, group_id, report_type}

    for form in all_forms:
        name = form["formName"]
        name_lower = name.lower()

        # Match to client group
        best_group = None
        best_score = 0
        for group in clients_groups:
            score = sum(1 for kw in group["keywords"] if kw.lower() in name_lower)
            if score > best_score:
                best_score = score
                best_group = group

        if not best_group or best_score == 0:
            continue

        # Detect report type
        if "incident" in name_lower and "vehicle" not in name_lower:
            rtype = "incident"
        elif "guest parking" in name_lower:
            rtype = "guest_parking"
        elif "vehicle patrol" in name_lower or "vehicle_patrol" in name_lower:
            rtype = "vehicle_dar"
        elif "dar" in name_lower or "fire watch" in name_lower:
            rtype = "dar"
        else:
            continue  # skip post checks, inspections, etc.

        form_map[form["formId"]] = {
            "form": form,
            "group_id": best_group["group_id"],
            "report_type": rtype,
            "form_name": name,
        }

    # Fetch submissions for each matched form
    results = {}  # group_id -> [report_dict, ...]

    for form_id, info in form_map.items():
        gid = info["group_id"]
        rtype = info["report_type"]
        form = info["form"]
        fname = info["form_name"]

        try:
            subs = get_form_submissions(form_id, since_hours=48)
            shift_subs, matched_prop = _filter_by_property_windows(subs, fname, property_windows)

            if not shift_subs:
                continue

            prop_label = f" [{matched_prop}]" if matched_prop else " [fallback window]"
            print(f"[API]   {fname}{prop_label}: {len(shift_subs)} submission(s)", flush=True)

            if rtype == "incident":
                report = submissions_to_incident(form_id, shift_subs, form)
            elif rtype == "guest_parking":
                report = submissions_to_guest_parking(form_id, shift_subs, form)
            else:
                report = submissions_to_dar(form_id, shift_subs, form)

            if report:
                report["form_name"] = fname
                report["form_id"] = form_id
                report["report_type"] = rtype
                # Vehicle patrol DARs are routine patrol logs, not incident reports.
                # Check-based flags (e.g. "Not Completed") must not be treated as incidents.
                if rtype == "vehicle_dar":
                    report["has_incidents"] = False
                    report["incident_rounds"] = []
                results.setdefault(gid, []).append(report)

        except Exception as e:
            print(f"[API]   WARNING: Failed to fetch {fname}: {e}", flush=True)
            continue

        _time.sleep(0.3)  # pace API calls

    print(f"[API] Done. Got data for {len(results)} client group(s).", flush=True)
    return results


def submissions_to_vehicle_inspection(form_id, submissions, form=None):
    """Convert vehicle inspection form submissions to inspection dicts.

    Returns a list of dicts compatible with supervisor_report._vehicle_table_html():
        {"type": "pre"|"post", "officer": str, "timestamp": datetime,
         "time_str": str, "mileage": int|None}
    """
    if form is None:
        form = get_form(form_id)
    qmap = _build_question_map(form)
    form_name_lower = form["formName"].lower()

    # Determine pre/post from form name (most Connecteam setups use separate forms)
    if "post" in form_name_lower:
        default_type = "post"
    elif "pre" in form_name_lower:
        default_type = "pre"
    else:
        default_type = None  # will try to detect from answers

    inspections = []
    for sub in submissions:
        officer = get_officer_name(sub["submittingUserId"])
        ts_unix = sub["submissionTimestamp"]
        tz_name = sub.get("submissionTimezone", "America/Los_Angeles")

        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_name)
            timestamp = datetime.fromtimestamp(ts_unix, tz=tz).replace(tzinfo=None)
        except Exception:
            timestamp = datetime.utcfromtimestamp(ts_unix) - timedelta(hours=7)

        time_str = timestamp.strftime("%I:%M %p").lstrip("0")

        mileage = None
        insp_type = default_type

        for ans in sub.get("answers", []):
            q = qmap.get(ans["questionId"], {})
            qtitle = q.get("title", "").lower()
            qtype = ans.get("questionType", q.get("questionType", ""))

            # Mileage / odometer field.
            # Connecteam "number" questions carry the value in `inputValue` (float).
            # Older / text-type forms use `value` / `textAnswer`.
            if mileage is None and any(
                kw in qtitle for kw in ("mile", "odometer", "mileage", "reading", "odo")
            ):
                iv = ans.get("inputValue")
                if isinstance(iv, (int, float)):
                    mileage = int(iv)
                else:
                    val = ans.get("value", "") or ans.get("textAnswer", "")
                    if val:
                        cleaned = str(val).replace(",", "")
                        nums = [int(n) for n in __import__("re").findall(r"\b(\d{4,7})\b", cleaned)]
                        if nums:
                            mileage = nums[0]

            # If form name didn't indicate type, check answers for pre/post
            if insp_type is None and ("pre" in qtitle or "post" in qtitle):
                if qtype == "multipleChoice":
                    selected = [a["text"].lower() for a in ans.get("selectedAnswers", [])]
                    if any("post" in s for s in selected):
                        insp_type = "post"
                    elif any("pre" in s for s in selected):
                        insp_type = "pre"
                elif qtype == "yesNo":
                    selected_id = ans.get("selectedOptionId")
                    for opt in q.get("allAnswers", []):
                        if opt.get("yesNoOptionId") == selected_id:
                            if "post" in opt["text"].lower():
                                insp_type = "post"
                            elif "pre" in opt["text"].lower():
                                insp_type = "pre"
                            break
                elif qtype == "openEnded":
                    val = (ans.get("value", "") or ans.get("textAnswer", "")).lower()
                    if "post" in val:
                        insp_type = "post"
                    elif "pre" in val:
                        insp_type = "pre"

        if insp_type is None:
            insp_type = "pre"  # default if can't determine

        # Final fallback: if title-keyword match missed (e.g. form renamed the
        # mileage field), take the largest 4-7 digit integer across all numeric
        # answers — odometer is almost always the largest number on the form.
        if mileage is None:
            candidates = []
            for ans in sub.get("answers", []):
                iv = ans.get("inputValue")
                if isinstance(iv, (int, float)) and 1000 <= iv < 10_000_000:
                    candidates.append(int(iv))
            if candidates:
                mileage = max(candidates)

        inspections.append({
            "type": insp_type,
            "officer": officer,
            "timestamp": timestamp,
            "time_str": time_str,
            "mileage": mileage,
            "form_name": form["formName"],
        })

    return inspections


def fetch_vehicle_inspections(report_date=None):
    """Fetch vehicle inspection data from Connecteam API for a given date.

    Discovers forms with 'vehicle' and 'inspection' in the name, fetches
    submissions within the shift window (±15 min buffer), and returns
    a list of inspection dicts for the supervisor report.

    Properties that use vehicle inspections:
        Stadium Plaza, LA Patrol, Ventura Patrol

    Args:
        report_date: date object (default: today).

    Returns:
        List of inspection dicts:
        [{"type": "pre"|"post", "officer": str, "timestamp": datetime,
          "time_str": str, "mileage": int|None}, ...]
    """
    import time as _time
    if report_date is None:
        report_date = datetime.now().date()

    print(f"[API] Fetching vehicle inspections for {report_date}...", flush=True)

    # Get per-property shift windows from schedule.json
    property_windows = _get_property_windows(report_date)

    # Discover vehicle inspection forms
    all_forms = list_forms()
    inspection_forms = []
    for form in all_forms:
        name_lower = form["formName"].lower()
        if "vehicle" in name_lower and "inspection" in name_lower:
            inspection_forms.append(form)
        elif "vehicle" in name_lower and ("pre" in name_lower or "post" in name_lower):
            inspection_forms.append(form)

    if not inspection_forms:
        print("[API]   No vehicle inspection forms found in Connecteam.", flush=True)
        return []

    print(f"[API]   Found {len(inspection_forms)} vehicle inspection form(s):", flush=True)
    for f in inspection_forms:
        print(f"[API]     - {f['formName']}", flush=True)

    all_inspections = []
    for form in inspection_forms:
        form_id = form["formId"]
        try:
            subs = get_form_submissions(form_id, since_hours=48)
            shift_subs, matched_prop = _filter_by_property_windows(subs, form["formName"], property_windows)

            if not shift_subs:
                continue

            prop_label = f" [{matched_prop}]" if matched_prop else ""
            print(f"[API]     {form['formName']}{prop_label}: {len(shift_subs)} submission(s)", flush=True)
            inspections = submissions_to_vehicle_inspection(form_id, shift_subs, form)
            all_inspections.extend(inspections)

        except Exception as e:
            print(f"[API]     WARNING: Failed to fetch {form['formName']}: {e}", flush=True)
            continue

        _time.sleep(0.3)

    print(f"[API]   Total: {len(all_inspections)} vehicle inspection(s)", flush=True)
    return all_inspections


def download_photo(url, timeout=15):
    """Download a photo from Connecteam CDN. Returns bytes or None."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content
    except Exception:
        return None


def filter_submissions_by_shift(submissions, shift_start, shift_end):
    """Filter submissions within a shift window (handles overnight).

    shift_start, shift_end: datetime objects.
    """
    filtered = []
    for sub in submissions:
        ts_unix = sub["submissionTimestamp"]
        tz_name = sub.get("submissionTimezone", "America/Los_Angeles")
        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_name)
            local_dt = datetime.fromtimestamp(ts_unix, tz=tz).replace(tzinfo=None)
        except Exception:
            local_dt = datetime.utcfromtimestamp(ts_unix) - timedelta(hours=7)

        if shift_start <= local_dt <= shift_end:
            filtered.append(sub)
    return filtered
