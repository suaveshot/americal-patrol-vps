# patrol_automation/branded_pdf.py
"""
Branded PDF Generator — Creates professional Americal Patrol PDFs
from Connecteam report data with polished text, photos, and premium features.

Features:
  - Company branding (logo, colors, footer)
  - Executive summary
  - Connecteam report number
  - Incident severity badges
  - Patrol timeline bar (DARs) — handles overnight shifts correctly
  - Per-round photo association (photos shown with their patrol round)
  - Confidentiality footer per client
"""

import base64
import logging
import os
import re
import shutil
from datetime import datetime
from io import BytesIO
from pathlib import Path

log = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
LOGO_PATH = PROJECT_DIR / "Company Logos" / "AmericalLogo-removebg-preview.png"
ORIGINALS_DIR = PROJECT_DIR / "Americal Patrol Morning Reports" / "originals"

# ── Branding constants ────────────────────────────────────────────────────────

COMPANY_NAME = "Americal Patrol, Inc."
PRIMARY_COLOR = "#1a3a5c"
ACCENT_COLOR = "#2c7bb6"
LIGHT_BG = "#f8fafc"
BORDER_COLOR = "#e2e8f0"

def _company_footer() -> str:
    now = datetime.now().strftime("%B %d, %Y at %I:%M %p").replace(" 0", " ")
    return f"""
<div style="margin-top:14px; padding-top:8px; border-top:2px solid {PRIMARY_COLOR};
            font-size:8px; color:#64748b; text-align:center">
  <strong style="color:{PRIMARY_COLOR}">{COMPANY_NAME}</strong><br>
  3301 Harbor Blvd., Oxnard, CA 93035<br>
  VC Office: (805) 844-9433 &nbsp;|&nbsp; LA &amp; OC Office: (714) 521-0855<br>
  www.americalpatrol.com<br>
  <span style="font-size:7px;color:#94a3b8">Report generated {now}</span>
</div>
"""

SEVERITY_STYLES = {
    "red":    {"bg": "#fef2f2", "border": "#dc2626", "text": "#991b1b"},
    "orange": {"bg": "#fff7ed", "border": "#ea580c", "text": "#9a3412"},
    "blue":   {"bg": "#eff6ff", "border": "#2563eb", "text": "#1e40af"},
    "green":  {"bg": "#f0fdf4", "border": "#16a34a", "text": "#166534"},
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _logo_img_tag() -> str:
    if LOGO_PATH.exists():
        data = base64.b64encode(LOGO_PATH.read_bytes()).decode()
        return f'<img src="data:image/png;base64,{data}" style="height:50px" alt="{COMPANY_NAME}">'
    return f'<strong style="font-size:18px;color:{PRIMARY_COLOR}">{COMPANY_NAME}</strong>'


def _severity_badge_html(incident_type: str, severity: str) -> str:
    s = SEVERITY_STYLES.get(severity, SEVERITY_STYLES["blue"])
    return (
        f'<span style="display:inline-block;padding:4px 12px;border-radius:4px;'
        f'background:{s["bg"]};border:1px solid {s["border"]};color:{s["text"]};'
        f'font-size:11px;font-weight:bold;letter-spacing:0.5px">'
        f'{incident_type}'
        f'</span>'
    )


def _confidentiality_footer(client_name: str = "") -> str:
    return (
        '<div style="margin-top:8px;font-size:8px;color:#94a3b8;'
        'text-align:center;font-style:italic">'
        'This report is confidential and prepared exclusively for the intended recipient. '
        'Unauthorized distribution is prohibited.'
        '</div>'
    )


def _clean_property_name(raw: str) -> str:
    cleaned = re.sub(r'^\d{8}[\s_]*', '', raw)
    cleaned = cleaned.replace('_', ' ')
    return cleaned.strip() or raw


def _resize_image(img_bytes: bytes, max_w: int = 600, max_h: int = 450) -> tuple[bytes, str]:
    """Resize image, fix EXIF rotation, fit within max dimensions. Returns (bytes, ext)."""
    try:
        from PIL import Image, ImageOps
        img = Image.open(BytesIO(img_bytes))
        # Fix EXIF orientation (sideways/upside-down photos)
        img = ImageOps.exif_transpose(img)
        ratio = min(max_w / max(img.width, 1), max_h / max(img.height, 1), 1.0)
        if ratio < 1.0:
            new_size = (int(img.width * ratio), int(img.height * ratio))
            img = img.resize(new_size, Image.LANCZOS)
        buf = BytesIO()
        img.save(buf, format="JPEG", quality=85)
        return buf.getvalue(), "jpeg"
    except ImportError:
        return img_bytes, "jpeg"


def _img_to_b64(img_bytes: bytes, ext: str = "jpeg") -> str:
    mime = f"image/{ext}" if ext != "jpg" else "image/jpeg"
    b64 = base64.b64encode(img_bytes).decode()
    return f"data:{mime};base64,{b64}"


def _is_logo(w: int, h: int) -> bool:
    """Detect company logos: square-ish images under 600px."""
    if w < 50 or h < 50:
        return True  # tiny icon
    aspect = max(w, h) / max(min(w, h), 1)
    return aspect < 1.35 and max(w, h) < 600


# ── API photo download ───────────────────────────────────────────────────────

def download_api_photos_by_round(parsed_data: dict) -> list[list[str]]:
    """Download photos from CDN URLs in API-sourced parsed data.
    Returns list of rounds, each containing list of base64 data URIs."""
    from connecteam_api import download_photo
    round_images = []
    for r in parsed_data.get("rounds", []):
        imgs = []
        for url in r.get("photos", []):
            photo_bytes = download_photo(url)
            if photo_bytes:
                resized, ext = _resize_image(photo_bytes)
                imgs.append(_img_to_b64(resized, ext))
        round_images.append(imgs)
    total = sum(len(r) for r in round_images)
    log.info(f"Downloaded {total} API photos across {len(round_images)} round(s)")
    return round_images


def download_api_photos_flat(parsed_data: dict) -> list[str]:
    """Download all photos as flat list from API-sourced incident data."""
    from connecteam_api import download_photo
    images = []
    for r in parsed_data.get("incident_rounds", []):
        for url in r.get("photos", []):
            photo_bytes = download_photo(url)
            if photo_bytes:
                resized, ext = _resize_image(photo_bytes)
                images.append(_img_to_b64(resized, ext))
    log.info(f"Downloaded {len(images)} API photos (flat)")
    return images


def download_api_photos_per_incident(parsed_data: dict) -> list[list[str]]:
    """Download photos grouped per incident entry from API data.
    Returns list of incidents, each with its own photo list."""
    from connecteam_api import download_photo
    per_incident = []
    for r in parsed_data.get("incident_rounds", []):
        imgs = []
        for url in r.get("photos", []):
            photo_bytes = download_photo(url)
            if photo_bytes:
                resized, ext = _resize_image(photo_bytes)
                imgs.append(_img_to_b64(resized, ext))
        per_incident.append(imgs)
    return per_incident


# ── Per-round image extraction ───────────────────────────────────────────────

def extract_images_by_round(pdf_path: Path) -> list[list[str]]:
    """
    Extract images grouped by patrol round.

    Connecteam DAR PDFs have this structure:
      - Text page = start of a new round (has timestamp, officer, checks)
      - Image-only pages after it = photos for that round
      - Rounds appear in REVERSE chronological order

    Returns: list of rounds, each containing list of base64 data URIs.
    """
    try:
        import fitz
        import pdfplumber
    except ImportError:
        return []

    round_images = []
    current_round_imgs = []

    try:
        doc = fitz.open(str(pdf_path))
        pdf_plumber = pdfplumber.open(str(pdf_path))
        seen_xrefs = set()
        _used_ocr_fallback = False
        _ocr_cache = {}

        for page_idx in range(len(doc)):
            fitz_page = doc[page_idx]
            plumber_page = pdf_plumber.pages[page_idx] if page_idx < len(pdf_plumber.pages) else None

            # Check if this is a text page (new round)
            text = plumber_page.extract_text() if plumber_page else ""
            if not text and not _used_ocr_fallback:
                # First time we see an image-only page — try OCR for all pages
                from ocr_fallback import is_ocr_available, ocr_pdf_pages
                if is_ocr_available():
                    _ocr_cache = ocr_pdf_pages(pdf_path)
                else:
                    _ocr_cache = {}
                _used_ocr_fallback = True
            if not text and _used_ocr_fallback:
                text = _ocr_cache.get(page_idx, "")
            is_text_page = bool(text and len(text.strip()) > 50)

            if is_text_page and current_round_imgs:
                # Save previous round's images and start new round
                round_images.append(current_round_imgs)
                current_round_imgs = []

            # Extract photos from this page (skip logos)
            for img_info in fitz_page.get_images(full=True):
                xref = img_info[0]
                if xref in seen_xrefs:
                    continue
                seen_xrefs.add(xref)

                try:
                    base_img = doc.extract_image(xref)
                    if not base_img:
                        continue
                    w, h = base_img.get("width", 0), base_img.get("height", 0)
                    if _is_logo(w, h):
                        continue

                    img_bytes, ext = _resize_image(base_img["image"])
                    current_round_imgs.append(_img_to_b64(img_bytes, ext))
                except Exception:
                    continue

        # Don't forget the last round
        if current_round_imgs:
            round_images.append(current_round_imgs)

        doc.close()
        pdf_plumber.close()

        total = sum(len(r) for r in round_images)
        log.info(f"Extracted {total} photos across {len(round_images)} round(s) from {pdf_path.name}")

    except Exception as e:
        log.warning(f"Per-round image extraction failed: {e}")

    return round_images


def extract_images_flat(pdf_path: Path) -> list[str]:
    """Extract all photos as a flat list (for incident reports)."""
    try:
        import fitz
    except ImportError:
        return []

    images = []
    try:
        doc = fitz.open(str(pdf_path))
        seen = set()
        for page in doc:
            for img_info in page.get_images(full=True):
                xref = img_info[0]
                if xref in seen:
                    continue
                seen.add(xref)
                try:
                    base_img = doc.extract_image(xref)
                    if not base_img:
                        continue
                    w, h = base_img.get("width", 0), base_img.get("height", 0)
                    if _is_logo(w, h):
                        continue
                    img_bytes, ext = _resize_image(base_img["image"])
                    images.append(_img_to_b64(img_bytes, ext))
                except Exception:
                    continue
        doc.close()
    except Exception as e:
        log.warning(f"Image extraction failed: {e}")
    return images


# ── Inline photos HTML ───────────────────────────────────────────────────────

def _inline_photos(images_b64: list[str]) -> str:
    """Render photos stacked vertically, one per row, full width for clarity."""
    if not images_b64:
        return ""
    html = ""
    for img in images_b64:
        html += (
            f'<div style="margin:4px 0">'
            f'<img src="{img}" style="border:1px solid #e2e8f0;border-radius:3px">'
            f'</div>'
        )
    return html


def _photos_section(images_b64: list[str]) -> str:
    """Render photos section for incident reports — stacked vertically."""
    if not images_b64:
        return ""
    html = f'<h3 style="color:{PRIMARY_COLOR};margin-top:16px;margin-bottom:6px">Photographs</h3>'
    for img in images_b64:
        html += (
            f'<div style="margin:4px 0">'
            f'<img src="{img}" style="border:1px solid #e2e8f0;border-radius:3px">'
            f'</div>'
        )
    return html


# ── Timeline bar ─────────────────────────────────────────────────────────────

def _build_timeline_html(rounds: list[dict]) -> str:
    """
    Build a patrol timeline as a Pillow-rendered PNG image.
    All labels below the bar. When labels are close together,
    tick marks get longer to stagger labels vertically and prevent overlap.
    Embedded as base64 <img> tag — bypasses xhtml2pdf layout quirks.
    """
    from PIL import Image, ImageDraw, ImageFont

    if not rounds:
        return ""

    # Collect timestamps
    entries = []
    for rnd in rounds:
        ts = rnd.get("timestamp")
        if ts:
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except ValueError:
                    continue
            suffix = "am" if ts.hour < 12 else "pm"
            label = f"{ts.hour % 12 or 12:02d}:{ts.minute:02d}{suffix}"
            entries.append((ts, label))

    if not entries:
        return ""

    entries.sort(key=lambda x: x[0])
    earliest = entries[0][0]
    latest = entries[-1][0]
    total_real_minutes = int((latest - earliest).total_seconds() / 60)
    buffer = 30
    total_span = max(total_real_minutes + 2 * buffer, 60)

    # Marker x-positions
    markers = []
    for ts, label in entries:
        t_min = int((ts - earliest).total_seconds() / 60)
        pct = (t_min + buffer) / total_span
        pct = max(0.03, min(pct, 0.97))
        markers.append((pct, label))

    # Canvas dimensions (2x render for sharpness)
    n = len(markers)
    canvas_w = 1400
    pad_x = 60
    bar_y = 18  # bar near the top — all labels go below
    draw_w = canvas_w - 2 * pad_x

    # Adaptive font size
    if n <= 6:
        font_size = 18
    elif n <= 12:
        font_size = 15
    else:
        font_size = 13

    try:
        font_bold = ImageFont.truetype("arialbd.ttf", font_size)
    except OSError:
        font_bold = ImageFont.load_default()

    # Tick length levels — short / medium / long for staggering
    tick_levels = [14, 30, 46]
    label_gap = 3  # pixels between tick bottom and label top

    # Compute x positions and measure label widths
    positioned = []
    for pct, label in markers:
        x = pad_x + int(pct * draw_w)
        bbox = font_bold.getbbox(label)
        w = bbox[2] - bbox[0]
        h = bbox[3] - bbox[1]
        positioned.append({"x": x, "label": label, "w": w, "h": h})

    # Assign tick levels: scan left to right, pick the shortest level
    # where the label doesn't overlap with any already-placed label at that level
    level_right_edges = {}  # level -> rightmost x edge used
    for m in positioned:
        x = m["x"]
        half_w = m["w"] // 2 + 6  # half label width + small padding

        assigned = 0
        for lvl in range(len(tick_levels)):
            right_edge = level_right_edges.get(lvl, -9999)
            if (x - half_w) > right_edge:
                assigned = lvl
                break
        else:
            # All levels overlap — use the deepest one
            assigned = len(tick_levels) - 1

        m["level"] = assigned
        level_right_edges[assigned] = x + half_w

    # Canvas height: enough for deepest label
    max_label_bottom = bar_y + tick_levels[-1] + label_gap + font_size + 8
    canvas_h = max(max_label_bottom + 6, 90)

    # Create canvas
    img = Image.new("RGB", (canvas_w, canvas_h), "#ffffff")
    draw = ImageDraw.Draw(img)

    # Draw the horizontal bar line
    draw.line([(pad_x, bar_y), (canvas_w - pad_x, bar_y)],
              fill=PRIMARY_COLOR, width=2)

    # Draw tick marks and labels
    for m in positioned:
        x = m["x"]
        tick_len = tick_levels[m["level"]]
        tick_bottom = bar_y + tick_len

        # Tick mark going down from bar
        draw.line([(x, bar_y), (x, tick_bottom)], fill=PRIMARY_COLOR, width=2)

        # Label below the tick
        draw.text((x, tick_bottom + label_gap), m["label"],
                  fill=PRIMARY_COLOR, font=font_bold, anchor="mt")

    # Convert to base64
    buf = BytesIO()
    img.save(buf, format="PNG", optimize=True)
    b64 = base64.b64encode(buf.getvalue()).decode()

    html = f'<div style="margin:8px 0 10px 0">'
    html += f'<div style="font-size:10px;font-weight:bold;color:{PRIMARY_COLOR};margin-bottom:2px">Patrol Timeline</div>'
    html += f'<img src="data:image/png;base64,{b64}" width="700">'
    html += '</div>'
    return html


# ── CSS ──────────────────────────────────────────────────────────────────────

def _page_css() -> str:
    return """
    <style>
        @page {
            size: letter;
            margin: 0.5in 0.6in 0.7in 0.6in;
            @frame footer {
                -pdf-frame-content: page-footer;
                bottom: 0.2in;
                margin-left: 0.6in;
                margin-right: 0.6in;
                height: 0.4in;
            }
        }
        body { font-family: Helvetica, Arial, sans-serif; font-size: 10px; color: #1e293b; line-height: 1.45; }
        h3 { font-size: 12px; margin-top: 14px; margin-bottom: 4px; }
        .header-table { width: 100%; border-collapse: collapse; margin-bottom: 0; }
        .meta-table { width: 100%; border-collapse: collapse; background: LIGHT_BG; margin-bottom: 10px; }
        .meta-table td { padding: 4px 8px; font-size: 9px; border-bottom: 1px solid BORDER; }
        .meta-label { font-weight: bold; color: PRIMARY; width: 110px; }
        .summary-box { background: #f0f9ff; border-left: 3px solid ACCENT; padding: 8px 12px; margin: 8px 0 10px 0; font-size: 10px; color: #334155; }
        .narrative { padding: 6px 0; font-size: 10px; line-height: 1.5; }
        .incident-type-line { font-size: 10px; margin: 4px 0 2px 0; color: #0f172a; }
        .incident-type-line strong { color: #0f172a; }
        .round-box { border: 1px solid #e2e8f0; border-radius: 4px; padding: 8px 10px; margin: 6px 0; background: white; }
        .round-header { font-size: 10px; font-weight: bold; color: PRIMARY; margin-bottom: 4px; }
        .round-meta { font-size: 9px; color: #64748b; }
        .check-item { font-size: 9px; color: #475569; margin: 1px 0; }
        .status-ok { color: #16a34a; }
        .status-incident { color: #dc2626; font-weight: bold; }
    </style>
    """.replace("PRIMARY", PRIMARY_COLOR).replace("ACCENT", ACCENT_COLOR).replace("LIGHT_BG", LIGHT_BG).replace("BORDER", BORDER_COLOR)


# ── Incident Report HTML ─────────────────────────────────────────────────────

def build_incident_html(parsed_data: dict, polished: dict | None,
                        images_b64, report_number: str | None,
                        severity: str, client_name: str,
                        classified_type: str = "") -> str:
    """Build incident report HTML.
    images_b64 can be:
      - list[list[str]]: per-incident photo groups (API data)
      - list[str]: flat photo list (legacy PDF extraction)
    """
    prop = _clean_property_name(parsed_data.get("property", "Unknown Property"))
    date = parsed_data.get("date", "")
    officers = parsed_data.get("officers", [])
    officer_str = ", ".join(officers) if officers else "Unknown"
    incidents = parsed_data.get("incident_rounds", [])

    # Detect if images are grouped per incident or flat
    per_incident_photos = []
    flat_photos = []
    if images_b64 and isinstance(images_b64[0], list):
        per_incident_photos = images_b64
    else:
        flat_photos = images_b64 or []

    exec_summary = ""
    if polished:
        exec_summary = polished.get("executive_summary", "")

    report_num_html = f'<span style="color:#64748b;font-size:11px">Report {report_number}</span>' if report_number else ""
    severity_style = SEVERITY_STYLES.get(severity, SEVERITY_STYLES["blue"])
    summary_border_color = severity_style["border"]

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">{_page_css()}</head>
<body>
<table class="header-table">
<tr>
  <td>{_logo_img_tag()}</td>
  <td style="text-align:right;vertical-align:top">{report_num_html}</td>
</tr>
</table>
<div style="background:{PRIMARY_COLOR};color:white;padding:7px 12px;margin:4px 0;border-radius:4px">
  <strong style="font-size:13px">INCIDENT REPORT</strong>
</div>
<table class="meta-table">
<tr><td class="meta-label">Property</td><td>{prop}</td></tr>
<tr><td class="meta-label">Date</td><td>{date}</td></tr>
<tr><td class="meta-label">Officer(s)</td><td>{officer_str}</td></tr>
<tr><td class="meta-label">Total Incidents</td><td>{len(incidents)}</td></tr>
</table>
{'<div style="background:#f0f9ff;border-left:4px solid ' + summary_border_color + ';padding:8px 12px;margin:8px 0 10px 0;font-size:10px;color:#334155"><strong>Executive Summary:</strong> ' + exec_summary + '</div>' if exec_summary else ''}
"""

    for i, inc in enumerate(incidents):
        incident_type = ""
        address = ""
        narrative = ""
        for note in inc.get("incident_notes", []):
            if note.startswith("Type of Incident:"):
                incident_type = note.replace("Type of Incident:", "").strip()
            elif note.startswith("Address:"):
                address = note.replace("Address:", "").strip()
            elif note.startswith("Report:"):
                narrative = note.replace("Report:", "").strip()

        time_str = inc.get("time_str", "")
        officer = inc.get("officer", officer_str)
        entry_num = inc.get("entry_num", "")
        entry_html = f' &mdash; #{entry_num}' if entry_num else ''

        # Photos for this specific incident
        inc_photos = per_incident_photos[i] if i < len(per_incident_photos) else []

        type_badge = _severity_badge_html(incident_type, severity) if incident_type else ""
        type_line = (
            f'<div class="incident-type-line">Type of Incident: <strong>{incident_type}</strong></div>'
            if incident_type else ''
        )
        narrative_html = (
            f'<strong style="color:#0f172a">{narrative}</strong>'
            if narrative
            else '<em style="color:#94a3b8">No narrative provided.</em>'
        )

        html += f"""
<div class="round-box" style="border-left:4px solid {severity_style['border']}">
  <div class="round-header">Incident {i + 1} &mdash; {time_str} &mdash; {officer}{entry_html}</div>
  {type_badge}
  {type_line}
  {'<div style="font-size:9px;margin:3px 0"><strong>Address:</strong> ' + address + '</div>' if address else ''}
  <div class="narrative" style="margin-top:4px">{narrative_html}</div>
  {_inline_photos(inc_photos)}
</div>"""

    # If flat photos (legacy), add them at the end
    if flat_photos:
        html += _photos_section(flat_photos)

    html += _company_footer()
    html += _confidentiality_footer(client_name)
    html += "</body></html>"
    return html


# ── DAR HTML ─────────────────────────────────────────────────────────────────

def build_dar_html(parsed_data: dict, polished: dict | None,
                   round_images: list[list[str]], report_number: str | None,
                   client_name: str) -> str:
    """
    Build DAR HTML with per-round photo association.
    round_images[i] = list of base64 images for round i (in PDF order = reverse chronological).
    """
    prop = _clean_property_name(parsed_data.get("property", "Unknown Property"))
    date = parsed_data.get("date", "")
    officers = parsed_data.get("officers", [])
    officer_str = ", ".join(officers) if officers else "Unknown"
    total_rounds = parsed_data.get("total_rounds", 0)
    first_time = parsed_data.get("first_time", "")
    last_time = parsed_data.get("last_time", "")
    rounds = parsed_data.get("rounds", [])

    if first_time and last_time:
        time_range = f" ({first_time})" if first_time == last_time else f" ({first_time} – {last_time})"
    else:
        time_range = ""
    exec_summary = polished.get("executive_summary", "") if polished else ""
    report_num_html = f'<span style="color:#64748b;font-size:11px">Report {report_number}</span>' if report_number else ""

    # Sort rounds chronologically for display
    sorted_rounds = sorted(rounds, key=lambda r: r.get("timestamp") or datetime.min)

    # The round_images are in PDF order (reverse chronological),
    # so reverse them to match chronological order
    chrono_images = list(reversed(round_images)) if round_images else []

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">{_page_css()}</head>
<body>
<table class="header-table">
<tr>
  <td>{_logo_img_tag()}</td>
  <td style="text-align:right;vertical-align:top">{report_num_html}</td>
</tr>
</table>
<div style="background:{PRIMARY_COLOR};color:white;padding:7px 12px;margin:4px 0;border-radius:4px">
  <strong style="font-size:13px">DAILY ACTIVITY REPORT</strong>
</div>
<table class="meta-table">
<tr><td class="meta-label">Property</td><td>{prop}</td></tr>
<tr><td class="meta-label">Date</td><td>{date}</td></tr>
<tr><td class="meta-label">Officer(s)</td><td>{officer_str}</td></tr>
<tr><td class="meta-label">Patrol Rounds</td><td>{total_rounds}{time_range}</td></tr>
</table>
{'<div class="summary-box"><strong>Executive Summary:</strong> ' + exec_summary + '</div>' if exec_summary else ''}
{_build_timeline_html(rounds)}
<h3 style="color:{PRIMARY_COLOR}">Patrol Rounds</h3>
"""

    # The "office, spa, bathrooms, and RV parking" narrative is Harbor Lights-
    # specific wording. Other properties that also hit the facility_areas key
    # (any DAR with an office/spa/bathroom/RV checkbox) get a neutral line.
    is_harbor_lights = (
        "harbor lights" in (client_name or "").lower()
        or "harbor lights" in (prop or "").lower()
    )

    for i, rnd in enumerate(sorted_rounds):
        time_str = rnd.get("time_str", "")
        officer = rnd.get("officer", "")
        checks = rnd.get("checks", {})
        has_inc = rnd.get("has_incident", False)
        notes = rnd.get("incident_notes", [])

        # Build checks list — show ALL checks with status
        checks_html = ""
        if has_inc:
            checks_html += '<div class="check-item"><span class="status-incident">INCIDENT REPORTED</span></div>'

        # Clear, human-readable check descriptions
        CHECK_CLEAR_TEXT = {
            "unwanted_persons": "No unwanted persons found on property",
            "illegal_dumping": "No illegal dumping found on property",
            "property_damage": "No property damage observed",
            "vandalism": "No vandalism observed",
            "homeless": "No homeless individuals found on property",
            "lights": "All lights on property working properly",
            "gates": "All gates locked and secured",
            "doors": "All exterior doors locked and secured",
            "tires": "Tires inflated and in good condition",
            "parking": "All vehicles have valid parking passes",
            "interior": "Interior building patrolled",
            "facility_areas": (
                "Officer checked office, spa, bathrooms, and RV parking"
                if is_harbor_lights
                else "Facility areas patrolled"
            ),
        }

        for key, val in checks.items():
            if not val:
                continue
            val_lower = val.lower().strip()
            if val_lower in ("completed", "yes", "no"):
                clear_text = CHECK_CLEAR_TEXT.get(key)
                if not clear_text:
                    # Generate a reasonable default from the key
                    label = key.replace("_", " ").title()
                    clear_text = f"No {label.lower()} found"
                checks_html += f'<div class="check-item"><span style="color:#16a34a">&#10003;</span> {clear_text}</div>'
            elif val_lower in ("unknown", "n/a", ""):
                continue
            else:
                label = key.replace("_", " ").title()
                checks_html += f'<div class="check-item"><span style="color:#ea580c;font-weight:bold">!</span> {label}: {val}</div>'

        # Notes
        notes_html = ""
        if notes:
            notes_html = '<div style="font-size:11px;color:#0f172a;margin-top:3px;font-style:italic">' + "; ".join(notes) + '</div>'

        # Photos for this round
        round_photos = chrono_images[i] if i < len(chrono_images) else []

        html += f"""
<div class="round-box">
  <div class="round-header">Round {i + 1} &mdash; {time_str} &mdash; {officer}</div>
  {checks_html}
  {notes_html}
  {_inline_photos(round_photos)}
</div>"""

    # Incident details (if any)
    inc_rounds = parsed_data.get("incident_rounds", [])
    if inc_rounds:
        html += f'<h3 style="color:#dc2626;margin-top:12px">Incident Details</h3>'
        for inc in inc_rounds:
            for note in inc.get("incident_notes", []):
                html += f'<p style="margin:3px 0;font-size:10px">{note}</p>'

    html += _company_footer()
    html += _confidentiality_footer(client_name)
    html += "</body></html>"
    return html


def build_vehicle_patrol_html(parsed_data: dict, polished: dict | None,
                              round_images: list[list[str]], report_number: str | None,
                              client_name: str) -> str:
    """Build Vehicle Patrol Report HTML — photo-driven patrol rounds without incident flags."""
    prop = _clean_property_name(parsed_data.get("property", "Unknown Property"))
    date = parsed_data.get("date", "")
    officers = parsed_data.get("officers", [])
    officer_str = ", ".join(officers) if officers else "Unknown"
    first_time = parsed_data.get("first_time", "")
    last_time = parsed_data.get("last_time", "")

    # Combine all rounds (rounds + incident_rounds may overlap, so use rounds which has everything)
    all_rounds = parsed_data.get("rounds", [])
    # If rounds is empty but incident_rounds has data (vehicle patrol misclassified), use those
    if not all_rounds:
        all_rounds = parsed_data.get("incident_rounds", [])
    total_rounds = len(all_rounds)

    if first_time and last_time:
        time_range = f" ({first_time})" if first_time == last_time else f" ({first_time} – {last_time})"
    else:
        time_range = ""
    report_num_html = f'<span style="color:#64748b;font-size:11px">Report {report_number}</span>' if report_number else ""

    sorted_rounds = sorted(all_rounds, key=lambda r: r.get("timestamp") or datetime.min)
    chrono_images = list(reversed(round_images)) if round_images else []

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">{_page_css()}</head>
<body>
<table class="header-table">
<tr>
  <td>{_logo_img_tag()}</td>
  <td style="text-align:right;vertical-align:top">{report_num_html}</td>
</tr>
</table>
<div style="background:{PRIMARY_COLOR};color:white;padding:7px 12px;margin:4px 0;border-radius:4px">
  <strong style="font-size:13px">VEHICLE PATROL REPORT</strong>
</div>
<table class="meta-table">
<tr><td class="meta-label">Property</td><td>{prop}</td></tr>
<tr><td class="meta-label">Date</td><td>{date}</td></tr>
<tr><td class="meta-label">Officer(s)</td><td>{officer_str}</td></tr>
<tr><td class="meta-label">Patrol Rounds</td><td>{total_rounds}{time_range}</td></tr>
</table>
{_build_timeline_html(sorted_rounds)}
<h3 style="color:{PRIMARY_COLOR}">Patrol Rounds</h3>
"""

    # See build_dar_html for the rationale: the facility_areas narrative is
    # Harbor Lights wording and should only render for that property.
    is_harbor_lights = (
        "harbor lights" in (client_name or "").lower()
        or "harbor lights" in (prop or "").lower()
    )
    CHECK_CLEAR_TEXT = {
        "unwanted_persons": "No unwanted persons found on property",
        "illegal_dumping": "No illegal dumping found on property",
        "property_damage": "No property damage observed",
        "vandalism": "No vandalism observed",
        "homeless": "No homeless individuals found on property",
        "lights": "All lights on property working properly",
        "gates": "All gates locked and secured",
        "doors": "All exterior doors locked and secured",
        "tires": "Tires inflated and in good condition",
        "parking": "All vehicles have valid parking passes",
        "interior": "Interior building patrolled",
        "facility_check": "Facility check completed",
        "facility_areas": (
            "Officer checked office, spa, bathrooms, and RV parking"
            if is_harbor_lights
            else "Facility areas patrolled"
        ),
    }

    for i, rnd in enumerate(sorted_rounds):
        time_str = rnd.get("time_str", "")
        officer = rnd.get("officer", "")
        entry_num = rnd.get("entry_num", "")
        entry_html = f' &mdash; #{entry_num}' if entry_num else ''
        checks = rnd.get("checks", {})
        notes = rnd.get("incident_notes", [])

        # Build checks list
        checks_html = ""
        for key, val in checks.items():
            if not val:
                continue
            val_lower = val.lower().strip()
            if val_lower in ("completed", "yes", "no"):
                clear_text = CHECK_CLEAR_TEXT.get(key)
                if not clear_text:
                    label = key.replace("_", " ").title()
                    clear_text = f"No {label.lower()} found"
                checks_html += f'<div class="check-item"><span style="color:#16a34a">&#10003;</span> {clear_text}</div>'
            elif val_lower in ("unknown", "n/a", ""):
                continue
            else:
                label = key.replace("_", " ").title()
                checks_html += f'<div class="check-item"><span style="color:#ea580c;font-weight:bold">!</span> {label}: {val}</div>'

        # Notes
        notes_html = ""
        if notes:
            notes_html = '<div style="font-size:11px;color:#0f172a;margin-top:3px;font-style:italic">' + "; ".join(notes) + '</div>'

        # Photos for this round
        round_photos = chrono_images[i] if i < len(chrono_images) else []

        html += f"""
<div class="round-box">
  <div class="round-header">Patrol Round {i + 1} &mdash; {time_str} &mdash; {officer}{entry_html}</div>
  {checks_html}
  {notes_html}
  {_inline_photos(round_photos)}
</div>"""

    html += _company_footer()
    html += _confidentiality_footer(client_name)
    html += "</body></html>"
    return html


def build_guest_parking_html(parsed_data: dict, round_images: list[list[str]],
                             report_number: str | None, client_name: str) -> str:
    """Build Guest Parking check HTML — shows each street with status and photos."""
    prop = _clean_property_name(parsed_data.get("property", "Unknown Property"))
    date = parsed_data.get("date", "")
    officers = parsed_data.get("officers", [])
    officer_str = ", ".join(officers) if officers else "Unknown"
    rounds = parsed_data.get("rounds", [])
    first_time = parsed_data.get("first_time", "")
    report_num_html = f'<span style="color:#64748b;font-size:11px">Report {report_number}</span>' if report_number else ""

    html = f"""<!DOCTYPE html>
<html><head><meta charset="utf-8">{_page_css()}</head>
<body>
<table class="header-table">
<tr>
  <td>{_logo_img_tag()}</td>
  <td style="text-align:right;vertical-align:top">{report_num_html}</td>
</tr>
</table>
<div style="background:{PRIMARY_COLOR};color:white;padding:7px 12px;margin:4px 0;border-radius:4px">
  <strong style="font-size:13px">GUEST PARKING CHECK</strong>
</div>
<table class="meta-table">
<tr><td class="meta-label">Property</td><td>{prop}</td></tr>
<tr><td class="meta-label">Date</td><td>{date}</td></tr>
<tr><td class="meta-label">Officer</td><td>{officer_str}</td></tr>
<tr><td class="meta-label">Time</td><td>{first_time}</td></tr>
</table>
<h3 style="color:{PRIMARY_COLOR}">Street-by-Street Parking Check</h3>
"""

    for i, rnd in enumerate(rounds):
        street = rnd.get("street_name", f"Street {i+1}")
        has_vehicles = rnd.get("has_incident", False)
        notes = rnd.get("incident_notes", [])
        photos = round_images[i] if i < len(round_images) else []

        if has_vehicles:
            status_html = '<span style="color:#ea580c;font-weight:bold">&#9679; Vehicles Found</span>'
            # Show plates from notes
            plates_html = ""
            for n in notes:
                if "Plates:" in n:
                    plates_html += f'<div style="font-size:11px;color:#0f172a;margin-top:2px">{n}</div>'
        else:
            status_html = '<span style="color:#16a34a">&#10003; No Vehicles</span>'
            plates_html = ""

        html += f"""
<div class="round-box">
  <div class="round-header">{street}</div>
  <div class="check-item">{status_html}</div>
  {plates_html}
  {_inline_photos(photos)}
</div>"""

    html += _company_footer()
    html += _confidentiality_footer(client_name)
    html += "</body></html>"
    return html


# ── PDF generation ───────────────────────────────────────────────────────────

def _html_to_pdf(html: str, output_path: Path) -> bool:
    try:
        from xhtml2pdf import pisa
        with open(output_path, "wb") as f:
            result = pisa.CreatePDF(html, dest=f)
        if result.err:
            raise RuntimeError(f"xhtml2pdf: {result.err} errors")
        return True
    except ImportError:
        pass
    except Exception as e:
        log.warning(f"xhtml2pdf failed: {e} — trying weasyprint")

    try:
        from weasyprint import HTML as WeasyHTML
        WeasyHTML(string=html).write_pdf(str(output_path))
        return True
    except (ImportError, OSError) as e:
        log.warning(f"weasyprint unavailable: {e}")

    return False


# ── Main orchestrator ────────────────────────────────────────────────────────

def generate_branded_pdf(pdf_path: Path, parsed_data: dict | None,
                         polished: dict | None = None,
                         client_name: str = "") -> Path | None:
    if not parsed_data:
        log.warning(f"No parsed data for {pdf_path.name} — skipping branding")
        return None

    is_api = parsed_data.get("source") == "api"
    is_guest_parking = parsed_data.get("report_type") == "guest_parking"
    is_vehicle_dar = parsed_data.get("report_type") == "vehicle_dar"
    is_incident = bool(parsed_data.get("incident_rounds")) and not is_vehicle_dar
    report_type = "incident" if is_incident else "dar"

    # Get report number: from API entry_num or from raw text
    report_number = ""
    if is_api and parsed_data.get("entry_num"):
        report_number = f"#{parsed_data['entry_num']}"
    else:
        raw_text = _extract_raw_text(pdf_path)
        if not raw_text and not is_api:
            log.warning(f"No extractable text in {pdf_path.name} — skipping branding")
            return None
        if raw_text:
            from report_polisher import extract_report_number
            report_number = extract_report_number(raw_text)

    from report_polisher import classify_severity

    # Polish text: fix grammar, spelling, and translate non-English
    if polished is None:
        from report_polisher import polish_report_text
        if is_api:
            # Build raw text from API structured data for the polisher
            raw_text = _build_raw_text_from_api(parsed_data)
        else:
            raw_text = raw_text if 'raw_text' in dir() else _extract_raw_text(pdf_path)
        if raw_text:
            polished = polish_report_text(raw_text, report_type, parsed_data)

    # Apply polished text back to incident notes (translations, grammar fixes)
    if polished and polished.get("polished_incidents") and is_incident:
        _apply_polished_incidents(parsed_data, polished["polished_incidents"])

    if is_guest_parking:
        # Guest parking: street-by-street layout with photos
        if is_api:
            round_images = download_api_photos_by_round(parsed_data)
        else:
            round_images = extract_images_by_round(pdf_path)
        html = build_guest_parking_html(parsed_data, round_images,
                                        report_number, client_name)
    elif is_vehicle_dar:
        # Vehicle patrols: photo-driven patrol rounds, no incident flags
        if is_api:
            round_images = download_api_photos_by_round(parsed_data)
        else:
            round_images = extract_images_by_round(pdf_path)
        html = build_vehicle_patrol_html(parsed_data, polished, round_images,
                                         report_number, client_name)
    elif is_incident:
        # Incident reports: photos grouped per incident entry
        if is_api:
            images_b64 = download_api_photos_per_incident(parsed_data)
        else:
            images_b64 = [extract_images_flat(pdf_path)]

        incident_type = ""
        if polished and polished.get("classified_type"):
            incident_type = polished["classified_type"]
        else:
            for inc in parsed_data.get("incident_rounds", []):
                for note in inc.get("incident_notes", []):
                    if "Type of Incident:" in note:
                        incident_type = note.split("Type of Incident:")[-1].strip()
                        break
                if incident_type:
                    break

        severity = classify_severity(incident_type)
        html = build_incident_html(parsed_data, polished, images_b64,
                                   report_number, severity, client_name,
                                   classified_type=incident_type)
    else:
        # DARs: photos grouped per round
        if is_api:
            round_images = download_api_photos_by_round(parsed_data)
        else:
            round_images = extract_images_by_round(pdf_path)
        html = build_dar_html(parsed_data, polished, round_images,
                              report_number, client_name)

    # Clean filename
    prop_clean = _clean_property_name(parsed_data.get("property", "Report"))
    prop_clean = re.sub(r'[^\w\s-]', '', prop_clean).strip().replace(' ', '_')
    date_str = parsed_data.get("date", "")
    try:
        date_compact = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        date_compact = datetime.now().strftime("%Y-%m-%d")

    # Label report type correctly
    if is_guest_parking:
        rtype_label = "Guest_Parking"
    elif is_vehicle_dar:
        rtype_label = "Vehicle_Patrol_Report"
    elif is_incident:
        rtype_label = "Incident_Report"
    else:
        rtype_label = "DAR"

    num_suffix = f"_{report_number.replace('#','')}" if report_number else ""
    branded_name = f"Americal_Patrol_{prop_clean}_{rtype_label}_{date_compact}{num_suffix}.pdf"
    branded_path = pdf_path.parent / branded_name
    if _html_to_pdf(html, branded_path):
        log.info(f"Branded PDF generated: {branded_path.name}")
        _archive_original(pdf_path)
        return branded_path
    else:
        log.warning(f"PDF generation failed for {pdf_path.name}")
        return None


def _extract_raw_text(pdf_path: Path) -> str:
    try:
        import pdfplumber
        texts = []
        with pdfplumber.open(str(pdf_path)) as pdf:
            for page in pdf.pages:
                t = page.extract_text()
                if t:
                    texts.append(t)
        if texts:
            return "\n".join(texts)

        # OCR fallback for image-only PDFs
        from ocr_fallback import is_ocr_available, ocr_pdf_pages
        if is_ocr_available():
            ocr_texts = ocr_pdf_pages(pdf_path)
            if ocr_texts:
                return "\n".join(text for _, text in sorted(ocr_texts.items()))
        return ""
    except Exception as e:
        log.warning(f"Text extraction failed for {pdf_path.name}: {e}")
        return ""


def _apply_polished_incidents(parsed_data: dict, polished_incidents: list):
    """Replace raw incident notes with polished/translated versions.

    Updates incident_rounds in-place so build_incident_html renders
    the professional, translated text instead of the guard's raw input.
    """
    incidents = parsed_data.get("incident_rounds", [])
    for i, inc in enumerate(incidents):
        if i >= len(polished_incidents):
            break
        pi = polished_incidents[i]
        new_notes = []
        for note in inc.get("incident_notes", []):
            if note.startswith("Type of Incident:") and pi.get("type"):
                new_notes.append(f"Type of Incident: {pi['type']}")
            elif note.startswith("Address:") and pi.get("address"):
                new_notes.append(f"Address: {pi['address']}")
            elif note.startswith("Report:") and pi.get("report"):
                new_notes.append(f"Report: {pi['report']}")
            else:
                new_notes.append(note)
        inc["incident_notes"] = new_notes


def _build_raw_text_from_api(parsed_data: dict) -> str:
    """Build raw text from API-sourced parsed data for the polisher.

    Extracts officer-written text (incident notes, check values, plates)
    so the polisher can fix grammar, spelling, and translate non-English.
    """
    lines = []
    prop = parsed_data.get("property", "")
    date = parsed_data.get("date", "")
    if prop:
        lines.append(f"Property: {prop}")
    if date:
        lines.append(f"Date: {date}")
    lines.append("")

    for rnd in parsed_data.get("rounds", []):
        officer = rnd.get("officer", "")
        time_str = rnd.get("time_str", "")
        if officer or time_str:
            lines.append(f"{officer} — {time_str}")
        for note in rnd.get("incident_notes", []):
            if note:
                lines.append(note)
        for key, val in rnd.get("checks", {}).items():
            if val and val.lower() not in ("completed", "yes", "no", "n/a", "unknown", ""):
                lines.append(f"{key.replace('_', ' ').title()}: {val}")
        # Guest parking street/plate text
        street = rnd.get("street_name", "")
        if street:
            lines.append(f"Street: {street}")
        lines.append("")

    for inc in parsed_data.get("incident_rounds", []):
        officer = inc.get("officer", "")
        time_str = inc.get("time_str", "")
        if officer or time_str:
            lines.append(f"{officer} — {time_str}")
        for note in inc.get("incident_notes", []):
            if note:
                lines.append(note)
        lines.append("")

    return "\n".join(lines).strip()


def _archive_original(pdf_path: Path):
    try:
        ORIGINALS_DIR.mkdir(parents=True, exist_ok=True)
        dest = ORIGINALS_DIR / pdf_path.name
        shutil.copy2(str(pdf_path), str(dest))
    except Exception as e:
        log.warning(f"Could not archive original {pdf_path.name}: {e}")


# ── Standalone test ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    sys.path.insert(0, str(SCRIPT_DIR.parent))
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env")

    from pdf_analyzer import parse_report

    test_pdfs = list((PROJECT_DIR / "Americal Patrol Morning Reports").glob("*Incident_Report*.pdf"))
    if not test_pdfs:
        test_pdfs = list((PROJECT_DIR / "Americal Patrol Morning Reports").glob("*DAR*.pdf"))

    if not test_pdfs:
        print("No test PDFs found")
        sys.exit(1)

    pdf_path = test_pdfs[0]
    print(f"Testing with: {pdf_path.name}")

    parsed = parse_report(pdf_path)
    if not parsed:
        print("Could not parse PDF")
        sys.exit(1)

    branded = generate_branded_pdf(pdf_path, parsed, client_name="Transwestern")
    if branded:
        print(f"\nBranded PDF: {branded}")
        print(f"Size: {branded.stat().st_size:,} bytes")
    else:
        print("Failed")
