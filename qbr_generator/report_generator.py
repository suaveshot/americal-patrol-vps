"""
Americal Patrol — QBR Report Generator

Renders the HTML template with data and converts to PDF.
"""

import base64
import logging
from datetime import datetime
from pathlib import Path

from config import (
    TEMPLATE_DIR, OUTPUT_DIR, LOGO_PATH,
    PRIMARY_COLOR, ACCENT_COLOR, LIGHT_BG, COMPANY_NAME,
)

log = logging.getLogger("qbr_generator")

DOW_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def _logo_img_tag():
    """Generate an <img> tag with base64-encoded logo for PDF embedding."""
    if LOGO_PATH.exists():
        logo_data = base64.b64encode(LOGO_PATH.read_bytes()).decode()
        return f'<img src="data:image/png;base64,{logo_data}" class="logo" alt="{COMPANY_NAME}">'
    return f"<strong>{COMPANY_NAME}</strong>"


def _incident_delta_html(trends):
    """Generate the delta indicator HTML for the incidents metric card."""
    delta = trends.get("incident_delta")
    if delta is None:
        return '<span class="delta-neutral">First quarter</span>'
    if delta > 0:
        pct = trends.get("incident_delta_pct", 0)
        return f'<span class="delta-up">&#9650; +{delta} ({pct:+.0f}%) vs prior Q</span>'
    elif delta < 0:
        pct = trends.get("incident_delta_pct", 0)
        return f'<span class="delta-down">&#9660; {delta} ({pct:.0f}%) vs prior Q</span>'
    else:
        return '<span class="delta-neutral">&#9644; No change vs prior Q</span>'


def _dow_chart_html(data):
    """Generate day-of-week bar chart HTML using tables (xhtml2pdf compatible)."""
    dow = data.get("incidents_by_day_of_week", {})
    if not dow and data["total_incidents"] == 0:
        return ""

    max_val = max(dow.values()) if dow else 1
    rows = []
    for i, name in enumerate(DOW_NAMES):
        count = dow.get(i, 0)
        pct = int((count / max_val * 100)) if max_val > 0 else 0
        # Use a fixed-width colored block to represent the bar
        bar_width = max(pct * 3, 2) if count > 0 else 0  # scale to ~300px max
        rows.append(
            f'<tr>'
            f'  <td class="bar-label-cell">{name}</td>'
            f'  <td class="bar-track-cell">'
            f'    <span class="bar-fill" style="width:{bar_width}px;">&nbsp;</span>'
            f'  </td>'
            f'  <td class="bar-value-cell">{count}</td>'
            f'</tr>'
        )

    return (
        '<h3 class="section-title">Incidents by Day of Week</h3>'
        '<table class="bar-chart-table">' + "\n".join(rows) + "</table>"
    )


def _timeline_section_html(data):
    """Generate incident timeline table."""
    dates = data.get("incident_dates", [])
    if not dates:
        return ""

    rows = []
    for d in dates[:15]:  # Cap at 15 most recent
        try:
            dt = datetime.strptime(d, "%Y-%m-%d")
            formatted = dt.strftime("%B %d, %Y")
            day_name = DOW_NAMES[dt.weekday()]
        except ValueError:
            formatted = d
            day_name = ""
        rows.append(f"<tr><td>{formatted}</td><td>{day_name}</td></tr>")

    if len(dates) > 15:
        rows.append(f'<tr><td colspan="2" style="color:#999;font-style:italic">'
                     f'... and {len(dates) - 15} more</td></tr>')

    return (
        '<h3 class="section-title">Incident Timeline</h3>'
        '<table class="data-table">'
        "<tr><th>Date</th><th>Day</th></tr>"
        + "\n".join(rows)
        + "</table>"
    )


def _recommendations_html(narrative):
    """Parse recommendations from narrative into <li> items."""
    recs = narrative.get("recommendations", "")
    if not recs:
        return "<li>No specific recommendations this quarter — maintain current coverage.</li>"

    items = []
    for line in recs.split("\n"):
        line = line.strip()
        if line.startswith("-") or line.startswith("•"):
            line = line.lstrip("-•").strip()
            if line:
                items.append(f"<li>{line}</li>")
    return "\n".join(items) if items else f"<li>{recs}</li>"


def render_report(data, trends, narrative):
    """
    Render the QBR HTML from template + data.

    Returns the HTML string.
    """
    template_path = TEMPLATE_DIR / "qbr_template.html"
    template = template_path.read_text(encoding="utf-8")

    property_names = ", ".join(data["property_names"])
    report_date = datetime.now().strftime("%B %d, %Y")

    replacements = {
        "{{primary_color}}": PRIMARY_COLOR,
        "{{accent_color}}": ACCENT_COLOR,
        "{{light_bg}}": LIGHT_BG,
        "{{company_name}}": COMPANY_NAME,
        "{{property_names}}": property_names,
        "{{quarter_label}}": data["quarter_label"],
        "{{report_date}}": report_date,
        "{{logo_img}}": _logo_img_tag(),
        "{{executive_summary}}": narrative.get("executive_summary", ""),
        "{{total_patrol_days}}": str(data["total_patrol_days"]),
        "{{incident_free_pct}}": str(trends["incident_free_pct"]),
        "{{total_incidents}}": str(data["total_incidents"]),
        "{{incident_delta_html}}": _incident_delta_html(trends),
        "{{longest_clean_streak}}": str(trends["longest_clean_streak"]),
        "{{dow_section}}": _dow_chart_html(data),
        "{{timeline_section}}": _timeline_section_html(data),
        "{{recommendations_html}}": _recommendations_html(narrative),
        "{{outlook}}": narrative.get("outlook", ""),
    }

    html = template
    for key, value in replacements.items():
        html = html.replace(key, value)

    return html


def generate_pdf(html, group_id, quarter_label):
    """
    Convert HTML to PDF. Tries xhtml2pdf first (pure Python, works on Windows),
    falls back to weasyprint, then to saving as HTML.

    Returns Path to the generated PDF file.
    """
    OUTPUT_DIR.mkdir(exist_ok=True)

    safe_quarter = quarter_label.replace(" ", "_")
    filename = f"QBR_{group_id}_{safe_quarter}.pdf"
    pdf_path = OUTPUT_DIR / filename

    # Try xhtml2pdf first (pure Python, no system deps)
    try:
        from xhtml2pdf import pisa
        with open(pdf_path, "wb") as f:
            result = pisa.CreatePDF(html, dest=f)
        if result.err:
            log.warning("xhtml2pdf reported %d errors, trying fallback", result.err)
            raise RuntimeError("xhtml2pdf errors")
        log.info("PDF generated (xhtml2pdf): %s", pdf_path)
        return pdf_path
    except ImportError:
        pass
    except Exception as e:
        log.warning("xhtml2pdf failed: %s — trying weasyprint", e)

    # Try weasyprint
    try:
        from weasyprint import HTML
        HTML(string=html).write_pdf(str(pdf_path))
        log.info("PDF generated (weasyprint): %s", pdf_path)
        return pdf_path
    except (ImportError, OSError) as e:
        log.warning("weasyprint unavailable: %s", e)

    # Fallback: save as HTML
    html_path = OUTPUT_DIR / filename.replace(".pdf", ".html")
    html_path.write_text(html, encoding="utf-8")
    log.warning("No PDF engine available — saved HTML: %s", html_path)
    return html_path
