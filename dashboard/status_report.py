"""
Americal Patrol — Status Report Generator
Generates a self-contained HTML status page from pipeline data.
Run: python status_report.py
"""

import sys
import webbrowser
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
from data_collector import get_all_pipelines_status, get_productivity_metrics

STATUS_COLORS = {
    "success": ("#22c55e", "OK"),
    "warning": ("#eab308", "Warning"),
    "error": ("#ef4444", "Error"),
    "overdue": ("#eab308", "Overdue"),
    "scheduled": ("#3b82f6", "Scheduled"),
    "unknown": ("#6b7280", "Unknown"),
    "running": ("#3b82f6", "Live"),
    "down": ("#ef4444", "Down"),
}

SCHEDULE_ORDER = {
    "ads": ("Monday", "6:00 AM"),
    "patrol": ("Daily", "7:00 AM"),
    "harbor_lights": ("Daily", "7:30 AM"),
    "seo": ("Monday", "7:00 AM"),
    "blog": ("Monday", "8:00 AM"),
    "sales_pipeline": ("Mon-Fri", "8:00 AM"),
    "gbp": ("Monday", "9:00 AM"),
    "social": ("Tue/Thu/Sat", "10:00 AM"),
    "weekly_update": ("Friday", "12:00 PM"),
    "voice": ("Always", "On (Vapi)"),
}


def _status_dot(status: str) -> str:
    color, _ = STATUS_COLORS.get(status, ("#6b7280", "?"))
    pulse = 'animation: pulse 2s infinite;' if status == "running" else ''
    return f'<span style="display:inline-block;width:12px;height:12px;border-radius:50%;background:{color};{pulse}margin-right:8px;vertical-align:middle;"></span>'


def _status_badge(status: str) -> str:
    color, label = STATUS_COLORS.get(status, ("#6b7280", "?"))
    return f'<span style="display:inline-block;padding:2px 10px;border-radius:12px;font-size:12px;font-weight:600;color:#fff;background:{color};">{label}</span>'


def generate_html(pipelines: list[dict], metrics: dict) -> str:
    now = datetime.now()
    generated = now.strftime("%B %d, %Y at %I:%M %p")

    # Count statuses for banner
    issues = sum(1 for p in pipelines if p["status"] in ("error", "overdue", "down"))
    if issues == 0:
        banner_color = "#22c55e"
        banner_text = "All systems operational"
    else:
        banner_color = "#ef4444"
        banner_text = f"{issues} pipeline{'s' if issues != 1 else ''} need{'s' if issues == 1 else ''} attention"

    # Build pipeline cards
    cards_html = ""
    for p in pipelines:
        cards_html += f"""
        <div style="background:#1e293b;border-radius:12px;padding:20px;border:1px solid #334155;">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
                <h3 style="margin:0;font-size:16px;color:#f1f5f9;">{_status_dot(p['status'])}{p['name']}</h3>
                {_status_badge(p['status'])}
            </div>
            <div style="font-size:13px;color:#94a3b8;margin-bottom:6px;">
                <strong>Schedule:</strong> {p['schedule']}
            </div>
            <div style="font-size:13px;color:#94a3b8;margin-bottom:6px;">
                <strong>Last run:</strong> {p['last_run_display']}
            </div>
            <div style="font-size:13px;color:#cbd5e1;margin-top:10px;padding-top:10px;border-top:1px solid #334155;">
                {p['summary']}
            </div>
        </div>"""

    # Build metric boxes
    metric_items = [
        ("Email Drafts", metrics["drafts_created"]),
        ("Incidents", metrics["total_incidents"]),
        ("Blog Posts", metrics["blog_posts"]),
        ("GBP Posts", metrics["gbp_posts"]),
        ("Social Posts", metrics["social_posts"]),
        ("SEO Reports", metrics["seo_reports"]),
        ("Estimates Sent", metrics.get("estimates_sent", 0)),
        ("Weekly Digests", metrics.get("weekly_digests", 0)),
    ]
    metrics_html = ""
    for label, value in metric_items:
        metrics_html += f"""
        <div style="background:#1e293b;border-radius:12px;padding:16px;text-align:center;border:1px solid #334155;">
            <div style="font-size:28px;font-weight:700;color:#f1f5f9;">{value}</div>
            <div style="font-size:12px;color:#94a3b8;margin-top:4px;">{label}</div>
        </div>"""

    # Build schedule table
    schedule_rows = ""
    for pid, (day, time) in SCHEDULE_ORDER.items():
        p = next((x for x in pipelines if x["id"] == pid), None)
        if not p:
            continue
        schedule_rows += f"""
        <tr>
            <td style="padding:8px 12px;border-bottom:1px solid #334155;color:#f1f5f9;">{_status_dot(p['status'])}{p['name']}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #334155;color:#94a3b8;">{day}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #334155;color:#94a3b8;">{time}</td>
            <td style="padding:8px 12px;border-bottom:1px solid #334155;color:#94a3b8;">{p['last_run_display']}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="3600">
    <title>Americal Patrol — Pipeline Status</title>
    <style>
        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.5; }}
        }}
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            min-height: 100vh;
            padding: 32px;
        }}
        a {{ color: #60a5fa; text-decoration: none; }}
    </style>
</head>
<body>
    <div style="max-width:960px;margin:0 auto;">
        <!-- Header -->
        <div style="margin-bottom:32px;">
            <h1 style="font-size:24px;color:#f1f5f9;margin-bottom:4px;">Americal Patrol</h1>
            <p style="font-size:14px;color:#64748b;">Pipeline Status &mdash; Generated {generated}</p>
        </div>

        <!-- Status Banner -->
        <div style="background:{banner_color}20;border:1px solid {banner_color};border-radius:8px;padding:12px 16px;margin-bottom:24px;">
            <span style="color:{banner_color};font-weight:600;">{banner_text}</span>
        </div>

        <!-- Pipeline Cards -->
        <h2 style="font-size:16px;color:#94a3b8;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;">Pipelines</h2>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:16px;margin-bottom:32px;">
            {cards_html}
        </div>

        <!-- Productivity Metrics -->
        <h2 style="font-size:16px;color:#94a3b8;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;">Productivity (Last {metrics['period_days']} Days)</h2>
        <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(130px,1fr));gap:12px;margin-bottom:32px;">
            {metrics_html}
        </div>

        <!-- Schedule Overview -->
        <h2 style="font-size:16px;color:#94a3b8;margin-bottom:12px;text-transform:uppercase;letter-spacing:1px;">Schedule</h2>
        <div style="background:#1e293b;border-radius:12px;border:1px solid #334155;overflow:hidden;margin-bottom:32px;">
            <table style="width:100%;border-collapse:collapse;font-size:14px;">
                <thead>
                    <tr style="background:#334155;">
                        <th style="padding:10px 12px;text-align:left;color:#94a3b8;font-weight:600;">Pipeline</th>
                        <th style="padding:10px 12px;text-align:left;color:#94a3b8;font-weight:600;">Day</th>
                        <th style="padding:10px 12px;text-align:left;color:#94a3b8;font-weight:600;">Time</th>
                        <th style="padding:10px 12px;text-align:left;color:#94a3b8;font-weight:600;">Last Run</th>
                    </tr>
                </thead>
                <tbody>
                    {schedule_rows}
                </tbody>
            </table>
        </div>

        <!-- Footer -->
        <div style="text-align:center;font-size:12px;color:#475569;padding-top:16px;border-top:1px solid #1e293b;">
            Americal Patrol, Inc. &mdash; Veteran-Owned Security Since 1986
        </div>
    </div>
</body>
</html>"""


def main(open_browser=True):
    print("Collecting pipeline data...")
    pipelines = get_all_pipelines_status()
    metrics = get_productivity_metrics(days=30)

    output_path = SCRIPT_DIR / "status.html"
    html = generate_html(pipelines, metrics)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Status page written to: {output_path}")
    if open_browser:
        webbrowser.open(str(output_path))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args()
    main(open_browser=not args.no_browser)
