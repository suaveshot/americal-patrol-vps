"""
Americal Patrol - GBP Weekly Digest Report Composer
Sends a combined HTML email covering:
  1. GBP completeness checklist + score
  2. NAP consistency audit (directory-by-directory)
  3. This week's GBP post preview
"""

import base64
import json
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from googleapiclient.discovery import build

from auth_setup import get_credentials

SCRIPT_DIR  = Path(__file__).parent
CONFIG_FILE = SCRIPT_DIR / 'gbp_config.json'


def _load_config() -> dict:
    with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_gmail_service():
    return build('gmail', 'v1', credentials=get_credentials())


# ── HTML section builders ─────────────────────────────────────────────────────

def _completeness_html(completeness: dict) -> str:
    score  = completeness.get('score', 0)
    issues = completeness.get('issues', [])
    fields = completeness.get('fields', {})

    score_color = '#2e7d32' if score >= 85 else ('#e65100' if score >= 60 else '#c62828')

    STATUS_LABELS = {
        'ok':      ('OK',      '#2e7d32'),
        'missing': ('MISSING', '#c62828'),
        'thin':    ('THIN',    '#e65100'),
    }

    rows = ''
    for field_name, f_data in fields.items():
        status        = f_data.get('status', 'ok')
        label, color  = STATUS_LABELS.get(status, ('?', '#888'))
        value         = str(f_data.get('value', '') or '')[:60]
        display       = field_name.replace('_', ' ').title()
        bg            = '#fff' if rows.count('<tr') % 2 == 0 else '#f9f9f9'
        rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;">{display}</td>'
            f'<td style="padding:7px 10px;text-align:center;color:{color};font-weight:bold;">{label}</td>'
            f'<td style="padding:7px 10px;font-size:12px;color:#555;">{value}</td>'
            f'</tr>'
        )

    if issues:
        items       = ''.join(f'<li style="margin-bottom:5px;">{i}</li>' for i in issues)
        action_html = f'<ul style="margin:8px 0;padding-left:20px;color:#c62828;">{items}</ul>'
    else:
        action_html = '<p style="color:#2e7d32;margin:8px 0;">No issues — listing is fully complete.</p>'

    return f"""
<div style="background:#f5f7fa;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 12px;font-size:16px;">GBP Completeness Check</h2>
  <div style="display:inline-block;background:{score_color};color:white;font-size:20px;
    font-weight:bold;padding:8px 16px;border-radius:4px;margin-bottom:12px;">
    Score: {score}/100
  </div>
  <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:10px;">
    <thead>
      <tr style="background:#1a3a5c;color:white;">
        <th style="padding:8px 10px;text-align:left;">Field</th>
        <th style="padding:8px 10px;text-align:center;">Status</th>
        <th style="padding:8px 10px;text-align:left;">Value</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  <strong style="font-size:13px;">Action Items:</strong>
  {action_html}
</div>"""


def _nap_html(nap_audit: dict) -> str:
    master    = nap_audit.get('master_nap', {})
    results   = nap_audit.get('results', {})
    issues    = nap_audit.get('issues', [])
    unchecked = nap_audit.get('unchecked', [])

    STATUS_STYLE = {
        'ok':        ('OK',       '#2e7d32', '#e8f5e9'),
        'mismatch':  ('MISMATCH', '#c62828', '#ffebee'),
        'unchecked': ('NOT SET',  '#888888', '#f5f5f5'),
        'error':     ('ERROR',    '#e65100', '#fff3e0'),
    }

    rows = ''
    for platform, r_data in results.items():
        status              = r_data.get('status', 'error')
        detail              = r_data.get('detail', '')
        label, color, bg    = STATUS_STYLE.get(status, ('?', '#888', '#fff'))
        rows += (
            f'<tr style="background:{bg};">'
            f'<td style="padding:7px 10px;font-size:12px;font-weight:bold;">{platform}</td>'
            f'<td style="padding:7px 10px;text-align:center;color:{color};font-weight:bold;">{label}</td>'
            f'<td style="padding:7px 10px;font-size:12px;color:#555;">{detail}</td>'
            f'</tr>'
        )

    if issues:
        items       = ''.join(f'<li style="margin-bottom:5px;">{i}</li>' for i in issues)
        summary_html = (
            f'<p style="color:#c62828;font-weight:bold;margin:8px 0 4px;">Issues Found:</p>'
            f'<ul style="margin:0;padding-left:20px;color:#c62828;">{items}</ul>'
        )
    else:
        ok_count     = sum(1 for r in results.values() if r['status'] == 'ok')
        checked      = sum(1 for r in results.values() if r['status'] != 'unchecked')
        summary_html = f'<p style="color:#2e7d32;margin:8px 0;">{ok_count}/{checked} checked listings match your master NAP.</p>'

    if unchecked:
        uc_list      = ', '.join(unchecked)
        summary_html += (
            f'<p style="color:#888;font-size:12px;margin-top:8px;">'
            f'Not yet configured: {uc_list}. '
            f'Add listing URLs to gbp_config.json &gt; directory_listings to enable monitoring.</p>'
        )

    return f"""
<div style="background:#f5f7fa;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 4px;font-size:16px;">NAP Consistency Audit</h2>
  <p style="font-size:12px;color:#555;margin:0 0 12px;">
    Master NAP: <strong>{master.get('business_name','')}</strong>
    &nbsp;|&nbsp; {master.get('phone','')}
    &nbsp;|&nbsp; {master.get('website','')}
  </p>
  <table style="width:100%;border-collapse:collapse;font-size:13px;margin-bottom:10px;">
    <thead>
      <tr style="background:#1a3a5c;color:white;">
        <th style="padding:8px 10px;text-align:left;">Directory</th>
        <th style="padding:8px 10px;text-align:center;">Status</th>
        <th style="padding:8px 10px;text-align:left;">Detail</th>
      </tr>
    </thead>
    <tbody>{rows}</tbody>
  </table>
  {summary_html}
</div>"""


def _post_preview_html(post_summary: str, topic: dict) -> str:
    topic_label = topic.get('subject', 'weekly update')
    char_count  = len(post_summary)
    return f"""
<div style="background:#f5f7fa;border-radius:6px;padding:16px;margin-bottom:20px;">
  <h2 style="color:#1a3a5c;margin:0 0 8px;font-size:16px;">This Week's GBP Post</h2>
  <p style="font-size:12px;color:#555;margin:0 0 10px;">
    Topic: <strong>{topic_label}</strong> &nbsp;|&nbsp; {char_count} / 1500 characters
  </p>
  <div style="background:white;border:1px solid #ddd;border-radius:4px;padding:14px;
    font-size:13px;line-height:1.6;color:#333;white-space:pre-wrap;">{post_summary}</div>
</div>"""


# ── Main compose & send ───────────────────────────────────────────────────────

def compose_and_send(completeness: dict, nap_audit: dict, post_summary: str,
                     topic: dict, errors: list, log=None) -> bool:
    config     = _load_config()
    recipients = config.get('recipients', [])

    if not recipients:
        if log: log('ERROR: No recipients configured in gbp_config.json')
        return False

    today_str = datetime.now().strftime('%B %d, %Y')
    subject   = f'Americal Patrol GBP Report — {today_str}'

    errors_html = ''
    if errors:
        items       = ''.join(f'<li>{e}</li>' for e in errors)
        errors_html = f"""
<div style="background:#ffebee;border-left:4px solid #c62828;padding:12px;margin-bottom:20px;border-radius:4px;">
  <strong style="color:#c62828;">Pipeline Warnings This Run:</strong>
  <ul style="margin:6px 0 0;padding-left:20px;color:#c62828;">{items}</ul>
</div>"""

    post_section = _post_preview_html(post_summary, topic) if post_summary else (
        '<p style="color:#888;font-style:italic;">No post generated this run.</p>'
    )

    full_html = f"""<html><body style="font-family:Arial,sans-serif;max-width:760px;margin:0 auto;color:#333;">
<div style="background:#1a3a5c;padding:20px;border-radius:8px 8px 0 0;">
  <h1 style="color:white;margin:0;font-size:22px;">Americal Patrol — GBP Weekly Report</h1>
  <p style="color:#cde;margin:5px 0 0;font-size:14px;">{today_str} &nbsp;|&nbsp; americalpatrol.com</p>
</div>
<div style="padding:24px;background:#fff;border:1px solid #ddd;border-top:none;border-radius:0 0 8px 8px;">
{errors_html}
{_completeness_html(completeness)}
<hr style="border:none;border-top:1px solid #e0e0e0;margin:24px 0;">
{_nap_html(nap_audit)}
<hr style="border:none;border-top:1px solid #e0e0e0;margin:24px 0;">
{post_section}
</div>
<p style="font-size:11px;color:#999;text-align:center;margin-top:16px;">
  Generated automatically by Americal Patrol GBP Automation
</p>
</body></html>"""

    msg            = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['To']      = ', '.join(recipients)
    msg['From']    = 'me'
    msg.attach(MIMEText(f'Americal Patrol GBP Weekly Report — {today_str}\n\nOpen in Gmail to view.', 'plain'))
    msg.attach(MIMEText(full_html, 'html'))

    raw   = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    gmail = _get_gmail_service()
    gmail.users().messages().send(userId='me', body={'raw': raw}).execute()

    if log:
        log(f'GBP digest email sent to: {", ".join(recipients)}')
        log(f'Subject: {subject}')

    return True
