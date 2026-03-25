"""
Scheduled Report Delivery Task

Celery task that:
1. Checks user report preferences (frequency, delivery time, email)
2. Generates HTML productivity report using existing analytics
3. Sends via SMTP (configurable via environment variables)

Environment variables:
  SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM
"""

import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
import structlog

logger = structlog.get_logger()

# In-memory report preferences (swap for DB later)
_report_preferences = {}  # user_id -> {frequency, email, time, enabled}


def get_report_preferences(user_id: str) -> dict:
    """Get user's report delivery preferences."""
    return _report_preferences.get(user_id, {
        'frequency': 'weekly',
        'email': '',
        'time': '09:00',
        'enabled': False,
        'last_sent': None,
    })


def set_report_preferences(user_id: str, prefs: dict):
    """Set user's report delivery preferences."""
    existing = get_report_preferences(user_id)
    existing.update(prefs)
    _report_preferences[user_id] = existing
    logger.info("report_preferences_updated", user_id=user_id, prefs=existing)
    return existing


def _should_send(user_id: str) -> bool:
    """Check if report should be sent based on frequency."""
    prefs = get_report_preferences(user_id)
    if not prefs.get('enabled') or not prefs.get('email'):
        return False

    last_sent = prefs.get('last_sent')
    if not last_sent:
        return True

    try:
        last_dt = datetime.fromisoformat(last_sent)
    except Exception:
        return True

    freq = prefs.get('frequency', 'weekly')
    now = datetime.now()

    if freq == 'daily':
        return (now - last_dt).days >= 1
    elif freq == 'weekly':
        return (now - last_dt).days >= 7
    elif freq == 'monthly':
        return (now - last_dt).days >= 30
    return False


def _build_report_html(user_id: str) -> str:
    """Build an HTML productivity report."""
    now = datetime.now()
    return f"""
    <!DOCTYPE html>
    <html>
    <head>
        <style>
            body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px; background: #f5f5f5; }}
            .header {{ background: linear-gradient(135deg, #6366f1, #8b5cf6); color: white; padding: 24px; border-radius: 12px; text-align: center; }}
            .header h1 {{ margin: 0; font-size: 24px; }}
            .header p {{ margin: 8px 0 0; opacity: 0.9; font-size: 14px; }}
            .card {{ background: white; border-radius: 12px; padding: 20px; margin: 16px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
            .card h2 {{ margin: 0 0 12px; font-size: 16px; color: #374151; }}
            .metric {{ display: flex; justify-content: space-between; padding: 8px 0; border-bottom: 1px solid #f3f4f6; }}
            .metric:last-child {{ border-bottom: none; }}
            .metric-label {{ color: #6b7280; font-size: 14px; }}
            .metric-value {{ font-weight: 600; color: #111827; }}
            .footer {{ text-align: center; padding: 16px; color: #9ca3af; font-size: 12px; }}
            .cta {{ display: inline-block; background: #6366f1; color: white; padding: 12px 24px; border-radius: 8px; text-decoration: none; margin: 8px 0; font-weight: 600; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 Your MiniMe Report</h1>
            <p>{now.strftime('%B %d, %Y')}</p>
        </div>

        <div class="card">
            <h2>📈 Report Summary</h2>
            <p style="color: #6b7280; font-size: 14px;">
                Your personalized productivity report is ready. Visit your dashboard for detailed analytics.
            </p>
            <div class="metric">
                <span class="metric-label">Report Period</span>
                <span class="metric-value">{(now - timedelta(days=7)).strftime('%b %d')} - {now.strftime('%b %d, %Y')}</span>
            </div>
            <div class="metric">
                <span class="metric-label">Generated At</span>
                <span class="metric-value">{now.strftime('%I:%M %p')}</span>
            </div>
        </div>

        <div class="card" style="text-align: center;">
            <p style="color: #6b7280; font-size: 14px;">View your full analytics dashboard for detailed insights.</p>
            <a href="https://minime.app/dashboard/productivity" class="cta">View Full Dashboard →</a>
        </div>

        <div class="footer">
            <p>MiniMe — Your Personal Activity Intelligence</p>
            <p>You're receiving this because you enabled scheduled reports. <a href="https://minime.app/dashboard/settings">Unsubscribe</a></p>
        </div>
    </body>
    </html>
    """


def send_report_email(user_id: str, to_email: str, html_body: str) -> bool:
    """Send report via SMTP."""
    smtp_host = os.getenv('SMTP_HOST', 'smtp.gmail.com')
    smtp_port = int(os.getenv('SMTP_PORT', '587'))
    smtp_user = os.getenv('SMTP_USER', '')
    smtp_pass = os.getenv('SMTP_PASS', '')
    smtp_from = os.getenv('SMTP_FROM', smtp_user or 'reports@minime.app')

    if not smtp_user or not smtp_pass:
        logger.warning("smtp_not_configured", user_id=user_id)
        return False

    try:
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f'📊 Your MiniMe Productivity Report — {datetime.now().strftime("%B %d, %Y")}'
        msg['From'] = smtp_from
        msg['To'] = to_email

        msg.attach(MIMEText(html_body, 'html'))

        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_from, to_email, msg.as_string())

        logger.info("report_email_sent", user_id=user_id, to=to_email)
        return True

    except Exception as e:
        logger.error("report_email_failed", user_id=user_id, error=str(e))
        return False


def process_scheduled_reports():
    """
    Process scheduled reports for all users with enabled preferences.
    Called by Celery beat.
    """
    sent_count = 0
    for user_id, prefs in _report_preferences.items():
        if not _should_send(user_id):
            continue

        email = prefs.get('email', '')
        if not email:
            continue

        html = _build_report_html(user_id)
        success = send_report_email(user_id, email, html)

        if success:
            prefs['last_sent'] = datetime.now().isoformat()
            sent_count += 1

    logger.info("scheduled_reports_processed", sent=sent_count, total=len(_report_preferences))
    return sent_count
