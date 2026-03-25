"""
Email Service for Analytics Delivery.

Handles sending daily summaries and weekly reports via email.
Supports multiple providers (SendGrid, AWS SES, SMTP).
"""

from typing import Optional, Dict, Any
from datetime import date, datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import structlog

logger = structlog.get_logger(__name__)


class EmailService:
    """Service for sending analytics emails."""
    
    def __init__(
        self,
        provider: str = "smtp",
        smtp_host: Optional[str] = None,
        smtp_port: int = 587,
        smtp_user: Optional[str] = None,
        smtp_password: Optional[str] = None,
        sendgrid_api_key: Optional[str] = None
    ):
        """
        Initialize email service.
        
        Args:
            provider: "smtp", "sendgrid", or "ses"
            smtp_*: SMTP configuration (if provider="smtp")
            sendgrid_api_key: SendGrid API key (if provider="sendgrid")
        """
        self.provider = provider
        self.smtp_host = smtp_host or "localhost"
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.sendgrid_api_key = sendgrid_api_key
        
        logger.info("Email service initialized", provider=provider)
    
    async def send_daily_summary_email(
        self,
        to_email: str,
        summary_date: date,
        summary_html: str,
        metrics: Dict[str, Any]
    ) -> str:
        """
        Send daily summary email.
        
        Args:
            to_email: Recipient email address
            summary_date: Date of summary
            summary_html: HTML content of summary
            metrics: Metrics data for preview
            
        Returns:
            Message ID or delivery status
        """
        subject = f"📊 Daily Summary - {summary_date.strftime('%B %d, %Y')}"
        
        # Create email body
        html_body = self._create_daily_email_template(
            summary_date=summary_date,
            summary_html=summary_html,
            metrics=metrics
        )
        
        # Send based on provider
        if self.provider == "sendgrid":
            return await self._send_via_sendgrid(to_email, subject, html_body)
        else:
            return await self._send_via_smtp(to_email, subject, html_body)
    
    async def send_weekly_report_email(
        self,
        to_email: str,
        week_start: date,
        report_html: str,
        summary_metrics: Dict[str, Any]
    ) -> str:
        """Send weekly report email."""
        week_end = week_start + timedelta(days=6)
        subject = f"📈 Weekly Report - {week_start.strftime('%b %d')} to {week_end.strftime('%b %d, %Y')}"
        
        # Create email body
        html_body = self._create_weekly_email_template(
            week_start=week_start,
            week_end=week_end,
            report_html=report_html,
            summary_metrics=summary_metrics
        )
        
        # Send
        if self.provider == "sendgrid":
            return await self._send_via_sendgrid(to_email, subject, html_body)
        else:
            return await self._send_via_smtp(to_email, subject, html_body)
    
    async def _send_via_smtp(
        self,
        to_email: str,
        subject: str,
        html_body: str
    ) -> str:
        """Send email via SMTP."""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = self.smtp_user or "noreply@minime.ai"
            msg['To'] = to_email
            
            # Attach HTML
            html_part = MIMEText(html_body, 'html')
            msg.attach(html_part)
            
            # Send
            with smtplib.SMTP(self.smtp_host, self.smtp_port) as server:
                if self.smtp_user and self.smtp_password:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
                
                server.send_message(msg)
            
            logger.info(
                "Email sent via SMTP",
                to=to_email,
                subject=subject
            )
            
            return f"smtp-{datetime.utcnow().timestamp()}"
            
        except Exception as e:
            logger.error("SMTP send failed", error=str(e), to=to_email)
            raise
    
    async def _send_via_sendgrid(
        self,
        to_email: str,
        subject: str,
        html_body: str
    ) -> str:
        """Send email via SendGrid."""
        try:
            from sendgrid import SendGridAPIClient
            from sendgrid.helpers.mail import Mail
            
            message = Mail(
                from_email="analytics@minime.ai",
                to_emails=to_email,
                subject=subject,
                html_content=html_body
            )
            
            sg = SendGridAPIClient(self.sendgrid_api_key)
            response = sg.send(message)
            
            message_id = response.headers.get('X-Message-Id', 'unknown')
            
            logger.info(
                "Email sent via SendGrid",
                to=to_email,
                message_id=message_id,
                status_code=response.status_code
            )
            
            return message_id
            
        except ImportError:
            logger.warning("SendGrid not installed, falling back to SMTP")
            return await self._send_via_smtp(to_email, subject, html_body)
        except Exception as e:
            logger.error("SendGrid send failed", error=str(e), to=to_email)
            raise
    
    def _create_daily_email_template(
        self,
        summary_date: date,
        summary_html: str,
        metrics: Dict[str, Any]
    ) -> str:
        """Create HTML email template for daily summary."""
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 600px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px 20px;
            text-align: center;
            border-radius: 10px 10px 0 0;
        }}
        .metrics {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            padding: 20px;
            background: #f8f9fa;
        }}
        .metric {{
            text-align: center;
            padding: 15px;
            background: white;
            border-radius: 8px;
        }}
        .metric-label {{
            font-size: 0.8em;
            color: #666;
            text-transform: uppercase;
        }}
        .metric-value {{
            font-size: 1.8em;
            font-weight: bold;
            color: #2c3e50;
            margin: 5px 0;
        }}
        .content {{
            padding: 20px;
            background: white;
        }}
        .footer {{
            padding: 20px;
            text-align: center;
            font-size: 0.85em;
            color: #666;
            background: #f8f9fa;
            border-radius: 0 0 10px 10px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Daily Summary</h1>
        <p>{summary_date.strftime('%A, %B %d, %Y')}</p>
    </div>
    
    <div class="metrics">
        <div class="metric">
            <div class="metric-label">Focus</div>
            <div class="metric-value">{metrics.get('focus_score', 0):.1f}</div>
        </div>
        <div class="metric">
            <div class="metric-label">Deep Work</div>
            <div class="metric-value">{metrics.get('deep_work_hours', 0):.1f}h</div>
        </div>
        <div class="metric">
            <div class="metric-label">Meetings</div>
            <div class="metric-value">{metrics.get('meeting_load_pct', 0):.0f}%</div>
        </div>
    </div>
    
    <div class="content">
        {summary_html}
    </div>
    
    <div class="footer">
        <p>MiniMe Personal Analytics</p>
        <p style="font-size: 0.8em;">
            <a href="https://minime.ai/unsubscribe">Unsubscribe</a> | 
            <a href="https://minime.ai/preferences">Email Preferences</a>
        </p>
    </div>
</body>
</html>
"""
    
    def _create_weekly_email_template(
        self,
        week_start: date,
        week_end: date,
        report_html: str,
        summary_metrics: Dict[str, Any]
    ) -> str:
        """Create HTML email template for weekly report."""
        from datetime import timedelta
        
        return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{
            font-family: 'Helvetica Neue', Arial, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 700px;
            margin: 0 auto;
            padding: 20px;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px 20px;
            text-align: center;
            border-radius: 10px 10px 0 0;
        }}
        .summary-stats {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            padding: 25px;
            background: #f8f9fa;
        }}
        .stat-box {{
            text-align: center;
            padding: 20px 10px;
            background: white;
            border-radius: 8px;
            border-top: 3px solid #3498db;
        }}
        .stat-label {{
            font-size: 0.75em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .stat-value {{
            font-size: 2em;
            font-weight: bold;
            color: #2c3e50;
            margin: 8px 0;
        }}
        .content {{
            padding: 30px 20px;
            background: white;
        }}
        .footer {{
            padding: 20px;
            text-align: center;
            font-size: 0.85em;
            color: #666;
            background: #f8f9fa;
            border-radius: 0 0 10px 10px;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📈 Weekly Report</h1>
        <p>Week of {week_start.strftime('%B %d')} - {week_end.strftime('%B %d, %Y')}</p>
    </div>
    
    <div class="summary-stats">
        <div class="stat-box">
            <div class="stat-label">Avg Focus</div>
            <div class="stat-value">{summary_metrics.get('avg_focus_score', 0):.1f}</div>
        </div>
        <div class="stat-box">
            <div class="stat-label">Deep Work</div>
            <div class="stat-value">{summary_metrics.get('total_deep_work', 0):.0f}h</div>
        </div>
        <div class="stat-box">
            <div class="stat-label">Meetings</div>
            <div class="stat-value">{summary_metrics.get('avg_meeting_load', 0):.0f}%</div>
        </div>
        <div class="stat-box">
            <div class="stat-label">Days</div>
            <div class="stat-value">{summary_metrics.get('days_tracked', 7)}</div>
        </div>
    </div>
    
    <div class="content">
        {report_html}
    </div>
    
    <div class="footer">
        <p>MiniMe Personal Analytics - Confidential Report</p>
        <p style="font-size: 0.8em;">
            <a href="https://minime.ai/reports">View in Dashboard</a> | 
            <a href="https://minime.ai/preferences">Email Preferences</a>
        </p>
    </div>
</body>
</html>
"""


# Helper to get email service instance
def get_email_service() -> EmailService:
    """Get email service instance with configuration from settings."""
    from core.config import settings
    
    # Try SendGrid first
    if hasattr(settings, 'SENDGRID_API_KEY') and settings.SENDGRID_API_KEY:
        return EmailService(
            provider="sendgrid",
            sendgrid_api_key=settings.SENDGRID_API_KEY
        )
    
    # Fall back to SMTP
    return EmailService(
        provider="smtp",
        smtp_host=getattr(settings, 'SMTP_HOST', 'localhost'),
        smtp_port=getattr(settings, 'SMTP_PORT', 587),
        smtp_user=getattr(settings, 'SMTP_USER', None),
        smtp_password=getattr(settings, 'SMTP_PASSWORD', None)
    )
