"""
Celery Tasks for Analytics (Daily & Weekly).

Scheduled jobs for:
- Computing daily metrics
- Generating daily summaries
- Sending daily emails
- Generating weekly reports
- Sending weekly emails
"""

from celery import shared_task
from datetime import date, datetime, timedelta
from typing import Optional
import structlog

logger = structlog.get_logger(__name__)


@shared_task(
    name="analytics.compute_daily_metrics",
    bind=True,
    max_retries=3,
    default_retry_delay=300  # 5 minutes
)
def compute_daily_metrics_task(self, user_id: str, target_date_str: str):
    """
    Compute daily metrics for a user.
    
    Scheduled at 08:30 local time (UTC conversion needed).
    
    Args:
        user_id: User UUID
        target_date_str: Date in ISO format (YYYY-MM-DD)
    """
    from database.postgres import async_session_factory
    from services.productivity_metrics_service import ProductivityMetricsService
    import asyncio
    
    target_date = date.fromisoformat(target_date_str)
    
    logger.info(
        "Starting daily metrics computation",
        user_id=user_id,
        date=target_date_str,
        task_id=self.request.id
    )
    
    async def run():
        async with async_session_factory() as db:
            service = ProductivityMetricsService(db=db)
            
            # Fetch activities for the day
            # TODO: Query from activities table
            activities = []  # Placeholder
            
            if not activities:
                logger.warning(
                    "No activities found for metrics computation",
                    user_id=user_id,
                    date=target_date_str
                )
                return None
            
            metrics = await service.compute_daily_metrics(
                user_id=user_id,
                target_date=target_date,
                activities=activities
            )
            
            logger.info(
                "Computed daily metrics",
                user_id=user_id,
                date=target_date_str,
                focus_score=float(metrics.focus_score or 0)
            )
            
            return metrics.id
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # If loop is already running, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(run())
        return result
        
    except Exception as exc:
        logger.error(
            "Failed to compute daily metrics",
            user_id=user_id,
            date=target_date_str,
            error=str(exc)
        )
        raise self.retry(exc=exc)


@shared_task(
    name="analytics.generate_daily_summary",
    bind=True,
    max_retries=3,
    default_retry_delay=300
)
def generate_daily_summary_task(self, user_id: str, target_date_str: str):
    """
    Generate daily summary for a user.
    
    Scheduled at 08:45 local time.
    
    Args:
        user_id: User UUID
        target_date_str: Date in ISO format
    """
    from database.postgres import async_session_factory
    from services.daily_summary_service import DailySummaryService
    from services.productivity_metrics_service import ProductivityMetricsService
    import asyncio
    
    target_date = date.fromisoformat(target_date_str)
    
    logger.info(
        "Starting daily summary generation",
        user_id=user_id,
        date=target_date_str,
        task_id=self.request.id
    )
    
    async def run():
        async with async_session_factory() as db:
            metrics_service = ProductivityMetricsService(db=db)
            summary_service = DailySummaryService(
                db=db,
                metrics_service=metrics_service
            )
            
            summary = await summary_service.generate_daily_summary(
                user_id=user_id,
                target_date=target_date
            )
            
            logger.info(
                "Generated daily summary",
                user_id=user_id,
                date=target_date_str,
                duration_ms=summary.generation_duration_ms
            )
            
            return summary.id
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(run())
        return result
        
    except Exception as exc:
        logger.error(
            "Failed to generate daily summary",
            user_id=user_id,
            date=target_date_str,
            error=str(exc)
        )
        raise self.retry(exc=exc)


@shared_task(
    name="analytics.send_daily_summary_email",
    bind=True,
    max_retries=3,
    default_retry_delay=600  # 10 minutes
)
def send_daily_summary_email_task(self, user_id: str, target_date_str: str):
    """
    Send daily summary email to user.
    
    Scheduled at 09:00 local time.
    
    Args:
        user_id: User UUID
        target_date_str: Date in ISO format
    """
    from database.postgres import async_session_factory
    from services.daily_summary_service import DailySummaryService
    from services.productivity_metrics_service import ProductivityMetricsService
    from models.analytics_models import AnalyticsEmail
    from jinja2 import Template
    import asyncio
    
    target_date = date.fromisoformat(target_date_str)
    
    logger.info(
        "Starting daily summary email",
        user_id=user_id,
        date=target_date_str,
        task_id=self.request.id
    )
    
    async def run():
        async with async_session_factory() as db:
            metrics_service = ProductivityMetricsService(db=db)
            summary_service = DailySummaryService(
                db=db,
                metrics_service=metrics_service
            )
            
            # Get summary
            summary = await summary_service.get_daily_summary(user_id, target_date)
            
            if not summary:
                logger.warning(
                    "No summary found to email",
                    user_id=user_id,
                    date=target_date_str
                )
                return None
            
            # Get metrics for email
            metrics = await metrics_service.get_daily_metrics(user_id, target_date)
            
            # Render email template
            with open('/home/ansari/Documents/MiniMe/backend/templates/daily_summary_email.html') as f:
                template = Template(f.read())
            
            # Determine focus color based on score
            focus_score_val = float(summary.focus_score or 0)
            if focus_score_val >= 8:
                focus_color = "#10b981"  # green
            elif focus_score_val >= 6:
                focus_color = "#f59e0b"  # yellow
            else:
                focus_color = "#ef4444"  # red
            
            email_html = template.render(
                date_formatted=target_date.strftime("%A, %B %d, %Y"),
                focus_score=f"{focus_score_val:.1f}",
                focus_color=focus_color,
                deep_work_hours=f"{float(summary.deep_work_hours or 0):.1f}",
                meeting_load=f"{float(metrics.meeting_load_pct or 0):.0f}" if metrics else "0",
                summary_html=summary.summary_html,
                dashboard_url="http://localhost:3000/dashboard",
                settings_url="http://localhost:3000/settings",
                unsubscribe_url="http://localhost:3000/unsubscribe"
            )
            
            # TODO: Send actual email via SendGrid/SES
            # For now, just log
            logger.info(
                "Email would be sent here",
                user_id=user_id,
                date=target_date_str,
                email_length=len(email_html)
            )
            
            # Track email delivery
            email_record = AnalyticsEmail(
                user_id=user_id,
                type='daily',
                reference_date=target_date,
                sent_at=datetime.utcnow(),
                status='sent',  # Would be 'pending' initially
                provider_message_id=f"mock-{datetime.utcnow().timestamp()}"
            )
            db.add(email_record)
            await db.commit()
            
            return email_record.id
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(run())
        
        logger.info(
            "Sent daily summary email",
            user_id=user_id,
            date=target_date_str,
            email_id=result
        )
        
        return result
        
    except Exception as exc:
        logger.error(
            "Failed to send daily summary email",
            user_id=user_id,
            date=target_date_str,
            error=str(exc)
        )
        raise self.retry(exc=exc)


# =============================================================================
# SCHEDULER FUNCTIONS (Called by Celery Beat)
# =============================================================================

@shared_task(name="analytics.schedule_daily_metrics")
def schedule_daily_metrics():
    """
    Schedule daily metrics computation for all users.
    
    Runs at 08:30 UTC daily.
    """
    from database.postgres import async_session_factory
    from sqlalchemy import select
    from models import User  # Assuming User model exists
    import asyncio
    
    logger.info("Scheduling daily metrics computation for all users")
    
    async def run():
        async with async_session_factory() as db:
            # Get all active users
            result = await db.execute(select(User).where(User.is_active == True))
            users = result.scalars().all()
            
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            
            for user in users:
                compute_daily_metrics_task.delay(str(user.id), yesterday)
            
            logger.info(f"Scheduled metrics for {len(users)} users")
            return len(users)
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        count = loop.run_until_complete(run())
        return count
    except Exception as e:
        logger.error(f"Failed to schedule daily metrics: {e}")
        return 0


@shared_task(name="analytics.schedule_daily_summaries")
def schedule_daily_summaries():
    """
    Schedule daily summary generation for all users.
    
    Runs at 08:45 UTC daily.
    """
    from database.postgres import async_session_factory
    from sqlalchemy import select
    from models import User
    import asyncio
    
    logger.info("Scheduling daily summary generation for all users")
    
    async def run():
        async with async_session_factory() as db:
            result = await db.execute(select(User).where(User.is_active == True))
            users = result.scalars().all()
            
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            
            for user in users:
                generate_daily_summary_task.delay(str(user.id), yesterday)
            
            logger.info(f"Scheduled summaries for {len(users)} users")
            return len(users)
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        count = loop.run_until_complete(run())
        return count
    except Exception as e:
        logger.error(f"Failed to schedule daily summaries: {e}")
        return 0


@shared_task(name="analytics.schedule_daily_emails")
def schedule_daily_emails():
    """
    Schedule daily summary emails for all users.
    
    Runs at 09:00 UTC daily.
    """
    from database.postgres import async_session_factory
    from sqlalchemy import select
    from models import User
    import asyncio
    
    logger.info("Scheduling daily emails for all users")
    
    async def run():
        async with async_session_factory() as db:
            result = await db.execute(select(User).where(User.is_active == True))
            users = result.scalars().all()
            
            yesterday = (date.today() - timedelta(days=1)).isoformat()
            
            for user in users:
                send_daily_summary_email_task.delay(str(user.id), yesterday)
            
            logger.info(f"Scheduled emails for {len(users)} users")
            return len(users)
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        count = loop.run_until_complete(run())
        return count
    except Exception as e:
        logger.error(f"Failed to schedule daily emails: {e}")
        return 0


# =============================================================================
# WEEKLY REPORT TASKS
# =============================================================================

@shared_task(
    name="analytics.generate_weekly_report",
    bind=True,
    max_retries=3,
    default_retry_delay=600  # 10 minutes
)
def generate_weekly_report_task(self, user_id: str, week_start_str: str):
    """
    Generate weekly report for a user.
    
    Scheduled every Monday at 09:00 local time.
    
    Args:
        user_id: User UUID
        week_start_str: Monday date in ISO format (YYYY-MM-DD)
    """
    from database.postgres import async_session_factory
    from services.weekly_report_service import WeeklyReportService
    from services.productivity_metrics_service import ProductivityMetricsService
    from services.collaboration_analytics_service import CollaborationAnalyticsService
    from services.skill_analytics_service import SkillAnalyticsService
    import asyncio
    
    week_start = date.fromisoformat(week_start_str)
    
    logger.info(
        "Starting weekly report generation",
        user_id=user_id,
        week_start=week_start_str,
        task_id=self.request.id
    )
    
    async def run():
        async with async_session_factory() as db:
            metrics_service = ProductivityMetricsService(db=db)
            collaboration_service = CollaborationAnalyticsService(db=db)
            skill_service = SkillAnalyticsService(db=db)
            
            report_service = WeeklyReportService(
                db=db,
                metrics_service=metrics_service,
                collaboration_service=collaboration_service,
                skill_service=skill_service
            )
            
            report = await report_service.generate_weekly_report(
                user_id=user_id,
                week_start_date=week_start
            )
            
            logger.info(
                "Generated weekly report",
                user_id=user_id,
                week_start=week_start_str,
                duration_ms=report.generation_duration_ms
            )
            
            return report.id
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(run())
        return result
        
    except Exception as exc:
        logger.error(
            "Failed to generate weekly report",
            user_id=user_id,
            week_start=week_start_str,
            error=str(exc)
        )
        raise self.retry(exc=exc)


@shared_task(
    name="analytics.send_weekly_report_email",
    bind=True,
    max_retries=3,
    default_retry_delay=600
)
def send_weekly_report_email_task(self, user_id: str, week_start_str: str):
    """
    Send weekly report email to user.
    
    Scheduled every Monday at 09:30 local time.
    
    Args:
        user_id: User UUID
        week_start_str: Monday date in ISO format
    """
    from database.postgres import async_session_factory
    from services.weekly_report_service import WeeklyReportService
    from services.productivity_metrics_service import ProductivityMetricsService
    from models.analytics_models import AnalyticsEmail
    from jinja2 import Template
    import asyncio
    
    week_start = date.fromisoformat(week_start_str)
    week_end = week_start + timedelta(days=6)
    
    logger.info(
        "Starting weekly report email",
        user_id=user_id,
        week_start=week_start_str,
        task_id=self.request.id
    )
    
    async def run():
        async with async_session_factory() as db:
            metrics_service = ProductivityMetricsService(db=db)
            report_service = WeeklyReportService(
                db=db,
                metrics_service=metrics_service
            )
            
            # Get report
            report = await report_service.get_weekly_report(user_id, week_start)
            
            if not report:
                logger.warning(
                    "No weekly report found to email",
                    user_id=user_id,
                    week_start=week_start_str
                )
                return None
            
            # Render email template
            with open('/home/ansari/Documents/MiniMe/backend/templates/weekly_report_email.html') as f:
                template = Template(f.read())
            
            # Determine focus color
            avg_focus = report.productivity_metrics.get('avg_focus_score', 0)
            if avg_focus >= 8:
                focus_color = "#10b981"  # green
            elif avg_focus >= 6:
                focus_color = "#f59e0b"  # yellow
            else:
                focus_color = "#ef4444"  # red
            
            email_html = template.render(
                week_formatted=f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}",
                overview_html=report.overview.get('summary', ''),
                avg_focus_score=f"{avg_focus:.1f}",
                focus_color=focus_color,
                total_deep_work=f"{report.time_analytics.get('total_deep_work', 0):.1f}",
                days_tracked=report.overview.get('days_tracked', 0),
                projects_html="<p>Projects section</p>",  # TODO: Format from report
                trends_html="<p>Trends section</p>",  # TODO: Format from report
                recommendations_html=report.recommendations_section.get('recommendations', ''),
                dashboard_url="http://localhost:3000/dashboard",
                settings_url="http://localhost:3000/settings",
                unsubscribe_url="http://localhost:3000/unsubscribe"
            )
            
            # TODO: Send actual email via SendGrid/SES
            logger.info(
                "Email would be sent here",
                user_id=user_id,
                week_start=week_start_str,
                email_length=len(email_html)
            )
            
            # Track email delivery
            email_record = AnalyticsEmail(
                user_id=user_id,
                type='weekly',
                reference_date=week_start,
                sent_at=datetime.utcnow(),
                status='sent',
                provider_message_id=f"mock-weekly-{datetime.utcnow().timestamp()}"
            )
            db.add(email_record)
            await db.commit()
            
            return email_record.id
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        result = loop.run_until_complete(run())
        
        logger.info(
            "Sent weekly report email",
            user_id=user_id,
            week_start=week_start_str,
            email_id=result
        )
        
        return result
        
    except Exception as exc:
        logger.error(
            "Failed to send weekly report email",
            user_id=user_id,
            week_start=week_start_str,
            error=str(exc)
        )
        raise self.retry(exc=exc)


# =============================================================================
# WEEKLY SCHEDULER FUNCTIONS (Called by Celery Beat)
# =============================================================================

@shared_task(name="analytics.schedule_weekly_reports")
def schedule_weekly_reports():
    """
    Schedule weekly report generation for all users.
    
    Runs every Monday at 09:00 UTC.
    """
    from database.postgres import async_session_factory
    from sqlalchemy import select
    from models import User
    import asyncio
    
    logger.info("Scheduling weekly reports for all users")
    
    async def run():
        async with async_session_factory() as db:
            result = await db.execute(select(User).where(User.is_active == True))
            users = result.scalars().all()
            
            # Get last Monday
            today = date.today()
            days_since_monday = (today.weekday()) % 7
            last_monday = today - timedelta(days=days_since_monday + 7)  # Previous week's Monday
            
            for user in users:
                generate_weekly_report_task.delay(str(user.id), last_monday.isoformat())
            
            logger.info(f"Scheduled weekly reports for {len(users)} users")
            return len(users)
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        count = loop.run_until_complete(run())
        return count
    except Exception as e:
        logger.error(f"Failed to schedule weekly reports: {e}")
        return 0


@shared_task(name="analytics.schedule_weekly_emails")
def schedule_weekly_emails():
    """
    Schedule weekly report emails for all users.
    
    Runs every Monday at 09:30 UTC.
    """
    from database.postgres import async_session_factory
    from sqlalchemy import select
    from models import User
    import asyncio
    
    logger.info("Scheduling weekly emails for all users")
    
    async def run():
        async with async_session_factory() as db:
            result = await db.execute(select(User).where(User.is_active == True))
            users = result.scalars().all()
            
            # Get last Monday
            today = date.today()
            days_since_monday = (today.weekday()) % 7
            last_monday = today - timedelta(days=days_since_monday + 7)
            
            for user in users:
                send_weekly_report_email_task.delay(str(user.id), last_monday.isoformat())
            
            logger.info(f"Scheduled weekly emails for {len(users)} users")
            return len(users)
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        
        count = loop.run_until_complete(run())
        return count
    except Exception as e:
        logger.error(f"Failed to schedule weekly emails: {e}")
        return 0
