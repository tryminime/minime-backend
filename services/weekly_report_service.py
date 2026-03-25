"""
Weekly Report Service for Month 6 Personal Analytics.

Generates comprehensive 9-section weekly reports:
1. Overview (LLM summary)
2. Time Analytics (hours breakdown)
3. Productivity Metrics (aggregated)
4. Projects (top by time)
5. Papers (research progress)
6. Collaboration (network analysis)
7. Skills (topics worked on)
8. Trends (week-over-week)
9. Recommendations (LLM actionable advice)

Uses Anthropic Claude for overview/recommendations, caches in Redis (7 days).
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
import structlog
import json
import asyncio
from anthropic import Anthropic, APIError, APITimeoutError
from sqlalchemy import select, and_, func
from sqlalchemy.ext.asyncio import AsyncSession

from models.analytics_models import WeeklyReport, DailyMetrics, DailySummary
from services.productivity_metrics_service import ProductivityMetricsService
from services.collaboration_analytics_service import CollaborationAnalyticsService
from services.skill_analytics_service import SkillAnalyticsService
from services.validation import validate_uuid, validate_week_start
from database.redis_client import get_redis_client
from config.settings import settings

logger = structlog.get_logger(__name__)


class WeeklyReportService:
    """Service for generating and managing weekly reports."""
    
    # Configuration
    LLM_MODEL = "claude-3-5-sonnet-20241022"
    LLM_MAX_TOKENS = 1000  # Longer for weekly summary
    LLM_TIMEOUT_SECONDS = 45
    LLM_MAX_RETRIES = 3
    
    CACHE_TTL_SECONDS = 604800  # 7 days
    
    def __init__(
        self,
        db: AsyncSession,
        metrics_service: ProductivityMetricsService,
        collaboration_service: Optional[CollaborationAnalyticsService] = None,
        skill_service: Optional[SkillAnalyticsService] = None,
        anthropic_client: Optional[Anthropic] = None,
        redis_client=None
    ):
        """
        Initialize the weekly report service.
        
        Args:
            db: SQLAlchemy async session
            metrics_service: ProductivityMetricsService instance
            collaboration_service: CollaborationAnalyticsService (optional)
            skill_service: SkillAnalyticsService (optional)
            anthropic_client: Anthropic client (optional)
            redis_client: Redis client for caching (optional)
        """
        self.db = db
        self.metrics_service = metrics_service
        self.collaboration_service = collaboration_service
        self.skill_service = skill_service
        self.redis = redis_client or get_redis_client()
        
        # Initialize Anthropic client
        if anthropic_client:
            self.anthropic = anthropic_client
        else:
            api_key = getattr(settings, 'ANTHROPIC_API_KEY', None)
            self.anthropic = Anthropic(api_key=api_key) if api_key else None
    
    async def generate_weekly_report(
        self,
        user_id: str,
        week_start_date: date,
        force_regenerate: bool = False
    ) -> WeeklyReport:
        """
        Generate a comprehensive weekly report.
        
        Week defined as Monday-Sunday.
        
        Args:
            user_id: User UUID
            week_start_date: Monday of the week
            force_regenerate: If True, regenerate even if exists
        
        Returns:
            WeeklyReport model instance
        """
        # Validate inputs
        validate_uuid(user_id, "user_id")
        week_start_date = validate_week_start(week_start_date)
        week_end_date = week_start_date + timedelta(days=6)
        
        logger.info(
            "Generating weekly report",
            user_id=user_id,
            week_start=week_start_date.isoformat(),
            week_end=week_end_date.isoformat(),
            force=force_regenerate
        )
        
        start_time = datetime.utcnow()
        
        # Check if report already exists
        if not force_regenerate:
            existing = await self.get_weekly_report(user_id, week_start_date)
            if existing:
                logger.info("Weekly report already exists", user_id=user_id)
                return existing
        
        # Generate all 9 sections
        sections = await self._generate_all_sections(user_id, week_start_date, week_end_date)
        
        # Generate markdown and HTML
        report_markdown = self._build_report_markdown(sections, week_start_date, week_end_date)
        report_html = self._build_report_html(sections, week_start_date, week_end_date)
        
        # Calculate generation time
        generation_duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # Store in database
        stmt = select(WeeklyReport).where(
            and_(
                WeeklyReport.user_id == user_id,
                WeeklyReport.week_start_date == week_start_date
            )
        )
        result = await self.db.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            # Update existing
            existing.week_end_date = week_end_date
            existing.overview = sections['overview']
            existing.time_analytics = sections['time_analytics']
            existing.productivity_metrics = sections['productivity_metrics']
            existing.projects_section = sections['projects_section']
            existing.papers_section = sections['papers_section']
            existing.collaboration_section = sections['collaboration_section']
            existing.skills_section = sections['skills_section']
            existing.trends_section = sections['trends_section']
            existing.recommendations_section = sections['recommendations_section']
            existing.report_markdown = report_markdown
            existing.report_html = report_html
            existing.generated_at = datetime.utcnow()
            existing.llm_model = self.LLM_MODEL
            existing.generation_duration_ms = generation_duration_ms
            existing.updated_at = datetime.utcnow()
            report_obj = existing
        else:
            # Create new
            report_obj = WeeklyReport(
                user_id=user_id,
                week_start_date=week_start_date,
                week_end_date=week_end_date,
                overview=sections['overview'],
                time_analytics=sections['time_analytics'],
                productivity_metrics=sections['productivity_metrics'],
                projects_section=sections['projects_section'],
                papers_section=sections['papers_section'],
                collaboration_section=sections['collaboration_section'],
                skills_section=sections['skills_section'],
                trends_section=sections['trends_section'],
                recommendations_section=sections['recommendations_section'],
                report_markdown=report_markdown,
                report_html=report_html,
                generated_at=datetime.utcnow(),
                llm_model=self.LLM_MODEL,
                generation_duration_ms=generation_duration_ms
            )
            self.db.add(report_obj)
        
        await self.db.commit()
        await self.db.refresh(report_obj)
        
        # Cache in Redis (7 days TTL)
        cache_key = f"analytics:weekly_report:{user_id}:{week_start_date.isoformat()}"
        await self.redis.setex(
            cache_key,
            self.CACHE_TTL_SECONDS,
            json.dumps(report_obj.to_dict())
        )
        
        logger.info(
            "Generated weekly report",
            user_id=user_id,
            week_start=week_start_date.isoformat(),
            duration_ms=generation_duration_ms
        )
        
        return report_obj
    
    async def get_weekly_report(
        self,
        user_id: str,
        week_start_date: date
    ) -> Optional[WeeklyReport]:
        """
        Get weekly report from cache or database.
        
        Args:
            user_id: User UUID
            week_start_date: Monday of the week
        
        Returns:
            WeeklyReport or None if not found
        """
        # Ensure Monday
        if week_start_date.weekday() != 0:
            week_start_date = week_start_date - timedelta(days=week_start_date.weekday())
        
        # Try cache first
        cache_key = f"analytics:weekly_report:{user_id}:{week_start_date.isoformat()}"
        cached = await self.redis.get(cache_key)
        
        if cached:
            logger.debug("Cache hit for weekly report", user_id=user_id, week_start=week_start_date.isoformat())
        
        # Query database
        stmt = select(WeeklyReport).where(
            and_(
                WeeklyReport.user_id == user_id,
                WeeklyReport.week_start_date == week_start_date
            )
        )
        result = await self.db.execute(stmt)
        report = result.scalar_one_or_none()
        
        if report and not cached:
            # Populate cache
            await self.redis.setex(
                cache_key,
                self.CACHE_TTL_SECONDS,
                json.dumps(report.to_dict())
            )
        
        return report
    
    # =========================================================================
    # SECTION GENERATION
    # =========================================================================
    
    async def _generate_all_sections(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """Generate all 9 report sections."""
        
        # Get weekly aggregate from metrics service
        weekly_agg = await self.metrics_service.get_weekly_aggregate(user_id, week_start)
        
        # Section 1: Overview (LLM)
        overview = await self._generate_overview_section(user_id, week_start, week_end, weekly_agg)
        
        # Section 2: Time Analytics
        time_analytics = self._generate_time_analytics_section(weekly_agg)
        
        # Section 3: Productivity Metrics
        productivity_metrics = self._generate_productivity_section(weekly_agg)
        
        # Section 4: Projects
        projects_section = await self._generate_projects_section(user_id, week_start, week_end)
        
        # Section 5: Papers
        papers_section = await self._generate_papers_section(user_id, week_start, week_end)
        
        # Section 6: Collaboration
        if self.collaboration_service:
            collaboration_section = await self.collaboration_service.get_weekly_collaboration(user_id, week_start, week_end)
        else:
            collaboration_section = {"message": "Collaboration service not available"}
        
        # Section 7: Skills
        if self.skill_service:
            skills_section = await self.skill_service.get_weekly_skills(user_id, week_start, week_end)
        else:
            skills_section = {"message": "Skills service not available"}
        
        # Section 8: Trends
        trends_section = await self._generate_trends_section(user_id, week_start)
        
        # Section 9: Recommendations (LLM)
        recommendations_section = await self._generate_recommendations_section(
            user_id, week_start, week_end, weekly_agg, trends_section
        )
        
        return {
            'overview': overview,
            'time_analytics': time_analytics,
            'productivity_metrics': productivity_metrics,
            'projects_section': projects_section,
            'papers_section': papers_section,
            'collaboration_section': collaboration_section,
            'skills_section': skills_section,
            'trends_section': trends_section,
            'recommendations_section': recommendations_section
        }
    
    async def _generate_overview_section(
        self,
        user_id: str,
        week_start: date,
        week_end: date,
        weekly_agg: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate LLM overview of the week."""
        
        prompt = f"""You are MiniMe, a personal analytics assistant.

Genera a concise week overview (150-200 words) for this researcher/developer.

Week: {week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}

Key Metrics (averages):
- Focus score: {weekly_agg.get('averages', {}).get('focus_score', 0):.1f}/10
- Deep work: {weekly_agg.get('averages', {}).get('deep_work_hours', 0):.1f} hours/day
- Meetings: {weekly_agg.get('averages', {}).get('meeting_load_pct', 0):.0f}%
- Days tracked: {weekly_agg.get('days_tracked', 0)}/7

Guidelines:
- Start with an overall assessment (productive week? challenging week?)
- Highlight the standout metric (best or worst)
- Mention consistency across days
- Keep it concise and encouraging
- Return raw markdown, no headers

Example: "This was a highly productive week with an average focus score of 8.2/10..."
"""
        
        overview_text = await self._generate_with_llm(prompt)
        
        return {
            'summary': overview_text,
            'days_tracked': weekly_agg.get('days_tracked', 0),
            'avg_focus_score': float(weekly_agg.get('averages', {}).get('focus_score', 0))
        }
    
    def _generate_time_analytics_section(self, weekly_agg: Dict[str, Any]) -> Dict[str, Any]:
        """Generate time breakdown section."""
        totals = weekly_agg.get('totals', {})
        averages = weekly_agg.get('averages', {})
        
        return {
            'total_deep_work': float(totals.get('deep_work_hours', 0)),
            'avg_deep_work_per_day': float(averages.get('deep_work_hours', 0)),
            'total_meeting_time_pct': float(averages.get('meeting_load_pct', 0)),
            'breakdown': {
                'deep_work': float(totals.get('deep_work_hours', 0)),
                'meetings': 'Calculated from meeting_load_pct',
                'focused_time': 'Total productive time',
                'distracted_time': 'From distraction_index'
            }
        }
    
    def _generate_productivity_section(self, weekly_agg: Dict[str, Any]) -> Dict[str, Any]:
        """Generate productivity metrics section."""
        averages = weekly_agg.get('averages', {})
        totals = weekly_agg.get('totals', {})
        
        return {
            'avg_focus_score': float(averages.get('focus_score', 0)),
            'avg_deep_work': float(averages.get('deep_work_hours', 0)),
            'total_context_switches': int(totals.get('context_switches', 0)),
            'avg_context_switches': float(averages.get('context_switches', 0)),
            'avg_distraction_index': float(averages.get('distraction_index', 0)),
            'avg_break_quality': float(averages.get('break_quality', 0)),
            'best_focus_day': weekly_agg.get('best_day', {}).get('focus_score'),
            'worst_focus_day': weekly_agg.get('worst_day', {}).get('focus_score')
        }
    
    async def _generate_projects_section(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """Generate top projects section."""
        
        # TODO: Query knowledge graph for projects worked on this week
        # For now, return mock data
        return {
            'top_projects': [
                {'name': 'Backend API', 'hours': 15.5, 'progress': 'Added analytics endpoints'},
                {'name': 'Frontend UI', 'hours': 8.2, 'progress': 'Dashboard redesign'},
                {'name': 'Documentation', 'hours': 4.1, 'progress': 'Updated API docs'}
            ],
            'total_projects': 3
        }
    
    async def _generate_papers_section(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """Generate research papers section."""
        
        # TODO: Query knowledge graph for papers
        return {
            'papers_read': 3,
            'papers_written': 1,
            'top_papers': [
                {'title': 'Attention Is All You Need', 'hours': 2.5},
                {'title': 'BERT Paper', 'hours': 1.8}
            ]
        }
    
    async def _generate_trends_section(
        self,
        user_id: str,
        week_start: date
    ) -> Dict[str, Any]:
        """Generate week-over-week trends."""
        
        # Get previous week's aggregate
        prev_week_start = week_start - timedelta(days=7)
        prev_weekly_agg = await self.metrics_service.get_weekly_aggregate(user_id, prev_week_start)
        curr_weekly_agg = await self.metrics_service.get_weekly_aggregate(user_id, week_start)
        
        trends = {}
        
        for metric in ['focus_score', 'deep_work_hours', 'context_switches', 'meeting_load_pct']:
            curr_val = float(curr_weekly_agg.get('averages', {}).get(metric, 0))
            prev_val = float(prev_weekly_agg.get('averages', {}).get(metric, 0))
            
            if prev_val > 0:
                change_pct = ((curr_val - prev_val) / prev_val) * 100
            else:
                change_pct = 0.0
            
            trends[metric] = {
                'current': curr_val,
                'previous': prev_val,
                'change_pct': change_pct,
                'direction': 'up' if change_pct > 0 else 'down' if change_pct < 0 else 'stable'
            }
        
        return {'metrics_trends': trends}
    
    async def _generate_recommendations_section(
        self,
        user_id: str,
        week_start: date,
        week_end: date,
        weekly_agg: Dict[str, Any],
        trends: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate LLM recommendations."""
        
        # Build prompt with data
        prompt = f"""You are MiniMe, a personal analytics assistant.

Based on this week's data, provide 3-4 actionable recommendations.

Week: {week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}

Metrics:
- Avg focus: {weekly_agg.get('averages', {}).get('focus_score', 0):.1f}/10
- Deep work: {weekly_agg.get('averages', {}).get('deep_work_hours', 0):.1f}h/day
- Context switches: {weekly_agg.get('averages', {}).get('context_switches', 0):.0f}/day
- Meeting load: {weekly_agg.get('averages', {}).get('meeting_load_pct', 0):.0f}%

Trends (vs last week):
- Focus: {trends.get('metrics_trends', {}).get('focus_score', {}).get('direction', 'stable')}
- Deep work: {trends.get('metrics_trends', {}).get('deep_work_hours', {}).get('direction', 'stable')}

Guidelines:
- Provide 3-4 specific, actionable recommendations
- Each should be 1-2 sentences
- Focus on concrete behaviors, not general advice
- Format as markdown list
- Prioritize biggest opportunities

Example:
"- Schedule 2-hour deep work blocks Monday/Wednesday mornings
- Reduce meeting load by declining non-essential standups
- Take a 10-min break every 90 minutes for better focus"
"""
        
        recommendations_text = await self._generate_with_llm(prompt)
        
        return {
            'recommendations': recommendations_text,
            'count': recommendations_text.count('-')  # Rough count of bullet points
        }
    
    # =========================================================================
    # REPORT FORMATTING
    # =========================================================================
    
    def _build_report_markdown(
        self,
        sections: Dict[str, Any],
        week_start: date,
        week_end: date
    ) -> str:
        """Build full markdown report from sections."""
        
        md = f"""# Weekly Report: {week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}

## Overview

{sections['overview']['summary']}

## Time Analytics

- **Total Deep Work**: {sections['time_analytics']['total_deep_work']:.1f} hours
- **Avg Deep Work/Day**: {sections['time_analytics']['avg_deep_work_per_day']:.1f} hours
- **Avg Meeting Load**: {sections['time_analytics']['total_meeting_time_pct']:.0f}%

## Productivity Metrics

- **Avg Focus Score**: {sections['productivity_metrics']['avg_focus_score']:.1f}/10
- **Avg Context Switches**: {sections['productivity_metrics']['avg_context_switches']:.0f}/day
- **Avg Break Quality**: {sections['productivity_metrics']['avg_break_quality']:.1f}/10

## Top Projects

{self._format_projects_markdown(sections['projects_section'])}

## Collaboration

{self._format_collaboration_markdown(sections['collaboration_section'])}

## Skills Developed

{self._format_skills_markdown(sections['skills_section'])}

## Trends (vs Last Week)

{self._format_trends_markdown(sections['trends_section'])}

## Recommendations

{sections['recommendations_section']['recommendations']}
"""
        
        return md.strip()
    
    def _build_report_html(
        self,
        sections: Dict[str, Any],
        week_start: date,
        week_end: date
    ) -> str:
        """Build HTML report (for email)."""
        # TODO: Use Jinja2 template
        return f"<p>HTML report for {week_start} - {week_end}</p>"
    
    def _format_projects_markdown(self, section: Dict[str, Any]) -> str:
        """Format projects as markdown."""
        lines = []
        for proj in section.get('top_projects', []):
            lines.append(f"- **{proj['name']}**: {proj['hours']:.1f}h - {proj['progress']}")
        return '\n'.join(lines) if lines else "- No project data available"
    
    def _format_collaboration_markdown(self, section: Dict[str, Any]) -> str:
        """Format collaboration as markdown."""
        if 'message' in section:
            return f"- {section['message']}"
        return "- Collaboration data available"
    
    def _format_skills_markdown(self, section: Dict[str, Any]) -> str:
        """Format skills as markdown."""
        if 'message' in section:
            return f"- {section['message']}"
        return "- Skills data available"
    
    def _format_trends_markdown(self, section: Dict[str, Any]) -> str:
        """Format trends as markdown."""
        lines = []
        for metric, data in section.get('metrics_trends', {}).items():
            direction_symbol = "↑" if data['direction'] == 'up' else "↓" if data['direction'] == 'down' else "→"
            lines.append(f"- {metric.replace('_', ' ').title()}: {direction_symbol} {data['change_pct']:.1f}%")
        return '\n'.join(lines) if lines else "- No trend data available"
    
    # =========================================================================
    # LLM INTEGRATION
    # =========================================================================
    
    async def _generate_with_llm(self, prompt: str) -> str:
        """Call Anthropic Claude to generate text."""
        
        if not self.anthropic:
            logger.warning("Anthropic client not initialized")
            return "LLM not available. Using template."
        
        for attempt in range(self.LLM_MAX_RETRIES):
            try:
                response = self.anthropic.messages.create(
                    model=self.LLM_MODEL,
                    max_tokens=self.LLM_MAX_TOKENS,
                    temperature=0.7,
                    messages=[{"role": "user", "content": prompt}],
                    timeout=self.LLM_TIMEOUT_SECONDS
                )
                
                text = response.content[0].text.strip()
                logger.info("LLM generation successful", attempt=attempt + 1, length=len(text))
                return text
                
            except (APITimeoutError, APIError) as e:
                logger.warning(f"LLM error: {e}", attempt=attempt + 1)
                if attempt >= self.LLM_MAX_RETRIES - 1:
                    return "LLM generation failed. Using template."
        
        return "LLM generation failed. Using template."


# Dependency injection helper
async def get_weekly_report_service(
    db: AsyncSession,
    metrics_service: ProductivityMetricsService,
    collaboration_service: Optional[CollaborationAnalyticsService] = None,
    skill_service: Optional[SkillAnalyticsService] = None
) -> WeeklyReportService:
    """Get WeeklyReportService instance for FastAPI routes."""
    return WeeklyReportService(
        db=db,
        metrics_service=metrics_service,
        collaboration_service=collaboration_service,
        skill_service=skill_service
    )
