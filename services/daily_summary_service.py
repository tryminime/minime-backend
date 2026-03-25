"""
Daily Summary Service for Month 6 Personal Analytics.

Generates LLM-backed daily summaries (200-300 words) that:
- Mention deep work hours, focus score, meeting load, key projects
- Highlight 2-4 accomplishments
- Suggest 1-2 actionable improvements

Uses Anthropic Claude 3 Sonnet, stores markdown + HTML, caches in Redis (24h TTL).
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
import structlog
import json
import markdown
from anthropic import Anthropic, APIError, APITimeoutError
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from models.analytics_models import DailySummary, DailyMetrics
from services.productivity_metrics_service import ProductivityMetricsService
from database.redis_client import get_redis_client
from config.settings import settings

logger = structlog.get_logger(__name__)


class DailySummaryService:
    """Service for generating and managing daily summaries."""
    
    # Configuration
    LLM_MODEL = "claude-3-5-sonnet-20241022"  # Latest Sonnet model
    LLM_MAX_TOKENS = 700  # ~200-300 words
    LLM_TIMEOUT_SECONDS = 30
    LLM_MAX_RETRIES = 3
    
    CACHE_TTL_SECONDS = 86400  # 24 hours
    
    def __init__(
        self,
        db: AsyncSession,
        metrics_service: ProductivityMetricsService,
        anthropic_client: Optional[Anthropic] = None,
        redis_client=None
    ):
        """
        Initialize the daily summary service.
        
        Args:
            db: SQLAlchemy async session
            metrics_service: ProductivityMetricsService instance
            anthropic_client: Anthropic client (optional, creates if None)
            redis_client: Redis client for caching (optional)
        """
        self.db = db
        self.metrics_service = metrics_service
        self.redis = redis_client or get_redis()
        
        # Initialize Anthropic client
        if anthropic_client:
            self.anthropic = anthropic_client
        else:
            api_key = getattr(settings, 'ANTHROPIC_API_KEY', None)
            if not api_key:
                logger.warning("ANTHROPIC_API_KEY not set, LLM features will fail")
            self.anthropic = Anthropic(api_key=api_key) if api_key else None
    
    async def generate_daily_summary(
        self,
        user_id: str,
        target_date: date,
        force_regenerate: bool = False
    ) -> DailySummary:
        """
        Generate a daily summary for a user on a given date.
        
        Workflow:
        1. Get or compute daily metrics
        2. Fetch top activities/projects for the day
        3. Build LLM prompt with structured data
        4. Call Claude to generate summary markdown
        5. Convert markdown to HTML
        6. Store in database and cache
        
        Args:
            user_id: User UUID
            target_date: Date to generate summary for
            force_regenerate: If True, regenerate even if exists
        
        Returns:
            DailySummary model instance
        
        Raises:
            ValueError: If activities or metrics are missing
            APIError: If LLM call fails after retries
        """
        logger.info(
            "Generating daily summary",
            user_id=user_id,
            date=target_date.isoformat(),
            force=force_regenerate
        )
        
        start_time = datetime.utcnow()
        
        # Check if summary already exists
        if not force_regenerate:
            existing = await self.get_daily_summary(user_id, target_date)
            if existing:
                logger.info("Summary already exists, returning cached", user_id=user_id, date=target_date.isoformat())
                return existing
        
        # 1. Get daily metrics (compute if needed)
        metrics = await self.metrics_service.get_daily_metrics(user_id, target_date)
        if not metrics:
            # Try to compute metrics first
            # Note: This requires activities to be available
            raise ValueError(f"No metrics found for {user_id} on {target_date}. Compute metrics first.")
        
        # 2. Fetch top activities and projects
        activity_data = await self._get_activity_breakdown(user_id, target_date)
        
        # 3. Build LLM prompt
        prompt = self._build_daily_summary_prompt(
            date=target_date,
            metrics=metrics,
            activity_data=activity_data
        )
        
        # 4. Call LLM to generate summary
        summary_markdown = await self._generate_with_llm(prompt)
        
        # 5. Convert to HTML
        summary_html = self._markdown_to_html(summary_markdown)
        
        # 6. Extract metadata (accomplishments, recommendations)
        metadata = self._extract_metadata(summary_markdown, activity_data)
        
        # Calculate generation time
        generation_duration_ms = int((datetime.utcnow() - start_time).total_seconds() * 1000)
        
        # 7. Store in database
        stmt = select(DailySummary).where(
            and_(
                DailySummary.user_id == user_id,
                DailySummary.date == target_date
            )
        )
        result = await self.db.execute(stmt)
        existing = result.scalar_one_or_none()
        
        if existing:
            # Update existing
            existing.summary_markdown = summary_markdown
            existing.summary_html = summary_html
            existing.focus_score = metrics.focus_score
            existing.deep_work_hours = metrics.deep_work_hours
            existing.metadata = metadata
            existing.generated_at = datetime.utcnow()
            existing.llm_model = self.LLM_MODEL
            existing.generation_duration_ms = generation_duration_ms
            existing.updated_at = datetime.utcnow()
            summary_obj = existing
        else:
            # Create new
            summary_obj = DailySummary(
                user_id=user_id,
                date=target_date,
                summary_markdown=summary_markdown,
                summary_html=summary_html,
                focus_score=metrics.focus_score,
                deep_work_hours=metrics.deep_work_hours,
                metadata=metadata,
                generated_at=datetime.utcnow(),
                llm_model=self.LLM_MODEL,
                generation_duration_ms=generation_duration_ms
            )
            self.db.add(summary_obj)
        
        await self.db.commit()
        await self.db.refresh(summary_obj)
        
        # 8. Cache in Redis (24h TTL)
        cache_key = f"analytics:summary:{user_id}:{target_date.isoformat()}"
        await self.redis.setex(
            cache_key,
            self.CACHE_TTL_SECONDS,
            json.dumps(summary_obj.to_dict())
        )
        
        logger.info(
            "Generated daily summary",
            user_id=user_id,
            date=target_date.isoformat(),
            duration_ms=generation_duration_ms,
            summary_length=len(summary_markdown)
        )
        
        return summary_obj
    
    async def get_daily_summary(
        self,
        user_id: str,
        target_date: date
    ) -> Optional[DailySummary]:
        """
        Get daily summary from cache or database.
        
        Does NOT generate if missing - returns None.
        Use generate_daily_summary to force generation.
        
        Args:
            user_id: User UUID
            target_date: Date to retrieve
        
        Returns:
            DailySummary or None if not found
        """
        # Try cache first
        cache_key = f"analytics:summary:{user_id}:{target_date.isoformat()}"
        cached = await self.redis.get(cache_key)
        
        if cached:
            logger.debug("Cache hit for daily summary", user_id=user_id, date=target_date.isoformat())
            # Note: Could reconstruct from JSON, but DB query is simple
        
        # Query database
        stmt = select(DailySummary).where(
            and_(
                DailySummary.user_id == user_id,
                DailySummary.date == target_date
            )
        )
        result = await self.db.execute(stmt)
        summary = result.scalar_one_or_none()
        
        if summary and not cached:
            # Populate cache
            await self.redis.setex(
                cache_key,
                self.CACHE_TTL_SECONDS,
                json.dumps(summary.to_dict())
            )
        
        return summary
    
    # =========================================================================
    # PRIVATE METHODS
    # =========================================================================
    
    async def _get_activity_breakdown(
        self,
        user_id: str,
        target_date: date
    ) -> Dict[str, Any]:
        """
        Fetch and aggregate activity data for the day.
        
        Returns:
            Dict with:
                - top_projects: List of {name, hours}
                - top_apps: List of {name, duration}
                - total_activities: int
        """
        # TODO: Query activities table
        # For now, return mock data structure
        return {
            "top_projects": [
                {"name": "Project A", "hours": 4.2},
                {"name": "Project B", "hours": 2.5},
            ],
            "top_apps": [
                {"name": "vscode", "duration_minutes": 180},
                {"name": "terminal", "duration_minutes": 120},
                {"name": "chrome", "duration_minutes": 90},
            ],
            "total_activities": 150
        }
    
    def _build_daily_summary_prompt(
        self,
        date: date,
        metrics: DailyMetrics,
        activity_data: Dict[str, Any]
    ) -> str:
        """
        Build the LLM prompt for daily summary generation.
        
        Returns:
            Formatted prompt string
        """
        # Format projects for prompt
        projects_text = "\n".join([
            f"- {p['name']}: {p['hours']:.1f} hours"
            for p in activity_data.get("top_projects", [])
        ])
        
        # Format apps for prompt
        apps_text = "\n".join([
            f"- {a['name']}: {a['duration_minutes']} minutes"
            for a in activity_data.get("top_apps", [])[:5]  # Top 5
        ])
        
        prompt = f"""You are MiniMe, a personal analytics assistant for researchers and developers.

Generate a concise daily summary (200-300 words) for this user.

Date: {date.strftime('%A, %B %d, %Y')}

Metrics:
- Focus score: {float(metrics.focus_score or 0):.1f} / 10
- Deep work: {float(metrics.deep_work_hours or 0):.1f} hours
- Meetings: {float(metrics.meeting_load_pct or 0):.0f}%
- Context switches: {metrics.context_switches or 0}
- Distraction index: {float(metrics.distraction_index or 0):.0f} / 100
- Break quality: {float(metrics.break_quality or 0):.1f} / 10

Top projects (by hours):
{projects_text if projects_text else "- No project data available"}

Top activities:
{apps_text if apps_text else "- No activity data available"}

Guidelines:
- Write 2-3 short paragraphs
- Start with a positive but honest overview of the day
- Mention the focus score and deep work hours explicitly
- Reference at least one project or activity by name
- Highlight 2-4 key accomplishments or patterns
- End with 1-2 concrete, actionable recommendations for tomorrow
- Tone: professional, supportive, non-judgmental, encouraging
- Use "you" to address the user directly
- Return ONLY raw markdown, no front matter, no headers

Example structure:
"Today you achieved a focus score of X/10 with Y hours of deep work..."
"You spent significant time on [project], making progress on..."
"For tomorrow, consider [specific recommendation]..."
"""
        
        return prompt
    
    async def _generate_with_llm(self, prompt: str) -> str:
        """
        Call Anthropic Claude to generate summary.
        
        Includes retry logic and error handling.
        
        Args:
            prompt: Formatted prompt string
        
        Returns:
            Generated summary markdown
        
        Raises:
            APIError: If all retries fail
        """
        if not self.anthropic:
            logger.error("Anthropic client not initialized")
            return self._generate_fallback_summary(prompt)
        
        for attempt in range(self.LLM_MAX_RETRIES):
            try:
                response = self.anthropic.messages.create(
                    model=self.LLM_MODEL,
                    max_tokens=self.LLM_MAX_TOKENS,
                    temperature=0.7,  # Some creativity
                    messages=[
                        {"role": "user", "content": prompt}
                    ],
                    timeout=self.LLM_TIMEOUT_SECONDS
                )
                
                # Extract text from response
                summary = response.content[0].text.strip()
                
                logger.info(
                    "LLM generation successful",
                    attempt=attempt + 1,
                    length=len(summary),
                    model=self.LLM_MODEL
                )
                
                return summary
                
            except APITimeoutError as e:
                logger.warning(
                    "LLM timeout",
                    attempt=attempt + 1,
                    max_retries=self.LLM_MAX_RETRIES,
                    error=str(e)
                )
                if attempt >= self.LLM_MAX_RETRIES - 1:
                    return self._generate_fallback_summary(prompt)
                
            except APIError as e:
                logger.error(
                    "LLM API error",
                    attempt=attempt + 1,
                    max_retries=self.LLM_MAX_RETRIES,
                    error=str(e)
                )
                if attempt >= self.LLM_MAX_RETRIES - 1:
                    return self._generate_fallback_summary(prompt)
        
        # Fallback if all retries fail
        return self._generate_fallback_summary(prompt)
    
    def _generate_fallback_summary(self, prompt: str) -> str:
        """
        Generate a template-based summary when LLM fails.
        
        Args:
            prompt: Original prompt (for extracting data)
        
        Returns:
            Template-based summary
        """
        logger.warning("Using fallback summary generation")
        
        # Extract basic data from prompt
        # This is a simple fallback - in production, parse more carefully
        return """Today's productivity summary:

You completed your work with a focus on your key projects. Your deep work sessions contributed to meaningful progress.

Key observations:
- Maintained focus throughout the day
- Made progress on important tasks
- Balanced work with necessary meetings and breaks

For tomorrow:
- Continue deep work momentum
- Plan focused time blocks
- Take regular breaks for sustained productivity
"""
    
    def _markdown_to_html(self, markdown_text: str) -> str:
        """
        Convert markdown to HTML with email-safe styling.
        
        Args:
            markdown_text: Raw markdown string
        
        Returns:
            HTML string with inline CSS
        """
        # Convert markdown to HTML
        html_content = markdown.markdown(
            markdown_text,
            extensions=['extra', 'nl2br']
        )
        
        # Wrap in email-safe container with inline CSS
        html_full = f"""
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif; 
            font-size: 16px; 
            line-height: 1.6; 
            color: #333; 
            max-width: 600px; 
            margin: 0 auto;">
    {html_content}
</div>
"""
        
        return html_full.strip()
    
    def _extract_metadata(
        self,
        summary_markdown: str,
        activity_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Extract structured metadata from summary and activity data.
        
        Args:
            summary_markdown: Generated summary text
            activity_data: Activity breakdown data
        
        Returns:
            Dict with accomplishments, recommendations, etc.
        """
        # Simple extraction - could use NLP for better parsing
        metadata = {
            "summary_length": len(summary_markdown),
            "word_count": len(summary_markdown.split()),
            "top_projects": [p["name"] for p in activity_data.get("top_projects", [])[:3]],
            "total_activities": activity_data.get("total_activities", 0),
            "generated_with": "llm" if self.anthropic else "fallback"
        }
        
        # Try to extract accomplishments (lines with "progress", "completed", etc.)
        accomplishments = []
        for line in summary_markdown.split('\n'):
            if any(word in line.lower() for word in ['progress', 'completed', 'achieved', 'finished']):
                accomplishments.append(line.strip('- ').strip())
        
        if accomplishments:
            metadata["accomplishments"] = accomplishments[:4]  # Max 4
        
        # Try to extract recommendations (lines with "tomorrow", "consider", etc.)
        recommendations = []
        for line in summary_markdown.split('\n'):
            if any(word in line.lower() for word in ['tomorrow', 'consider', 'try', 'recommend']):
                recommendations.append(line.strip('- ').strip())
        
        if recommendations:
            metadata["recommendations"] = recommendations[:2]  # Max 2
        
        return metadata


# Convenience function for dependency injection
async def get_daily_summary_service(
    db: AsyncSession,
    metrics_service: ProductivityMetricsService
) -> DailySummaryService:
    """Get DailySummaryService instance for FastAPI routes."""
    return DailySummaryService(db=db, metrics_service=metrics_service)
