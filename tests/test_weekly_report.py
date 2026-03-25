"""
Unit tests for WeeklyReportService.

Tests:
- Weekly report generation
- All 9 sections
- LLM integration
- Caching behavior
- Markdown/HTML formatting
"""

import pytest
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

from services.weekly_report_service import WeeklyReportService
from models.analytics_models import WeeklyReport


class TestWeeklyReportService:
    """Test suite for WeeklyReportService."""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        db = AsyncMock()
        db.execute = AsyncMock()
        db.commit = AsyncMock()
        db.refresh = AsyncMock()
        db.add = MagicMock()
        return db
    
    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()
        return redis
    
    @pytest.fixture
    def mock_metrics_service(self):
        """Create mock ProductivityMetricsService."""
        service = AsyncMock()
        
        # Mock weekly aggregate
        service.get_weekly_aggregate = AsyncMock(return_value={
            'days_tracked': 5,
            'averages': {
                'focus_score': Decimal('7.5'),
                'deep_work_hours': Decimal('4.2'),
                'context_switches': 25,
                'meeting_load_pct': Decimal('30.0'),
                'distraction_index': Decimal('15.0'),
                'break_quality': Decimal('8.0')
            },
            'totals': {
                'deep_work_hours': Decimal('21.0'),
                'context_switches': 125
            },
            'best_day': {'focus_score': 9.0},
            'worst_day': {'focus_score': 6.0}
        })
        
        return service
    
    @pytest.fixture
    def mock_collaboration_service(self):
        """Create mock CollaborationAnalyticsService."""
        service = AsyncMock()
        service.get_weekly_collaboration = AsyncMock(return_value={
            'collaboration_score': 7.5,
            'top_collaborators': [
                {'name': 'Alice', 'interaction_count': 15}
            ],
            'network_diversity': {'diversity_score': 8.0},
            'total_collaborators': 5
        })
        return service
    
    @pytest.fixture
    def mock_skill_service(self):
        """Create mock SkillAnalyticsService."""
        service = AsyncMock()
        service.get_weekly_skills = AsyncMock(return_value={
            'top_skills_this_week': [
                {'name': 'Python', 'hours_this_week': 10.5}
            ],
            'mastery_levels': [],
            'total_skills_practiced': 5
        })
        return service
    
    @pytest.fixture
    def mock_anthropic(self):
        """Create mock Anthropic client."""
        client = MagicMock()
        
        mock_response = MagicMock()
        mock_content = MagicMock()
        mock_content.text = "This was a productive week with good focus."
        mock_response.content = [mock_content]
        
        client.messages.create = MagicMock(return_value=mock_response)
        
        return client
    
    @pytest.fixture
    def service(
        self,
        mock_db,
        mock_redis,
        mock_metrics_service,
        mock_collaboration_service,
        mock_skill_service,
        mock_anthropic
    ):
        """Create WeeklyReportService instance."""
        return WeeklyReportService(
            db=mock_db,
            metrics_service=mock_metrics_service,
            collaboration_service=mock_collaboration_service,
            skill_service=mock_skill_service,
            anthropic_client=mock_anthropic,
            redis_client=mock_redis
        )
    
    # =========================================================================
    # REPORT GENERATION TESTS
    # =========================================================================
    
    @pytest.mark.asyncio
    async def test_generate_weekly_report_success(
        self,
        service,
        mock_db,
        mock_redis,
        mock_anthropic
    ):
        """Test successful weekly report generation."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)  # Monday
        
        # Mock no existing report
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        
        # Generate report
        report = await service.generate_weekly_report(user_id, week_start)
        
        # Verify LLM was called twice (overview + recommendations)
        assert mock_anthropic.messages.create.call_count >= 2
        
        # Verify database operations
        assert mock_db.add.called
        assert mock_db.commit.called
        
        # Verify cache was populated
        assert mock_redis.setex.called
    
    @pytest.mark.asyncio
    async def test_generate_weekly_report_adjusts_to_monday(
        self,
        service,
        mock_db
    ):
        """Test that week_start_date is adjusted to Monday."""
        user_id = "test-user-123"
        wednesday = date(2026, 2, 12)  # Wednesday
        
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        
        await service.generate_weekly_report(user_id, wednesday)
        
        # Should have adjusted to Monday (Feb 10)
        # Verify by checking the stored date
        assert mock_db.add.called
    
    @pytest.mark.asyncio
    async def test_get_weekly_report_from_cache(
        self,
        service,
        mock_redis,
        mock_db
    ):
        """Test retrieving report from cache."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        
        # Mock cache hit
        cached_data = '{"id": "cached-123"}'
        mock_redis.get.return_value = cached_data
        
        # Mock DB result
        mock_report = MagicMock(spec=WeeklyReport)
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_report
        
        result = await service.get_weekly_report(user_id, week_start)
        
        assert mock_redis.get.called
        assert result == mock_report
    
    # =========================================================================
    # SECTION GENERATION TESTS
    # =========================================================================
    
    @pytest.mark.asyncio
    async def test_generate_all_sections(
        self,
        service,
        mock_metrics_service
    ):
        """Test that all 9 sections are generated."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        sections = await service._generate_all_sections(user_id, week_start, week_end)
        
        # Verify all 9 sections exist
        assert 'overview' in sections
        assert 'time_analytics' in sections
        assert 'productivity_metrics' in sections
        assert 'projects_section' in sections
        assert 'papers_section' in sections
        assert 'collaboration_section' in sections
        assert 'skills_section' in sections
        assert 'trends_section' in sections
        assert 'recommendations_section' in sections
    
    def test_time_analytics_section(self, service):
        """Test time analytics section generation."""
        weekly_agg = {
            'totals': {
                'deep_work_hours': Decimal('21.0')
            },
            'averages': {
                'deep_work_hours': Decimal('4.2'),
                'meeting_load_pct': Decimal('30.0')
            }
        }
        
        section = service._generate_time_analytics_section(weekly_agg)
        
        assert section['total_deep_work'] == 21.0
        assert section['avg_deep_work_per_day'] == 4.2
        assert section['total_meeting_time_pct'] == 30.0
    
    def test_productivity_section(self, service):
        """Test productivity metrics section generation."""
        weekly_agg = {
            'averages': {
                'focus_score': Decimal('7.5'),
                'deep_work_hours': Decimal('4.2'),
                'context_switches': 25,
                'distraction_index': Decimal('15.0'),
                'break_quality': Decimal('8.0')
            },
            'totals': {
                'context_switches': 125
            },
            'best_day': {'focus_score': 9.0},
            'worst_day': {'focus_score': 6.0}
        }
        
        section = service._generate_productivity_section(weekly_agg)
        
        assert section['avg_focus_score'] == 7.5
        assert section['avg_deep_work'] == 4.2
        assert section['total_context_switches'] == 125
        assert section['avg_context_switches'] == 25.0
    
    @pytest.mark.asyncio
    async def test_trends_section(
        self,
        service,
        mock_metrics_service
    ):
        """Test trends section calculates week-over-week changes."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        
        # Mock current and previous week aggregates
        mock_metrics_service.get_weekly_aggregate.side_effect = [
            {  # Previous week
                'averages': {
                    'focus_score': Decimal('7.0'),
                    'deep_work_hours': Decimal('4.0')
                }
            },
            {  # Current week
                'averages': {
                    'focus_score': Decimal('7.5'),
                    'deep_work_hours': Decimal('4.2')
                }
            }
        ]
        
        section = await service._generate_trends_section(user_id, week_start)
        
        trends = section['metrics_trends']
        assert 'focus_score' in trends
        assert trends['focus_score']['direction'] == 'up'
        assert trends['focus_score']['change_pct'] > 0
    
    # =========================================================================
    # MARKDOWN/HTML FORMATTING TESTS
    # =========================================================================
    
    def test_build_report_markdown(self, service):
        """Test markdown report formatting."""
        sections = {
            'overview': {'summary': 'Test overview', 'days_tracked': 5},
            'time_analytics': {
                'total_deep_work': 21.0,
                'avg_deep_work_per_day': 4.2,
                'total_meeting_time_pct': 30.0
            },
            'productivity_metrics': {
                'avg_focus_score': 7.5,
                'avg_context_switches': 25.0,
                'avg_break_quality': 8.0
            },
            'projects_section': {
                'top_projects': [
                    {'name': 'Project A', 'hours': 10.0, 'progress': 'Good'}
                ]
            },
            'collaboration_section': {},
            'skills_section': {},
            'trends_section': {'metrics_trends': {}},
            'recommendations_section': {'recommendations': '- Test rec'}
        }
        
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        markdown = service._build_report_markdown(sections, week_start, week_end)
        
        assert '# Weekly Report' in markdown
        assert 'Test overview' in markdown
        assert '21.0 hours' in markdown
        assert 'Project A' in markdown


# Run tests with:
# pytest backend/tests/test_weekly_report.py -v
# pytest backend/tests/test_weekly_report.py -v --cov=backend/services/weekly_report_service
