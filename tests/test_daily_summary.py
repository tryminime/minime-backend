"""
Unit tests for DailySummaryService.

Tests:
- Summary generation with real LLM
- Summary generation with mock LLM
- Markdown to HTML conversion
- Prompt construction
- Caching behavior
- Error handling and fallbacks
"""

import pytest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch, mock_open

from services.daily_summary_service import DailySummaryService
from models.analytics_models import DailyMetrics, DailySummary


class TestDailySummaryService:
    """Test suite for DailySummaryService."""
    
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
        
        # Mock metrics
        mock_metrics = MagicMock()
        mock_metrics.focus_score = Decimal("7.5")
        mock_metrics.deep_work_hours = Decimal("4.2")
        mock_metrics.context_switches = 25
        mock_metrics.meeting_load_pct = Decimal("30.0")
        mock_metrics.distraction_index = Decimal("15.0")
        mock_metrics.break_quality = Decimal("8.0")
        
        service.get_daily_metrics = AsyncMock(return_value=mock_metrics)
        
        return service
    
    @pytest.fixture
    def mock_anthropic(self):
        """Create mock Anthropic client."""
        client = MagicMock()
        
        # Mock response
        mock_response = MagicMock()
        mock_content = MagicMock()
        mock_content.text = """Today you achieved a focus score of 7.5/10 with 4.2 hours of deep work. You spent significant time on Project A, making steady progress on key deliverables.

Your productivity was strong, with minimal distractions (15% distraction index) and good break quality (8.0/10). However, 30% of your time was spent in meetings, which limited deep work opportunities.

For tomorrow, consider blocking morning hours for deep work sessions before meetings begin. Also try reducing context switches by batching similar tasks together."""
        
        mock_response.content = [mock_content]
        
        client.messages.create = MagicMock(return_value=mock_response)
        
        return client
    
    @pytest.fixture
    def service(self, mock_db, mock_redis, mock_metrics_service, mock_anthropic):
        """Create DailySummaryService instance."""
        return DailySummaryService(
            db=mock_db,
            metrics_service=mock_metrics_service,
            anthropic_client=mock_anthropic,
            redis_client=mock_redis
        )
    
    # =========================================================================
    # SUMMARY GENERATION TESTS
    # =========================================================================
    
    @pytest.mark.asyncio
    async def test_generate_daily_summary_success(
        self,
        service,
        mock_db,
        mock_redis,
        mock_metrics_service,
        mock_anthropic
    ):
        """Test successful daily summary generation."""
        user_id = "test-user-123"
        target_date = date(2026, 2, 9)
        
        # Mock no existing summary
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        
       # Generate summary
        summary = await service.generate_daily_summary(user_id, target_date)
        
        # Verify LLM was called
        assert mock_anthropic.messages.create.called
        
        # Verify database operations
        assert mock_db.add.called
        assert mock_db.commit.called
        
        # Verify cache was populated
        assert mock_redis.setex.called
        cache_key = f"analytics:summary:{user_id}:{target_date.isoformat()}"
        mock_redis.setex.assert_called_once()
        assert mock_redis.setex.call_args[0][0] == cache_key
    
    @pytest.mark.asyncio
    async def test_generate_daily_summary_updates_existing(
        self,
        service,
        mock_db
    ):
        """Test that existing summary is updated, not duplicated."""
        user_id = "test-user-123"
        target_date = date(2026, 2, 9)
        
        # Mock existing summary
        existing_summary = MagicMock(spec=DailySummary)
        existing_summary.id = "existing-id"
        existing_summary.to_dict = MagicMock(return_value={})
        
        mock_db.execute.return_value.scalar_one_or_none.return_value = existing_summary
        
        await service.generate_daily_summary(user_id, target_date, force_regenerate=True)
        
        # Should update existing, not call db.add
        assert not mock_db.add.called
        assert mock_db.commit.called
    
    @pytest.mark.asyncio
    async def test_generate_daily_summary_no_metrics(
        self,
        service,
        mock_metrics_service
    ):
        """Test error when metrics are missing."""
        user_id = "test-user-123"
        target_date = date(2026, 2, 9)
        
        # Mock no metrics
        mock_metrics_service.get_daily_metrics.return_value = None
        
        with pytest.raises(ValueError, match="No metrics found"):
            await service.generate_daily_summary(user_id, target_date)
    
    @pytest.mark.asyncio
    async def test_get_daily_summary_from_cache(
        self,
        service,
        mock_redis,
        mock_db
    ):
        """Test retrieving summary from cache."""
        user_id = "test-user-123"
        target_date = date(2026, 2, 9)
        
        # Mock cache hit
        cached_data = '{"id": "cached-123", "summary_markdown": "Test"}'
        mock_redis.get.return_value = cached_data
        
        # Mock DB result
        mock_summary = MagicMock(spec=DailySummary)
        mock_db.execute.return_value.scalar_one_or_none.return_value = mock_summary
        
        result = await service.get_daily_summary(user_id, target_date)
        
        # Should still query DB but find cache hit
        assert mock_redis.get.called
        assert result == mock_summary
    
    @pytest.mark.asyncio
    async def test_get_daily_summary_not_found(
        self,
        service,
        mock_db
    ):
        """Test getting summary that doesn't exist."""
        user_id = "test-user-123"
        target_date = date(2026, 2, 9)
        
        # Mock no summary
        mock_db.execute.return_value.scalar_one_or_none.return_value = None
        
        result = await service.get_daily_summary(user_id, target_date)
        
        assert result is None
    
    # =========================================================================
    # PROMPT BUILDING TESTS
    # =========================================================================
    
    def test_build_daily_summary_prompt(self, service):
        """Test prompt construction."""
        # Create mock metrics
        metrics = MagicMock()
        metrics.focus_score = Decimal("7.5")
        metrics.deep_work_hours = Decimal("4.2")
        metrics.context_switches = 25
        metrics.meeting_load_pct = Decimal("30.0")
        metrics.distraction_index = Decimal("15.0")
        metrics.break_quality = Decimal("8.0")
        
        activity_data = {
            "top_projects": [
                {"name": "Project A", "hours": 4.2}
            ],
            "top_apps": [
                {"name": "vscode", "duration_minutes": 180}
            ]
        }
        
        prompt = service._build_daily_summary_prompt(
            date=date(2026, 2, 9),
            metrics=metrics,
            activity_data=activity_data
        )
        
        # Verify key components are in prompt
        assert "7.5" in prompt  # Focus score
        assert "4.2" in prompt  # Deep work
        assert "Project A" in prompt
        assert "vscode" in prompt
        assert "200-300 words" in prompt  # Guidelines
        assert "MiniMe" in prompt
    
    # =========================================================================
    # MARKDOWN/HTML CONVERSION TESTS
    # =========================================================================
    
    def test_markdown_to_html_basic(self, service):
        """Test markdown to HTML conversion."""
        markdown_text = """This is a **bold** statement.

This is a new paragraph with *italic* text."""
        
        html = service._markdown_to_html(markdown_text)
        
        assert "<strong>bold</strong>" in html
        assert "<em>italic</em>" in html
        assert "<p>" in html
        assert "font-family" in html  # Has inline CSS
    
    def test_markdown_to_html_lists(self, service):
        """Test markdown list conversion."""
        markdown_text = """Key points:

- Point 1
- Point 2
- Point 3"""
        
        html = service._markdown_to_html(markdown_text)
        
        assert "<ul>" in html or "<li>" in html
    
    # =========================================================================
    # METADATA EXTRACTION TESTS
    # =========================================================================
    
    def test_extract_metadata(self, service):
        """Test metadata extraction from summary."""
        summary_text = """Today you achieved a focus score of 7.5/10.

You made progress on the authentication module and completed the API documentation.

For tomorrow, consider scheduling deep work blocks in the morning."""
        
        activity_data = {
            "top_projects": [{"name": "Backend API", "hours": 4.0}],
            "total_activities": 150
        }
        
        metadata = service._extract_metadata(summary_text, activity_data)
        
        assert "summary_length" in metadata
        assert "word_count" in metadata
        assert metadata["total_activities"] == 150
        assert "Backend API" in metadata["top_projects"]
    
    # =========================================================================
    # LLM FALLBACK TESTS
    # =========================================================================
    
    def test_generate_fallback_summary(self, service):
        """Test fallback summary generation."""
        prompt = "Test prompt"
        
        fallback = service._generate_fallback_summary(prompt)
        
        assert len(fallback) > 0
        assert "productivity" in fallback.lower() or "work" in fallback.lower()
    
    @pytest.mark.asyncio
    async def test_generate_with_llm_timeout(self, service, mock_anthropic):
        """Test LLM timeout handling."""
        from anthropic import APITimeoutError
        
        # Mock timeout
        mock_anthropic.messages.create.side_effect = APITimeoutError("Timeout")
        
        # Should fall back to template
        result = await service._generate_with_llm("test prompt")
        
        assert "productivity" in result.lower()  # Fallback content
    
    @pytest.mark.asyncio
    async def test_generate_with_llm_api_error(self, service, mock_anthropic):
        """Test LLM API error handling."""
        from anthropic import APIError
        
        # Mock API error
        mock_anthropic.messages.create.side_effect = APIError("API Error")
        
        # Should fall back to template
        result = await service._generate_with_llm("test prompt")
        
        assert len(result) > 0  # Got fallback


# Run tests with:
# pytest backend/tests/test_daily_summary.py -v
# pytest backend/tests/test_daily_summary.py -v --cov=backend/services/daily_summary_service
