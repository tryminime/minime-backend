"""
Unit tests for Collaboration and Skills Analytics Services.

Tests:
- CollaborationAnalyticsService
- SkillAnalyticsService
- Neo4j queries
- Score calculations
"""

import pytest
from datetime import date, timedelta
from unittest.mock import AsyncMock, MagicMock

from services.collaboration_analytics_service import CollaborationAnalyticsService
from services.skill_analytics_service import SkillAnalyticsService


# =============================================================================
# COLLABORATION ANALYTICS TESTS
# =============================================================================

class TestCollaborationAnalyticsService:
    """Test suite for CollaborationAnalyticsService."""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return AsyncMock()
    
    @pytest.fixture
    def mock_neo4j(self):
        """Create mock Neo4j driver."""
        driver = MagicMock()
        session = AsyncMock()
        
        # Mock collaboration score query
        result = AsyncMock()
        record = {
            'unique_collaborators': 8,
            'total_interactions': 25,
            'interaction_types': ['meeting', 'chat', 'email']
        }
        result.single = AsyncMock(return_value=record)
        session.run = AsyncMock(return_value=result)
        
        driver.session = MagicMock(return_value=session)
        
        return driver
    
    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        return AsyncMock()
    
    @pytest.fixture
    def service(self, mock_db, mock_neo4j, mock_redis):
        """Create CollaborationAnalyticsService instance."""
        return CollaborationAnalyticsService(
            db=mock_db,
            neo4j_driver=mock_neo4j,
            redis_client=mock_redis
        )
    
    @pytest.mark.asyncio
    async def test_calculate_collaboration_score(
        self,
        service,
        mock_neo4j
    ):
        """Test collaboration score calculation."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        score = await service._calculate_collaboration_score(user_id, week_start, week_end)
        
        # Should be > 0 based on mock data (8 collaborators, 25 interactions, 3 types)
        assert score > 0.0
        assert score <= 10.0
        
        # Verify Neo4j was queried
        assert mock_neo4j.session.called
    
    @pytest.mark.asyncio
    async def test_get_top_collaborators(
        self,
        service,
        mock_neo4j
    ):
        """Test getting top collaborators."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        # Mock collaborators result
        session = mock_neo4j.session.return_value
        result = AsyncMock()
        result.fetch = AsyncMock(return_value=[
            {
                'collaborator_id': 'collab-1',
                'collaborator_name': 'Alice',
                'interaction_count': 15,
                'interaction_types': ['meeting', 'chat']
            },
            {
                'collaborator_id': 'collab-2',
                'collaborator_name': 'Bob',
                'interaction_count': 10,
                'interaction_types': ['email']
            }
        ])
        session.run = AsyncMock(return_value=result)
        
        collaborators = await service._get_top_collaborators(user_id, week_start, week_end)
        
        assert len(collaborators) == 2
        assert collaborators[0]['name'] == 'Alice'
        assert collaborators[0]['interaction_count'] == 15
        assert collaborators[1]['name'] == 'Bob'
    
    @pytest.mark.asyncio
    async def test_calculate_network_diversity(
        self,
        service,
        mock_neo4j
    ):
        """Test network diversity calculation."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        # Mock diversity result
        session = mock_neo4j.session.return_value
        result = AsyncMock()
        result.single = AsyncMock(return_value={
            'unique_teams': 3,
            'unique_roles': 4,
            'unique_locations': 2
        })
        session.run = AsyncMock(return_value=result)
        
        diversity = await service._calculate_network_diversity(user_id, week_start, week_end)
        
        assert 'diversity_score' in diversity
        assert diversity['diversity_score'] > 0.0
        assert diversity['unique_teams'] == 3
        assert diversity['unique_roles'] == 4
    
    @pytest.mark.asyncio
    async def test_weekly_collaboration(
        self,
        service
    ):
        """Test getting weekly collaboration analytics."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        result = await service.get_weekly_collaboration(user_id, week_start, week_end)
        
        # Verify all required fields
        assert 'collaboration_score' in result
        assert 'top_collaborators' in result
        assert 'network_diversity' in result
        assert 'meeting_patterns' in result
        assert 'total_collaborators' in result


# =============================================================================
# SKILLS ANALYTICS TESTS
# =============================================================================

class TestSkillAnalyticsService:
    """Test suite for SkillAnalyticsService."""
    
    @pytest.fixture
    def mock_db(self):
        """Create mock database session."""
        return AsyncMock()
    
    @pytest.fixture
    def mock_neo4j(self):
        """Create mock Neo4j driver."""
        driver = MagicMock()
        session = AsyncMock()
        
        result = AsyncMock()
        session.run = AsyncMock(return_value=result)
        
        driver.session = MagicMock(return_value=session)
        
        return driver
    
    @pytest.fixture
    def mock_redis(self):
        """Create mock Redis client."""
        return AsyncMock()
    
    @pytest.fixture
    def service(self, mock_db, mock_neo4j, mock_redis):
        """Create SkillAnalyticsService instance."""
        return SkillAnalyticsService(
            db=mock_db,
            neo4j_driver=mock_neo4j,
            redis_client=mock_redis
        )
    
    def test_hours_to_mastery_level(self, service):
        """Test mastery level calculation from hours."""
        assert service._hours_to_mastery_level(5) == 'beginner'
        assert service._hours_to_mastery_level(25) == 'intermediate'
        assert service._hours_to_mastery_level(60) == 'advanced'
        assert service._hours_to_mastery_level(120) == 'expert'
        assert service._hours_to_mastery_level(250) == 'master'
    
    def test_calculate_progress_to_next_level(self, service):
        """Test progress calculation to next mastery level."""
        # Beginner with 10 hours (0-20 range)
        progress = service._calculate_progress_to_next_level(10, 'beginner')
        assert progress == 50.0  # Halfway to intermediate
        
        # Intermediate with 35 hours (20-50 range)
        progress = service._calculate_progress_to_next_level(35, 'intermediate')
        assert progress == 50.0  # Halfway to advanced
        
        # Master (already at max)
        progress = service._calculate_progress_to_next_level(250, 'master')
        assert progress == 100.0
    
    @pytest.mark.asyncio
    async def test_get_top_skills(
        self,
        service,
        mock_neo4j
    ):
        """Test getting top skills for a week."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        # Mock skills result
        session = mock_neo4j.session.return_value
        result = AsyncMock()
        result.fetch = AsyncMock(return_value=[
            {
                'skill_id': 'skill-1',
                'skill_name': 'Python',
                'category': 'Programming',
                'total_hours': 10.5,
                'practice_sessions': 8
            },
            {
                'skill_id': 'skill-2',
                'skill_name': 'Machine Learning',
                'category': 'AI',
                'total_hours': 6.0,
                'practice_sessions': 4
            }
        ])
        session.run = AsyncMock(return_value=result)
        
        skills = await service._get_top_skills(user_id, week_start, week_end)
        
        assert len(skills) == 2
        assert skills[0]['name'] == 'Python'
        assert skills[0]['hours_this_week'] == 10.5
        assert skills[1]['name'] == 'Machine Learning'
    
    @pytest.mark.asyncio
    async def test_calculate_mastery_levels(
        self,
        service,
        mock_neo4j
    ):
        """Test mastery level calculation for all skills."""
        user_id = "test-user-123"
        
        # Mock mastery result
        session = mock_neo4j.session.return_value
        result = AsyncMock()
        result.fetch = AsyncMock(return_value=[
            {
                'skill_id': 'skill-1',
                'skill_name': 'Python',
                'total_hours': 75.0  # Advanced level
            },
            {
                'skill_id': 'skill-2',
                'skill_name': 'SQL',
                'total_hours': 15.0  # Beginner level
            }
        ])
        session.run = AsyncMock(return_value=result)
        
        mastery = await service._calculate_mastery_levels(user_id)
        
        assert len(mastery) == 2
        assert mastery[0]['skill_name'] == 'Python'
        assert mastery[0]['mastery_level'] == 'advanced'
        assert mastery[1]['skill_name'] == 'SQL'
        assert mastery[1]['mastery_level'] == 'beginner'
    
    @pytest.mark.asyncio
    async def test_analyze_growth_trajectories(
        self,
        service,
        mock_neo4j
    ):
        """Test growth trajectory analysis."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        # Mock trajectory result
        session = mock_neo4j.session.return_value
        result = AsyncMock()
        result.fetch = AsyncMock(return_value=[
            {
                'skill_id': 'skill-1',
                'skill_name': 'Python',
                'hours_this_week': 10.0,
                'avg_hours_prev_weeks': 5.0,
                'growth_pct': 100.0  # Doubled
            }
        ])
        session.run = AsyncMock(return_value=result)
        
        trajectories = await service._analyze_growth_trajectories(user_id, week_start, week_end)
        
        assert len(trajectories) == 1
        assert trajectories[0]['skill_name'] == 'Python'
        assert trajectories[0]['growth_pct'] == 100.0
        assert trajectories[0]['trend'] == 'increasing'
    
    @pytest.mark.asyncio
    async def test_get_skill_recommendations(
        self,
        service,
        mock_neo4j
    ):
        """Test skill recommendations."""
        user_id = "test-user-123"
        
        # Mock recommendations result
        session = mock_neo4j.session.return_value
        result = AsyncMock()
        result.fetch = AsyncMock(return_value=[
            {
                'skill_id': 'skill-new-1',
                'skill_name': 'Docker',
                'category': 'DevOps',
                'relevance_score': 5
            }
        ])
        session.run = AsyncMock(return_value=result)
        
        recommendations = await service._get_skill_recommendations(user_id)
        
        assert len(recommendations) == 1
        assert recommendations[0]['skill_name'] == 'Docker'
        assert recommendations[0]['relevance_score'] == 5
    
    @pytest.mark.asyncio
    async def test_weekly_skills(
        self,
        service
    ):
        """Test getting weekly skills analytics."""
        user_id = "test-user-123"
        week_start = date(2026, 2, 10)
        week_end = week_start + timedelta(days=6)
        
        result = await service.get_weekly_skills(user_id, week_start, week_end)
        
        # Verify all required fields
        assert 'top_skills_this_week' in result
        assert 'mastery_levels' in result
        assert 'growth_trajectories' in result
        assert 'recommendations' in result
        assert 'total_skills_practiced' in result


# Run tests with:
# pytest backend/tests/test_collaboration_skills.py -v
# pytest backend/tests/test_collaboration_skills.py -v --cov
