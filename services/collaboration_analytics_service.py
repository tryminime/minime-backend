"""
Collaboration Analytics Service for Month 6 Personal Analytics.

Analyzes:
- Collaboration patterns
- Network diversity
- Top collaborators
- Collaboration frequency/strength
- Meeting patterns

Uses Neo4j knowledge graph for network analysis.
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from database.neo4j_client import get_neo4j_driver
from database.redis_client import get_redis_client

logger = structlog.get_logger(__name__)


class CollaborationAnalyticsService:
    """Service for collaboration network analysis."""
    
    CACHE_TTL_SECONDS = 86400  # 24 hours
    
    def __init__(
        self,
        db: AsyncSession,
        neo4j_driver=None,
        redis_client=None
    ):
        """
        Initialize collaboration analytics service.
        
        Args:
            db: SQLAlchemy async session
            neo4j_driver: Neo4j driver instance
            redis_client: Redis client for caching
        """
        self.db = db
        self.neo4j = neo4j_driver or get_neo4j_driver()
        self.redis = redis_client or get_redis()
    
    async def get_weekly_collaboration(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """
        Get collaboration analytics for a week.
        
        Args:
            user_id: User UUID
            week_start: Start of week
            week_end: End of week
        
        Returns:
            Dict with collaboration metrics
        """
        logger.info(
            "Analyzing weekly collaboration",
            user_id=user_id,
            week_start=week_start.isoformat(),
            week_end=week_end.isoformat()
        )
        
        # Query Neo4j for collaboration data
        collab_score = await self._calculate_collaboration_score(user_id, week_start, week_end)
        top_collaborators = await self._get_top_collaborators(user_id, week_start, week_end)
        network_diversity = await self._calculate_network_diversity(user_id, week_start, week_end)
        meeting_patterns = await self._analyze_meeting_patterns(user_id, week_start, week_end)
        
        return {
            'collaboration_score': collab_score,
            'top_collaborators': top_collaborators,
            'network_diversity': network_diversity,
            'meeting_patterns': meeting_patterns,
            'total_collaborators': len(top_collaborators)
        }
    
    async def _calculate_collaboration_score(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> float:
        """
        Calculate overall collaboration score (0-10).
        
        Based on:
        - Number of unique collaborators
        - Frequency of interaction
        - Diversity of collaboration types
        """
        
        # Neo4j query to get collaboration activities
        query = """
        MATCH (u:User {id: $user_id})-[r:COLLABORATED_WITH]->(c:User)
        WHERE r.date >= $start_date AND r.date <= $end_date
        RETURN count(DISTINCT c) as unique_collaborators,
               count(r) as total_interactions,
               collect(DISTINCT r.type) as interaction_types
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(
                query,
                user_id=user_id,
                start_date=week_start.isoformat(),
                end_date=week_end.isoformat()
            )
            record = await result.single()
            
            if not record:
                return 0.0
            
            unique_collaborators = record['unique_collaborators'] or 0
            total_interactions = record['total_interactions'] or 0
            interaction_types = record['interaction_types'] or []
            
            # Score formula (0-10):
            # - 3 points for number of collaborators (up to 10+)
            # - 4 points for frequency (up to 20+ interactions)
            # - 3 points for diversity (up to 3+ types)
            
            collab_score = min(unique_collaborators / 10 * 3, 3.0)
            freq_score = min(total_interactions / 20 * 4, 4.0)
            diversity_score = min(len(interaction_types) / 3 * 3, 3.0)
            
            total_score = collab_score + freq_score + diversity_score
            
            logger.info(
                "Calculated collaboration score",
                user_id=user_id,
                score=total_score,
                collaborators=unique_collaborators,
                interactions=total_interactions
            )
            
            return round(total_score, 1)
    
    async def _get_top_collaborators(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> List[Dict[str, Any]]:
        """Get top collaborators by interaction count."""
        
        query = """
        MATCH (u:User {id: $user_id})-[r:COLLABORATED_WITH]->(c:User)
        WHERE r.date >= $start_date AND r.date <= $end_date
        RETURN c.id as collaborator_id,
               c.name as collaborator_name,
               count(r) as interaction_count,
               collect(DISTINCT r.type) as interaction_types
        ORDER BY interaction_count DESC
        LIMIT 10
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(
                query,
                user_id=user_id,
                start_date=week_start.isoformat(),
                end_date=week_end.isoformat()
            )
            records = await result.fetch()
            
            collaborators = []
            for record in records:
                collaborators.append({
                    'id': record['collaborator_id'],
                    'name': record['collaborator_name'],
                    'interaction_count': record['interaction_count'],
                    'interaction_types': record['interaction_types']
                })
            
            return collaborators
    
    async def _calculate_network_diversity(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """
        Calculate network diversity.
        
        Measures:
        - Cross-team collaboration
        - Cross-role collaboration
        - Geographic diversity
        """
        
        query = """
        MATCH (u:User {id: $user_id})-[r:COLLABORATED_WITH]->(c:User)
        WHERE r.date >= $start_date AND r.date <= $end_date
        RETURN count(DISTINCT c.team) as unique_teams,
               count(DISTINCT c.role) as unique_roles,
               count(DISTINCT c.location) as unique_locations
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(
                query,
                user_id=user_id,
                start_date=week_start.isoformat(),
                end_date=week_end.isoformat()
            )
            record = await result.single()
            
            if not record:
                return {'diversity_score': 0.0}
            
            # Diversity score (0-10)
            team_diversity = min((record['unique_teams'] or 0) / 3 * 3.3, 3.3)
            role_diversity = min((record['unique_roles'] or 0) / 3 * 3.3, 3.3)
            location_diversity = min((record['unique_locations'] or 0) / 3 * 3.3, 3.3)
            
            diversity_score = team_diversity + role_diversity + location_diversity
            
            return {
                'diversity_score': round(diversity_score, 1),
                'unique_teams': record['unique_teams'] or 0,
                'unique_roles': record['unique_roles'] or 0,
                'unique_locations': record['unique_locations'] or 0
            }
    
    async def _analyze_meeting_patterns(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """Analyze meeting patterns within collaboration."""
        
        query = """
        MATCH (u:User {id: $user_id})-[r:ATTENDED_MEETING]->(m:Meeting)
        WHERE m.date >= $start_date AND m.date <= $end_date
        RETURN count(m) as total_meetings,
               avg(m.duration_minutes) as avg_duration,
               avg(size((m)<-[:ATTENDED_MEETING]-(:User))) as avg_attendees
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(
                query,
                user_id=user_id,
                start_date=week_start.isoformat(),
                end_date=week_end.isoformat()
            )
            record = await result.single()
            
            if not record:
                return {'total_meetings': 0}
            
            return {
                'total_meetings': record['total_meetings'] or 0,
                'avg_duration_minutes': round(record['avg_duration'] or 0, 1),
                'avg_attendees': round(record['avg_attendees'] or 0, 1)
            }
