"""
Skills Analytics Service for Month 6 Personal Analytics.

Analyzes:
- Skills mastery levels
- Growth trajectories
- Recency scores
- Skill recommendations
- Hours per skill

Uses Neo4j knowledge graph for skill relationships.
"""

from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from database.neo4j_client import get_neo4j_driver
from database.redis_client import get_redis_client

logger = structlog.get_logger(__name__)


class SkillAnalyticsService:
    """Service for skills mastery analysis."""
    
    CACHE_TTL_SECONDS = 86400  # 24 hours
    
    # Mastery levels (hours required)
    MASTERY_LEVELS = {
        'beginner': 0,
        'intermediate': 20,
        'advanced': 50,
        'expert': 100,
        'master': 200
    }
    
    def __init__(
        self,
        db: AsyncSession,
        neo4j_driver=None,
        redis_client=None
    ):
        """
        Initialize skills analytics service.
        
        Args:
            db: SQLAlchemy async session
            neo4j_driver: Neo4j driver instance
            redis_client: Redis client for caching
        """
        self.db = db
        self.neo4j = neo4j_driver or get_neo4j_driver()
        self.redis = redis_client or get_redis()
    
    async def get_weekly_skills(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> Dict[str, Any]:
        """
        Get skills analytics for a week.
        
        Args:
            user_id: User UUID
            week_start: Start of week
            week_end: End of week
        
        Returns:
            Dict with skills metrics
        """
        logger.info(
            "Analyzing weekly skills",
            user_id=user_id,
            week_start=week_start.isoformat(),
            week_end=week_end.isoformat()
        )
        
        # Query Neo4j for skills data
        top_skills = await self._get_top_skills(user_id, week_start, week_end)
        mastery_levels = await self._calculate_mastery_levels(user_id)
        growth_trajectories = await self._analyze_growth_trajectories(user_id, week_start, week_end)
        skill_recommendations = await self._get_skill_recommendations(user_id)
        
        return {
            'top_skills_this_week': top_skills,
            'mastery_levels': mastery_levels,
            'growth_trajectories': growth_trajectories,
            'recommendations': skill_recommendations,
            'total_skills_practiced': len(top_skills)
        }
    
    async def _get_top_skills(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> List[Dict[str, Any]]:
        """Get top skills worked on this week."""
        
        query = """
        MATCH (u:User {id: $user_id})-[r:WORKED_ON]->(s:Skill)
        WHERE r.date >= $start_date AND r.date <= $end_date
        RETURN s.id as skill_id,
               s.name as skill_name,
               s.category as category,
               sum(r.hours) as total_hours,
               count(r) as practice_sessions
        ORDER BY total_hours DESC
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
            
            skills = []
            for record in records:
                skills.append({
                    'id': record['skill_id'],
                    'name': record['skill_name'],
                    'category': record['category'],
                    'hours_this_week': round(record['total_hours'], 1),
                    'practice_sessions': record['practice_sessions']
                })
            
            return skills
    
    async def _calculate_mastery_levels(
        self,
        user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Calculate mastery level for each skill.
        
        Mastery based on cumulative hours:
        - Beginner: 0-20h
        - Intermediate: 20-50h
        - Advanced: 50-100h
        - Expert: 100-200h
        - Master: 200+h
        """
        
        query = """
        MATCH (u:User {id: $user_id})-[r:WORKED_ON]->(s:Skill)
        RETURN s.id as skill_id,
               s.name as skill_name,
               sum(r.hours) as total_hours
        ORDER BY total_hours DESC
        LIMIT 20
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(query, user_id=user_id)
            records = await result.fetch()
            
            skills_with_mastery = []
            for record in records:
                total_hours = record['total_hours']
                mastery_level = self._hours_to_mastery_level(total_hours)
                progress_to_next = self._calculate_progress_to_next_level(total_hours, mastery_level)
                
                skills_with_mastery.append({
                    'skill_id': record['skill_id'],
                    'skill_name': record['skill_name'],
                    'total_hours': round(total_hours, 1),
                    'mastery_level': mastery_level,
                    'progress_to_next_level_pct': progress_to_next
                })
            
            return skills_with_mastery
    
    def _hours_to_mastery_level(self, hours: float) -> str:
        """Convert hours to mastery level."""
        if hours >= self.MASTERY_LEVELS['master']:
            return 'master'
        elif hours >= self.MASTERY_LEVELS['expert']:
            return 'expert'
        elif hours >= self.MASTERY_LEVELS['advanced']:
            return 'advanced'
        elif hours >= self.MASTERY_LEVELS['intermediate']:
            return 'intermediate'
        else:
            return 'beginner'
    
    def _calculate_progress_to_next_level(self, hours: float, current_level: str) -> float:
        """Calculate progress percentage to next mastery level."""
        level_order = ['beginner', 'intermediate', 'advanced', 'expert', 'master']
        current_idx = level_order.index(current_level)
        
        if current_idx >= len(level_order) - 1:
            return 100.0  # Already at max level
        
        current_threshold = self.MASTERY_LEVELS[current_level]
        next_level = level_order[current_idx + 1]
        next_threshold = self.MASTERY_LEVELS[next_level]
        
        progress = (hours - current_threshold) / (next_threshold - current_threshold) * 100
        return round(min(progress, 100.0), 1)
    
    async def _analyze_growth_trajectories(
        self,
        user_id: str,
        week_start: date,
        week_end: date
    ) -> List[Dict[str, Any]]:
        """
        Analyze growth trajectory for top skills.
        
        Compares this week's hours to previous weeks.
        """
        
        # Get previous 4 weeks for comparison
        prev_week_start = week_start - timedelta(days=28)
        
        query = """
        MATCH (u:User {id: $user_id})-[r:WORKED_ON]->(s:Skill)
        WHERE r.date >= $prev_start AND r.date <= $week_end
        WITH s,
             sum(CASE WHEN r.date >= $week_start THEN r.hours ELSE 0 END) as hours_this_week,
             sum(CASE WHEN r.date < $week_start THEN r.hours ELSE 0 END) / 4.0 as avg_hours_prev_weeks
        WHERE hours_this_week > 0
        RETURN s.id as skill_id,
               s.name as skill_name,
               hours_this_week,
               avg_hours_prev_weeks,
               CASE 
                   WHEN avg_hours_prev_weeks = 0 THEN 100.0
                   ELSE ((hours_this_week - avg_hours_prev_weeks) / avg_hours_prev_weeks * 100)
               END as growth_pct
        ORDER BY abs(growth_pct) DESC
        LIMIT 10
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(
                query,
                user_id=user_id,
                week_start=week_start.isoformat(),
                week_end=week_end.isoformat(),
                prev_start=prev_week_start.isoformat()
            )
            records = await result.fetch()
            
            trajectories = []
            for record in records:
                growth_pct = record['growth_pct']
                trend = 'increasing' if growth_pct > 10 else 'decreasing' if growth_pct < -10 else 'stable'
                
                trajectories.append({
                    'skill_id': record['skill_id'],
                    'skill_name': record['skill_name'],
                    'hours_this_week': round(record['hours_this_week'], 1),
                    'avg_hours_prev_weeks': round(record['avg_hours_prev_weeks'], 1),
                    'growth_pct': round(growth_pct, 1),
                    'trend': trend
                })
            
            return trajectories
    
    async def _get_skill_recommendations(
        self,
        user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get skill recommendations based on:
        - Related skills to current expertise
        - Skills used by collaborators
        - Skills needed for project goals
        """
        
        query = """
        MATCH (u:User {id: $user_id})-[:WORKED_ON]->(s1:Skill)
        MATCH (s1)-[:RELATED_TO]->(s2:Skill)
        WHERE NOT (u)-[:WORKED_ON]->(s2)
        WITH s2, count(*) as relevance_score
        RETURN s2.id as skill_id,
               s2.name as skill_name,
               s2.category as category,
               relevance_score
        ORDER BY relevance_score DESC
        LIMIT 5
        """
        
        async with self.neo4j.session() as session:
            result = await session.run(query, user_id=user_id)
            records = await result.fetch()
            
            recommendations = []
            for record in records:
                recommendations.append({
                    'skill_id': record['skill_id'],
                    'skill_name': record['skill_name'],
                    'category': record['category'],
                    'relevance_score': record['relevance_score'],
                    'reason': 'Related to your current skills'
                })
            
            return recommendations
