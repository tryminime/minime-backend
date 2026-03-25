"""
Career Development Service

Provides career trajectory and development insights:
- Career trajectory analysis (skill growth over time)
- Role readiness scoring
- Skill gap analysis vs target career paths
- Growth velocity tracking (learning rate)
- Career milestone detection
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime, timedelta, date
from collections import defaultdict
import math
import structlog

logger = structlog.get_logger()


# ============================================================================
# CAREER PATH DEFINITIONS
# ============================================================================

CAREER_PATHS = {
    'fullstack_developer': {
        'title': 'Full-Stack Developer',
        'required_skills': {
            'javascript', 'typescript', 'python', 'html', 'css',
            'react', 'nodejs', 'sql', 'git', 'docker',
        },
        'nice_to_have': {'kubernetes', 'aws', 'graphql', 'redis', 'testing'},
        'min_hours': 500,
    },
    'ml_engineer': {
        'title': 'ML Engineer',
        'required_skills': {
            'python', 'machine_learning', 'deep_learning', 'pytorch',
            'numpy', 'pandas', 'sql', 'statistics', 'git',
        },
        'nice_to_have': {'mlops', 'docker', 'kubernetes', 'spark', 'tensorflow'},
        'min_hours': 600,
    },
    'data_scientist': {
        'title': 'Data Scientist',
        'required_skills': {
            'python', 'statistics', 'machine_learning', 'sql',
            'pandas', 'numpy', 'data_visualization', 'r',
        },
        'nice_to_have': {'deep_learning', 'spark', 'tableau', 'a_b_testing'},
        'min_hours': 500,
    },
    'devops_engineer': {
        'title': 'DevOps Engineer',
        'required_skills': {
            'linux', 'docker', 'kubernetes', 'ci_cd', 'terraform',
            'aws', 'python', 'bash', 'monitoring', 'git',
        },
        'nice_to_have': {'ansible', 'helm', 'prometheus', 'grafana', 'security'},
        'min_hours': 500,
    },
    'tech_lead': {
        'title': 'Tech Lead',
        'required_skills': {
            'system_design', 'code_review', 'mentoring', 'architecture',
            'project_management', 'communication', 'python', 'git',
        },
        'nice_to_have': {'agile', 'documentation', 'hiring', 'strategy'},
        'min_hours': 1000,
    },
}

CAREER_MILESTONES = [
    {'hours': 10, 'name': 'Getting Started', 'icon': '🌱'},
    {'hours': 50, 'name': 'Apprentice', 'icon': '📖'},
    {'hours': 100, 'name': 'Practitioner', 'icon': '🔧'},
    {'hours': 250, 'name': 'Skilled', 'icon': '⚡'},
    {'hours': 500, 'name': 'Professional', 'icon': '💼'},
    {'hours': 1000, 'name': 'Expert', 'icon': '🏆'},
    {'hours': 2500, 'name': 'Master', 'icon': '👑'},
    {'hours': 5000, 'name': 'Grandmaster', 'icon': '🌟'},
]


class CareerDevelopmentService:
    """
    Service for career trajectory analysis and development insights.
    """

    def __init__(self):
        pass

    # ========================================================================
    # CAREER TRAJECTORY
    # ========================================================================

    def analyze_career_trajectory(
        self,
        skill_history: List[Dict[str, Any]],
        total_hours: float = 0,
    ) -> Dict[str, Any]:
        """
        Analyze career trajectory based on skill history.

        Args:
            skill_history: List of {skill, hours, period, growth_rate}
            total_hours: Total tracked career hours

        Returns:
            Trajectory analysis with trends, velocity, and phase
        """
        if not skill_history:
            return {
                'phase': 'exploration',
                'phase_description': 'Early stage — exploring different skills',
                'total_hours': total_hours,
                'skill_count': 0,
                'top_domains': [],
                'growth_velocity': 0,
                'trajectory': 'neutral',
            }

        # Group skills by domain
        domains = self._group_by_domain(skill_history)

        # Compute growth velocity
        growth_rates = [s.get('growth_rate', 0) for s in skill_history if s.get('growth_rate')]
        avg_growth = sum(growth_rates) / len(growth_rates) if growth_rates else 0

        # Determine career phase
        phase = self._determine_phase(total_hours, len(skill_history), domains)

        # Trajectory direction
        if avg_growth > 10:
            trajectory = 'accelerating'
        elif avg_growth > 0:
            trajectory = 'growing'
        elif avg_growth > -5:
            trajectory = 'stable'
        else:
            trajectory = 'declining'

        # Top skills by hours
        sorted_skills = sorted(skill_history, key=lambda x: x.get('hours', 0), reverse=True)

        return {
            'phase': phase['name'],
            'phase_description': phase['description'],
            'total_hours': total_hours,
            'skill_count': len(skill_history),
            'top_skills': [
                {'skill': s['skill'], 'hours': s.get('hours', 0)}
                for s in sorted_skills[:5]
            ],
            'top_domains': list(domains.keys())[:3],
            'growth_velocity': round(avg_growth, 2),
            'trajectory': trajectory,
        }

    # ========================================================================
    # ROLE READINESS
    # ========================================================================

    def assess_role_readiness(
        self,
        user_skills: Dict[str, float],
        target_role: str,
    ) -> Dict[str, Any]:
        """
        Score readiness for a target role.

        Args:
            user_skills: {skill_name: hours_invested}
            target_role: Key from CAREER_PATHS

        Returns:
            Readiness assessment with score, gaps, and recommendations
        """
        path = CAREER_PATHS.get(target_role)
        if path is None:
            return {
                'error': f'Unknown role: {target_role}',
                'available_roles': list(CAREER_PATHS.keys()),
            }

        user_skill_set = set(s.lower() for s in user_skills.keys())
        required = path['required_skills']
        nice_to_have = path['nice_to_have']

        # Required skill coverage
        covered_required = required & user_skill_set
        missing_required = required - user_skill_set
        required_pct = len(covered_required) / max(len(required), 1)

        # Nice-to-have coverage
        covered_nice = nice_to_have & user_skill_set
        nice_pct = len(covered_nice) / max(len(nice_to_have), 1)

        # Hours readiness
        total_hours = sum(user_skills.values())
        hours_pct = min(total_hours / max(path['min_hours'], 1), 1.0)

        # Overall readiness (weighted)
        readiness = (required_pct * 0.5 + hours_pct * 0.3 + nice_pct * 0.2)

        # Determine readiness level
        if readiness >= 0.85:
            level = 'ready'
            level_label = '🟢 Ready'
        elif readiness >= 0.6:
            level = 'almost_ready'
            level_label = '🟡 Almost Ready'
        elif readiness >= 0.3:
            level = 'developing'
            level_label = '🟠 Developing'
        else:
            level = 'early'
            level_label = '🔴 Early Stage'

        # Prioritized skill gaps
        skill_gaps = []
        for skill in sorted(missing_required):
            skill_gaps.append({
                'skill': skill,
                'priority': 'required',
                'estimated_hours': 50,
            })
        for skill in sorted(nice_to_have - user_skill_set):
            skill_gaps.append({
                'skill': skill,
                'priority': 'recommended',
                'estimated_hours': 30,
            })

        return {
            'target_role': path['title'],
            'readiness_score': round(readiness * 100, 1),
            'readiness_level': level,
            'readiness_label': level_label,
            'required_skills_covered': len(covered_required),
            'required_skills_total': len(required),
            'required_coverage_pct': round(required_pct * 100, 1),
            'nice_to_have_covered': len(covered_nice),
            'nice_to_have_total': len(nice_to_have),
            'total_hours': total_hours,
            'target_hours': path['min_hours'],
            'skill_gaps': skill_gaps,
        }

    # ========================================================================
    # SKILL GAP ANALYSIS
    # ========================================================================

    def analyze_skill_gaps(
        self,
        user_skills: Dict[str, float],
        target_roles: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Analyze skill gaps across one or more target roles.

        Args:
            user_skills: {skill: hours}
            target_roles: Roles to compare against (all if None)

        Returns:
            Gap analysis with prioritized recommendations
        """
        roles = target_roles or list(CAREER_PATHS.keys())
        user_skill_set = set(s.lower() for s in user_skills.keys())

        all_gaps: Dict[str, Dict[str, Any]] = {}
        role_results = []

        for role_key in roles:
            path = CAREER_PATHS.get(role_key)
            if not path:
                continue

            required = path['required_skills']
            missing = required - user_skill_set

            role_results.append({
                'role': path['title'],
                'role_key': role_key,
                'coverage': round(len(required - missing) / max(len(required), 1) * 100, 1),
                'missing_count': len(missing),
            })

            for skill in missing:
                if skill not in all_gaps:
                    all_gaps[skill] = {
                        'skill': skill,
                        'required_by': [],
                        'priority_score': 0,
                    }
                all_gaps[skill]['required_by'].append(path['title'])
                all_gaps[skill]['priority_score'] += 1

        # Sort gaps by how many roles need them
        prioritized = sorted(all_gaps.values(), key=lambda x: x['priority_score'], reverse=True)

        return {
            'user_skill_count': len(user_skills),
            'roles_analyzed': len(role_results),
            'role_coverage': role_results,
            'prioritized_gaps': prioritized[:10],
            'most_versatile_skills': [g['skill'] for g in prioritized[:5]],
        }

    # ========================================================================
    # GROWTH VELOCITY
    # ========================================================================

    def calculate_growth_velocity(
        self,
        weekly_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Track learning rate and growth velocity over time.

        Args:
            weekly_data: List of {week, hours, skills_used, new_skills}

        Returns:
            Growth velocity metrics
        """
        if not weekly_data:
            return {
                'current_velocity': 0,
                'avg_velocity': 0,
                'trend': 'neutral',
                'weeks_tracked': 0,
                'total_hours': 0,
                'total_new_skills': 0,
            }

        total_hours = sum(w.get('hours', 0) for w in weekly_data)
        total_new_skills = sum(w.get('new_skills', 0) for w in weekly_data)
        weeks = len(weekly_data)

        # Compute weekly velocities
        velocities = []
        for w in weekly_data:
            hours = w.get('hours', 0)
            skills = w.get('skills_used', 1)
            velocity = hours * (1 + 0.1 * skills)  # More diverse = higher velocity
            velocities.append(velocity)

        avg_velocity = sum(velocities) / weeks
        current_velocity = velocities[-1] if velocities else 0

        # Trend: compare recent vs earlier
        if weeks >= 4:
            recent = sum(velocities[-2:]) / 2
            earlier = sum(velocities[:2]) / 2
            if earlier > 0:
                change = (recent - earlier) / earlier
                if change > 0.1:
                    trend = 'accelerating'
                elif change < -0.1:
                    trend = 'decelerating'
                else:
                    trend = 'steady'
            else:
                trend = 'starting'
        else:
            trend = 'insufficient_data'

        return {
            'current_velocity': round(current_velocity, 2),
            'avg_velocity': round(avg_velocity, 2),
            'trend': trend,
            'weeks_tracked': weeks,
            'total_hours': round(total_hours, 1),
            'total_new_skills': total_new_skills,
            'avg_hours_per_week': round(total_hours / weeks, 1),
        }

    # ========================================================================
    # MILESTONES
    # ========================================================================

    def detect_milestones(
        self,
        total_hours: float,
        previous_hours: float = 0,
    ) -> List[Dict[str, Any]]:
        """
        Detect career milestones crossed.

        Args:
            total_hours: Current total hours
            previous_hours: Hours at last check

        Returns:
            List of newly achieved milestones
        """
        achieved = []
        for ms in CAREER_MILESTONES:
            if total_hours >= ms['hours'] and previous_hours < ms['hours']:
                achieved.append({
                    'name': ms['name'],
                    'icon': ms['icon'],
                    'hours_required': ms['hours'],
                    'achieved_at': total_hours,
                })

        return achieved

    def get_current_milestone(self, total_hours: float) -> Dict[str, Any]:
        """Get the current milestone level."""
        current = CAREER_MILESTONES[0]
        next_milestone = CAREER_MILESTONES[1] if len(CAREER_MILESTONES) > 1 else None

        for i, ms in enumerate(CAREER_MILESTONES):
            if total_hours >= ms['hours']:
                current = ms
                next_milestone = CAREER_MILESTONES[i + 1] if i + 1 < len(CAREER_MILESTONES) else None

        progress = 0
        if next_milestone:
            range_size = next_milestone['hours'] - current['hours']
            if range_size > 0:
                progress = (total_hours - current['hours']) / range_size * 100

        return {
            'current': current,
            'next': next_milestone,
            'progress_to_next': round(min(progress, 100), 1),
            'total_hours': total_hours,
        }

    # ========================================================================
    # COMPREHENSIVE REPORT
    # ========================================================================

    def generate_career_report(
        self,
        user_skills: Dict[str, float],
        skill_history: Optional[List[Dict[str, Any]]] = None,
        weekly_data: Optional[List[Dict[str, Any]]] = None,
        target_roles: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Generate a comprehensive career development report."""
        total_hours = sum(user_skills.values())

        # Trajectory
        trajectory = self.analyze_career_trajectory(
            skill_history or [{'skill': k, 'hours': v} for k, v in user_skills.items()],
            total_hours=total_hours,
        )

        # Growth velocity
        velocity = self.calculate_growth_velocity(weekly_data or [])

        # Skill gaps
        gaps = self.analyze_skill_gaps(user_skills, target_roles)

        # Milestones
        milestone = self.get_current_milestone(total_hours)

        # Best-fit role
        best_fit = None
        best_score = 0
        for role_key in CAREER_PATHS:
            assessment = self.assess_role_readiness(user_skills, role_key)
            if assessment.get('readiness_score', 0) > best_score:
                best_score = assessment['readiness_score']
                best_fit = assessment

        return {
            'trajectory': trajectory,
            'growth_velocity': velocity,
            'skill_gaps': gaps,
            'milestone': milestone,
            'best_fit_role': best_fit,
            'total_hours': total_hours,
            'total_skills': len(user_skills),
        }

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _group_by_domain(self, skill_history: List[Dict[str, Any]]) -> Dict[str, float]:
        """Group skills into domains with total hours."""
        DOMAIN_MAP = {
            'python': 'backend', 'javascript': 'frontend', 'typescript': 'frontend',
            'react': 'frontend', 'nodejs': 'backend', 'fastapi': 'backend',
            'sql': 'data', 'pandas': 'data', 'numpy': 'data',
            'machine_learning': 'ml', 'deep_learning': 'ml', 'pytorch': 'ml',
            'docker': 'devops', 'kubernetes': 'devops', 'terraform': 'devops',
            'aws': 'cloud', 'gcp': 'cloud', 'azure': 'cloud',
        }

        domains: Dict[str, float] = defaultdict(float)
        for entry in skill_history:
            skill = entry.get('skill', '').lower()
            domain = DOMAIN_MAP.get(skill, 'other')
            domains[domain] += entry.get('hours', 0)

        return dict(sorted(domains.items(), key=lambda x: x[1], reverse=True))

    def _determine_phase(
        self,
        total_hours: float,
        skill_count: int,
        domains: Dict[str, float],
    ) -> Dict[str, str]:
        """Determine career development phase."""
        if total_hours < 100:
            return {'name': 'exploration', 'description': 'Exploring different skills and domains'}
        elif total_hours < 500:
            if len(domains) > 3:
                return {'name': 'generalist', 'description': 'Building breadth across multiple domains'}
            return {'name': 'focusing', 'description': 'Narrowing focus to key areas'}
        elif total_hours < 1500:
            return {'name': 'specializing', 'description': 'Deepening expertise in core areas'}
        elif total_hours < 3000:
            return {'name': 'mastering', 'description': 'Achieving mastery in primary domain'}
        else:
            return {'name': 'leading', 'description': 'Expert-level contributor and mentor'}


# Global instance
career_development_service = CareerDevelopmentService()
