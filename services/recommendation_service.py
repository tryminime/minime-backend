"""
Recommendation Service

AI-powered recommendation engine:
- Tool/app recommendations based on usage patterns
- Workflow optimization suggestions
- Collaboration partner suggestions
- Skill-based learning recommendations
- Time management recommendations
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from collections import defaultdict, Counter
import math
import structlog

logger = structlog.get_logger()


# ============================================================================
# RECOMMENDATION CATEGORIES
# ============================================================================

RECOMMENDATION_TYPES = {
    'tool': {'icon': '🔧', 'label': 'Tool Recommendation'},
    'workflow': {'icon': '⚡', 'label': 'Workflow Optimization'},
    'collaboration': {'icon': '🤝', 'label': 'Collaboration'},
    'learning': {'icon': '📚', 'label': 'Learning'},
    'time_management': {'icon': '⏰', 'label': 'Time Management'},
    'focus': {'icon': '🎯', 'label': 'Focus'},
}

# ============================================================================
# TOOL & WORKFLOW KNOWLEDGE BASE
# ============================================================================

TOOL_SUGGESTIONS = {
    'python': [
        {'name': 'PyCharm', 'reason': 'Full-featured Python IDE with debugging and refactoring'},
        {'name': 'Black', 'reason': 'Auto-formats Python code for consistency'},
        {'name': 'pytest', 'reason': 'Powerful testing framework'},
        {'name': 'mypy', 'reason': 'Static type checking for Python'},
    ],
    'javascript': [
        {'name': 'ESLint', 'reason': 'Catches common JS errors and enforces style'},
        {'name': 'Prettier', 'reason': 'Auto-formats JS/TS/CSS code'},
        {'name': 'Vite', 'reason': 'Blazing fast build tool for modern web projects'},
    ],
    'typescript': [
        {'name': 'ts-node', 'reason': 'Run TypeScript directly without compilation step'},
        {'name': 'zod', 'reason': 'TypeScript-first schema validation'},
    ],
    'writing': [
        {'name': 'Grammarly', 'reason': 'AI-powered writing assistant'},
        {'name': 'Hemingway Editor', 'reason': 'Simplifies complex writing'},
        {'name': 'Notion', 'reason': 'All-in-one workspace for notes and docs'},
    ],
    'data_science': [
        {'name': 'Jupyter Lab', 'reason': 'Interactive notebooks for data exploration'},
        {'name': 'pandas', 'reason': 'Data manipulation and analysis library'},
        {'name': 'DVC', 'reason': 'Version control for ML datasets and models'},
    ],
    'devops': [
        {'name': 'Terraform', 'reason': 'Infrastructure as code'},
        {'name': 'k9s', 'reason': 'Terminal-based Kubernetes dashboard'},
        {'name': 'Grafana', 'reason': 'Monitoring and observability platform'},
    ],
}

WORKFLOW_TEMPLATES = [
    {
        'id': 'pomodoro',
        'name': 'Pomodoro Technique',
        'description': '25-min focused work + 5-min breaks. 4 cycles then 15-min break.',
        'best_for': 'Tasks requiring sustained concentration',
        'productivity_gain': '20-30%',
    },
    {
        'id': 'time_blocking',
        'name': 'Time Blocking',
        'description': 'Schedule specific tasks into calendar blocks. Group similar tasks.',
        'best_for': 'Managing multiple projects or responsibilities',
        'productivity_gain': '15-25%',
    },
    {
        'id': 'eat_the_frog',
        'name': 'Eat the Frog',
        'description': 'Tackle the hardest/most important task first each morning.',
        'best_for': 'Procrastination and prioritization',
        'productivity_gain': '10-20%',
    },
    {
        'id': 'batching',
        'name': 'Task Batching',
        'description': 'Group similar tasks (emails, reviews, meetings) into single blocks.',
        'best_for': 'Reducing context switching',
        'productivity_gain': '15-30%',
    },
    {
        'id': 'two_minute_rule',
        'name': 'Two-Minute Rule',
        'description': 'If a task takes < 2 minutes, do it immediately.',
        'best_for': 'Managing small tasks and maintaining inbox zero',
        'productivity_gain': '5-10%',
    },
]


class RecommendationService:
    """
    Service for generating personalized recommendations.

    Analyzes user activity patterns to suggest tools, workflows,
    collaboration opportunities, and learning paths.
    """

    MAX_RECOMMENDATIONS = 10

    def __init__(self):
        pass

    # ========================================================================
    # TOOL RECOMMENDATIONS
    # ========================================================================

    def recommend_tools(
        self,
        user_skills: List[str],
        current_tools: Optional[List[str]] = None,
        activity_categories: Optional[Dict[str, float]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Recommend tools based on user's skills and current tool usage.

        Args:
            user_skills: List of skill/technology names
            current_tools: Tools already in use (to avoid re-recommending)
            activity_categories: Category → hours mapping

        Returns:
            List of tool recommendations with relevance scores
        """
        current = set(t.lower() for t in (current_tools or []))
        recommendations = []

        for skill in user_skills:
            skill_lower = skill.lower()
            # Check direct match or category match
            suggestions = TOOL_SUGGESTIONS.get(skill_lower, [])

            for tool in suggestions:
                if tool['name'].lower() in current:
                    continue

                relevance = 0.7  # Base relevance for skill match

                # Boost if this skill category has high activity
                if activity_categories:
                    hours = activity_categories.get(skill_lower, 0)
                    if hours > 10:
                        relevance += 0.2
                    elif hours > 5:
                        relevance += 0.1

                recommendations.append({
                    'type': 'tool',
                    'type_meta': RECOMMENDATION_TYPES['tool'],
                    'name': tool['name'],
                    'reason': tool['reason'],
                    'related_skill': skill,
                    'relevance': round(min(relevance, 1.0), 2),
                })

        # Sort by relevance
        recommendations.sort(key=lambda x: x['relevance'], reverse=True)
        return recommendations[:self.MAX_RECOMMENDATIONS]

    # ========================================================================
    # WORKFLOW RECOMMENDATIONS
    # ========================================================================

    def recommend_workflows(
        self,
        productivity_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Recommend workflows based on productivity patterns.

        Args:
            productivity_data: Dict with keys like:
                - avg_focus_score, deep_work_hours, meeting_hours,
                - context_switches, avg_session_length, total_hours

        Returns:
            List of workflow recommendations
        """
        recommendations = []
        avg_focus = productivity_data.get('avg_focus_score', 5)
        context_switches = productivity_data.get('context_switches', 0)
        avg_session = productivity_data.get('avg_session_length_min', 30)
        meeting_hours = productivity_data.get('meeting_hours', 0)
        deep_work_hours = productivity_data.get('deep_work_hours', 0)
        total_hours = productivity_data.get('total_hours', 8)

        # Low focus → Pomodoro
        if avg_focus < 6:
            template = next(w for w in WORKFLOW_TEMPLATES if w['id'] == 'pomodoro')
            recommendations.append({
                'type': 'workflow',
                'type_meta': RECOMMENDATION_TYPES['workflow'],
                'name': template['name'],
                'description': template['description'],
                'reason': f'Your average focus score is {avg_focus}/10. '
                         'Pomodoro can improve concentration significantly.',
                'expected_improvement': template['productivity_gain'],
                'relevance': round(min(0.9, 1 - avg_focus / 10), 2),
            })

        # High context switching → Batching
        if context_switches > 15:
            template = next(w for w in WORKFLOW_TEMPLATES if w['id'] == 'batching')
            recommendations.append({
                'type': 'workflow',
                'type_meta': RECOMMENDATION_TYPES['workflow'],
                'name': template['name'],
                'description': template['description'],
                'reason': f'You had {context_switches} context switches today. '
                         'Batching similar tasks reduces cognitive overhead.',
                'expected_improvement': template['productivity_gain'],
                'relevance': round(min(0.85, context_switches / 30), 2),
            })

        # Short sessions → Time Blocking
        if avg_session < 25:
            template = next(w for w in WORKFLOW_TEMPLATES if w['id'] == 'time_blocking')
            recommendations.append({
                'type': 'workflow',
                'type_meta': RECOMMENDATION_TYPES['workflow'],
                'name': template['name'],
                'description': template['description'],
                'reason': f'Your avg session is {avg_session:.0f} min. '
                         'Time blocking creates protected focus windows.',
                'expected_improvement': template['productivity_gain'],
                'relevance': round(min(0.8, 1 - avg_session / 60), 2),
            })

        # Weak morning start → Eat the Frog
        morning_focus = productivity_data.get('morning_focus_score', avg_focus)
        if morning_focus < avg_focus * 0.8:
            template = next(w for w in WORKFLOW_TEMPLATES if w['id'] == 'eat_the_frog')
            recommendations.append({
                'type': 'workflow',
                'type_meta': RECOMMENDATION_TYPES['workflow'],
                'name': template['name'],
                'description': template['description'],
                'reason': 'Your morning focus is lower than your average. '
                         'Starting with the hardest task can set a productive tone.',
                'expected_improvement': template['productivity_gain'],
                'relevance': 0.65,
            })

        # Low deep-work ratio → Time Blocking
        if total_hours > 0 and deep_work_hours / total_hours < 0.3:
            if not any(r['name'] == 'Time Blocking' for r in recommendations):
                template = next(w for w in WORKFLOW_TEMPLATES if w['id'] == 'time_blocking')
                recommendations.append({
                    'type': 'workflow',
                    'type_meta': RECOMMENDATION_TYPES['workflow'],
                    'name': template['name'],
                    'description': template['description'],
                    'reason': f'Only {deep_work_hours/total_hours:.0%} of your time is deep work. '
                             'Block dedicated focus time in your calendar.',
                    'expected_improvement': template['productivity_gain'],
                    'relevance': 0.75,
                })

        recommendations.sort(key=lambda x: x['relevance'], reverse=True)
        return recommendations

    # ========================================================================
    # COLLABORATION RECOMMENDATIONS
    # ========================================================================

    def recommend_collaborators(
        self,
        user_skills: Set[str],
        team_profiles: List[Dict[str, Any]],
        current_collaborators: Optional[Set[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Suggest potential collaborators based on skill complementarity.

        Args:
            user_skills: Set of the user's skills
            team_profiles: List of {user_id, name, skills: [...]}
            current_collaborators: IDs of people already collaborating with

        Returns:
            List of collaboration suggestions
        """
        current = current_collaborators or set()
        recommendations = []

        for profile in team_profiles:
            member_id = profile.get('user_id', '')
            if member_id in current:
                continue

            member_skills = set(profile.get('skills', []))
            if not member_skills:
                continue

            # Complementary skills (they have what you don't)
            complementary = member_skills - user_skills
            # Shared skills (common ground)
            shared = member_skills & user_skills

            if not complementary and not shared:
                continue

            # Score: value complementary skills more
            complementary_score = len(complementary) / max(len(member_skills), 1) * 0.7
            shared_score = len(shared) / max(len(user_skills), 1) * 0.3
            relevance = complementary_score + shared_score

            recommendations.append({
                'type': 'collaboration',
                'type_meta': RECOMMENDATION_TYPES['collaboration'],
                'user_id': member_id,
                'name': profile.get('name', 'Team Member'),
                'complementary_skills': sorted(complementary),
                'shared_skills': sorted(shared),
                'reason': f'Has {len(complementary)} complementary skill(s) '
                         f'and {len(shared)} shared skill(s)',
                'relevance': round(min(relevance, 1.0), 2),
            })

        recommendations.sort(key=lambda x: x['relevance'], reverse=True)
        return recommendations[:self.MAX_RECOMMENDATIONS]

    # ========================================================================
    # LEARNING RECOMMENDATIONS
    # ========================================================================

    def recommend_learning(
        self,
        user_skills: List[str],
        skill_scores: Optional[Dict[str, float]] = None,
        career_goals: Optional[List[str]] = None,
        trending_skills: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Recommend skills/topics to learn based on current profile and goals.

        Args:
            user_skills: Current skills
            skill_scores: Skill → proficiency (0-100) mapping
            career_goals: Target roles/skills
            trending_skills: Currently trending skills in the field

        Returns:
            List of learning recommendations
        """
        scores = skill_scores or {}
        goals = set(career_goals or [])
        trending = set(trending_skills or [])
        current = set(s.lower() for s in user_skills)

        recommendations = []

        # 1. Skills in career goals not yet acquired
        for goal_skill in goals:
            if goal_skill.lower() not in current:
                recommendations.append({
                    'type': 'learning',
                    'type_meta': RECOMMENDATION_TYPES['learning'],
                    'skill': goal_skill,
                    'reason': f'Required for your career goal. Not yet in your skillset.',
                    'priority': 'high',
                    'relevance': 0.95,
                })
            elif scores.get(goal_skill, 0) < 50:
                recommendations.append({
                    'type': 'learning',
                    'type_meta': RECOMMENDATION_TYPES['learning'],
                    'skill': goal_skill,
                    'reason': f'In your goals but proficiency is only '
                             f'{scores.get(goal_skill, 0):.0f}/100. Worth deepening.',
                    'priority': 'high',
                    'relevance': 0.85,
                })

        # 2. Trending skills not yet acquired
        for trend_skill in trending:
            if trend_skill.lower() not in current:
                recommendations.append({
                    'type': 'learning',
                    'type_meta': RECOMMENDATION_TYPES['learning'],
                    'skill': trend_skill,
                    'reason': f'Trending in your field. Consider adding to your toolkit.',
                    'priority': 'medium',
                    'relevance': 0.7,
                })

        # 3. Low-proficiency skills worth improving
        for skill, score in scores.items():
            if score < 30 and skill.lower() in current:
                recommendations.append({
                    'type': 'learning',
                    'type_meta': RECOMMENDATION_TYPES['learning'],
                    'skill': skill,
                    'reason': f'Your proficiency ({score:.0f}/100) is low. '
                             'A focused learning session could help.',
                    'priority': 'low',
                    'relevance': round(0.4 + (30 - score) / 100, 2),
                })

        # Deduplicate by skill
        seen_skills: Set[str] = set()
        unique_recs = []
        for rec in recommendations:
            skill = rec['skill'].lower()
            if skill not in seen_skills:
                seen_skills.add(skill)
                unique_recs.append(rec)

        unique_recs.sort(key=lambda x: x['relevance'], reverse=True)
        return unique_recs[:self.MAX_RECOMMENDATIONS]

    # ========================================================================
    # TIME MANAGEMENT RECOMMENDATIONS
    # ========================================================================

    def recommend_time_management(
        self,
        time_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """
        Generate time management recommendations.

        Args:
            time_data: Dict with keys like:
                - peak_hours, total_hours, meeting_hours,
                - break_frequency, longest_continuous_session

        Returns:
            List of time management recommendations
        """
        recommendations = []

        peak_hours = time_data.get('peak_hours', [])
        total_hours = time_data.get('total_hours', 8)
        longest_session = time_data.get('longest_continuous_session_min', 60)
        break_count = time_data.get('break_count', 0)
        meeting_hours = time_data.get('meeting_hours', 0)

        # No breaks detected
        if break_count == 0 and total_hours > 4:
            recommendations.append({
                'type': 'time_management',
                'type_meta': RECOMMENDATION_TYPES['time_management'],
                'title': 'Take regular breaks',
                'description': 'No breaks detected today. Studies show '
                              'regular breaks improve productivity by 15-25%.',
                'action': 'Set a timer for 25-minute work sessions followed by 5-minute breaks.',
                'relevance': 0.9,
            })

        # Very long continuous session
        if longest_session > 120:
            recommendations.append({
                'type': 'time_management',
                'type_meta': RECOMMENDATION_TYPES['time_management'],
                'title': 'Break up long sessions',
                'description': f'Your longest session was {longest_session:.0f} min. '
                              'Cognitive performance drops after 90 minutes.',
                'action': 'Try the 90/20 rule: 90 min focus, 20 min recovery.',
                'relevance': 0.8,
            })

        # Peak hours suggestion
        if peak_hours:
            peak_str = ', '.join(f'{h}:00' for h in peak_hours[:3])
            recommendations.append({
                'type': 'time_management',
                'type_meta': RECOMMENDATION_TYPES['time_management'],
                'title': 'Leverage your peak hours',
                'description': f'Your peak productivity hours are: {peak_str}.',
                'action': 'Schedule your most important tasks during these hours.',
                'relevance': 0.65,
            })

        # Meeting-heavy day
        if meeting_hours > 4 and total_hours > 0:
            meeting_pct = meeting_hours / total_hours
            recommendations.append({
                'type': 'time_management',
                'type_meta': RECOMMENDATION_TYPES['time_management'],
                'title': 'Optimize meeting time',
                'description': f'Meetings consumed {meeting_pct:.0%} of your day '
                              f'({meeting_hours:.1f}h). Consider batch scheduling.',
                'action': 'Try a "no-meeting morning" policy to protect focus time.',
                'relevance': round(min(0.85, meeting_pct), 2),
            })

        recommendations.sort(key=lambda x: x['relevance'], reverse=True)
        return recommendations

    # ========================================================================
    # AGGREGATE RECOMMENDATIONS
    # ========================================================================

    def get_all_recommendations(
        self,
        user_skills: List[str],
        productivity_data: Optional[Dict[str, Any]] = None,
        time_data: Optional[Dict[str, Any]] = None,
        current_tools: Optional[List[str]] = None,
        team_profiles: Optional[List[Dict[str, Any]]] = None,
        skill_scores: Optional[Dict[str, float]] = None,
        career_goals: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Generate all recommendation types and return a combined, prioritized list.
        """
        all_recs = []

        # Tools
        tool_recs = self.recommend_tools(
            user_skills=user_skills,
            current_tools=current_tools,
        )
        all_recs.extend(tool_recs)

        # Workflows
        if productivity_data:
            workflow_recs = self.recommend_workflows(productivity_data)
            all_recs.extend(workflow_recs)

        # Time Management
        if time_data:
            time_recs = self.recommend_time_management(time_data)
            all_recs.extend(time_recs)

        # Collaborators
        if team_profiles:
            collab_recs = self.recommend_collaborators(
                user_skills=set(user_skills),
                team_profiles=team_profiles,
            )
            all_recs.extend(collab_recs)

        # Learning
        learning_recs = self.recommend_learning(
            user_skills=user_skills,
            skill_scores=skill_scores,
            career_goals=career_goals,
        )
        all_recs.extend(learning_recs)

        # Sort all by relevance
        all_recs.sort(key=lambda x: x.get('relevance', 0), reverse=True)

        # Group by type
        by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in all_recs:
            by_type[rec.get('type', 'other')].append(rec)

        return {
            'recommendations': all_recs[:self.MAX_RECOMMENDATIONS],
            'by_type': dict(by_type),
            'total': len(all_recs),
            'types': list(by_type.keys()),
        }


# Global instance
recommendation_service = RecommendationService()
