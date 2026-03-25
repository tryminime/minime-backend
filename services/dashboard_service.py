"""
Dashboard Aggregation Service

Unified service that pulls from all Module 5 analytics services
to power the dashboard API endpoints:

- Overview (KPIs)
- Productivity summary (metrics + time allocation + comparisons)
- Collaboration summary
- Skills summary (mastery levels + growth)
- Career summary (trajectory + role readiness)
- Wellness summary (balance + burnout risk)
- Weekly digest (combined highlights + suggestions)
- Analytics export (JSON/CSV)
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, date
from collections import defaultdict
import json
import csv
import io
import structlog

from services.career_development_service import CareerDevelopmentService
from services.wellness_metrics_service import WellnessMetricsService
from services.goal_tracking_service import GoalTrackingService
from services.productivity_metrics_service import (
    get_time_allocation_by_project,
    get_comparative_analytics,
)

logger = structlog.get_logger()


class DashboardService:
    """
    Aggregation service that powers all dashboard API endpoints.
    Pulls from the individual analytics services and composes
    unified responses for the frontend.
    """

    def __init__(self):
        self.career_service = CareerDevelopmentService()
        self.wellness_service = WellnessMetricsService()
        self.goal_service = GoalTrackingService()

    # ========================================================================
    # 1. DASHBOARD OVERVIEW (KPIs)
    # ========================================================================

    def get_dashboard_overview(
        self,
        user_id: str,
        daily_data: Optional[List[Dict[str, Any]]] = None,
        activities: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        High-level KPI data for the dashboard overview page.

        Returns focus score, deep work hours, meeting count,
        wellness score, active goals, and quick trends.
        """
        # Productivity KPIs
        productivity = self._extract_productivity_kpis(activities or [])

        # Wellness KPIs
        wellness = {}
        if daily_data:
            wellness_report = self.wellness_service.generate_wellness_report(daily_data)
            wellness = {
                'wellness_score': wellness_report['overall_score'],
                'burnout_risk': wellness_report['burnout_risk']['risk_level'],
                'work_life_balance': wellness_report['work_life_balance']['level'],
            }

        # Goal KPIs
        goal_stats = self.goal_service.get_goal_stats(user_id)

        return {
            'user_id': user_id,
            'generated_at': datetime.now().isoformat(),
            'kpis': {
                **productivity,
                **wellness,
                'active_goals': goal_stats['active'],
                'goals_completed': goal_stats['completed'],
                'goal_completion_rate': goal_stats['completion_rate'],
            },
            'quick_actions': self._generate_quick_actions(productivity, wellness, goal_stats),
        }

    # ========================================================================
    # 2. PRODUCTIVITY SUMMARY
    # ========================================================================

    def get_productivity_summary(
        self,
        activities: List[Dict[str, Any]],
        current_metrics: Optional[Dict[str, Any]] = None,
        previous_metrics: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Full productivity summary with time allocation and comparisons.
        """
        # Time allocation by project
        allocation = get_time_allocation_by_project(activities)

        # Comparative analytics (week-over-week)
        comparison = {}
        if current_metrics and previous_metrics:
            comparison = get_comparative_analytics(current_metrics, previous_metrics)

        # Core metrics
        total_hours = allocation['total_hours']
        productive_hours = sum(
            p['hours'] for p in allocation.get('by_category', [])
            if p.get('category') in ('productive', 'coding', 'development')
        )

        return {
            'total_hours': total_hours,
            'productive_hours': round(productive_hours, 2),
            'productivity_ratio': round(productive_hours / max(total_hours, 0.1) * 100, 1),
            'time_allocation': allocation,
            'comparison': comparison,
            'metrics': current_metrics or {},
            'top_apps': allocation['by_project'][:5],
        }

    # ========================================================================
    # 3. COLLABORATION SUMMARY
    # ========================================================================

    def get_collaboration_summary(
        self,
        collaboration_data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Collaboration metrics summary.
        Wraps data from CollaborationAnalyticsService.
        """
        if not collaboration_data:
            return {
                'collaboration_score': 0,
                'unique_collaborators': 0,
                'meetings_count': 0,
                'communication_volume': 0,
                'network_size': 0,
                'top_collaborators': [],
                'network_diversity': {},
                'meeting_patterns': {},
            }

        return {
            'collaboration_score': collaboration_data.get('collaboration_score', 0),
            'unique_collaborators': len(collaboration_data.get('top_collaborators', [])),
            'meetings_count': collaboration_data.get('meeting_patterns', {}).get('total_meetings', 0),
            'communication_volume': collaboration_data.get('communication_volume', 0),
            'network_size': collaboration_data.get('network_size', 0),
            'top_collaborators': collaboration_data.get('top_collaborators', [])[:5],
            'network_diversity': collaboration_data.get('network_diversity', {}),
            'meeting_patterns': collaboration_data.get('meeting_patterns', {}),
        }

    # ========================================================================
    # 4. SKILLS SUMMARY
    # ========================================================================

    def get_skill_summary(
        self,
        skill_data: Optional[Dict[str, Any]] = None,
        user_skills: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Skills summary with mastery levels and growth trajectories.
        """
        result = {
            'total_skills': 0,
            'active_skills': [],
            'learning_velocity': 0,
            'skill_usage': {},
            'expertise_distribution': {},
            'mastery_levels': {},
            'growth_trajectories': [],
        }

        if skill_data:
            result.update({
                'total_skills': skill_data.get('total_skills', 0),
                'active_skills': skill_data.get('top_skills', []),
                'learning_velocity': skill_data.get('learning_velocity', 0),
                'skill_usage': {s.get('name', ''): s.get('hours', 0) for s in skill_data.get('top_skills', [])},
                'mastery_levels': skill_data.get('mastery_levels', {}),
                'growth_trajectories': skill_data.get('growth_trajectories', []),
            })

        # Expertise distribution from user_skills
        if user_skills:
            distribution = {'beginner': 0, 'intermediate': 0, 'advanced': 0, 'expert': 0, 'master': 0}
            for skill, hours in user_skills.items():
                if hours >= 200:
                    distribution['master'] += 1
                elif hours >= 100:
                    distribution['expert'] += 1
                elif hours >= 50:
                    distribution['advanced'] += 1
                elif hours >= 20:
                    distribution['intermediate'] += 1
                else:
                    distribution['beginner'] += 1
            result['expertise_distribution'] = distribution
            result['total_skills'] = len(user_skills)

        return result

    # ========================================================================
    # 5. CAREER SUMMARY
    # ========================================================================

    def get_career_summary(
        self,
        user_skills: Dict[str, float],
        skill_history: List[Dict[str, Any]],
        weekly_data: Optional[List[Dict[str, Any]]] = None,
        target_roles: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Career summary with trajectory, role readiness, and recommendations.
        """
        report = self.career_service.generate_career_report(
            user_skills=user_skills,
            skill_history=skill_history,
            weekly_data=weekly_data or [],
            target_roles=target_roles,
        )

        return {
            'growth_trajectory': report['trajectory']['trajectory'],
            'career_phase': report['trajectory']['phase'],
            'growth_velocity': report.get('growth_velocity', {}),
            'skill_gaps': report.get('skill_gaps', {}).get('prioritized_gaps', [])[:5],
            'best_fit_role': report.get('best_fit_role', {}),
            'milestone': report.get('milestone', {}),
            'total_skills': report.get('total_skills', 0),
            'recommended_next_steps': self._generate_career_recommendations(report),
        }

    # ========================================================================
    # 6. WELLNESS SUMMARY
    # ========================================================================

    def get_wellness_summary(
        self,
        daily_data: List[Dict[str, Any]],
        weekly_data: Optional[List[Dict[str, Any]]] = None,
        hourly_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Wellness summary with balance, burnout risk, and energy.
        """
        report = self.wellness_service.generate_wellness_report(
            daily_data, weekly_data, hourly_data,
        )

        return {
            'overall_score': report['overall_score'],
            'work_life_balance': report['work_life_balance'],
            'burnout_risk': {
                'score': report['burnout_risk']['risk_score'],
                'level': report['burnout_risk']['risk_level'],
                'label': report['burnout_risk']['risk_label'],
                'indicators': report['burnout_risk']['indicators'],
                'recommendations': report['burnout_risk']['recommendations'],
            },
            'rest_recovery': report['rest_recovery'],
            'energy_levels': report.get('energy_levels', {}),
        }

    # ========================================================================
    # 7. WEEKLY DIGEST
    # ========================================================================

    def get_weekly_digest(
        self,
        user_id: str,
        week_offset: int = 0,
        activities: Optional[List[Dict[str, Any]]] = None,
        daily_data: Optional[List[Dict[str, Any]]] = None,
        skill_history: Optional[List[Dict[str, Any]]] = None,
        user_skills: Optional[Dict[str, float]] = None,
    ) -> Dict[str, Any]:
        """
        Combined weekly summary with highlights and suggestions.
        """
        today = datetime.now()
        week_start = today - timedelta(days=today.weekday() + (week_offset * 7))
        week_end = week_start + timedelta(days=6)

        # Productivity
        allocation = get_time_allocation_by_project(activities or [])

        # Wellness
        wellness = {}
        if daily_data:
            wellness = self.wellness_service.generate_wellness_report(daily_data)

        # Goals
        goal_stats = self.goal_service.get_goal_stats(user_id)
        streaks = self.goal_service.get_completion_streaks(user_id)

        # Generate highlights
        highlights = self._generate_weekly_highlights(
            allocation, wellness, goal_stats, streaks,
        )

        # Generate suggestions
        suggestions = self._generate_weekly_suggestions(
            allocation, wellness, goal_stats,
        )

        return {
            'week_start': week_start.strftime('%Y-%m-%d'),
            'week_end': week_end.strftime('%Y-%m-%d'),
            'total_hours': allocation['total_hours'],
            'productivity_score': self._compute_productivity_score(allocation),
            'top_activities': allocation['by_project'][:5],
            'highlights': highlights,
            'suggestions': suggestions,
            'wellness_score': wellness.get('overall_score', 0) if wellness else 0,
            'goals_summary': {
                'active': goal_stats['active'],
                'completed_this_week': goal_stats['completed'],
                'streak': streaks.get('current_streak', 0),
            },
        }

    # ========================================================================
    # 8. EXPORT
    # ========================================================================

    def export_analytics_data(
        self,
        user_id: str,
        format: str = 'json',
        activities: Optional[List[Dict[str, Any]]] = None,
        daily_data: Optional[List[Dict[str, Any]]] = None,
        user_skills: Optional[Dict[str, float]] = None,
        skill_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Export analytics data in JSON or CSV format.
        """
        # Gather all data
        overview = self.get_dashboard_overview(user_id, daily_data, activities)
        productivity = self.get_productivity_summary(activities or [])
        wellness = {}
        if daily_data:
            wellness = self.get_wellness_summary(daily_data)
        goals = self.goal_service.get_goal_stats(user_id)
        career = {}
        if user_skills and skill_history:
            career = self.get_career_summary(user_skills, skill_history)

        export_data = {
            'exported_at': datetime.now().isoformat(),
            'user_id': user_id,
            'overview': overview.get('kpis', {}),
            'productivity': productivity,
            'wellness': wellness,
            'goals': goals,
            'career': career,
        }

        if format == 'csv':
            return {
                'format': 'csv',
                'content': self._to_csv(export_data),
                'filename': f'analytics_export_{datetime.now().strftime("%Y%m%d")}.csv',
            }

        return {
            'format': 'json',
            'content': export_data,
            'filename': f'analytics_export_{datetime.now().strftime("%Y%m%d")}.json',
        }

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _extract_productivity_kpis(
        self,
        activities: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Extract productivity KPIs from activities."""
        if not activities:
            return {
                'focus_score': 0,
                'deep_work_hours': 0,
                'total_hours': 0,
                'context_switches': 0,
                'meeting_hours': 0,
            }

        total_seconds = sum(a.get('duration_seconds', 0) for a in activities)
        meeting_seconds = sum(
            a.get('duration_seconds', 0) for a in activities
            if a.get('category', '') in ('meetings', 'meeting', 'video_call')
        )
        deep_seconds = sum(
            a.get('duration_seconds', 0) for a in activities
            if a.get('category', '') in ('productive', 'coding', 'development', 'deep_work')
        )

        # Count switches
        switches = 0
        last_app = None
        for a in sorted(activities, key=lambda x: x.get('occurred_at', '')):
            app = a.get('application_name', '')
            if last_app and app != last_app:
                switches += 1
            last_app = app

        total_hours = total_seconds / 3600
        deep_hours = deep_seconds / 3600
        meeting_hours = meeting_seconds / 3600

        # Simple focus score
        if total_hours > 0:
            deep_ratio = deep_hours / total_hours
            switch_penalty = min(switches / 40, 1.0)
            meeting_penalty = min(meeting_hours / total_hours / 0.5, 1.0)
            focus = (deep_ratio * 0.5 + (1 - switch_penalty) * 0.25 + (1 - meeting_penalty) * 0.25) * 10
        else:
            focus = 0

        return {
            'focus_score': round(min(focus, 10), 1),
            'deep_work_hours': round(deep_hours, 1),
            'total_hours': round(total_hours, 1),
            'context_switches': switches,
            'meeting_hours': round(meeting_hours, 1),
        }

    def _generate_quick_actions(
        self,
        productivity: Dict[str, Any],
        wellness: Dict[str, Any],
        goal_stats: Dict[str, Any],
    ) -> List[Dict[str, str]]:
        """Generate quick action suggestions for the dashboard."""
        actions = []

        if productivity.get('focus_score', 0) < 5:
            actions.append({
                'action': 'Try a 25-minute focused work block',
                'type': 'productivity',
                'icon': '🎯',
            })

        if wellness.get('burnout_risk') in ('high', 'critical'):
            actions.append({
                'action': 'Take a break — burnout risk is elevated',
                'type': 'wellness',
                'icon': '💚',
            })

        if goal_stats.get('overdue', 0) > 0:
            actions.append({
                'action': f'Review {goal_stats["overdue"]} overdue goals',
                'type': 'goals',
                'icon': '⚠️',
            })

        if goal_stats.get('active', 0) == 0:
            actions.append({
                'action': 'Set a new weekly goal',
                'type': 'goals',
                'icon': '🎯',
            })

        if not actions:
            actions.append({
                'action': 'You\'re on track — keep going!',
                'type': 'positive',
                'icon': '✅',
            })

        return actions

    def _generate_career_recommendations(
        self,
        report: Dict[str, Any],
    ) -> List[str]:
        """Generate career next steps."""
        recs = []

        gaps = report.get('skill_gaps', {}).get('prioritized_gaps', [])
        if gaps:
            top_gap = gaps[0]
            recs.append(f'Focus on learning {top_gap.get("skill", "a new skill")} — it appears in {top_gap.get("role_count", 0)} target roles.')

        best = report.get('best_fit_role', {})
        if best and best.get('readiness_score', 0) > 70:
            recs.append(f'You\'re {best.get("readiness_score", 0):.0f}% ready for {best.get("role", "your target role")}.')

        velocity = report.get('growth_velocity', {})
        if velocity.get('trend') == 'accelerating':
            recs.append('Your growth is accelerating — great momentum!')
        elif velocity.get('trend') == 'decelerating':
            recs.append('Growth has slowed. Try dedicating 2h/week to new skills.')

        return recs or ['Keep building your skills — the journey is the reward.']

    def _generate_weekly_highlights(
        self,
        allocation: Dict[str, Any],
        wellness: Dict[str, Any],
        goal_stats: Dict[str, Any],
        streaks: Dict[str, Any],
    ) -> List[str]:
        """Generate weekly highlights."""
        highlights = []

        hours = allocation.get('total_hours', 0)
        if hours > 0:
            highlights.append(f'Tracked {hours:.1f} hours across {allocation.get("project_count", 0)} projects.')

        if goal_stats.get('completed', 0) > 0:
            highlights.append(f'Completed {goal_stats["completed"]} goals.')

        if streaks.get('current_streak', 0) >= 2:
            highlights.append(f'Goal completion streak: {streaks["current_streak"]} weeks! 🔥')

        balance = wellness.get('work_life_balance', {})
        if isinstance(balance, dict) and balance.get('level') in ('excellent', 'good'):
            highlights.append('Work-life balance rated as ' + balance['level'] + '.')

        if not highlights:
            highlights.append('No activity data for this week yet.')

        return highlights

    def _generate_weekly_suggestions(
        self,
        allocation: Dict[str, Any],
        wellness: Dict[str, Any],
        goal_stats: Dict[str, Any],
    ) -> List[str]:
        """Generate weekly suggestions."""
        suggestions = []

        hours = allocation.get('total_hours', 0)
        if hours > 45:
            suggestions.append('Consider reducing hours to stay under 40h for better balance.')
        elif hours < 20 and hours > 0:
            suggestions.append('Low tracked hours — make sure your activity tracker is running.')

        burnout = wellness.get('burnout_risk', {})
        if isinstance(burnout, dict):
            recs = burnout.get('recommendations', [])
            suggestions.extend(recs[:2])

        if goal_stats.get('active', 0) == 0:
            suggestions.append('Set at least one goal for next week.')

        if goal_stats.get('avg_progress', 0) < 30:
            suggestions.append('Your goals are under 30% progress — try breaking them into smaller steps.')

        return suggestions or ['Keep up the great work!']

    def _compute_productivity_score(
        self,
        allocation: Dict[str, Any],
    ) -> float:
        """Compute a simple productivity score from allocation data."""
        total = allocation.get('total_hours', 0)
        if total == 0:
            return 0

        productive = sum(
            p['hours'] for p in allocation.get('by_category', [])
            if p.get('category') in ('productive', 'coding', 'development')
        )
        ratio = productive / total
        return round(min(ratio * 100, 100), 1)

    def _to_csv(self, data: Dict[str, Any]) -> str:
        """Convert flat analytics data to CSV string."""
        output = io.StringIO()
        writer = csv.writer(output)

        # Header
        writer.writerow(['Section', 'Metric', 'Value'])

        def _flatten(d: Dict, section: str = ''):
            for key, value in d.items():
                if isinstance(value, dict):
                    _flatten(value, f'{section}.{key}' if section else key)
                elif isinstance(value, list):
                    writer.writerow([section, key, json.dumps(value)])
                else:
                    writer.writerow([section, key, value])

        _flatten(data)
        return output.getvalue()


# Global instance
dashboard_service = DashboardService()
