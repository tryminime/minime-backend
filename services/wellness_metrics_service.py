"""
Wellness Metrics Service

Tracks energy, wellness, and burnout risk:
- Work-life balance score
- Burnout risk assessment (overwork patterns, streaks)
- Energy level estimation from activity patterns
- Break quality and recovery analysis
- Rest pattern inference from activity gaps
- Stress indicators from meeting load + context switches
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, date
from collections import defaultdict
import math
import structlog

logger = structlog.get_logger()


# ============================================================================
# WELLNESS THRESHOLDS
# ============================================================================

WELLNESS_THRESHOLDS = {
    'max_healthy_hours': 9.0,
    'ideal_hours': 7.5,
    'min_break_frequency_per_day': 3,
    'max_meeting_pct': 0.4,
    'max_context_switches': 20,
    'late_work_hour': 21,   # 9 PM
    'early_work_hour': 7,   # 7 AM
    'overwork_streak_days': 3,
    'overwork_hours': 10,
}

BURNOUT_RISK_LEVELS = {
    'low': {'label': '🟢 Low Risk', 'color': '#10B981', 'max_score': 30},
    'moderate': {'label': '🟡 Moderate', 'color': '#F59E0B', 'max_score': 60},
    'high': {'label': '🟠 High Risk', 'color': '#F97316', 'max_score': 80},
    'critical': {'label': '🔴 Critical', 'color': '#EF4444', 'max_score': 100},
}


class WellnessMetricsService:
    """
    Service for tracking energy, wellness, and burnout risk.
    """

    def __init__(self):
        pass

    # ========================================================================
    # WORK-LIFE BALANCE
    # ========================================================================

    def calculate_work_life_balance(
        self,
        daily_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Calculate work-life balance score (0-100).

        Args:
            daily_data: List of {date, total_hours, start_time, end_time, break_count}

        Returns:
            Balance score with breakdown
        """
        if not daily_data:
            return {
                'score': 50,
                'level': 'neutral',
                'factors': {},
                'days_analyzed': 0,
            }

        factors = {}

        # Factor 1: Average daily hours (ideal ~7.5h)
        avg_hours = sum(d.get('total_hours', 0) for d in daily_data) / len(daily_data)
        hours_deviation = abs(avg_hours - WELLNESS_THRESHOLDS['ideal_hours'])
        hours_score = max(0, 100 - hours_deviation * 15)
        factors['work_hours'] = {
            'score': round(hours_score, 1),
            'value': round(avg_hours, 1),
            'ideal': WELLNESS_THRESHOLDS['ideal_hours'],
        }

        # Factor 2: Late/early work frequency
        boundary_violations = 0
        for d in daily_data:
            start = d.get('start_hour', 9)
            end = d.get('end_hour', 17)
            if start < WELLNESS_THRESHOLDS['early_work_hour']:
                boundary_violations += 1
            if end > WELLNESS_THRESHOLDS['late_work_hour']:
                boundary_violations += 1

        boundary_pct = boundary_violations / max(len(daily_data), 1)
        boundary_score = max(0, 100 - boundary_pct * 200)
        factors['work_boundaries'] = {
            'score': round(boundary_score, 1),
            'violations': boundary_violations,
            'description': f'{boundary_violations} out-of-hours work sessions',
        }

        # Factor 3: Weekend work
        weekend_days = sum(1 for d in daily_data if d.get('is_weekend', False) and d.get('total_hours', 0) > 1)
        weekend_score = max(0, 100 - weekend_days * 30)
        factors['weekend_work'] = {
            'score': round(weekend_score, 1),
            'weekend_work_days': weekend_days,
        }

        # Factor 4: Break regularity
        avg_breaks = sum(d.get('break_count', 0) for d in daily_data) / len(daily_data)
        break_target = WELLNESS_THRESHOLDS['min_break_frequency_per_day']
        break_score = min(100, avg_breaks / max(break_target, 1) * 100)
        factors['break_quality'] = {
            'score': round(break_score, 1),
            'avg_breaks': round(avg_breaks, 1),
            'target': break_target,
        }

        # Weighted composite
        total_score = (
            hours_score * 0.35 +
            boundary_score * 0.25 +
            weekend_score * 0.20 +
            break_score * 0.20
        )

        if total_score >= 80:
            level = 'excellent'
        elif total_score >= 60:
            level = 'good'
        elif total_score >= 40:
            level = 'fair'
        else:
            level = 'poor'

        return {
            'score': round(total_score, 1),
            'level': level,
            'factors': factors,
            'days_analyzed': len(daily_data),
        }

    # ========================================================================
    # BURNOUT RISK ASSESSMENT
    # ========================================================================

    def assess_burnout_risk(
        self,
        daily_data: List[Dict[str, Any]],
        weekly_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        Assess burnout risk based on work patterns.

        Args:
            daily_data: Recent daily metrics (last 14 days recommended)
            weekly_data: Weekly summaries for trend analysis

        Returns:
            Burnout risk assessment with risk score and indicators
        """
        if not daily_data:
            return {
                'risk_score': 0,
                'risk_level': 'low',
                'risk_label': BURNOUT_RISK_LEVELS['low']['label'],
                'indicators': [],
                'recommendations': [],
            }

        indicators = []
        risk_score = 0

        # Indicator 1: Overwork streak
        overwork_streak = 0
        max_streak = 0
        for d in daily_data:
            if d.get('total_hours', 0) > WELLNESS_THRESHOLDS['overwork_hours']:
                overwork_streak += 1
                max_streak = max(max_streak, overwork_streak)
            else:
                overwork_streak = 0

        if max_streak >= WELLNESS_THRESHOLDS['overwork_streak_days']:
            risk_score += 25
            indicators.append({
                'name': 'Overwork Streak',
                'severity': 'high',
                'detail': f'{max_streak} consecutive days exceeding {WELLNESS_THRESHOLDS["overwork_hours"]}h',
            })

        # Indicator 2: Average hours trend
        avg_hours = sum(d.get('total_hours', 0) for d in daily_data) / len(daily_data)
        if avg_hours > WELLNESS_THRESHOLDS['max_healthy_hours']:
            excess = avg_hours - WELLNESS_THRESHOLDS['max_healthy_hours']
            risk_score += min(20, excess * 10)
            indicators.append({
                'name': 'High Average Hours',
                'severity': 'moderate',
                'detail': f'Averaging {avg_hours:.1f}h/day (threshold: {WELLNESS_THRESHOLDS["max_healthy_hours"]}h)',
            })

        # Indicator 3: Low break frequency
        avg_breaks = sum(d.get('break_count', 0) for d in daily_data) / len(daily_data)
        if avg_breaks < WELLNESS_THRESHOLDS['min_break_frequency_per_day']:
            risk_score += 15
            indicators.append({
                'name': 'Insufficient Breaks',
                'severity': 'moderate',
                'detail': f'Only {avg_breaks:.1f} breaks/day (recommended: {WELLNESS_THRESHOLDS["min_break_frequency_per_day"]}+)',
            })

        # Indicator 4: Late-night work frequency
        late_days = sum(1 for d in daily_data if d.get('end_hour', 17) > WELLNESS_THRESHOLDS['late_work_hour'])
        late_pct = late_days / max(len(daily_data), 1)
        if late_pct > 0.3:
            risk_score += 15
            indicators.append({
                'name': 'Frequent Late Work',
                'severity': 'moderate',
                'detail': f'Late work on {late_pct:.0%} of days',
            })

        # Indicator 5: High meeting load
        avg_meeting_pct = sum(d.get('meeting_pct', 0) for d in daily_data) / len(daily_data)
        if avg_meeting_pct > WELLNESS_THRESHOLDS['max_meeting_pct'] * 100:
            risk_score += 10
            indicators.append({
                'name': 'Meeting Overload',
                'severity': 'low',
                'detail': f'Meetings consuming {avg_meeting_pct:.0f}% of time',
            })

        # Indicator 6: Increasing hours trend
        if weekly_data and len(weekly_data) >= 3:
            recent = weekly_data[-1].get('total_hours', 0)
            earlier = sum(w.get('total_hours', 0) for w in weekly_data[:-1]) / (len(weekly_data) - 1)
            if earlier > 0 and (recent - earlier) / earlier > 0.2:
                risk_score += 15
                indicators.append({
                    'name': 'Increasing Workload',
                    'severity': 'moderate',
                    'detail': f'This week {recent:.0f}h vs avg {earlier:.0f}h (+{(recent-earlier)/earlier:.0%})',
                })

        risk_score = min(risk_score, 100)

        # Determine risk level
        if risk_score <= 30:
            risk_level = 'low'
        elif risk_score <= 60:
            risk_level = 'moderate'
        elif risk_score <= 80:
            risk_level = 'high'
        else:
            risk_level = 'critical'

        # Generate recommendations
        recommendations = self._generate_burnout_recommendations(indicators, risk_level)

        return {
            'risk_score': round(risk_score, 1),
            'risk_level': risk_level,
            'risk_label': BURNOUT_RISK_LEVELS[risk_level]['label'],
            'risk_color': BURNOUT_RISK_LEVELS[risk_level]['color'],
            'indicators': indicators,
            'recommendations': recommendations,
            'days_analyzed': len(daily_data),
        }

    # ========================================================================
    # ENERGY ESTIMATION
    # ========================================================================

    def estimate_energy_levels(
        self,
        hourly_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Estimate energy levels throughout the day from activity patterns.

        Args:
            hourly_data: List of {hour (0-23), activity_count, deep_work_mins, context_switches}

        Returns:
            Energy profile with peak/low hours
        """
        if not hourly_data:
            return {
                'peak_hours': [],
                'low_hours': [],
                'energy_curve': [],
                'avg_energy': 50,
            }

        # Calculate energy for each hour
        energy_curve = []
        for entry in hourly_data:
            hour = entry.get('hour', 0)
            activity = entry.get('activity_count', 0)
            deep_work = entry.get('deep_work_mins', 0)
            switches = entry.get('context_switches', 0)

            # Energy proxy: high deep work + low switches = high energy
            if activity == 0:
                energy = 0  # Not active this hour
            else:
                deep_work_ratio = deep_work / 60 if deep_work > 0 else 0
                switch_penalty = min(switches / 10, 1.0)
                energy = int((deep_work_ratio * 0.7 + (1 - switch_penalty) * 0.3) * 100)
                energy = max(10, min(100, energy))

            energy_curve.append({'hour': hour, 'energy': energy})

        # Find peaks and lows (only during active hours)
        active_hours = [e for e in energy_curve if e['energy'] > 0]
        if active_hours:
            sorted_active = sorted(active_hours, key=lambda x: x['energy'], reverse=True)
            peak_hours = [e['hour'] for e in sorted_active[:3]]
            low_hours = [e['hour'] for e in sorted_active[-3:]]
            avg_energy = sum(e['energy'] for e in active_hours) / len(active_hours)
        else:
            peak_hours = []
            low_hours = []
            avg_energy = 0

        return {
            'peak_hours': peak_hours,
            'low_hours': low_hours,
            'energy_curve': energy_curve,
            'avg_energy': round(avg_energy, 1),
        }

    # ========================================================================
    # STRESS INDICATORS
    # ========================================================================

    def calculate_stress_index(
        self,
        daily_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Calculate a daily stress index from observable patterns.

        Args:
            daily_data: {total_hours, meeting_hours, context_switches,
                         break_count, late_work, messages_sent}

        Returns:
            Stress index (0-100) with contributing factors
        """
        total_hours = daily_data.get('total_hours', 8)
        meeting_hours = daily_data.get('meeting_hours', 0)
        context_switches = daily_data.get('context_switches', 0)
        break_count = daily_data.get('break_count', 3)
        late_work = daily_data.get('late_work', False)
        messages = daily_data.get('messages_sent', 0)

        factors = {}

        # Meeting load stress
        meeting_pct = meeting_hours / max(total_hours, 1)
        meeting_stress = min(meeting_pct / 0.5 * 100, 100)
        factors['meeting_load'] = round(meeting_stress, 1)

        # Context switching stress
        switch_stress = min(context_switches / 30 * 100, 100)
        factors['context_switching'] = round(switch_stress, 1)

        # Overwork stress
        overwork_stress = max(0, (total_hours - WELLNESS_THRESHOLDS['max_healthy_hours']) / 3 * 100)
        overwork_stress = min(overwork_stress, 100)
        factors['overwork'] = round(overwork_stress, 1)

        # Break deficit stress
        break_deficit = max(0, WELLNESS_THRESHOLDS['min_break_frequency_per_day'] - break_count)
        break_stress = break_deficit / WELLNESS_THRESHOLDS['min_break_frequency_per_day'] * 100
        factors['break_deficit'] = round(min(break_stress, 100), 1)

        # Late work stress
        late_stress = 30 if late_work else 0
        factors['late_work'] = late_stress

        # Communication overload
        comm_stress = min(messages / 50 * 100, 100) if messages > 20 else 0
        factors['communication'] = round(comm_stress, 1)

        # Weighted composite
        stress_index = (
            meeting_stress * 0.25 +
            switch_stress * 0.20 +
            overwork_stress * 0.20 +
            break_stress * 0.15 +
            late_stress * 0.10 +
            comm_stress * 0.10
        )

        if stress_index < 25:
            level = 'low'
        elif stress_index < 50:
            level = 'moderate'
        elif stress_index < 75:
            level = 'high'
        else:
            level = 'very_high'

        return {
            'stress_index': round(min(stress_index, 100), 1),
            'level': level,
            'factors': factors,
            'dominant_factor': max(factors, key=factors.get),
        }

    # ========================================================================
    # REST & RECOVERY ANALYSIS
    # ========================================================================

    def analyze_rest_patterns(
        self,
        daily_data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Analyze rest and recovery patterns.

        Args:
            daily_data: List of {date, start_hour, end_hour, total_hours, is_weekend}

        Returns:
            Rest analysis with recovery score
        """
        if not daily_data:
            return {
                'recovery_score': 50,
                'avg_rest_hours': 15,
                'rest_days': 0,
                'consecutive_work_days': 0,
                'recommendations': [],
            }

        rest_hours_list = []
        consecutive_work = 0
        max_consecutive = 0
        rest_days = 0

        for d in daily_data:
            start = d.get('start_hour', 9)
            end = d.get('end_hour', 17)
            total = d.get('total_hours', 8)

            # Rest = 24 - work span
            work_span = max(end - start, total)
            rest = 24 - work_span
            rest_hours_list.append(rest)

            if total < 1:  # Rest day
                rest_days += 1
                consecutive_work = 0
            else:
                consecutive_work += 1
                max_consecutive = max(max_consecutive, consecutive_work)

        avg_rest = sum(rest_hours_list) / len(rest_hours_list) if rest_hours_list else 15

        # Recovery score (0-100)
        rest_score = min(100, avg_rest / 15 * 60)  # 15h rest = 60 points
        day_off_score = min(40, rest_days / max(len(daily_data) / 7, 1) * 40)
        consecutive_penalty = max(0, (max_consecutive - 5) * 5)

        recovery_score = max(0, rest_score + day_off_score - consecutive_penalty)

        recommendations = []
        if max_consecutive > 6:
            recommendations.append('Take a rest day — you\'ve worked 7+ consecutive days.')
        if avg_rest < 12:
            recommendations.append('Average rest under 12h/day. End work earlier when possible.')
        if rest_days == 0:
            recommendations.append('No rest days detected. Schedule at least one day off per week.')

        return {
            'recovery_score': round(min(recovery_score, 100), 1),
            'avg_rest_hours': round(avg_rest, 1),
            'rest_days': rest_days,
            'consecutive_work_days': max_consecutive,
            'recommendations': recommendations,
        }

    # ========================================================================
    # COMPREHENSIVE WELLNESS REPORT
    # ========================================================================

    def generate_wellness_report(
        self,
        daily_data: List[Dict[str, Any]],
        weekly_data: Optional[List[Dict[str, Any]]] = None,
        hourly_data: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Generate a comprehensive wellness report."""
        balance = self.calculate_work_life_balance(daily_data)
        burnout = self.assess_burnout_risk(daily_data, weekly_data)
        rest = self.analyze_rest_patterns(daily_data)

        energy = {}
        if hourly_data:
            energy = self.estimate_energy_levels(hourly_data)

        # Overall wellness score (0-100)
        overall = (
            balance['score'] * 0.35 +
            (100 - burnout['risk_score']) * 0.35 +
            rest['recovery_score'] * 0.30
        )

        return {
            'overall_score': round(overall, 1),
            'work_life_balance': balance,
            'burnout_risk': burnout,
            'rest_recovery': rest,
            'energy_levels': energy,
            'days_analyzed': len(daily_data),
        }

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _generate_burnout_recommendations(
        self,
        indicators: List[Dict[str, Any]],
        risk_level: str,
    ) -> List[str]:
        """Generate recommendations based on burnout indicators."""
        recs = []

        indicator_names = {i['name'] for i in indicators}

        if 'Overwork Streak' in indicator_names:
            recs.append('Take a recovery day off within the next 48 hours.')
        if 'High Average Hours' in indicator_names:
            recs.append('Set a daily work limit and use a timer to enforce it.')
        if 'Insufficient Breaks' in indicator_names:
            recs.append('Set hourly reminders to take 5-minute breaks.')
        if 'Frequent Late Work' in indicator_names:
            recs.append('Establish a firm end-of-day time and stick to it.')
        if 'Meeting Overload' in indicator_names:
            recs.append('Decline or shorten non-essential meetings this week.')
        if 'Increasing Workload' in indicator_names:
            recs.append('Review your commitments and postpone non-critical tasks.')

        if risk_level in ('high', 'critical') and not recs:
            recs.append('Consider speaking with your manager about workload balance.')

        return recs


# Global instance
wellness_metrics_service = WellnessMetricsService()
