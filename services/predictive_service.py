"""
Predictive Productivity Forecasting Service.

Uses linear regression on historical daily productivity scores
to predict future productivity and detect patterns.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timezone, timedelta, date
from typing import Dict, List, Any, Optional, Tuple
from uuid import UUID

import numpy as np
import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, cast, String

from models import Activity

logger = structlog.get_logger()


def _day_name(day_idx: int) -> str:
    return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"][day_idx]


class PredictiveService:
    """Forecasts productivity using historical activity patterns."""

    async def _get_daily_scores(
        self, user_id: UUID, db: AsyncSession, days: int = 90
    ) -> List[Dict[str, Any]]:
        """Compute daily productivity scores from activities."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        result = await db.execute(
            select(
                func.date(Activity.occurred_at).label("day"),
                func.count(Activity.id).label("activity_count"),
                func.sum(Activity.duration_seconds).label("total_seconds"),
            )
            .where(and_(
                Activity.user_id == user_id,
                Activity.occurred_at >= cutoff,
            ))
            .group_by(func.date(Activity.occurred_at))
            .order_by(func.date(Activity.occurred_at))
        )
        rows = result.all()

        daily = []
        for row in rows:
            day = row.day
            count = row.activity_count or 0
            seconds = row.total_seconds or 0
            hours = seconds / 3600

            # Simple productivity score: normalize activity count + hours
            # Score 0-100 based on: activities (max 200 → 50pts) + hours (max 8 → 50pts)
            activity_score = min(count / 200.0, 1.0) * 50
            hours_score = min(hours / 8.0, 1.0) * 50
            score = round(activity_score + hours_score, 1)

            if isinstance(day, str):
                day_dt = datetime.strptime(day, "%Y-%m-%d").date()
            elif isinstance(day, datetime):
                day_dt = day.date()
            else:
                day_dt = day

            daily.append({
                "date": day_dt.isoformat() if day_dt else str(day),
                "score": score,
                "activity_count": count,
                "hours": round(hours, 1),
                "day_of_week": day_dt.weekday() if hasattr(day_dt, 'weekday') else 0,
            })

        return daily

    async def get_forecast(
        self, user_id: UUID, db: AsyncSession, forecast_days: int = 14
    ) -> Dict[str, Any]:
        """
        Generate productivity forecast using linear regression + weekly patterns.
        """
        daily = await self._get_daily_scores(user_id, db, days=90)

        if len(daily) < 7:
            return {
                "predictions": [],
                "weekly_pattern": [],
                "peak_hours": [],
                "trend_direction": "insufficient_data",
                "forecast_summary": "Need at least 7 days of data for predictions.",
                "historical_days": len(daily),
            }

        # 1. Extract scores and fit linear trend
        scores = np.array([d["score"] for d in daily], dtype=np.float64)
        x = np.arange(len(scores), dtype=np.float64)

        # Linear regression: y = mx + b
        coeffs = np.polyfit(x, scores, 1)
        slope, intercept = float(coeffs[0]), float(coeffs[1])

        # 2. Weekly pattern (average score per day-of-week)
        dow_scores: Dict[int, List[float]] = defaultdict(list)
        for d in daily:
            dow_scores[d["day_of_week"]].append(d["score"])

        weekly_pattern = []
        for dow in range(7):
            day_scores = dow_scores.get(dow, [])
            avg = float(np.mean(day_scores)) if day_scores else 0
            weekly_pattern.append({
                "day": _day_name(dow),
                "day_index": dow,
                "average_score": round(avg, 1),
                "sample_count": len(day_scores),
            })

        # Best/worst days
        best_day = max(weekly_pattern, key=lambda d: d["average_score"])
        worst_day = min(weekly_pattern, key=lambda d: d["average_score"])

        # 3. Generate predictions
        predictions = []
        today = datetime.now(timezone.utc).date()
        recent_avg = float(np.mean(scores[-14:])) if len(scores) >= 14 else float(np.mean(scores))

        for i in range(1, forecast_days + 1):
            future_date = today + timedelta(days=i)
            future_x = len(scores) + i

            # Trend component
            trend_score = slope * future_x + intercept

            # Weekly pattern component
            dow = future_date.weekday()
            dow_avg = weekly_pattern[dow]["average_score"]

            # Blend: 40% trend + 40% weekly pattern + 20% recent average
            predicted = 0.4 * trend_score + 0.4 * dow_avg + 0.2 * recent_avg
            predicted = max(0, min(100, predicted))

            # Confidence decreases with distance
            confidence = max(0.3, 1.0 - (i * 0.04))

            predictions.append({
                "date": future_date.isoformat(),
                "day_name": _day_name(future_date.weekday()),
                "predicted_score": round(predicted, 1),
                "confidence": round(confidence, 2),
                "lower_bound": round(max(0, predicted - (1 - confidence) * 30), 1),
                "upper_bound": round(min(100, predicted + (1 - confidence) * 30), 1),
            })

        # 4. Trend direction
        if slope > 0.3:
            trend = "improving"
        elif slope < -0.3:
            trend = "declining"
        else:
            trend = "stable"

        # 5. Peak hours analysis
        hour_result = await db.execute(
            select(
                func.extract('hour', Activity.occurred_at).label("hour"),
                func.count(Activity.id).label("count"),
            )
            .where(and_(
                Activity.user_id == user_id,
                Activity.occurred_at >= datetime.now(timezone.utc) - timedelta(days=30),
            ))
            .group_by(func.extract('hour', Activity.occurred_at))
            .order_by(func.count(Activity.id).desc())
        )
        hour_rows = hour_result.all()

        peak_hours = []
        for row in hour_rows[:5]:
            h = int(row.hour) if row.hour is not None else 0
            peak_hours.append({
                "hour": h,
                "label": f"{h:02d}:00",
                "activity_count": row.count,
            })

        # 6. Summary
        avg_score = round(float(np.mean(scores)), 1)
        next_week_avg = round(
            float(np.mean([p["predicted_score"] for p in predictions[:7]])), 1
        )

        summary_parts = [
            f"Your average productivity is {avg_score}/100.",
            f"Trend is {trend} (slope: {slope:+.2f}/day).",
            f"Best day: {best_day['day']} ({best_day['average_score']}).",
            f"Worst day: {worst_day['day']} ({worst_day['average_score']}).",
            f"Next week forecast: ~{next_week_avg}/100.",
        ]

        return {
            "predictions": predictions,
            "weekly_pattern": weekly_pattern,
            "peak_hours": peak_hours,
            "trend_direction": trend,
            "trend_slope": round(slope, 4),
            "current_average": avg_score,
            "forecast_average": next_week_avg,
            "best_day": best_day,
            "worst_day": worst_day,
            "forecast_summary": " ".join(summary_parts),
            "historical_days": len(daily),
        }


# Global singleton
predictive_service = PredictiveService()
