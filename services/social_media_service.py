"""
Social Media Analytics Service.

Aggregates and analyzes social media usage data from browser extension activity events.
Provides per-platform time tracking, usage trends, and engagement metrics.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from collections import defaultdict
import structlog

logger = structlog.get_logger()


# Social media platform categories
PLATFORM_CATEGORIES = {
    "social_network": ["Twitter/X", "Facebook", "Instagram", "LinkedIn", "Pinterest", "Tumblr", "Mastodon", "Threads", "Bluesky"],
    "messaging": ["Discord", "WhatsApp", "Telegram", "Signal", "Snapchat"],
    "video": ["YouTube", "TikTok"],
    "streaming": ["Twitch"],
    "forum": ["Reddit"],
    "blogging": ["Medium", "DEV", "Hashnode", "Substack"],
    "professional": ["LinkedIn"],
}


class SocialMediaService:
    """Analyzes social media activity patterns from captured browsing data."""

    def __init__(self, db_session=None):
        self.db = db_session

    async def get_usage_summary(
        self,
        user_id: str,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get social media usage summary for a user over the past N days.

        Returns per-platform time, activity breakdown, and trends.
        """
        if not self.db:
            return self._empty_summary()

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    data->>'socialPlatform' AS platform,
                    data->>'socialCategory' AS category,
                    data->>'socialActivityType' AS activity_type,
                    SUM(duration_seconds) AS total_seconds,
                    COUNT(*) AS visit_count,
                    MIN(occurred_at) AS first_seen,
                    MAX(occurred_at) AS last_seen
                FROM activities
                WHERE user_id = :uid
                  AND data->>'isSocialMedia' = 'true'
                  AND occurred_at >= :cutoff
                GROUP BY
                    data->>'socialPlatform',
                    data->>'socialCategory',
                    data->>'socialActivityType'
                ORDER BY total_seconds DESC
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        rows = result.fetchall()
        return self._build_summary(rows, days)

    def _build_summary(self, rows, days: int) -> Dict[str, Any]:
        """Build structured summary from query results."""
        platforms: Dict[str, Dict] = defaultdict(lambda: {
            "total_seconds": 0,
            "visit_count": 0,
            "activity_types": defaultdict(int),
            "category": "unknown",
        })

        total_social_seconds = 0
        total_visits = 0

        for row in rows:
            platform = row[0] or "Unknown"
            category = row[1] or "unknown"
            activity_type = row[2] or "browsing"
            seconds = row[3] or 0
            visits = row[4] or 0

            platforms[platform]["total_seconds"] += seconds
            platforms[platform]["visit_count"] += visits
            platforms[platform]["activity_types"][activity_type] += seconds
            platforms[platform]["category"] = category

            total_social_seconds += seconds
            total_visits += visits

        # Format output
        platform_list = []
        for name, data in sorted(platforms.items(), key=lambda x: x[1]["total_seconds"], reverse=True):
            platform_list.append({
                "platform": name,
                "category": data["category"],
                "total_minutes": round(data["total_seconds"] / 60, 1),
                "total_hours": round(data["total_seconds"] / 3600, 2),
                "visit_count": data["visit_count"],
                "activity_breakdown": dict(data["activity_types"]),
                "percentage": round((data["total_seconds"] / total_social_seconds * 100), 1) if total_social_seconds > 0 else 0,
            })

        return {
            "period_days": days,
            "total_social_minutes": round(total_social_seconds / 60, 1),
            "total_social_hours": round(total_social_seconds / 3600, 2),
            "daily_average_minutes": round(total_social_seconds / 60 / max(days, 1), 1),
            "total_visits": total_visits,
            "platform_count": len(platforms),
            "platforms": platform_list,
        }

    def _empty_summary(self) -> Dict[str, Any]:
        """Return empty summary when no DB is available."""
        return {
            "period_days": 0,
            "total_social_minutes": 0,
            "total_social_hours": 0,
            "daily_average_minutes": 0,
            "total_visits": 0,
            "platform_count": 0,
            "platforms": [],
        }

    async def get_daily_breakdown(
        self,
        user_id: str,
        days: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        Get day-by-day social media usage for trend analysis.
        Returns: [{date, total_minutes, platforms: {name: minutes}}]
        """
        if not self.db:
            return []

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    DATE(occurred_at) AS day,
                    data->>'socialPlatform' AS platform,
                    SUM(duration_seconds) AS total_seconds
                FROM activities
                WHERE user_id = :uid
                  AND data->>'isSocialMedia' = 'true'
                  AND occurred_at >= :cutoff
                GROUP BY DATE(occurred_at), data->>'socialPlatform'
                ORDER BY day
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        days_map: Dict[str, Dict] = defaultdict(lambda: {"total_seconds": 0, "platforms": defaultdict(int)})

        for row in result.fetchall():
            day_str = str(row[0])
            platform = row[1] or "Unknown"
            seconds = row[2] or 0
            days_map[day_str]["total_seconds"] += seconds
            days_map[day_str]["platforms"][platform] += seconds

        return [
            {
                "date": day,
                "total_minutes": round(data["total_seconds"] / 60, 1),
                "platforms": {p: round(s / 60, 1) for p, s in data["platforms"].items()},
            }
            for day, data in sorted(days_map.items())
        ]

    async def get_peak_hours(
        self,
        user_id: str,
        days: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Get social media peak usage hours (0-23).
        Returns: [{hour, total_minutes, avg_minutes_per_day}]
        """
        if not self.db:
            return []

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    EXTRACT(HOUR FROM occurred_at)::int AS hour,
                    SUM(duration_seconds) AS total_seconds,
                    COUNT(DISTINCT DATE(occurred_at)) AS distinct_days
                FROM activities
                WHERE user_id = :uid
                  AND data->>'isSocialMedia' = 'true'
                  AND occurred_at >= :cutoff
                GROUP BY EXTRACT(HOUR FROM occurred_at)::int
                ORDER BY hour
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        return [
            {
                "hour": row[0],
                "total_minutes": round((row[1] or 0) / 60, 1),
                "avg_minutes_per_day": round((row[1] or 0) / 60 / max(row[2] or 1, 1), 1),
            }
            for row in result.fetchall()
        ]
