"""
Meeting / Video Call Analytics Service.

Aggregates meeting data from browser extension activity events.
Provides meeting load analysis, duration stats, and free-time calculations.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any
from collections import defaultdict
import structlog

logger = structlog.get_logger()

# Supported meeting platforms
MEETING_PLATFORMS = [
    "Zoom", "Google Meet", "Microsoft Teams", "Cisco Webex",
    "Slack Huddle", "Whereby", "Around", "Gather", "Pop",
    "Cal.com", "Loom",
]


class MeetingService:
    """Analyzes meeting/video call patterns from captured browsing data."""

    def __init__(self, db_session=None):
        self.db = db_session

    async def get_meeting_summary(
        self,
        user_id: str,
        days: int = 7,
    ) -> Dict[str, Any]:
        """
        Get meeting summary for a user over the past N days.

        Returns total meeting time, count, per-platform breakdown,
        meeting load percentage, and meeting-free blocks.
        """
        if not self.db:
            return self._empty_summary()

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    data->>'meetingPlatform' AS platform,
                    SUM(duration_seconds) AS total_seconds,
                    COUNT(*) AS meeting_count,
                    AVG(duration_seconds) AS avg_duration,
                    MIN(duration_seconds) AS min_duration,
                    MAX(duration_seconds) AS max_duration,
                    MIN(occurred_at) AS first_meeting,
                    MAX(occurred_at) AS last_meeting
                FROM activities
                WHERE user_id = :uid
                  AND data->>'isMeeting' = 'true'
                  AND data->>'meetingActive' = 'true'
                  AND occurred_at >= :cutoff
                GROUP BY data->>'meetingPlatform'
                ORDER BY total_seconds DESC
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        rows = result.fetchall()
        return self._build_summary(rows, days)

    def _build_summary(self, rows, days: int) -> Dict[str, Any]:
        """Build structured meeting summary from query results."""
        platforms = []
        total_seconds = 0
        total_count = 0

        for row in rows:
            platform = row[0] or "Unknown"
            seconds = int(row[1] or 0)
            count = int(row[2] or 0)
            avg = float(row[3] or 0)

            total_seconds += seconds
            total_count += count

            platforms.append({
                "platform": platform,
                "total_minutes": round(seconds / 60, 1),
                "total_hours": round(seconds / 3600, 2),
                "meeting_count": count,
                "avg_duration_minutes": round(avg / 60, 1),
                "min_duration_minutes": round(int(row[4] or 0) / 60, 1),
                "max_duration_minutes": round(int(row[5] or 0) / 60, 1),
            })

        # Calculate meeting load (% of 8h workday spent in meetings)
        workday_hours = 8
        work_seconds_in_period = days * workday_hours * 3600
        meeting_load_pct = round(
            (total_seconds / work_seconds_in_period * 100) if work_seconds_in_period > 0 else 0,
            1,
        )

        return {
            "period_days": days,
            "total_meeting_minutes": round(total_seconds / 60, 1),
            "total_meeting_hours": round(total_seconds / 3600, 2),
            "total_meeting_count": total_count,
            "daily_average_minutes": round(total_seconds / 60 / max(days, 1), 1),
            "daily_average_count": round(total_count / max(days, 1), 1),
            "avg_meeting_duration_minutes": round(total_seconds / max(total_count, 1) / 60, 1),
            "meeting_load_percentage": meeting_load_pct,
            "meeting_load_status": self._meeting_load_label(meeting_load_pct),
            "platforms": platforms,
        }

    @staticmethod
    def _meeting_load_label(pct: float) -> str:
        """Classify meeting load for burnout risk."""
        if pct < 15:
            return "low"
        elif pct < 30:
            return "moderate"
        elif pct < 50:
            return "high"
        else:
            return "critical"

    def _empty_summary(self) -> Dict[str, Any]:
        return {
            "period_days": 0,
            "total_meeting_minutes": 0,
            "total_meeting_hours": 0,
            "total_meeting_count": 0,
            "daily_average_minutes": 0,
            "daily_average_count": 0,
            "avg_meeting_duration_minutes": 0,
            "meeting_load_percentage": 0,
            "meeting_load_status": "low",
            "platforms": [],
        }

    async def get_daily_breakdown(
        self,
        user_id: str,
        days: int = 7,
    ) -> List[Dict[str, Any]]:
        """
        Get day-by-day meeting breakdown for trend analysis.
        Returns: [{date, total_minutes, meeting_count, platforms: {name: minutes}}]
        """
        if not self.db:
            return []

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    DATE(occurred_at) AS day,
                    data->>'meetingPlatform' AS platform,
                    SUM(duration_seconds) AS total_seconds,
                    COUNT(*) AS meeting_count
                FROM activities
                WHERE user_id = :uid
                  AND data->>'isMeeting' = 'true'
                  AND data->>'meetingActive' = 'true'
                  AND occurred_at >= :cutoff
                GROUP BY DATE(occurred_at), data->>'meetingPlatform'
                ORDER BY day
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        days_map: Dict[str, Dict] = defaultdict(lambda: {
            "total_seconds": 0,
            "meeting_count": 0,
            "platforms": defaultdict(int),
        })

        for row in result.fetchall():
            day_str = str(row[0])
            platform = row[1] or "Unknown"
            seconds = int(row[2] or 0)
            count = int(row[3] or 0)
            days_map[day_str]["total_seconds"] += seconds
            days_map[day_str]["meeting_count"] += count
            days_map[day_str]["platforms"][platform] += seconds

        return [
            {
                "date": day,
                "total_minutes": round(data["total_seconds"] / 60, 1),
                "meeting_count": data["meeting_count"],
                "platforms": {p: round(s / 60, 1) for p, s in data["platforms"].items()},
            }
            for day, data in sorted(days_map.items())
        ]

    async def get_meeting_free_blocks(
        self,
        user_id: str,
        date: str = None,
    ) -> Dict[str, Any]:
        """
        Calculate meeting-free time blocks for a given day.
        Useful for focus time planning.

        Returns: [{start_hour, end_hour, duration_minutes}]
        """
        if not self.db:
            return {"date": date or str(datetime.utcnow().date()), "free_blocks": [], "total_free_hours": 8}

        from sqlalchemy import text

        target_date = date or str(datetime.utcnow().date())

        result = await self.db.execute(
            text("""
                SELECT
                    EXTRACT(HOUR FROM occurred_at)::int AS start_hour,
                    duration_seconds
                FROM activities
                WHERE user_id = :uid
                  AND data->>'isMeeting' = 'true'
                  AND data->>'meetingActive' = 'true'
                  AND DATE(occurred_at) = :target_date
                ORDER BY occurred_at
            """),
            {"uid": user_id, "target_date": target_date},
        )

        # Track occupied hours (9 AM - 5 PM workday)
        meeting_hours = set()
        for row in result.fetchall():
            start = int(row[0])
            duration_hrs = max(1, int((row[1] or 0) / 3600))
            for h in range(start, min(start + duration_hrs, 24)):
                meeting_hours.add(h)

        # Find free blocks within workday (9-17)
        work_start, work_end = 9, 17
        free_blocks = []
        block_start = None

        for hour in range(work_start, work_end):
            if hour not in meeting_hours:
                if block_start is None:
                    block_start = hour
            else:
                if block_start is not None:
                    free_blocks.append({
                        "start_hour": block_start,
                        "end_hour": hour,
                        "duration_minutes": (hour - block_start) * 60,
                    })
                    block_start = None

        # Close last block
        if block_start is not None:
            free_blocks.append({
                "start_hour": block_start,
                "end_hour": work_end,
                "duration_minutes": (work_end - block_start) * 60,
            })

        total_free = sum(b["duration_minutes"] for b in free_blocks)

        return {
            "date": target_date,
            "free_blocks": free_blocks,
            "total_free_hours": round(total_free / 60, 1),
            "total_meeting_hours": round((8 * 60 - total_free) / 60, 1),
        }
