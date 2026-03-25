"""
Focus Period Service — Backend analytics for focus sessions.

Analyzes focus period data from desktop and browser extension to provide:
- Focus session summaries (deep work vs shallow time)
- Daily/weekly focus trends
- Productivity-weighted focus scoring
- Optimal focus time recommendations
- Context switch analysis
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import structlog

logger = structlog.get_logger()


class FocusPeriodService:
    """Analyze and surface insights from focus period data."""

    def __init__(self, db_session=None):
        self.db = db_session

    async def get_focus_summary(
        self, user_id: str, days: int = 7
    ) -> Dict[str, Any]:
        """
        Get a comprehensive focus summary over the given period.

        Returns:
            - total_focus_minutes: Total time in focus sessions (≥2 min on same context)
            - deep_work_minutes: Time in DeepWork sessions (≥25 min)
            - focused_minutes: Time in Focused sessions (≥15 min)
            - shallow_minutes: Time in Moderate/Shallow sessions
            - avg_session_minutes: Average focus session length
            - longest_session_minutes: Longest single session
            - total_sessions: Number of focus sessions
            - deep_work_sessions: Number of deep work sessions
            - avg_focus_score: Average focus quality score (0-100)
            - focus_ratio: deep_work_minutes / total_focus_minutes
            - daily_breakdown: Per-day stats
        """
        if not self.db:
            return self._empty_summary(days)

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        # Query activity events that have focus metadata
        result = await self.db.execute(
            text("""
                SELECT
                    date_trunc('day', a.created_at) as day,
                    a.metadata
                FROM activities a
                WHERE a.user_id = :uid
                  AND a.created_at >= :cutoff
                  AND a.metadata->>'focus_period' IS NOT NULL
                ORDER BY a.created_at
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        rows = result.fetchall()

        # Also check for explicitly stored focus sessions
        focus_result = await self.db.execute(
            text("""
                SELECT
                    date_trunc('day', created_at) as day,
                    duration_seconds,
                    depth,
                    score,
                    app_name,
                    category,
                    created_at
                FROM focus_sessions
                WHERE user_id = :uid
                  AND created_at >= :cutoff
                ORDER BY created_at
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        focus_rows = focus_result.fetchall()

        # Process focus sessions
        sessions = []
        for row in focus_rows:
            sessions.append({
                "day": row[0].strftime("%Y-%m-%d") if row[0] else "",
                "duration_seconds": row[1] or 0,
                "depth": row[2] or "Shallow",
                "score": row[3] or 0,
                "app_name": row[4] or "",
                "category": row[5] or "",
            })

        if not sessions:
            return self._empty_summary(days)

        # Compute aggregates
        total_seconds = sum(s["duration_seconds"] for s in sessions)
        deep_seconds = sum(s["duration_seconds"] for s in sessions if s["depth"] == "DeepWork")
        focused_seconds = sum(s["duration_seconds"] for s in sessions if s["depth"] in ("DeepWork", "Focused"))
        shallow_seconds = total_seconds - focused_seconds

        deep_count = sum(1 for s in sessions if s["depth"] == "DeepWork")
        focused_count = sum(1 for s in sessions if s["depth"] in ("DeepWork", "Focused"))

        avg_duration = total_seconds / len(sessions) if sessions else 0
        longest = max(s["duration_seconds"] for s in sessions) if sessions else 0
        avg_score = sum(s["score"] for s in sessions) / len(sessions) if sessions else 0

        # Daily breakdown
        daily = {}
        for s in sessions:
            day = s["day"]
            if day not in daily:
                daily[day] = {
                    "date": day,
                    "sessions": 0,
                    "deep_work_sessions": 0,
                    "total_minutes": 0,
                    "deep_work_minutes": 0,
                    "avg_score": 0,
                    "_scores": [],
                }
            daily[day]["sessions"] += 1
            daily[day]["total_minutes"] += s["duration_seconds"] / 60
            daily[day]["_scores"].append(s["score"])
            if s["depth"] == "DeepWork":
                daily[day]["deep_work_sessions"] += 1
                daily[day]["deep_work_minutes"] += s["duration_seconds"] / 60

        # Finalize daily averages
        daily_breakdown = []
        for day_data in sorted(daily.values(), key=lambda d: d["date"]):
            scores = day_data.pop("_scores")
            day_data["avg_score"] = round(sum(scores) / len(scores), 1) if scores else 0
            day_data["total_minutes"] = round(day_data["total_minutes"], 1)
            day_data["deep_work_minutes"] = round(day_data["deep_work_minutes"], 1)
            daily_breakdown.append(day_data)

        return {
            "period_days": days,
            "total_sessions": len(sessions),
            "deep_work_sessions": deep_count,
            "focused_sessions": focused_count,
            "total_focus_minutes": round(total_seconds / 60, 1),
            "deep_work_minutes": round(deep_seconds / 60, 1),
            "focused_minutes": round(focused_seconds / 60, 1),
            "shallow_minutes": round(shallow_seconds / 60, 1),
            "avg_session_minutes": round(avg_duration / 60, 1),
            "longest_session_minutes": round(longest / 60, 1),
            "avg_focus_score": round(avg_score, 1),
            "focus_ratio": round(focused_seconds / total_seconds, 2) if total_seconds > 0 else 0,
            "daily_breakdown": daily_breakdown,
        }

    async def get_optimal_focus_times(self, user_id: str, days: int = 30) -> Dict[str, Any]:
        """
        Analyze when the user achieves their best focus sessions.

        Returns hour-of-day distribution showing when deep work occurs most often.
        """
        if not self.db:
            return {"hours": [], "best_hours": [], "recommendation": ""}

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    extract(hour from created_at) as hour,
                    depth,
                    duration_seconds,
                    score
                FROM focus_sessions
                WHERE user_id = :uid
                  AND created_at >= :cutoff
                ORDER BY created_at
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        rows = result.fetchall()
        if not rows:
            return {
                "hours": [],
                "best_hours": [],
                "recommendation": "Not enough data yet. Keep working and we'll find your optimal focus times!",
            }

        # Build hour-by-hour stats
        hour_data = {h: {"hour": h, "sessions": 0, "deep_count": 0, "total_minutes": 0, "avg_score": 0, "_scores": []} for h in range(24)}

        for row in rows:
            h = int(row[0])
            hour_data[h]["sessions"] += 1
            hour_data[h]["total_minutes"] += (row[2] or 0) / 60
            hour_data[h]["_scores"].append(row[3] or 0)
            if row[1] == "DeepWork":
                hour_data[h]["deep_count"] += 1

        # Finalize
        hours = []
        for h in range(24):
            d = hour_data[h]
            scores = d.pop("_scores")
            d["avg_score"] = round(sum(scores) / len(scores), 1) if scores else 0
            d["total_minutes"] = round(d["total_minutes"], 1)
            hours.append(d)

        # Find best hours (highest deep work count + score)
        ranked = sorted(hours, key=lambda h: (h["deep_count"], h["avg_score"]), reverse=True)
        best_hours = [h["hour"] for h in ranked[:3] if h["sessions"] > 0]

        # Generate recommendation
        if best_hours:
            time_strs = [f"{h}:00-{h+1}:00" for h in best_hours]
            recommendation = f"Your peak focus hours are {', '.join(time_strs)}. Schedule deep work during these times for best results."
        else:
            recommendation = "Not enough focus data to determine your optimal hours yet."

        return {
            "hours": hours,
            "best_hours": best_hours,
            "recommendation": recommendation,
        }

    async def get_context_switch_analysis(self, user_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Analyze context switching patterns from focus session gaps.

        A context switch = transition between different apps/domains.
        High switch rates correlate with reduced deep work.
        """
        if not self.db:
            return {"avg_switches_per_hour": 0, "daily": []}

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        result = await self.db.execute(
            text("""
                SELECT
                    date_trunc('day', created_at) as day,
                    app_name,
                    category,
                    created_at
                FROM focus_sessions
                WHERE user_id = :uid
                  AND created_at >= :cutoff
                ORDER BY created_at
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        rows = result.fetchall()
        if not rows:
            return {"avg_switches_per_hour": 0, "daily": [], "period_days": days}

        # Count context switches per day
        daily = {}
        prev_app = None
        for row in rows:
            day = row[0].strftime("%Y-%m-%d") if row[0] else ""
            app = row[1]

            if day not in daily:
                daily[day] = {"date": day, "switches": 0, "sessions": 0, "hours_tracked": 0}

            daily[day]["sessions"] += 1

            if prev_app and app != prev_app:
                daily[day]["switches"] += 1
            prev_app = app

        daily_list = sorted(daily.values(), key=lambda d: d["date"])

        total_switches = sum(d["switches"] for d in daily_list)
        total_sessions = sum(d["sessions"] for d in daily_list)
        avg_per_day = total_switches / len(daily_list) if daily_list else 0

        return {
            "period_days": days,
            "total_context_switches": total_switches,
            "total_sessions": total_sessions,
            "avg_switches_per_day": round(avg_per_day, 1),
            "daily": daily_list,
        }

    async def store_focus_sessions(
        self, user_id: str, sessions: List[Dict[str, Any]], source: str = "desktop"
    ) -> int:
        """Store focus sessions from desktop or browser into the DB."""
        if not self.db or not sessions:
            return 0

        from sqlalchemy import text

        # Ensure table exists
        await self.db.execute(text("""
            CREATE TABLE IF NOT EXISTS focus_sessions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id UUID NOT NULL,
                app_name TEXT,
                category TEXT,
                duration_seconds INTEGER NOT NULL,
                depth TEXT NOT NULL,
                score INTEGER DEFAULT 0,
                source TEXT DEFAULT 'desktop',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_focus_user ON focus_sessions(user_id, created_at);
        """))
        await self.db.commit()

        inserted = 0
        for s in sessions:
            try:
                await self.db.execute(
                    text("""
                        INSERT INTO focus_sessions (user_id, app_name, category, duration_seconds, depth, score, source, created_at)
                        VALUES (:uid, :app, :cat, :dur, :depth, :score, :source, :created)
                    """),
                    {
                        "uid": user_id,
                        "app": s.get("app_name", s.get("domain", "")),
                        "cat": s.get("category", ""),
                        "dur": s.get("duration_seconds", 0),
                        "depth": s.get("depth", "Shallow"),
                        "score": s.get("score", 0),
                        "source": source,
                        "created": s.get("started_at", datetime.utcnow().isoformat()),
                    },
                )
                inserted += 1
            except Exception as e:
                logger.warning("focus_session_insert_error", error=str(e))

        await self.db.commit()
        logger.info("focus_sessions_stored", user_id=user_id, count=inserted, source=source)
        return inserted

    def _empty_summary(self, days: int) -> Dict[str, Any]:
        return {
            "period_days": days,
            "total_sessions": 0,
            "deep_work_sessions": 0,
            "focused_sessions": 0,
            "total_focus_minutes": 0,
            "deep_work_minutes": 0,
            "focused_minutes": 0,
            "shallow_minutes": 0,
            "avg_session_minutes": 0,
            "longest_session_minutes": 0,
            "avg_focus_score": 0,
            "focus_ratio": 0,
            "daily_breakdown": [],
        }
