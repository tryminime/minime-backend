"""
Wearable Data Sync Service.

Handles data synchronization from wearable providers (Fitbit, Oura Ring).
Normalizes data across providers into a unified schema.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import httpx
import structlog

logger = structlog.get_logger()


# Metric type normalization map
METRIC_TYPES = {
    "steps": {"unit": "steps", "description": "Daily step count"},
    "heart_rate": {"unit": "bpm", "description": "Heart rate"},
    "heart_rate_resting": {"unit": "bpm", "description": "Resting heart rate"},
    "hrv": {"unit": "ms", "description": "Heart rate variability (RMSSD)"},
    "sleep_duration": {"unit": "minutes", "description": "Total sleep duration"},
    "sleep_efficiency": {"unit": "percent", "description": "Sleep efficiency score"},
    "sleep_deep": {"unit": "minutes", "description": "Deep sleep duration"},
    "sleep_rem": {"unit": "minutes", "description": "REM sleep duration"},
    "sleep_light": {"unit": "minutes", "description": "Light sleep duration"},
    "calories": {"unit": "kcal", "description": "Calories burned"},
    "active_minutes": {"unit": "minutes", "description": "Active minutes"},
    "readiness_score": {"unit": "score", "description": "Readiness score (0-100)"},
    "stress_score": {"unit": "score", "description": "Stress level score"},
    "spo2": {"unit": "percent", "description": "Blood oxygen saturation"},
    "body_temperature": {"unit": "celsius_deviation", "description": "Body temperature deviation"},
    "respiratory_rate": {"unit": "breaths_per_min", "description": "Respiratory rate"},
}


class WearableService:
    """Sync and normalize data from wearable providers."""

    def __init__(self, db_session=None):
        self.db = db_session

    async def sync_fitbit(self, access_token: str, user_id: str, days: int = 1) -> Dict[str, Any]:
        """
        Sync data from Fitbit API.
        Pulls: steps, heart rate, sleep, calories, active minutes.
        """
        data_points = []
        today = datetime.utcnow().strftime("%Y-%m-%d")
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")

        headers = {"Authorization": f"Bearer {access_token}"}

        async with httpx.AsyncClient(timeout=30) as client:
            # --- Steps & Calories ---
            try:
                resp = await client.get(
                    f"https://api.fitbit.com/1/user/-/activities/date/{today}.json",
                    headers=headers,
                )
                if resp.status_code == 200:
                    summary = resp.json().get("summary", {})
                    data_points.append({
                        "metric_type": "steps",
                        "metric_value": summary.get("steps", 0),
                        "metric_unit": "steps",
                        "recorded_at": today,
                    })
                    data_points.append({
                        "metric_type": "calories",
                        "metric_value": summary.get("caloriesOut", 0),
                        "metric_unit": "kcal",
                        "recorded_at": today,
                    })
                    data_points.append({
                        "metric_type": "active_minutes",
                        "metric_value": (summary.get("fairlyActiveMinutes", 0) +
                                        summary.get("veryActiveMinutes", 0)),
                        "metric_unit": "minutes",
                        "recorded_at": today,
                    })
            except Exception as e:
                logger.warning("fitbit_activity_sync_error", error=str(e))

            # --- Heart Rate ---
            try:
                resp = await client.get(
                    f"https://api.fitbit.com/1/user/-/activities/heart/date/{today}/1d.json",
                    headers=headers,
                )
                if resp.status_code == 200:
                    hr_data = resp.json().get("activities-heart", [])
                    if hr_data:
                        resting = hr_data[0].get("value", {}).get("restingHeartRate")
                        if resting:
                            data_points.append({
                                "metric_type": "heart_rate_resting",
                                "metric_value": resting,
                                "metric_unit": "bpm",
                                "recorded_at": today,
                            })
            except Exception as e:
                logger.warning("fitbit_heart_rate_error", error=str(e))

            # --- Sleep ---
            try:
                resp = await client.get(
                    f"https://api.fitbit.com/1.2/user/-/sleep/date/{today}.json",
                    headers=headers,
                )
                if resp.status_code == 200:
                    sleep_data = resp.json().get("sleep", [])
                    if sleep_data:
                        main_sleep = sleep_data[0]
                        data_points.append({
                            "metric_type": "sleep_duration",
                            "metric_value": main_sleep.get("minutesAsleep", 0),
                            "metric_unit": "minutes",
                            "recorded_at": today,
                        })
                        data_points.append({
                            "metric_type": "sleep_efficiency",
                            "metric_value": main_sleep.get("efficiency", 0),
                            "metric_unit": "percent",
                            "recorded_at": today,
                        })
                        # Sleep stages
                        stages = main_sleep.get("levels", {}).get("summary", {})
                        if stages:
                            for stage, key in [("sleep_deep", "deep"), ("sleep_rem", "rem"), ("sleep_light", "light")]:
                                if key in stages:
                                    data_points.append({
                                        "metric_type": stage,
                                        "metric_value": stages[key].get("minutes", 0),
                                        "metric_unit": "minutes",
                                        "recorded_at": today,
                                    })
            except Exception as e:
                logger.warning("fitbit_sleep_error", error=str(e))

            # --- SpO2 ---
            try:
                resp = await client.get(
                    f"https://api.fitbit.com/1/user/-/spo2/date/{today}.json",
                    headers=headers,
                )
                if resp.status_code == 200:
                    spo2 = resp.json().get("value", {}).get("avg")
                    if spo2:
                        data_points.append({
                            "metric_type": "spo2",
                            "metric_value": spo2,
                            "metric_unit": "percent",
                            "recorded_at": today,
                        })
            except Exception as e:
                logger.warning("fitbit_spo2_error", error=str(e))

        logger.info("fitbit_sync_complete", user_id=user_id, data_points=len(data_points))
        return {"provider": "fitbit", "data_points": data_points, "synced_at": datetime.utcnow().isoformat()}

    async def sync_oura(self, access_token: str, user_id: str, days: int = 1) -> Dict[str, Any]:
        """
        Sync data from Oura Ring API v2.
        Pulls: readiness, sleep, heart rate, HRV, temperature.
        """
        data_points = []
        start_date = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        end_date = datetime.utcnow().strftime("%Y-%m-%d")

        headers = {"Authorization": f"Bearer {access_token}"}

        async with httpx.AsyncClient(timeout=30) as client:
            # --- Daily Readiness ---
            try:
                resp = await client.get(
                    f"https://api.ouraring.com/v2/usercollection/daily_readiness"
                    f"?start_date={start_date}&end_date={end_date}",
                    headers=headers,
                )
                if resp.status_code == 200:
                    for item in resp.json().get("data", []):
                        data_points.append({
                            "metric_type": "readiness_score",
                            "metric_value": item.get("score", 0),
                            "metric_unit": "score",
                            "recorded_at": item.get("day", end_date),
                        })
            except Exception as e:
                logger.warning("oura_readiness_error", error=str(e))

            # --- Daily Sleep ---
            try:
                resp = await client.get(
                    f"https://api.ouraring.com/v2/usercollection/daily_sleep"
                    f"?start_date={start_date}&end_date={end_date}",
                    headers=headers,
                )
                if resp.status_code == 200:
                    for item in resp.json().get("data", []):
                        day = item.get("day", end_date)
                        contributors = item.get("contributors", {})
                        data_points.append({
                            "metric_type": "sleep_efficiency",
                            "metric_value": contributors.get("efficiency", 0),
                            "metric_unit": "percent",
                            "recorded_at": day,
                        })
            except Exception as e:
                logger.warning("oura_sleep_error", error=str(e))

            # --- Sleep Sessions for detailed stages ---
            try:
                resp = await client.get(
                    f"https://api.ouraring.com/v2/usercollection/sleep"
                    f"?start_date={start_date}&end_date={end_date}",
                    headers=headers,
                )
                if resp.status_code == 200:
                    for item in resp.json().get("data", []):
                        day = item.get("day", end_date)
                        data_points.append({
                            "metric_type": "sleep_duration",
                            "metric_value": item.get("total_sleep_duration", 0) / 60,  # seconds → minutes
                            "metric_unit": "minutes",
                            "recorded_at": day,
                        })
                        data_points.append({
                            "metric_type": "sleep_deep",
                            "metric_value": item.get("deep_sleep_duration", 0) / 60,
                            "metric_unit": "minutes",
                            "recorded_at": day,
                        })
                        data_points.append({
                            "metric_type": "sleep_rem",
                            "metric_value": item.get("rem_sleep_duration", 0) / 60,
                            "metric_unit": "minutes",
                            "recorded_at": day,
                        })
                        if item.get("average_heart_rate"):
                            data_points.append({
                                "metric_type": "heart_rate_resting",
                                "metric_value": item["average_heart_rate"],
                                "metric_unit": "bpm",
                                "recorded_at": day,
                            })
                        if item.get("average_hrv"):
                            data_points.append({
                                "metric_type": "hrv",
                                "metric_value": item["average_hrv"],
                                "metric_unit": "ms",
                                "recorded_at": day,
                            })
            except Exception as e:
                logger.warning("oura_sleep_sessions_error", error=str(e))

            # --- Body Temperature ---
            try:
                resp = await client.get(
                    f"https://api.ouraring.com/v2/usercollection/daily_spo2"
                    f"?start_date={start_date}&end_date={end_date}",
                    headers=headers,
                )
                if resp.status_code == 200:
                    for item in resp.json().get("data", []):
                        avg = item.get("spo2_percentage", {}).get("average")
                        if avg:
                            data_points.append({
                                "metric_type": "spo2",
                                "metric_value": avg,
                                "metric_unit": "percent",
                                "recorded_at": item.get("day", end_date),
                            })
            except Exception as e:
                logger.warning("oura_spo2_error", error=str(e))

        logger.info("oura_sync_complete", user_id=user_id, data_points=len(data_points))
        return {"provider": "oura", "data_points": data_points, "synced_at": datetime.utcnow().isoformat()}

    async def store_data_points(
        self,
        user_id: str,
        provider: str,
        data_points: List[Dict[str, Any]],
    ) -> int:
        """Store normalized data points in the database."""
        if not self.db or not data_points:
            return 0

        from sqlalchemy import text

        inserted = 0
        for dp in data_points:
            try:
                await self.db.execute(
                    text("""
                        INSERT INTO wearable_data (user_id, provider, metric_type, metric_value, metric_unit, recorded_at)
                        VALUES (:uid, :provider, :type, :value, :unit, :recorded)
                    """),
                    {
                        "uid": user_id,
                        "provider": provider,
                        "type": dp["metric_type"],
                        "value": dp["metric_value"],
                        "unit": dp.get("metric_unit", ""),
                        "recorded": dp["recorded_at"],
                    },
                )
                inserted += 1
            except Exception as e:
                logger.warning("wearable_data_insert_error", error=str(e), metric=dp.get("metric_type"))

        await self.db.commit()

        # Update last_synced timestamp
        await self.db.execute(
            text("""UPDATE wearable_integrations SET last_synced_at = NOW() WHERE user_id = :uid AND provider = :p"""),
            {"uid": user_id, "p": provider},
        )
        await self.db.commit()

        return inserted

    async def get_wellness_dashboard(self, user_id: str, days: int = 7) -> Dict[str, Any]:
        """
        Build a wellness dashboard from all wearable data.
        Returns latest values for key metrics + trends.
        """
        if not self.db:
            return {"metrics": {}, "trends": []}

        from sqlalchemy import text

        cutoff = datetime.utcnow() - timedelta(days=days)

        # Get latest value for each metric type
        result = await self.db.execute(
            text("""
                SELECT DISTINCT ON (metric_type)
                    metric_type, metric_value, metric_unit, recorded_at, provider
                FROM wearable_data
                WHERE user_id = :uid AND recorded_at >= :cutoff
                ORDER BY metric_type, recorded_at DESC
            """),
            {"uid": user_id, "cutoff": cutoff},
        )

        metrics = {}
        for row in result.fetchall():
            meta = METRIC_TYPES.get(row[0], {"unit": row[2], "description": row[0]})
            metrics[row[0]] = {
                "value": row[1],
                "unit": row[2] or meta["unit"],
                "description": meta["description"],
                "recorded_at": row[3].isoformat() if row[3] else "",
                "provider": row[4],
            }

        return {
            "period_days": days,
            "metrics": metrics,
            "metric_count": len(metrics),
        }
