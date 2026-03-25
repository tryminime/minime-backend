"""
Knowledge Decay Modeling Service.

Tracks knowledge/skill freshness over time using exponential decay.
Skills not recently used decay in mastery score.

Formula: freshness = exp(-λ × days_since_last_seen)
Where λ is adjusted by occurrence_count (more uses = slower decay).
"""

from __future__ import annotations

import math
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_

from models import Entity, ActivityEntityLink

logger = structlog.get_logger()


# Decay constants
BASE_LAMBDA = 0.015  # Base decay rate (~50% at 46 days)
MIN_LAMBDA = 0.005   # Minimum decay for heavily-used entities (~50% at 139 days)
OCCURRENCE_DAMPENING = 0.1  # How much each occurrence slows decay


def _compute_freshness(days_since: float, occurrence_count: int) -> float:
    """
    Compute freshness score using exponential decay.
    More occurrences = slower decay rate.
    """
    # Adjust lambda: more occurrences → lower lambda → slower decay
    adjusted_lambda = max(
        MIN_LAMBDA,
        BASE_LAMBDA / (1 + OCCURRENCE_DAMPENING * max(occurrence_count - 1, 0))
    )
    return math.exp(-adjusted_lambda * days_since)


def _freshness_status(score: float) -> str:
    """Map freshness score to human-readable status."""
    if score >= 0.7:
        return "fresh"
    elif score >= 0.4:
        return "fading"
    elif score >= 0.15:
        return "stale"
    else:
        return "forgotten"


def _status_color(status: str) -> str:
    """Map status to UI color."""
    return {
        "fresh": "#22c55e",
        "fading": "#eab308",
        "stale": "#f97316",
        "forgotten": "#ef4444",
    }.get(status, "#6b7280")


class KnowledgeDecayService:
    """Computes knowledge freshness for user entities."""

    async def get_decay_analysis(
        self, user_id: UUID, db: AsyncSession, limit: int = 100
    ) -> Dict[str, Any]:
        """
        Compute freshness scores for all user entities.
        Returns sorted by freshness (most stale first for attention).
        """
        now = datetime.now(timezone.utc)

        # Fetch user entities with occurrence data
        result = await db.execute(
            select(Entity)
            .where(and_(
                Entity.user_id == user_id,
                Entity.last_seen.isnot(None),
            ))
            .order_by(Entity.last_seen.asc())
            .limit(limit)
        )
        entities = result.scalars().all()

        items: List[Dict[str, Any]] = []
        status_counts = {"fresh": 0, "fading": 0, "stale": 0, "forgotten": 0}
        total_freshness = 0.0

        for e in entities:
            last_seen = e.last_seen
            if last_seen and last_seen.tzinfo is None:
                last_seen = last_seen.replace(tzinfo=timezone.utc)

            days_since = (now - last_seen).total_seconds() / 86400 if last_seen else 999
            occ = e.occurrence_count or 1
            freshness = _compute_freshness(days_since, occ)
            status = _freshness_status(freshness)

            status_counts[status] = status_counts.get(status, 0) + 1
            total_freshness += freshness

            items.append({
                "id": str(e.id),
                "name": e.name,
                "entity_type": e.entity_type,
                "freshness_score": round(freshness, 3),
                "status": status,
                "color": _status_color(status),
                "days_since_last_seen": round(days_since, 1),
                "last_seen": last_seen.isoformat() if last_seen else None,
                "occurrence_count": occ,
                "decay_rate": round(
                    BASE_LAMBDA / (1 + OCCURRENCE_DAMPENING * max(occ - 1, 0)), 4
                ),
            })

        # Sort: most stale first (lowest freshness)
        items.sort(key=lambda x: x["freshness_score"])

        avg_freshness = round(total_freshness / len(items), 3) if items else 0

        return {
            "entities": items,
            "total": len(items),
            "average_freshness": avg_freshness,
            "status_breakdown": status_counts,
            "overall_health": (
                "healthy" if avg_freshness >= 0.6
                else "needs_attention" if avg_freshness >= 0.3
                else "deteriorating"
            ),
        }

    async def get_at_risk(
        self, user_id: UUID, db: AsyncSession, threshold: float = 0.4
    ) -> Dict[str, Any]:
        """
        Return entities at risk of being forgotten (freshness < threshold).
        Includes refresh suggestions.
        """
        full = await self.get_decay_analysis(user_id, db, limit=500)
        at_risk = [e for e in full["entities"] if e["freshness_score"] < threshold]

        suggestions = []
        for e in at_risk[:10]:
            days = e["days_since_last_seen"]
            suggestions.append({
                "entity": e["name"],
                "entity_type": e["entity_type"],
                "freshness": e["freshness_score"],
                "days_inactive": round(days),
                "suggestion": f"You haven't engaged with {e['name']} in {round(days)} days. "
                              f"Consider revisiting to maintain proficiency.",
                "urgency": "high" if e["freshness_score"] < 0.15 else "medium",
            })

        return {
            "at_risk_entities": at_risk,
            "total_at_risk": len(at_risk),
            "refresh_suggestions": suggestions,
            "threshold": threshold,
        }


# Global singleton
knowledge_decay_service = KnowledgeDecayService()
