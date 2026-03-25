"""
Feature Gating — Tier-based access control for MiniMe API.

Usage:
    gate = FeatureGate(user)
    gate.require("cloud_sync")                       # raises HTTP 403 if not allowed
    gate.require_limit("knowledge_base_items", 24)   # raises HTTP 402 if at limit
    allowed = gate.can("ai_copilot")                 # bool check
    limit   = gate.limit("knowledge_base_items")     # -1 = unlimited
"""
from __future__ import annotations
from fastapi import HTTPException
from models import User

# ── Tier → feature map ────────────────────────────────────────────────────────
# DB stores: "free" | "premium" (= Pro) | "enterprise"
TIER_FEATURES: dict[str, dict[str, int | bool]] = {
    "free": {
        # Knowledge Base
        "knowledge_base_items": 25,        # max items
        "knowledge_base_upload": False,    # manual document upload
        "knowledge_base_collections": False,
        "knowledge_base_export": False,
        # Intelligence
        "ai_copilot": False,
        "analytics_advanced": False,
        # Sync
        "cloud_sync": False,
        # API
        "api_access": False,
        # Activity tracking
        "activities_per_month": 100,
    },
    "premium": {  # = "Pro" plan
        "knowledge_base_items": 500,
        "knowledge_base_upload": True,
        "knowledge_base_collections": True,
        "knowledge_base_export": True,
        "ai_copilot": True,
        "analytics_advanced": True,
        "cloud_sync": True,
        "api_access": False,
        "activities_per_month": -1,        # unlimited
    },
    "enterprise": {
        "knowledge_base_items": -1,        # unlimited
        "knowledge_base_upload": True,
        "knowledge_base_collections": True,
        "knowledge_base_export": True,
        "ai_copilot": True,
        "analytics_advanced": True,
        "cloud_sync": True,
        "api_access": True,
        "activities_per_month": -1,
    },
}

# Human-readable names for error messages
FEATURE_LABELS: dict[str, str] = {
    "knowledge_base_items": "Knowledge Base items",
    "knowledge_base_upload": "Manual document upload",
    "knowledge_base_collections": "Knowledge Base collections",
    "knowledge_base_export": "Knowledge Base export",
    "ai_copilot": "AI Copilot",
    "analytics_advanced": "Advanced analytics",
    "cloud_sync": "Cloud sync",
    "api_access": "API access",
    "activities_per_month": "monthly activity tracking",
}

UPGRADE_HINT = " Upgrade at https://tryminime.com/pricing"


class FeatureGate:
    """
    Instantiate once per request with the authenticated User object.

    Example:
        gate = FeatureGate(user)
        gate.require("cloud_sync")
    """

    def __init__(self, user: User):
        # Normalise tier value — DB may store "premium" for the "Pro" plan
        raw_tier = (user.tier or "free").lower()
        # Accept aliases
        tier = {"pro": "premium"}.get(raw_tier, raw_tier)
        self._features: dict[str, int | bool] = TIER_FEATURES.get(
            tier, TIER_FEATURES["free"]
        )
        self.tier = tier

    # ── Public helpers ────────────────────────────────────────────────────────

    def can(self, feature: str) -> bool:
        """Return True if the user's tier allows *feature*."""
        val = self._features.get(feature, False)
        return bool(val) and val != 0

    def limit(self, feature: str) -> int:
        """Return the numeric limit for *feature*. -1 means unlimited."""
        val = self._features.get(feature, 0)
        if isinstance(val, bool):
            return -1 if val else 0
        return int(val)

    def require(self, feature: str, detail: str | None = None) -> None:
        """
        Raise HTTP 403 if the feature is not allowed for the current tier.
        """
        if not self.can(feature):
            label = FEATURE_LABELS.get(feature, feature)
            raise HTTPException(
                status_code=403,
                detail=detail or f"{label} requires a Pro or Enterprise plan.{UPGRADE_HINT}",
            )

    def require_limit(
        self,
        feature: str,
        current_count: int,
        detail: str | None = None,
    ) -> None:
        """
        Raise HTTP 402 if *current_count* has reached or exceeded the feature limit.
        Does nothing when the limit is -1 (unlimited).
        """
        lim = self.limit(feature)
        if lim == -1:
            return  # unlimited
        if current_count >= lim:
            label = FEATURE_LABELS.get(feature, feature)
            raise HTTPException(
                status_code=402,
                detail=detail or (
                    f"You have reached your {label} limit ({lim}). "
                    f"Upgrade to Pro for more.{UPGRADE_HINT}"
                ),
            )

    def usage_info(self, feature: str, current_count: int) -> dict:
        """Return a dict with usage stats suitable for API responses."""
        lim = self.limit(feature)
        if lim == -1:
            return {"limit": -1, "current": current_count, "remaining": -1,
                    "percent_used": 0, "exceeded": False, "warning": False}
        remaining = max(0, lim - current_count)
        pct = round((current_count / max(lim, 1)) * 100, 1)
        return {
            "limit": lim,
            "current": current_count,
            "remaining": remaining,
            "percent_used": pct,
            "exceeded": current_count >= lim,
            "warning": pct >= 80,
        }
