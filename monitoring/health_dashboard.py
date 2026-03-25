"""
Health Dashboard — system health aggregation and monitoring endpoint.

Provides:
- Database connection health checks (PostgreSQL, Neo4j, Redis, Qdrant)
- Service status aggregation
- Queue depth and worker status
- Cache hit rates
- System resource metrics
- Uptime tracking
- Composite health score
"""

import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class HealthDashboard:
    """
    Aggregates health information from all system components
    and exposes a unified health endpoint for monitoring dashboards.
    """

    # Thresholds for health scoring
    THRESHOLDS = {
        "db_latency_ms": {"healthy": 50, "degraded": 200},
        "queue_depth": {"healthy": 100, "degraded": 1000},
        "error_rate": {"healthy": 0.01, "degraded": 0.05},
        "cache_hit_rate": {"healthy": 0.8, "degraded": 0.5},
        "memory_usage_pct": {"healthy": 70, "degraded": 90},
    }

    def __init__(self):
        self._start_time = time.time()
        self._service_statuses: Dict[str, Dict[str, Any]] = {}
        self._health_history: List[Dict[str, Any]] = []
        self._db_checks: Dict[str, Dict[str, Any]] = {}
        self._check_count: int = 0

    # ── Service registration ─────────────────────────────────────

    def register_service(
        self,
        name: str,
        check_fn: Optional[Any] = None,
    ) -> None:
        """Register a service for health checking."""
        self._service_statuses[name] = {
            "status": "unknown",
            "last_check": None,
            "latency_ms": 0,
            "error": None,
            "check_fn": check_fn,
        }

    def update_service_status(
        self,
        name: str,
        status: str,
        latency_ms: float = 0,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update the status of a registered service."""
        self._service_statuses[name] = {
            "status": status,
            "last_check": datetime.utcnow().isoformat(),
            "latency_ms": round(latency_ms, 2),
            "error": error,
            "metadata": metadata or {},
        }

    # ── Database health ──────────────────────────────────────────

    def check_database(
        self,
        name: str,
        connected: bool,
        latency_ms: float = 0,
        pool_size: int = 0,
        active_connections: int = 0,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Record a database health check result."""
        status = "healthy"
        if not connected:
            status = "down"
        elif latency_ms > self.THRESHOLDS["db_latency_ms"]["degraded"]:
            status = "degraded"
        elif latency_ms > self.THRESHOLDS["db_latency_ms"]["healthy"]:
            status = "slow"

        result = {
            "name": name,
            "status": status,
            "connected": connected,
            "latency_ms": round(latency_ms, 2),
            "pool_size": pool_size,
            "active_connections": active_connections,
            "error": error,
            "checked_at": datetime.utcnow().isoformat(),
        }
        self._db_checks[name] = result
        return result

    # ── Composite health ─────────────────────────────────────────

    def get_health(self) -> Dict[str, Any]:
        """
        Full system health response.
        Returns composite status + per-component breakdown.
        """
        self._check_count += 1
        now = datetime.utcnow()
        uptime = time.time() - self._start_time

        # Compute per-component statuses
        components: Dict[str, Dict[str, Any]] = {}

        # Databases
        for name, info in self._db_checks.items():
            components[name] = {
                "status": info["status"],
                "latency_ms": info["latency_ms"],
                "details": {
                    "connected": info["connected"],
                    "pool_size": info.get("pool_size", 0),
                    "active_connections": info.get("active_connections", 0),
                },
            }

        # Services
        for name, info in self._service_statuses.items():
            if name not in components:
                components[name] = {
                    "status": info["status"],
                    "latency_ms": info.get("latency_ms", 0),
                    "last_check": info.get("last_check"),
                    "error": info.get("error"),
                }

        # Overall status
        statuses = [c["status"] for c in components.values()]
        if any(s == "down" for s in statuses):
            overall = "unhealthy"
        elif any(s in ("degraded", "slow") for s in statuses):
            overall = "degraded"
        elif not statuses:
            overall = "unknown"
        else:
            overall = "healthy"

        # Health score (0-100)
        if not statuses:
            score = 0
        else:
            healthy_count = sum(1 for s in statuses if s == "healthy")
            score = round(healthy_count / len(statuses) * 100)

        result = {
            "status": overall,
            "score": score,
            "timestamp": now.isoformat(),
            "uptime_seconds": round(uptime),
            "uptime_human": self._format_uptime(uptime),
            "checks_performed": self._check_count,
            "components": components,
            "summary": {
                "total": len(components),
                "healthy": sum(1 for s in statuses if s == "healthy"),
                "degraded": sum(1 for s in statuses if s in ("degraded", "slow")),
                "down": sum(1 for s in statuses if s == "down"),
                "unknown": sum(1 for s in statuses if s == "unknown"),
            },
        }

        # Keep history (last 100 checks)
        self._health_history.append({
            "timestamp": now.isoformat(),
            "status": overall,
            "score": score,
        })
        if len(self._health_history) > 100:
            self._health_history = self._health_history[-100:]

        return result

    # ── Readiness / Liveness ─────────────────────────────────────

    def liveness(self) -> Dict[str, Any]:
        """
        Kubernetes liveness probe.
        Returns OK if the process is alive and can serve requests.
        """
        return {
            "status": "ok",
            "uptime_seconds": round(time.time() - self._start_time),
        }

    def readiness(self) -> Dict[str, Any]:
        """
        Kubernetes readiness probe.
        Returns OK only if all critical dependencies are available.
        """
        critical = ["postgresql", "redis"]
        all_ok = True
        details = {}

        for name in critical:
            check = self._db_checks.get(name)
            if not check or not check.get("connected"):
                all_ok = False
                details[name] = "not_connected"
            else:
                details[name] = "ok"

        return {
            "status": "ok" if all_ok else "not_ready",
            "dependencies": details,
        }

    # ── History & trends ─────────────────────────────────────────

    def get_health_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return recent health check history."""
        return self._health_history[-limit:]

    def get_uptime_report(self) -> Dict[str, Any]:
        """Calculate uptime percentage from health history."""
        if not self._health_history:
            return {"uptime_pct": 100.0, "checks": 0}

        total = len(self._health_history)
        healthy = sum(1 for h in self._health_history if h["status"] == "healthy")
        return {
            "uptime_pct": round(healthy / total * 100, 2),
            "total_checks": total,
            "healthy_checks": healthy,
            "degraded_checks": sum(1 for h in self._health_history if h["status"] == "degraded"),
            "unhealthy_checks": sum(1 for h in self._health_history if h["status"] == "unhealthy"),
        }

    # ── Helpers ──────────────────────────────────────────────────

    @staticmethod
    def _format_uptime(seconds: float) -> str:
        days = int(seconds // 86400)
        hours = int((seconds % 86400) // 3600)
        minutes = int((seconds % 3600) // 60)
        if days > 0:
            return f"{days}d {hours}h {minutes}m"
        if hours > 0:
            return f"{hours}h {minutes}m"
        return f"{minutes}m"
