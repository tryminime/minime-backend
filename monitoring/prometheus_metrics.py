"""
Prometheus Metrics — metric definitions and FastAPI middleware.

Provides:
- Request latency histogram (by method, endpoint, status)
- Active users gauge
- Request counter (total, by status code)
- Queue depth gauge (Celery tasks)
- Error rate counter
- Custom business metrics (activities ingested, AI queries, etc.)
- FastAPI middleware for automatic instrumentation
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()


# ── Metric types ─────────────────────────────────────────────────────

class Counter:
    """Monotonically increasing counter."""

    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.label_names = labels or []
        self._values: Dict[Tuple, float] = defaultdict(float)

    def inc(self, amount: float = 1.0, **labels) -> None:
        key = tuple(labels.get(l, "") for l in self.label_names)
        self._values[key] += amount

    def get(self, **labels) -> float:
        key = tuple(labels.get(l, "") for l in self.label_names)
        return self._values.get(key, 0.0)

    def collect(self) -> List[Dict[str, Any]]:
        return [
            {"labels": dict(zip(self.label_names, k)), "value": v}
            for k, v in self._values.items()
        ]


class Gauge:
    """Value that can go up and down."""

    def __init__(self, name: str, description: str, labels: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.label_names = labels or []
        self._values: Dict[Tuple, float] = defaultdict(float)

    def set(self, value: float, **labels) -> None:
        key = tuple(labels.get(l, "") for l in self.label_names)
        self._values[key] = value

    def inc(self, amount: float = 1.0, **labels) -> None:
        key = tuple(labels.get(l, "") for l in self.label_names)
        self._values[key] += amount

    def dec(self, amount: float = 1.0, **labels) -> None:
        key = tuple(labels.get(l, "") for l in self.label_names)
        self._values[key] -= amount

    def get(self, **labels) -> float:
        key = tuple(labels.get(l, "") for l in self.label_names)
        return self._values.get(key, 0.0)

    def collect(self) -> List[Dict[str, Any]]:
        return [
            {"labels": dict(zip(self.label_names, k)), "value": v}
            for k, v in self._values.items()
        ]


class Histogram:
    """Distribution of observations in configurable buckets."""

    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float("inf"))

    def __init__(
        self,
        name: str,
        description: str,
        labels: Optional[List[str]] = None,
        buckets: Optional[Tuple[float, ...]] = None,
    ):
        self.name = name
        self.description = description
        self.label_names = labels or []
        self.buckets = buckets or self.DEFAULT_BUCKETS
        self._observations: Dict[Tuple, List[float]] = defaultdict(list)

    def observe(self, value: float, **labels) -> None:
        key = tuple(labels.get(l, "") for l in self.label_names)
        self._observations[key].append(value)

    def collect(self) -> List[Dict[str, Any]]:
        results = []
        for key, obs in self._observations.items():
            bucket_counts = {b: 0 for b in self.buckets}
            for v in obs:
                for b in self.buckets:
                    if v <= b:
                        bucket_counts[b] += 1
            results.append({
                "labels": dict(zip(self.label_names, key)),
                "count": len(obs),
                "sum": sum(obs),
                "buckets": bucket_counts,
                "avg": sum(obs) / max(len(obs), 1),
                "p50": sorted(obs)[len(obs) // 2] if obs else 0,
                "p95": sorted(obs)[int(len(obs) * 0.95)] if obs else 0,
                "p99": sorted(obs)[int(len(obs) * 0.99)] if obs else 0,
            })
        return results


# ── Metric registry ──────────────────────────────────────────────────

class MetricsRegistry:
    """
    Central registry of all application metrics.
    Exposes a Prometheus-compatible /metrics text format.
    """

    def __init__(self):
        # HTTP metrics
        self.http_requests_total = Counter(
            "http_requests_total",
            "Total HTTP requests",
            labels=["method", "endpoint", "status_code"],
        )
        self.http_request_duration_seconds = Histogram(
            "http_request_duration_seconds",
            "HTTP request latency in seconds",
            labels=["method", "endpoint"],
        )
        self.http_requests_in_progress = Gauge(
            "http_requests_in_progress",
            "Currently in-progress requests",
            labels=["method"],
        )

        # User metrics
        self.active_users = Gauge("active_users", "Number of active users")
        self.user_sessions = Counter(
            "user_sessions_total", "Total user sessions", labels=["tier"]
        )

        # Business metrics
        self.activities_ingested = Counter(
            "activities_ingested_total",
            "Total activities ingested",
            labels=["activity_type"],
        )
        self.ai_queries_total = Counter(
            "ai_queries_total", "Total AI chat queries", labels=["model"]
        )
        self.entities_extracted = Counter(
            "entities_extracted_total",
            "Total entities extracted by NLP",
            labels=["entity_type"],
        )
        self.graph_queries_total = Counter(
            "graph_queries_total", "Total knowledge graph queries", labels=["query_type"]
        )

        # Infrastructure metrics
        self.celery_queue_depth = Gauge(
            "celery_queue_depth", "Celery task queue depth", labels=["queue"]
        )
        self.db_connection_pool_size = Gauge(
            "db_connection_pool_size",
            "Database connection pool size",
            labels=["database"],
        )
        self.cache_hit_rate = Gauge(
            "cache_hit_rate", "Cache hit rate (0-1)", labels=["cache"]
        )

        # Error metrics
        self.errors_total = Counter(
            "errors_total", "Total errors", labels=["error_type", "endpoint"]
        )

    def collect_all(self) -> Dict[str, Any]:
        """Collect all metrics in a structured format."""
        return {
            "http_requests_total": self.http_requests_total.collect(),
            "http_request_duration_seconds": self.http_request_duration_seconds.collect(),
            "http_requests_in_progress": self.http_requests_in_progress.collect(),
            "active_users": self.active_users.collect(),
            "activities_ingested": self.activities_ingested.collect(),
            "ai_queries_total": self.ai_queries_total.collect(),
            "entities_extracted": self.entities_extracted.collect(),
            "graph_queries_total": self.graph_queries_total.collect(),
            "celery_queue_depth": self.celery_queue_depth.collect(),
            "errors_total": self.errors_total.collect(),
        }

    def to_prometheus_text(self) -> str:
        """Export metrics in Prometheus text exposition format."""
        lines: List[str] = []

        def _emit_counter(metric: Counter) -> None:
            lines.append(f"# HELP {metric.name} {metric.description}")
            lines.append(f"# TYPE {metric.name} counter")
            for entry in metric.collect():
                label_str = ",".join(f'{k}="{v}"' for k, v in entry["labels"].items())
                label_part = f"{{{label_str}}}" if label_str else ""
                lines.append(f"{metric.name}{label_part} {entry['value']}")

        def _emit_gauge(metric: Gauge) -> None:
            lines.append(f"# HELP {metric.name} {metric.description}")
            lines.append(f"# TYPE {metric.name} gauge")
            for entry in metric.collect():
                label_str = ",".join(f'{k}="{v}"' for k, v in entry["labels"].items())
                label_part = f"{{{label_str}}}" if label_str else ""
                lines.append(f"{metric.name}{label_part} {entry['value']}")

        def _emit_histogram(metric: Histogram) -> None:
            lines.append(f"# HELP {metric.name} {metric.description}")
            lines.append(f"# TYPE {metric.name} histogram")
            for entry in metric.collect():
                label_str = ",".join(f'{k}="{v}"' for k, v in entry["labels"].items())
                base = f"{{{label_str}}}" if label_str else ""
                for bucket, count in entry["buckets"].items():
                    le = "+Inf" if bucket == float("inf") else str(bucket)
                    lines.append(f'{metric.name}_bucket{{le="{le}",{label_str}}} {count}')
                lines.append(f"{metric.name}_count{base} {entry['count']}")
                lines.append(f"{metric.name}_sum{base} {entry['sum']:.6f}")

        _emit_counter(self.http_requests_total)
        _emit_histogram(self.http_request_duration_seconds)
        _emit_gauge(self.http_requests_in_progress)
        _emit_gauge(self.active_users)
        _emit_counter(self.activities_ingested)
        _emit_counter(self.ai_queries_total)
        _emit_counter(self.entities_extracted)
        _emit_counter(self.graph_queries_total)
        _emit_gauge(self.celery_queue_depth)
        _emit_counter(self.errors_total)

        return "\n".join(lines) + "\n"


# ── Global instance ──────────────────────────────────────────────────
metrics = MetricsRegistry()
