"""
Advanced Rate Limiter Middleware — Token Bucket + Sliding Window.

Provides:
- Per-user, per-endpoint, per-tier rate limiting
- Token bucket algorithm for burst-friendly limits
- Sliding window counter for sustained load control
- Configurable tier limits (free/personal/pro/enterprise)
- Redis-compatible distributed counter interface
- Request logging and analytics
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

import structlog

logger = structlog.get_logger()


# ── Configuration ────────────────────────────────────────────────────

@dataclass
class TierConfig:
    """Rate limit configuration for a subscription tier."""
    name: str
    requests_per_minute: int
    requests_per_hour: int
    requests_per_day: int
    burst_size: int                 # max tokens in bucket
    token_refill_rate: float        # tokens per second
    concurrent_limit: int           # max concurrent requests
    endpoints: Dict[str, int] = field(default_factory=dict)
    # per-endpoint overrides: {"/api/v1/chat": 10}  (reqs/min)


DEFAULT_TIERS: Dict[str, TierConfig] = {
    "free": TierConfig(
        name="free",
        requests_per_minute=30,
        requests_per_hour=500,
        requests_per_day=5_000,
        burst_size=10,
        token_refill_rate=0.5,
        concurrent_limit=5,
        endpoints={
            "/api/ai/chat": 5,
            "/api/ai/chat/stream": 5,
            "/api/v1/graph": 20,
        },
    ),
    "personal": TierConfig(
        name="personal",
        requests_per_minute=60,
        requests_per_hour=2_000,
        requests_per_day=20_000,
        burst_size=20,
        token_refill_rate=1.0,
        concurrent_limit=10,
        endpoints={
            "/api/ai/chat": 15,
            "/api/ai/chat/stream": 15,
            "/api/v1/graph": 40,
        },
    ),
    "pro": TierConfig(
        name="pro",
        requests_per_minute=120,
        requests_per_hour=5_000,
        requests_per_day=50_000,
        burst_size=40,
        token_refill_rate=2.0,
        concurrent_limit=20,
        endpoints={
            "/api/ai/chat": 30,
            "/api/ai/chat/stream": 30,
        },
    ),
    "enterprise": TierConfig(
        name="enterprise",
        requests_per_minute=300,
        requests_per_hour=20_000,
        requests_per_day=200_000,
        burst_size=100,
        token_refill_rate=5.0,
        concurrent_limit=50,
    ),
}


# ── Token Bucket ─────────────────────────────────────────────────────

class TokenBucket:
    """
    Token bucket algorithm for burst-friendly rate limiting.
    Tokens refill at a constant rate up to a maximum capacity.
    """

    def __init__(self, capacity: int, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.tokens = float(capacity)
        self.last_refill = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_refill
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        self.last_refill = now

    def consume(self, count: int = 1) -> bool:
        """Try to consume tokens.  Returns True if allowed."""
        self._refill()
        if self.tokens >= count:
            self.tokens -= count
            return True
        return False

    def tokens_available(self) -> float:
        self._refill()
        return self.tokens

    def time_until_available(self, count: int = 1) -> float:
        """Seconds until `count` tokens will be available."""
        self._refill()
        if self.tokens >= count:
            return 0.0
        deficit = count - self.tokens
        return deficit / self.refill_rate


# ── Sliding Window Counter ───────────────────────────────────────────

class SlidingWindowCounter:
    """
    Sliding window counter for sustained-load rate limiting.
    Tracks request timestamps and counts within a configurable window.
    """

    def __init__(self, window_seconds: int, max_requests: int):
        self.window = window_seconds
        self.max_requests = max_requests
        self._timestamps: List[float] = []

    def _prune(self) -> None:
        cutoff = time.monotonic() - self.window
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    def record(self) -> bool:
        """Record a request.  Returns True if within limit."""
        self._prune()
        if len(self._timestamps) >= self.max_requests:
            return False
        self._timestamps.append(time.monotonic())
        return True

    def current_count(self) -> int:
        self._prune()
        return len(self._timestamps)

    def remaining(self) -> int:
        self._prune()
        return max(0, self.max_requests - len(self._timestamps))

    def reset_at(self) -> float:
        """Epoch time when the oldest entry will expire."""
        self._prune()
        if not self._timestamps:
            return 0.0
        return self._timestamps[0] + self.window


# ── Rate Limiter Service ─────────────────────────────────────────────

@dataclass
class RateLimitResult:
    allowed: bool
    remaining: int
    limit: int
    reset_after: float          # seconds until reset
    retry_after: Optional[float] = None
    reason: Optional[str] = None


class RateLimiterService:
    """
    Composite rate limiter combining token bucket (burst) and
    sliding window (sustained load).  Supports per-user, per-endpoint,
    and per-tier limits.
    """

    def __init__(self, tiers: Optional[Dict[str, TierConfig]] = None):
        self.tiers = tiers or DEFAULT_TIERS
        # keyed by (user_id, scope) → limiter instance
        self._buckets: Dict[Tuple[str, str], TokenBucket] = {}
        self._windows_minute: Dict[Tuple[str, str], SlidingWindowCounter] = {}
        self._windows_hour: Dict[Tuple[str, str], SlidingWindowCounter] = {}
        self._windows_day: Dict[Tuple[str, str], SlidingWindowCounter] = {}
        self._concurrent: Dict[str, int] = defaultdict(int)
        self._request_log: List[Dict[str, Any]] = []
        self._blocked_count: int = 0
        self._total_checked: int = 0

    def _get_tier(self, tier_name: str) -> TierConfig:
        return self.tiers.get(tier_name, self.tiers["free"])

    def _ensure_limiters(
        self, user_id: str, endpoint: str, tier: TierConfig
    ) -> None:
        """Lazily initialise limiter instances for a user+endpoint."""
        global_key = (user_id, "global")
        ep_key = (user_id, endpoint)

        if global_key not in self._buckets:
            self._buckets[global_key] = TokenBucket(
                tier.burst_size, tier.token_refill_rate
            )
            self._windows_minute[global_key] = SlidingWindowCounter(
                60, tier.requests_per_minute
            )
            self._windows_hour[global_key] = SlidingWindowCounter(
                3600, tier.requests_per_hour
            )
            self._windows_day[global_key] = SlidingWindowCounter(
                86400, tier.requests_per_day
            )

        # per-endpoint window (if configured)
        if endpoint in tier.endpoints and ep_key not in self._windows_minute:
            self._windows_minute[ep_key] = SlidingWindowCounter(
                60, tier.endpoints[endpoint]
            )

    def check(
        self,
        user_id: str,
        endpoint: str,
        tier_name: str = "free",
    ) -> RateLimitResult:
        """
        Check whether a request is allowed.  Does NOT consume tokens
        until `record()` is called.
        """
        tier = self._get_tier(tier_name)
        self._ensure_limiters(user_id, endpoint, tier)
        self._total_checked += 1

        global_key = (user_id, "global")
        ep_key = (user_id, endpoint)

        # 1. Concurrent limit
        if self._concurrent[user_id] >= tier.concurrent_limit:
            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=tier.concurrent_limit,
                reset_after=1.0,
                retry_after=1.0,
                reason="concurrent_limit_exceeded",
            )

        # 2. Token bucket (burst)
        bucket = self._buckets[global_key]
        if bucket.tokens_available() < 1:
            retry = bucket.time_until_available(1)
            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=tier.burst_size,
                reset_after=retry,
                retry_after=retry,
                reason="burst_limit_exceeded",
            )

        # 3. Minute window
        wm = self._windows_minute[global_key]
        if wm.remaining() <= 0:
            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=tier.requests_per_minute,
                reset_after=60.0,
                retry_after=60.0 - (time.monotonic() % 60),
                reason="minute_limit_exceeded",
            )

        # 4. Per-endpoint minute window
        if ep_key in self._windows_minute:
            ep_wm = self._windows_minute[ep_key]
            if ep_wm.remaining() <= 0:
                return RateLimitResult(
                    allowed=False,
                    remaining=0,
                    limit=tier.endpoints.get(endpoint, 0),
                    reset_after=60.0,
                    retry_after=60.0,
                    reason="endpoint_limit_exceeded",
                )

        # 5. Hour window
        wh = self._windows_hour[global_key]
        if wh.remaining() <= 0:
            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=tier.requests_per_hour,
                reset_after=3600.0,
                retry_after=3600.0,
                reason="hour_limit_exceeded",
            )

        # 6. Day window
        wd = self._windows_day[global_key]
        if wd.remaining() <= 0:
            return RateLimitResult(
                allowed=False,
                remaining=0,
                limit=tier.requests_per_day,
                reset_after=86400.0,
                retry_after=86400.0,
                reason="day_limit_exceeded",
            )

        # All checks passed
        return RateLimitResult(
            allowed=True,
            remaining=wm.remaining(),
            limit=tier.requests_per_minute,
            reset_after=60.0,
        )

    def record(
        self,
        user_id: str,
        endpoint: str,
        tier_name: str = "free",
    ) -> RateLimitResult:
        """Check AND consume a request slot.  Returns the result."""
        result = self.check(user_id, endpoint, tier_name)
        if not result.allowed:
            self._blocked_count += 1
            self._log_request(user_id, endpoint, tier_name, blocked=True, reason=result.reason)
            return result

        tier = self._get_tier(tier_name)
        global_key = (user_id, "global")
        ep_key = (user_id, endpoint)

        self._buckets[global_key].consume(1)
        self._windows_minute[global_key].record()
        self._windows_hour[global_key].record()
        self._windows_day[global_key].record()

        if ep_key in self._windows_minute:
            self._windows_minute[ep_key].record()

        self._log_request(user_id, endpoint, tier_name, blocked=False)

        wm = self._windows_minute[global_key]
        return RateLimitResult(
            allowed=True,
            remaining=wm.remaining(),
            limit=tier.requests_per_minute,
            reset_after=60.0,
        )

    # ── Concurrent request tracking ─────────────────────────────

    def acquire_concurrent(self, user_id: str) -> None:
        """Mark a concurrent request as started."""
        self._concurrent[user_id] += 1

    def release_concurrent(self, user_id: str) -> None:
        """Mark a concurrent request as finished."""
        self._concurrent[user_id] = max(0, self._concurrent[user_id] - 1)

    # ── IP-based limiting ────────────────────────────────────────

    _ip_windows: Dict[str, SlidingWindowCounter] = {}

    def check_ip(
        self, ip_address: str, max_per_minute: int = 100
    ) -> RateLimitResult:
        """Simple IP-based rate check (for unauthenticated endpoints)."""
        if ip_address not in self._ip_windows:
            self._ip_windows[ip_address] = SlidingWindowCounter(60, max_per_minute)
        w = self._ip_windows[ip_address]
        allowed = w.record()
        return RateLimitResult(
            allowed=allowed,
            remaining=w.remaining(),
            limit=max_per_minute,
            reset_after=60.0,
            reason=None if allowed else "ip_rate_exceeded",
        )

    # ── Admin / analytics ────────────────────────────────────────

    def get_user_usage(self, user_id: str) -> Dict[str, Any]:
        """Get current usage stats for a user."""
        global_key = (user_id, "global")
        minute = self._windows_minute.get(global_key)
        hour = self._windows_hour.get(global_key)
        day = self._windows_day.get(global_key)
        bucket = self._buckets.get(global_key)

        return {
            "user_id": user_id,
            "minute": {"used": minute.current_count() if minute else 0, "remaining": minute.remaining() if minute else 0},
            "hour": {"used": hour.current_count() if hour else 0, "remaining": hour.remaining() if hour else 0},
            "day": {"used": day.current_count() if day else 0, "remaining": day.remaining() if day else 0},
            "burst_tokens": bucket.tokens_available() if bucket else 0,
            "concurrent": self._concurrent.get(user_id, 0),
        }

    def get_stats(self) -> Dict[str, Any]:
        """Global rate limiter statistics."""
        return {
            "total_checked": self._total_checked,
            "total_blocked": self._blocked_count,
            "block_rate": (
                round(self._blocked_count / max(self._total_checked, 1) * 100, 2)
            ),
            "active_users": len(set(k[0] for k in self._buckets)),
            "total_log_entries": len(self._request_log),
        }

    def reset_user(self, user_id: str) -> None:
        """Reset all rate limit state for a user."""
        keys_to_del = [k for k in self._buckets if k[0] == user_id]
        for k in keys_to_del:
            self._buckets.pop(k, None)
            self._windows_minute.pop(k, None)
            self._windows_hour.pop(k, None)
            self._windows_day.pop(k, None)
        self._concurrent.pop(user_id, None)

    # ── Logging ──────────────────────────────────────────────────

    def _log_request(
        self,
        user_id: str,
        endpoint: str,
        tier: str,
        blocked: bool,
        reason: Optional[str] = None,
    ) -> None:
        entry = {
            "user_id": user_id,
            "endpoint": endpoint,
            "tier": tier,
            "blocked": blocked,
            "reason": reason,
            "timestamp": time.time(),
        }
        self._request_log.append(entry)
        # keep last 10k entries
        if len(self._request_log) > 10_000:
            self._request_log = self._request_log[-5_000:]

    def get_recent_blocks(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Get recent blocked requests for monitoring."""
        blocked = [e for e in self._request_log if e["blocked"]]
        return blocked[-limit:]
