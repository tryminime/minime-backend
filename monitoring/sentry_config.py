"""
Sentry Integration — Error tracking and performance monitoring for FastAPI.

Provides:
- Sentry SDK initialization with DSN, environment, release tagging
- Custom error context enrichment (user, request, activity)
- Performance tracing with configurable sample rates
- Before-send hooks for PII scrubbing
- Custom breadcrumbs for service-level events
"""

import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import structlog

logger = structlog.get_logger()


class SentryConfig:
    """
    Sentry configuration and integration for the MiniMe backend.
    In tests or local dev the SDK is replaced by this in-memory stub
    so that no real calls are made.
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        environment: str = "development",
        release: Optional[str] = None,
        traces_sample_rate: float = 0.1,
        profiles_sample_rate: float = 0.1,
        enable_tracing: bool = True,
        pii_scrub: bool = True,
    ):
        self.dsn = dsn or os.environ.get("SENTRY_DSN", "")
        self.environment = environment
        self.release = release or os.environ.get("SENTRY_RELEASE", "minime@0.1.0")
        self.traces_sample_rate = traces_sample_rate
        self.profiles_sample_rate = profiles_sample_rate
        self.enable_tracing = enable_tracing
        self.pii_scrub = pii_scrub

        self._initialized = False
        self._captured_events: List[Dict[str, Any]] = []
        self._breadcrumbs: List[Dict[str, Any]] = []
        self._user_context: Dict[str, Any] = {}
        self._tags: Dict[str, str] = {}
        self._before_send_hooks: List[Callable] = []

        if pii_scrub:
            self._before_send_hooks.append(self._scrub_pii)

    # ── Initialization ───────────────────────────────────────────

    def init(self) -> Dict[str, Any]:
        """
        Initialize Sentry SDK.
        Production: calls sentry_sdk.init(...)
        Dev/test: configures in-memory capture.
        """
        config = {
            "dsn": self.dsn,
            "environment": self.environment,
            "release": self.release,
            "traces_sample_rate": self.traces_sample_rate,
            "profiles_sample_rate": self.profiles_sample_rate,
            "enable_tracing": self.enable_tracing,
            "integrations": ["fastapi", "sqlalchemy", "celery", "redis"],
            "before_send": "pii_scrub_enabled" if self.pii_scrub else "disabled",
        }

        if self.dsn:
            try:
                import sentry_sdk
                from sentry_sdk.integrations.fastapi import FastApiIntegration
                from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

                sentry_sdk.init(
                    dsn=self.dsn,
                    environment=self.environment,
                    release=self.release,
                    traces_sample_rate=self.traces_sample_rate,
                    profiles_sample_rate=self.profiles_sample_rate,
                    enable_tracing=self.enable_tracing,
                    before_send=self._before_send,
                    integrations=[
                        FastApiIntegration(),
                        SqlalchemyIntegration(),
                    ],
                )
                self._initialized = True
                logger.info("sentry_initialized", environment=self.environment)
            except ImportError:
                logger.warning("sentry_sdk not installed, using stub")
                self._initialized = True
        else:
            logger.info("sentry_stub_mode", reason="no DSN configured")
            self._initialized = True

        return config

    # ── Context enrichment ───────────────────────────────────────

    def set_user(self, user_id: str, email: Optional[str] = None, tier: Optional[str] = None) -> None:
        """Set user context for error reports."""
        self._user_context = {
            "id": user_id,
            "email": email if not self.pii_scrub else self._mask_email(email),
            "tier": tier,
        }
        try:
            import sentry_sdk
            sentry_sdk.set_user(self._user_context)
        except (ImportError, Exception):
            pass

    def set_tag(self, key: str, value: str) -> None:
        """Set a custom tag on the current scope."""
        self._tags[key] = value
        try:
            import sentry_sdk
            sentry_sdk.set_tag(key, value)
        except (ImportError, Exception):
            pass

    def set_context(self, name: str, data: Dict[str, Any]) -> None:
        """Set custom context data."""
        try:
            import sentry_sdk
            sentry_sdk.set_context(name, data)
        except (ImportError, Exception):
            pass

    def add_breadcrumb(
        self,
        message: str,
        category: str = "custom",
        level: str = "info",
        data: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Add a breadcrumb for debugging context."""
        crumb = {
            "message": message,
            "category": category,
            "level": level,
            "data": data or {},
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._breadcrumbs.append(crumb)
        try:
            import sentry_sdk
            sentry_sdk.add_breadcrumb(
                message=message, category=category, level=level, data=data,
            )
        except (ImportError, Exception):
            pass

    # ── Error capture ────────────────────────────────────────────

    def capture_exception(
        self,
        error: Exception,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Capture an exception and return the event ID."""
        event = {
            "event_id": f"evt-{len(self._captured_events)}",
            "type": "exception",
            "exception": {
                "type": type(error).__name__,
                "value": str(error),
            },
            "user": self._user_context,
            "tags": dict(self._tags),
            "extra": extra or {},
            "breadcrumbs": list(self._breadcrumbs),
            "timestamp": datetime.utcnow().isoformat(),
            "environment": self.environment,
        }

        # Run before_send hooks
        for hook in self._before_send_hooks:
            event = hook(event)

        self._captured_events.append(event)

        try:
            import sentry_sdk
            return sentry_sdk.capture_exception(error)
        except (ImportError, Exception):
            return event["event_id"]

    def capture_message(
        self,
        message: str,
        level: str = "info",
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Capture a message event."""
        event = {
            "event_id": f"msg-{len(self._captured_events)}",
            "type": "message",
            "message": message,
            "level": level,
            "user": self._user_context,
            "tags": dict(self._tags),
            "extra": extra or {},
            "timestamp": datetime.utcnow().isoformat(),
        }
        self._captured_events.append(event)

        try:
            import sentry_sdk
            return sentry_sdk.capture_message(message, level=level)
        except (ImportError, Exception):
            return event["event_id"]

    # ── Performance tracing ──────────────────────────────────────

    def start_transaction(
        self,
        name: str,
        op: str = "http.server",
    ) -> Dict[str, Any]:
        """Start a performance transaction."""
        txn = {
            "transaction_id": f"txn-{len(self._captured_events)}",
            "name": name,
            "op": op,
            "start_time": datetime.utcnow().isoformat(),
            "spans": [],
        }
        return txn

    def finish_transaction(self, txn: Dict[str, Any]) -> None:
        """Finish and record a transaction."""
        txn["end_time"] = datetime.utcnow().isoformat()
        self._captured_events.append(txn)

    # ── PII scrubbing ────────────────────────────────────────────

    def _before_send(self, event: Dict, hint: Dict) -> Optional[Dict]:
        """Before-send hook for Sentry SDK."""
        for hook in self._before_send_hooks:
            event = hook(event)
        return event

    @staticmethod
    def _scrub_pii(event: Dict[str, Any]) -> Dict[str, Any]:
        """Remove PII from event data before sending to Sentry."""
        if "request" in event:
            req = event["request"]
            if "headers" in req:
                for key in ["authorization", "cookie", "x-api-key"]:
                    if key in req["headers"]:
                        req["headers"][key] = "[REDACTED]"
            if "data" in req and isinstance(req["data"], dict):
                for key in ["password", "token", "secret", "api_key", "ssn", "credit_card"]:
                    if key in req["data"]:
                        req["data"][key] = "[REDACTED]"
        return event

    @staticmethod
    def _mask_email(email: Optional[str]) -> Optional[str]:
        if not email or "@" not in email:
            return email
        local, domain = email.split("@", 1)
        masked = local[0] + "***" + local[-1] if len(local) > 1 else "***"
        return f"{masked}@{domain}"

    # ── Stats ────────────────────────────────────────────────────

    def get_stats(self) -> Dict[str, Any]:
        """Return captured events stats."""
        exceptions = [e for e in self._captured_events if e.get("type") == "exception"]
        messages = [e for e in self._captured_events if e.get("type") == "message"]
        return {
            "initialized": self._initialized,
            "environment": self.environment,
            "total_events": len(self._captured_events),
            "exceptions": len(exceptions),
            "messages": len(messages),
            "breadcrumbs": len(self._breadcrumbs),
            "pii_scrub_enabled": self.pii_scrub,
        }
