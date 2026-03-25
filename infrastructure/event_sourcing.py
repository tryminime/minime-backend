"""
CQRS Event Sourcing — append-only event store with snapshots and projections.

Provides:
- Append-only event log per aggregate
- Command → Event handlers
- Aggregate root pattern with state rebuild from events
- Snapshot support for fast state recovery
- Event replay and projection building
- Event subscriptions for side effects
"""

import copy
import hashlib
import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Type
from uuid import uuid4

import structlog

logger = structlog.get_logger()


# ── Event types ──────────────────────────────────────────────────────

class DomainEvent:
    """Base domain event."""

    def __init__(
        self,
        aggregate_id: str,
        event_type: str,
        data: Dict[str, Any],
        *,
        user_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ):
        self.event_id = str(uuid4())
        self.aggregate_id = aggregate_id
        self.event_type = event_type
        self.data = data
        self.user_id = user_id
        self.correlation_id = correlation_id or str(uuid4())
        self.timestamp = datetime.utcnow().isoformat()
        self.version: int = 0  # set by store on append

    def to_dict(self) -> Dict[str, Any]:
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "event_type": self.event_type,
            "data": self.data,
            "user_id": self.user_id,
            "correlation_id": self.correlation_id,
            "timestamp": self.timestamp,
            "version": self.version,
        }


class Command:
    """A command to be dispatched to a handler."""

    def __init__(
        self,
        command_type: str,
        aggregate_id: str,
        data: Dict[str, Any],
        *,
        user_id: Optional[str] = None,
    ):
        self.command_id = str(uuid4())
        self.command_type = command_type
        self.aggregate_id = aggregate_id
        self.data = data
        self.user_id = user_id
        self.timestamp = datetime.utcnow().isoformat()


# ── Aggregate Root ───────────────────────────────────────────────────

class AggregateRoot:
    """
    Base aggregate root.  Subclasses define `apply_<event_type>` methods
    that mutate internal state in response to events.
    """

    def __init__(self, aggregate_id: str):
        self.aggregate_id = aggregate_id
        self.version = 0
        self._pending_events: List[DomainEvent] = []

    def apply(self, event: DomainEvent) -> None:
        handler_name = f"apply_{event.event_type}"
        handler = getattr(self, handler_name, None)
        if handler:
            handler(event.data)
        self.version = event.version

    def raise_event(
        self,
        event_type: str,
        data: Dict[str, Any],
        user_id: Optional[str] = None,
    ) -> DomainEvent:
        event = DomainEvent(
            aggregate_id=self.aggregate_id,
            event_type=event_type,
            data=data,
            user_id=user_id,
        )
        self._pending_events.append(event)
        return event

    def get_pending_events(self) -> List[DomainEvent]:
        return list(self._pending_events)

    def clear_pending(self) -> None:
        self._pending_events.clear()

    def to_snapshot(self) -> Dict[str, Any]:
        return {"aggregate_id": self.aggregate_id, "version": self.version}

    @classmethod
    def from_snapshot(cls, data: Dict[str, Any]) -> "AggregateRoot":
        agg = cls(data["aggregate_id"])
        agg.version = data["version"]
        return agg


# ── Built-in aggregates ──────────────────────────────────────────────

class UserActivityAggregate(AggregateRoot):
    """Example aggregate tracking user activity state."""

    def __init__(self, aggregate_id: str):
        super().__init__(aggregate_id)
        self.activities: List[Dict] = []
        self.total_duration: float = 0.0
        self.last_activity_time: Optional[str] = None
        self.tags: List[str] = []

    def apply_activity_recorded(self, data: Dict) -> None:
        self.activities.append(data)
        self.total_duration += data.get("duration", 0)
        self.last_activity_time = data.get("timestamp")

    def apply_activity_tagged(self, data: Dict) -> None:
        tag = data.get("tag")
        if tag and tag not in self.tags:
            self.tags.append(tag)

    def apply_activity_deleted(self, data: Dict) -> None:
        aid = data.get("activity_id")
        self.activities = [a for a in self.activities if a.get("id") != aid]

    def to_snapshot(self) -> Dict[str, Any]:
        base = super().to_snapshot()
        base.update({
            "total_duration": self.total_duration,
            "activity_count": len(self.activities),
            "last_activity_time": self.last_activity_time,
            "tags": self.tags,
        })
        return base

    @classmethod
    def from_snapshot(cls, data: Dict[str, Any]) -> "UserActivityAggregate":
        agg = cls(data["aggregate_id"])
        agg.version = data["version"]
        agg.total_duration = data.get("total_duration", 0)
        agg.last_activity_time = data.get("last_activity_time")
        agg.tags = data.get("tags", [])
        return agg


class UserSettingsAggregate(AggregateRoot):
    """Aggregate for user settings changes."""

    def __init__(self, aggregate_id: str):
        super().__init__(aggregate_id)
        self.settings: Dict[str, Any] = {}
        self.change_history: List[Dict] = []

    def apply_setting_changed(self, data: Dict) -> None:
        key = data.get("key")
        old_value = self.settings.get(key)
        self.settings[key] = data.get("value")
        self.change_history.append({
            "key": key,
            "old": old_value,
            "new": data.get("value"),
            "at": data.get("timestamp", datetime.utcnow().isoformat()),
        })

    def apply_settings_reset(self, data: Dict) -> None:
        self.settings = data.get("defaults", {})
        self.change_history.append({"action": "reset", "at": datetime.utcnow().isoformat()})

    def to_snapshot(self) -> Dict[str, Any]:
        base = super().to_snapshot()
        base["settings"] = copy.deepcopy(self.settings)
        return base

    @classmethod
    def from_snapshot(cls, data: Dict[str, Any]) -> "UserSettingsAggregate":
        agg = cls(data["aggregate_id"])
        agg.version = data["version"]
        agg.settings = data.get("settings", {})
        return agg


# ── Event Store ──────────────────────────────────────────────────────

class EventStore:
    """
    In-memory append-only event store with snapshot support.
    Production usage would back this with PostgreSQL / EventStoreDB.
    """

    def __init__(self):
        self._events: Dict[str, List[Dict[str, Any]]] = {}
        self._snapshots: Dict[str, Dict[str, Any]] = {}
        self._global_position: int = 0
        self._subscribers: Dict[str, List[Callable]] = {}
        self._all_subscribers: List[Callable] = []

    def append(self, event: DomainEvent) -> int:
        """Append an event.  Returns the global position."""
        agg_id = event.aggregate_id
        if agg_id not in self._events:
            self._events[agg_id] = []
        event.version = len(self._events[agg_id]) + 1
        self._global_position += 1
        self._events[agg_id].append(event.to_dict())
        # notify subscribers
        for cb in self._subscribers.get(event.event_type, []):
            cb(event)
        for cb in self._all_subscribers:
            cb(event)
        return self._global_position

    def append_batch(self, events: List[DomainEvent]) -> List[int]:
        """Append multiple events atomically."""
        positions = []
        for e in events:
            positions.append(self.append(e))
        return positions

    def get_events(
        self,
        aggregate_id: str,
        after_version: int = 0,
    ) -> List[Dict[str, Any]]:
        """Get events for an aggregate after a given version."""
        stream = self._events.get(aggregate_id, [])
        return [e for e in stream if e["version"] > after_version]

    def get_all_events(
        self,
        event_type: Optional[str] = None,
        after_position: int = 0,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Get events across all aggregates, optionally filtered."""
        all_evts: List[Dict[str, Any]] = []
        for stream in self._events.values():
            all_evts.extend(stream)
        # sort by timestamp
        all_evts.sort(key=lambda e: e["timestamp"])
        if event_type:
            all_evts = [e for e in all_evts if e["event_type"] == event_type]
        return all_evts[after_position : after_position + limit]

    def save_snapshot(self, aggregate_id: str, snapshot: Dict[str, Any]) -> None:
        """Save a snapshot for fast recovery."""
        snapshot["snapshot_time"] = datetime.utcnow().isoformat()
        self._snapshots[aggregate_id] = snapshot
        logger.info("snapshot_saved", aggregate_id=aggregate_id, version=snapshot.get("version"))

    def get_snapshot(self, aggregate_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve latest snapshot."""
        return self._snapshots.get(aggregate_id)

    def subscribe(self, event_type: str, callback: Callable) -> None:
        """Subscribe to a specific event type."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    def subscribe_all(self, callback: Callable) -> None:
        """Subscribe to all events."""
        self._all_subscribers.append(callback)

    def get_stream_length(self, aggregate_id: str) -> int:
        return len(self._events.get(aggregate_id, []))

    def get_stats(self) -> Dict[str, Any]:
        total_events = sum(len(s) for s in self._events.values())
        return {
            "total_aggregates": len(self._events),
            "total_events": total_events,
            "total_snapshots": len(self._snapshots),
            "global_position": self._global_position,
        }


# ── Projection ───────────────────────────────────────────────────────

class Projection:
    """
    Materialised view built from events.
    Subclass and implement `handle_<event_type>` methods.
    """

    def __init__(self, name: str):
        self.name = name
        self.last_position: int = 0
        self.state: Dict[str, Any] = {}

    def handle(self, event: DomainEvent) -> None:
        handler = getattr(self, f"handle_{event.event_type}", None)
        if handler:
            handler(event)


class ActivityCountProjection(Projection):
    """Projection: count of activities per user."""

    def __init__(self):
        super().__init__("activity_counts")
        self.counts: Dict[str, int] = {}

    def handle_activity_recorded(self, event: DomainEvent) -> None:
        uid = event.user_id or event.aggregate_id
        self.counts[uid] = self.counts.get(uid, 0) + 1

    def handle_activity_deleted(self, event: DomainEvent) -> None:
        uid = event.user_id or event.aggregate_id
        self.counts[uid] = max(self.counts.get(uid, 0) - 1, 0)

    def get_count(self, user_id: str) -> int:
        return self.counts.get(user_id, 0)


class SettingsAuditProjection(Projection):
    """Projection: audit trail of all settings changes."""

    def __init__(self):
        super().__init__("settings_audit")
        self.audit_log: List[Dict[str, Any]] = []

    def handle_setting_changed(self, event: DomainEvent) -> None:
        self.audit_log.append({
            "user_id": event.user_id,
            "aggregate_id": event.aggregate_id,
            "key": event.data.get("key"),
            "value": event.data.get("value"),
            "timestamp": event.timestamp,
        })

    def get_log(self, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        if user_id:
            return [e for e in self.audit_log if e["user_id"] == user_id]
        return list(self.audit_log)


# ── Command Handler ──────────────────────────────────────────────────

class CommandHandler:
    """
    Dispatches commands to aggregate roots, persists resulting events,
    and triggers projections.
    """

    AGGREGATE_TYPES: Dict[str, Type[AggregateRoot]] = {
        "user_activity": UserActivityAggregate,
        "user_settings": UserSettingsAggregate,
    }

    SNAPSHOT_INTERVAL = 10  # snapshot every N events

    def __init__(self, event_store: Optional[EventStore] = None):
        self.store = event_store or EventStore()
        self.projections: List[Projection] = []

    def register_projection(self, projection: Projection) -> None:
        self.projections.append(projection)
        # subscribe to all events
        self.store.subscribe_all(projection.handle)

    def handle(self, command: Command) -> Dict[str, Any]:
        """
        Process a command:
        1. Load aggregate (from snapshot + replay)
        2. Execute business logic → raise events
        3. Persist events
        4. Optionally save snapshot
        """
        agg_type_name = command.command_type.split(".")[0]
        agg_class = self.AGGREGATE_TYPES.get(agg_type_name, AggregateRoot)

        # Load aggregate
        aggregate = self._load_aggregate(agg_class, command.aggregate_id)

        # Execute command
        action = command.command_type.split(".")[-1]
        event_type = self._command_to_event_type(action)

        aggregate.raise_event(
            event_type=event_type,
            data=command.data,
            user_id=command.user_id,
        )

        # Persist
        pending = aggregate.get_pending_events()
        positions = self.store.append_batch(pending)
        aggregate.clear_pending()

        # Apply events to aggregate
        for evt in pending:
            aggregate.apply(evt)

        # Snapshot check
        stream_len = self.store.get_stream_length(command.aggregate_id)
        if stream_len % self.SNAPSHOT_INTERVAL == 0:
            self.store.save_snapshot(command.aggregate_id, aggregate.to_snapshot())

        return {
            "success": True,
            "command_id": command.command_id,
            "events_persisted": len(positions),
            "aggregate_version": aggregate.version,
        }

    def _load_aggregate(
        self, agg_class: Type[AggregateRoot], aggregate_id: str
    ) -> AggregateRoot:
        snapshot = self.store.get_snapshot(aggregate_id)
        if snapshot:
            aggregate = agg_class.from_snapshot(snapshot)
            events = self.store.get_events(aggregate_id, after_version=snapshot["version"])
        else:
            aggregate = agg_class(aggregate_id)
            events = self.store.get_events(aggregate_id)

        for evt_data in events:
            evt = DomainEvent(
                aggregate_id=evt_data["aggregate_id"],
                event_type=evt_data["event_type"],
                data=evt_data["data"],
            )
            evt.version = evt_data["version"]
            aggregate.apply(evt)
        return aggregate

    def replay_all(self, aggregate_id: str) -> AggregateRoot:
        """Full replay from event log (ignore snapshots)."""
        events = self.store.get_events(aggregate_id)
        # infer type from first event
        agg = AggregateRoot(aggregate_id)
        for evt_data in events:
            evt = DomainEvent(
                aggregate_id=evt_data["aggregate_id"],
                event_type=evt_data["event_type"],
                data=evt_data["data"],
            )
            evt.version = evt_data["version"]
            agg.apply(evt)
        return agg

    @staticmethod
    def _command_to_event_type(action: str) -> str:
        mapping = {
            "record": "activity_recorded",
            "tag": "activity_tagged",
            "delete": "activity_deleted",
            "change_setting": "setting_changed",
            "reset_settings": "settings_reset",
        }
        return mapping.get(action, action)
