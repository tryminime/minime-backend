"""
GraphQL API Layer — Strawberry-based schema for MiniMe platform.

Provides Query and Mutation types for:
- Activities (list, search, aggregate)
- Entities (list, search by type)
- Analytics (productivity summary, collaboration, skills)
- User (profile, settings)
- Knowledge Graph (nodes, edges, paths)

Uses DataLoader pattern to prevent N+1 queries.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import uuid4

import structlog

logger = structlog.get_logger()


# ── Enums ────────────────────────────────────────────────────────────

class EntityType(Enum):
    PERSON = "PERSON"
    ORGANIZATION = "ORG"
    PROJECT = "PROJECT"
    TECHNOLOGY = "TECHNOLOGY"
    DOCUMENT = "DOCUMENT"
    LOCATION = "LOCATION"
    EVENT = "EVENT"
    TOPIC = "TOPIC"


class ActivityType(Enum):
    CODING = "coding"
    BROWSING = "browsing"
    MEETING = "meeting"
    DOCUMENT = "document"
    COMMUNICATION = "communication"
    DESIGN = "design"
    OTHER = "other"


class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"


# ── Data types ───────────────────────────────────────────────────────

@dataclass
class ActivityNode:
    id: str
    user_id: str
    activity_type: str
    application: str
    title: str
    duration: float
    timestamp: str
    tags: List[str] = field(default_factory=list)
    entities: List[str] = field(default_factory=list)
    productivity_score: float = 0.0


@dataclass
class EntityNode:
    id: str
    canonical_name: str
    entity_type: str
    mention_count: int = 0
    first_seen: Optional[str] = None
    last_seen: Optional[str] = None
    aliases: List[str] = field(default_factory=list)


@dataclass
class UserProfile:
    id: str
    email: str
    name: str
    tier: str = "personal"
    created_at: Optional[str] = None
    settings: Optional[Dict[str, Any]] = None


@dataclass
class ProductivitySummary:
    total_hours: float
    productive_hours: float
    neutral_hours: float
    unproductive_hours: float
    productivity_score: float
    top_applications: List[Dict[str, Any]]
    top_categories: List[Dict[str, Any]]
    trend: str  # "improving", "declining", "stable"


@dataclass
class CollaborationSummary:
    total_collaborators: int
    meetings_count: int
    meeting_hours: float
    top_collaborators: List[Dict[str, Any]]
    communication_channels: List[Dict[str, Any]]
    network_density: float


@dataclass
class SkillSummary:
    total_skills: int
    top_skills: List[Dict[str, Any]]
    growing_skills: List[Dict[str, Any]]
    skill_gaps: List[Dict[str, Any]]
    learning_paths: List[Dict[str, Any]]


@dataclass
class GraphNode:
    id: str
    label: str
    node_type: str
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphEdge:
    source: str
    target: str
    relationship: str
    weight: float = 1.0
    properties: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GraphPath:
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    total_weight: float = 0.0


@dataclass
class SearchResult:
    id: str
    score: float
    result_type: str  # "activity", "entity", "document"
    title: str
    snippet: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PaginationInfo:
    total: int
    page: int
    page_size: int
    has_next: bool
    has_previous: bool


@dataclass
class PaginatedActivities:
    items: List[ActivityNode]
    pagination: PaginationInfo


# ── Input types ──────────────────────────────────────────────────────

@dataclass
class ActivityFilter:
    activity_types: Optional[List[str]] = None
    applications: Optional[List[str]] = None
    tags: Optional[List[str]] = None
    date_from: Optional[str] = None
    date_to: Optional[str] = None
    min_duration: Optional[float] = None
    search_text: Optional[str] = None


@dataclass
class EntityFilter:
    entity_types: Optional[List[str]] = None
    min_mentions: Optional[int] = None
    search_text: Optional[str] = None


@dataclass
class AnalyticsRange:
    start_date: str
    end_date: str
    granularity: str = "day"  # "hour", "day", "week", "month"


@dataclass
class CreateActivityInput:
    activity_type: str
    application: str
    title: str
    duration: float
    tags: List[str] = field(default_factory=list)


@dataclass
class UpdateSettingsInput:
    key: str
    value: str


# ── DataLoader ───────────────────────────────────────────────────────

class DataLoader:
    """
    Generic DataLoader for batching + caching lookups.
    Prevents N+1 queries by collecting IDs and resolving in a single batch.
    """

    def __init__(self, batch_fn):
        self._batch_fn = batch_fn
        self._cache: Dict[str, Any] = {}
        self._queue: List[str] = []

    def load(self, key: str) -> Any:
        if key in self._cache:
            return self._cache[key]
        self._queue.append(key)
        return None  # will be resolved on flush

    def flush(self) -> Dict[str, Any]:
        if not self._queue:
            return {}
        keys = list(set(self._queue))
        results = self._batch_fn(keys)
        self._cache.update(results)
        self._queue.clear()
        return results

    def clear(self) -> None:
        self._cache.clear()
        self._queue.clear()


# ── Schema (resolver-based, framework-agnostic) ─────────────────────

class GraphQLSchema:
    """
    GraphQL schema with Query and Mutation resolvers.

    Designed to be plugged into Strawberry, Ariadne, or Graphene.
    Each method is a resolver function.
    """

    def __init__(self):
        self._activities: List[ActivityNode] = []
        self._entities: List[EntityNode] = []
        self._users: Dict[str, UserProfile] = {}
        self._graph_nodes: List[GraphNode] = []
        self._graph_edges: List[GraphEdge] = []
        self._entity_loader = DataLoader(self._batch_load_entities)

    # ── Query resolvers ──────────────────────────────────────────

    def resolve_activities(
        self,
        user_id: str,
        filter: Optional[ActivityFilter] = None,
        page: int = 1,
        page_size: int = 20,
        sort_by: str = "timestamp",
        sort_order: str = "desc",
    ) -> PaginatedActivities:
        """List activities with filtering and pagination."""
        items = [a for a in self._activities if a.user_id == user_id]

        if filter:
            items = self._apply_activity_filter(items, filter)

        # Sort
        reverse = sort_order == "desc"
        items.sort(key=lambda x: getattr(x, sort_by, ""), reverse=reverse)

        # Paginate
        total = len(items)
        start = (page - 1) * page_size
        end = start + page_size
        page_items = items[start:end]

        return PaginatedActivities(
            items=page_items,
            pagination=PaginationInfo(
                total=total,
                page=page,
                page_size=page_size,
                has_next=end < total,
                has_previous=page > 1,
            ),
        )

    def resolve_activity(self, activity_id: str) -> Optional[ActivityNode]:
        """Get a single activity by ID."""
        for a in self._activities:
            if a.id == activity_id:
                return a
        return None

    def resolve_entities(
        self,
        user_id: str,
        filter: Optional[EntityFilter] = None,
        limit: int = 50,
    ) -> List[EntityNode]:
        """List entities with optional filtering."""
        items = list(self._entities)
        if filter:
            if filter.entity_types:
                items = [e for e in items if e.entity_type in filter.entity_types]
            if filter.min_mentions:
                items = [e for e in items if e.mention_count >= filter.min_mentions]
            if filter.search_text:
                q = filter.search_text.lower()
                items = [e for e in items if q in e.canonical_name.lower()]
        return items[:limit]

    def resolve_entity(self, entity_id: str) -> Optional[EntityNode]:
        """Get a single entity by ID."""
        for e in self._entities:
            if e.id == entity_id:
                return e
        return None

    def resolve_productivity(
        self, user_id: str, range: Optional[AnalyticsRange] = None
    ) -> ProductivitySummary:
        """Get productivity analytics."""
        user_acts = [a for a in self._activities if a.user_id == user_id]
        total = sum(a.duration for a in user_acts) / 3600
        productive = sum(
            a.duration for a in user_acts if a.productivity_score > 0.6
        ) / 3600
        neutral = sum(
            a.duration
            for a in user_acts
            if 0.3 <= a.productivity_score <= 0.6
        ) / 3600
        unproductive = total - productive - neutral

        app_counts: Dict[str, float] = {}
        for a in user_acts:
            app_counts[a.application] = app_counts.get(a.application, 0) + a.duration
        top_apps = sorted(app_counts.items(), key=lambda x: x[1], reverse=True)[:5]

        return ProductivitySummary(
            total_hours=round(total, 2),
            productive_hours=round(productive, 2),
            neutral_hours=round(neutral, 2),
            unproductive_hours=round(max(unproductive, 0), 2),
            productivity_score=round(productive / max(total, 0.01) * 100, 1),
            top_applications=[{"name": n, "hours": round(h / 3600, 2)} for n, h in top_apps],
            top_categories=[],
            trend="stable",
        )

    def resolve_collaboration(
        self, user_id: str, range: Optional[AnalyticsRange] = None
    ) -> CollaborationSummary:
        """Get collaboration analytics."""
        meetings = [
            a for a in self._activities
            if a.user_id == user_id and a.activity_type == "meeting"
        ]
        return CollaborationSummary(
            total_collaborators=len(set(e for m in meetings for e in m.entities)),
            meetings_count=len(meetings),
            meeting_hours=round(sum(m.duration for m in meetings) / 3600, 2),
            top_collaborators=[],
            communication_channels=[],
            network_density=0.0,
        )

    def resolve_skills(
        self, user_id: str, range: Optional[AnalyticsRange] = None
    ) -> SkillSummary:
        """Get skill analytics."""
        return SkillSummary(
            total_skills=0,
            top_skills=[],
            growing_skills=[],
            skill_gaps=[],
            learning_paths=[],
        )

    def resolve_graph_nodes(
        self,
        user_id: str,
        node_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[GraphNode]:
        """Get knowledge graph nodes."""
        nodes = list(self._graph_nodes)
        if node_type:
            nodes = [n for n in nodes if n.node_type == node_type]
        return nodes[:limit]

    def resolve_graph_edges(
        self,
        node_id: Optional[str] = None,
        relationship: Optional[str] = None,
        limit: int = 100,
    ) -> List[GraphEdge]:
        """Get knowledge graph edges."""
        edges = list(self._graph_edges)
        if node_id:
            edges = [e for e in edges if e.source == node_id or e.target == node_id]
        if relationship:
            edges = [e for e in edges if e.relationship == relationship]
        return edges[:limit]

    def resolve_graph_path(
        self, source_id: str, target_id: str, max_depth: int = 5
    ) -> Optional[GraphPath]:
        """Find shortest path between two graph nodes (BFS)."""
        adj: Dict[str, List[GraphEdge]] = {}
        for edge in self._graph_edges:
            adj.setdefault(edge.source, []).append(edge)
            adj.setdefault(edge.target, []).append(
                GraphEdge(
                    source=edge.target,
                    target=edge.source,
                    relationship=edge.relationship,
                    weight=edge.weight,
                )
            )

        from collections import deque
        visited = {source_id}
        queue = deque([(source_id, [], [])])

        while queue:
            current, path_nodes, path_edges = queue.popleft()
            if current == target_id:
                nodes = [self._find_graph_node(nid) for nid in [source_id] + [e.target for e in path_edges]]
                nodes = [n for n in nodes if n]
                return GraphPath(
                    nodes=nodes,
                    edges=path_edges,
                    total_weight=sum(e.weight for e in path_edges),
                )
            if len(path_edges) >= max_depth:
                continue
            for edge in adj.get(current, []):
                if edge.target not in visited:
                    visited.add(edge.target)
                    queue.append((edge.target, path_nodes + [current], path_edges + [edge]))
        return None

    def resolve_search(
        self,
        user_id: str,
        query: str,
        result_types: Optional[List[str]] = None,
        limit: int = 20,
    ) -> List[SearchResult]:
        """Full-text search across activities and entities."""
        results: List[SearchResult] = []
        q = query.lower()

        if not result_types or "activity" in result_types:
            for a in self._activities:
                if a.user_id == user_id and q in a.title.lower():
                    results.append(SearchResult(
                        id=a.id,
                        score=1.0,
                        result_type="activity",
                        title=a.title,
                        snippet=f"{a.activity_type} in {a.application}",
                        metadata={"duration": a.duration},
                    ))

        if not result_types or "entity" in result_types:
            for e in self._entities:
                if q in e.canonical_name.lower():
                    results.append(SearchResult(
                        id=e.id,
                        score=0.9,
                        result_type="entity",
                        title=e.canonical_name,
                        snippet=f"{e.entity_type} ({e.mention_count} mentions)",
                        metadata={"type": e.entity_type},
                    ))

        return results[:limit]

    def resolve_user(self, user_id: str) -> Optional[UserProfile]:
        """Get user profile."""
        return self._users.get(user_id)

    # ── Mutation resolvers ───────────────────────────────────────

    def mutate_create_activity(
        self, user_id: str, input: CreateActivityInput
    ) -> ActivityNode:
        """Create a new activity."""
        activity = ActivityNode(
            id=str(uuid4()),
            user_id=user_id,
            activity_type=input.activity_type,
            application=input.application,
            title=input.title,
            duration=input.duration,
            timestamp=datetime.utcnow().isoformat(),
            tags=input.tags,
        )
        self._activities.append(activity)
        return activity

    def mutate_delete_activity(self, activity_id: str) -> bool:
        """Delete an activity."""
        before = len(self._activities)
        self._activities = [a for a in self._activities if a.id != activity_id]
        return len(self._activities) < before

    def mutate_update_settings(
        self, user_id: str, input: UpdateSettingsInput
    ) -> UserProfile:
        """Update a user setting."""
        user = self._users.get(user_id)
        if not user:
            user = UserProfile(id=user_id, email="", name="")
            self._users[user_id] = user
        if user.settings is None:
            user.settings = {}
        user.settings[input.key] = input.value
        return user

    def mutate_add_entity(
        self,
        canonical_name: str,
        entity_type: str,
        aliases: Optional[List[str]] = None,
    ) -> EntityNode:
        """Add a new entity."""
        entity = EntityNode(
            id=str(uuid4()),
            canonical_name=canonical_name,
            entity_type=entity_type,
            first_seen=datetime.utcnow().isoformat(),
            last_seen=datetime.utcnow().isoformat(),
            aliases=aliases or [],
        )
        self._entities.append(entity)
        return entity

    # ── Helpers ───────────────────────────────────────────────────

    def _apply_activity_filter(
        self, items: List[ActivityNode], f: ActivityFilter
    ) -> List[ActivityNode]:
        if f.activity_types:
            items = [a for a in items if a.activity_type in f.activity_types]
        if f.applications:
            items = [a for a in items if a.application in f.applications]
        if f.tags:
            items = [a for a in items if any(t in a.tags for t in f.tags)]
        if f.date_from:
            items = [a for a in items if a.timestamp >= f.date_from]
        if f.date_to:
            items = [a for a in items if a.timestamp <= f.date_to]
        if f.min_duration:
            items = [a for a in items if a.duration >= f.min_duration]
        if f.search_text:
            q = f.search_text.lower()
            items = [a for a in items if q in a.title.lower()]
        return items

    def _batch_load_entities(self, ids: List[str]) -> Dict[str, EntityNode]:
        result = {}
        for e in self._entities:
            if e.id in ids:
                result[e.id] = e
        return result

    def _find_graph_node(self, node_id: str) -> Optional[GraphNode]:
        for n in self._graph_nodes:
            if n.id == node_id:
                return n
        return None

    def get_schema_info(self) -> Dict[str, Any]:
        """Introspection-like info about available queries and mutations."""
        return {
            "queries": [
                "activities", "activity", "entities", "entity",
                "productivity", "collaboration", "skills",
                "graphNodes", "graphEdges", "graphPath",
                "search", "user",
            ],
            "mutations": [
                "createActivity", "deleteActivity",
                "updateSettings", "addEntity",
            ],
            "types": [
                "ActivityNode", "EntityNode", "UserProfile",
                "ProductivitySummary", "CollaborationSummary", "SkillSummary",
                "GraphNode", "GraphEdge", "GraphPath", "SearchResult",
            ],
        }
