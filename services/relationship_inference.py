"""
Relationship Inference Engine
Automatically infers relationships between entities based on co-occurrence,
citations, and other signals from user's activity data.
"""

from typing import Dict, List, Tuple, Optional, Any, Set
from datetime import datetime, timedelta
from collections import defaultdict, Counter
import logging
import math

from prometheus_client import Counter as PrometheusCounter, Histogram, Gauge

from models.graph_models import NodeType, RelationshipType
from services.graph_ingestion import graph_ingestion_service
from services.relationship_validator import relationship_validator

logger = logging.getLogger(__name__)


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

inference_relationships_total = PrometheusCounter(
    'inference_relationships_total',
    'Total inferred relationships',
    ['relationship_type', 'inference_method']
)

inference_confidence_distribution = Histogram(
    'inference_confidence_distribution',
    'Distribution of inference confidence scores',
    ['relationship_type'],
    buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
)

inference_execution_time = Histogram(
    'inference_execution_time_seconds',
    'Time to execute inference',
    ['inference_method']
)

inference_candidates_evaluated = PrometheusCounter(
    'inference_candidates_evaluated_total',
    'Total relationship candidates evaluated',
    ['entity_type_pair']
)


# ============================================================================
# ENTITY TYPE PAIR TO RELATIONSHIP MAPPING
# ============================================================================

# ─── Activity type → relationship signal mapping ─────────────────────────────
# Maps activity types (from MiniMe tracker) to relevant relationship signals
ACTIVITY_TYPE_SIGNALS = {
    'reading_analytics': 'reading',
    'app_focus':         'usage',
    'video_watching':    'watching',
    'social_media':      'social',
    'search_query':      'research',
    'code_edit':         'coding',
    'document_edit':     'editing',
}

# Session gap: activities within this window are considered same-session
SESSION_GAP_MINUTES = 30


ENTITY_PAIR_TO_RELATIONSHIP = {
    # Co-occurrence patterns
    (NodeType.PERSON, NodeType.PERSON): [
        (RelationshipType.COLLABORATES_WITH, "co_occurrence")
    ],
    (NodeType.PERSON, NodeType.PAPER): [
        (RelationshipType.AUTHORED, "co_occurrence"),
        (RelationshipType.WORKS_ON, "co_occurrence")
    ],
    (NodeType.PERSON, NodeType.TOPIC): [
        (RelationshipType.WORKS_ON, "co_occurrence")
    ],
    (NodeType.PERSON, NodeType.PROJECT): [
        (RelationshipType.WORKS_ON, "co_occurrence"),
        (RelationshipType.CONTRIBUTES_TO, "co_occurrence")
    ],
    (NodeType.PERSON, NodeType.INSTITUTION): [
        (RelationshipType.AFFILIATED_WITH, "co_occurrence")
    ],
    # ── Organisation relationships ──────────────────────────────────────────
    # User spent time on a platform/org → they "used" or are affiliated with it
    (NodeType.PERSON, NodeType.ORGANIZATION): [
        (RelationshipType.AFFILIATED_WITH, "co_occurrence"),
        (RelationshipType.WORKS_ON, "co_occurrence"),
    ],
    (NodeType.PROJECT, NodeType.ORGANIZATION): [
        (RelationshipType.USES, "mention"),
        (RelationshipType.AFFILIATED_WITH, "co_occurrence"),
    ],
    (NodeType.TOPIC, NodeType.ORGANIZATION): [
        (RelationshipType.RELATED_TO, "co_occurrence"),
    ],
    (NodeType.INSTITUTION, NodeType.ORGANIZATION): [
        (RelationshipType.RELATED_TO, "co_occurrence"),
    ],
    (NodeType.TOOL, NodeType.ORGANIZATION): [
        (RelationshipType.AFFILIATED_WITH, "mention"),
    ],
    # ── Existing paper / project pairs ─────────────────────────────────────
    (NodeType.PAPER, NodeType.PAPER): [
        (RelationshipType.CITES, "citation"),
        (RelationshipType.RELATED_TO, "co_occurrence")
    ],
    (NodeType.PAPER, NodeType.TOPIC): [
        (RelationshipType.ON_TOPIC, "co_occurrence")
    ],
    (NodeType.PAPER, NodeType.DATASET): [
        (RelationshipType.USES, "mention")
    ],
    (NodeType.PAPER, NodeType.TOOL): [
        (RelationshipType.USES, "mention")
    ],
    (NodeType.PAPER, NodeType.VENUE): [
        (RelationshipType.PUBLISHED_AT, "co_occurrence")
    ],
    (NodeType.PROJECT, NodeType.TOPIC): [
        (RelationshipType.ON_TOPIC, "co_occurrence")
    ],
    (NodeType.PROJECT, NodeType.DATASET): [
        (RelationshipType.USES, "mention")
    ],
    (NodeType.PROJECT, NodeType.TOOL): [
        (RelationshipType.USES, "mention")
    ],
    (NodeType.TOPIC, NodeType.TOPIC): [
        (RelationshipType.RELATED_TO, "co_occurrence")
    ],
    (NodeType.TOOL, NodeType.TOOL): [
        (RelationshipType.DEPENDS_ON, "mention")
    ],
    # ── Organization-aware pairs (activity-derived) ──────────────────────────
    # USER ↔ ORG: user visited / worked at / learned from an organization
    (NodeType.PERSON, NodeType.INSTITUTION): [
        (RelationshipType.AFFILIATED_WITH, "co_occurrence")
    ],
    # PROJECT ↔ INSTITUTION: project is hosted / affiliated with org
    (NodeType.PROJECT, NodeType.INSTITUTION): [
        (RelationshipType.AFFILIATED_WITH, "mention")
    ],
    # TOPIC ↔ INSTITUTION: topic co-occurs with org (e.g., MIT + ML)
    (NodeType.TOPIC, NodeType.INSTITUTION): [
        (RelationshipType.RELATED_TO, "co_occurrence")
    ],
    (NodeType.TOOL, NodeType.INSTITUTION): [
        (RelationshipType.AFFILIATED_WITH, "mention")
    ],
    # PAPER ↔ INSTITUTION: paper published from org's researchers
    (NodeType.PAPER, NodeType.INSTITUTION): [
        (RelationshipType.AFFILIATED_WITH, "co_occurrence")
    ],
}


class RelationshipInferenceService:
    """
    Service for inferring relationships between entities based on various signals.
    """
    
    def __init__(
        self,
        min_confidence: float = 0.5,
        recency_decay_days: int = 365,
        min_co_occurrences: int = 2
    ):
        """
        Initialize relationship inference service.
        
        Args:
            min_confidence: Minimum confidence threshold for inferred relationships
            recency_decay_days: Days for recency decay (half-life)
            min_co_occurrences: Minimum co-occurrences to consider
        """
        self.logger = logging.getLogger(__name__)
        self.min_confidence = min_confidence
        self.recency_decay_days = recency_decay_days
        self.min_co_occurrences = min_co_occurrences
    
    def infer_relationships_from_co_occurrence(
        self,
        user_id: str,
        entities: List[Dict[str, Any]],
        context: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Infer relationships based on entity co-occurrence in documents/activities.
        
        Args:
            user_id: User ID for multi-tenancy
            entities: List of entities that co-occur (in same document, activity, etc.)
            context: Additional context (timestamp, document type, etc.)
            
        Returns:
            List of inferred relationships with confidence scores
        """
        inferred = []
        context = context or {}
        timestamp = context.get("timestamp", datetime.utcnow())
        
        # Detect all entity pairs
        for i in range(len(entities)):
            for j in range(i + 1, len(entities)):
                entity_a = entities[i]
                entity_b = entities[j]
                
                # Get entity types
                type_a = NodeType(entity_a["type"])
                type_b = NodeType(entity_b["type"])
                
                # Check both directions for valid relationship mappings
                candidates = []
                
                # A -> B
                if (type_a, type_b) in ENTITY_PAIR_TO_RELATIONSHIP:
                    for rel_type, method in ENTITY_PAIR_TO_RELATIONSHIP[(type_a, type_b)]:
                        candidates.append({
                            "from_id": entity_a["id"],
                            "from_type": type_a,
                            "to_id": entity_b["id"],
                            "to_type": type_b,
                            "rel_type": rel_type,
                            "method": method
                        })
                
                # B -> A (reverse direction)
                if (type_b, type_a) in ENTITY_PAIR_TO_RELATIONSHIP:
                    for rel_type, method in ENTITY_PAIR_TO_RELATIONSHIP[(type_b, type_a)]:
                        candidates.append({
                            "from_id": entity_b["id"],
                            "from_type": type_b,
                            "to_id": entity_a["id"],
                            "to_type": type_a,
                            "rel_type": rel_type,
                            "method": method
                        })
                
                # Compute weight and confidence for each candidate
                for candidate in candidates:
                    inference_candidates_evaluated.labels(
                        entity_type_pair=f"{candidate['from_type'].value}-{candidate['to_type'].value}"
                    ).inc()
                    
                    # Compute weight with recency decay
                    weight = self._compute_weight(
                        co_occurrence_count=context.get("frequency", 1),
                        timestamp=timestamp,
                        context_strength=context.get("context_strength", 1.0)
                    )
                    
                    # Compute confidence based on method and signals
                    confidence = self._compute_confidence(
                        method=candidate["method"],
                        weight=weight,
                        entity_a=entity_a,
                        entity_b=entity_b,
                        context=context
                    )
                    
                    # Only include if above threshold
                    if confidence >= self.min_confidence:
                        inferred.append({
                            "from_id": candidate["from_id"],
                            "from_type": candidate["from_type"].value,
                            "to_id": candidate["to_id"],
                            "to_type": candidate["to_type"].value,
                            "rel_type": candidate["rel_type"].value,
                            "weight": weight,
                            "confidence": confidence,
                            "source": ["inference"],
                            "inference_method": candidate["method"],
                            "inferred": True,
                            "timestamp": timestamp.isoformat()
                        })
                        
                        # Track metrics
                        inference_relationships_total.labels(
                            relationship_type=candidate["rel_type"].value,
                            inference_method=candidate["method"]
                        ).inc()
                        
                        inference_confidence_distribution.labels(
                            relationship_type=candidate["rel_type"].value
                        ).observe(confidence)
        
        return inferred
    
    def _compute_weight(
        self,
        co_occurrence_count: int,
        timestamp: datetime,
        context_strength: float = 1.0
    ) -> float:
        """
        Compute relationship weight with frequency and recency factors.
        
        Weight formula:
        weight = frequency_weight * recency_decay * context_strength
        
        Args:
            co_occurrence_count: Number of times entities co-occurred
            timestamp: When the co-occurrence happened
            context_strength: Strength of context (0.0-1.0)
            
        Returns:
            Computed weight
        """
        # Frequency weighting (logarithmic scale)
        # 1 occurrence = 1.0, 10 occurrences = 2.0, 100 occurrences = 3.0
        frequency_weight = 1.0 + math.log10(max(1, co_occurrence_count))
        
        # Recency decay (exponential decay with half-life)
        # More recent co-occurrences get higher weight
        days_old = (datetime.utcnow() - timestamp).days
        half_life = self.recency_decay_days
        recency_decay = math.exp(-0.693 * days_old / half_life)  # 0.693 = ln(2)
        
        # Combined weight
        weight = frequency_weight * recency_decay * context_strength
        
        # Clamp to reasonable range
        return max(0.1, min(5.0, weight))
    
    def _compute_confidence(
        self,
        method: str,
        weight: float,
        entity_a: Dict[str, Any],
        entity_b: Dict[str, Any],
        context: Dict[str, Any]
    ) -> float:
        """
        Compute confidence score for inferred relationship.
        
        Args:
            method: Inference method (co_occurrence, citation, mention)
            weight: Computed weight
            entity_a: First entity
            entity_b: Second entity
            context: Inference context
            
        Returns:
            Confidence score (0.0-1.0)
        """
        # Base confidence by method
        base_confidence = {
            "co_occurrence": 0.6,
            "citation": 0.8,      # Citations are more reliable
            "mention": 0.7        # Explicit mentions are reliable
        }.get(method, 0.5)
        
        # Boost based on weight (higher weight = more confidence)
        weight_boost = min(0.2, weight * 0.05)  # Cap at +0.2
        
        # Boost if entities are well-established (have metadata)
        entity_quality_boost = 0.0
        if entity_a.get("metadata") and entity_b.get("metadata"):
            entity_quality_boost = 0.1
        
        # Boost based on context signals
        context_boost = 0.0
        if context.get("verified"):
            context_boost += 0.15
        if context.get("user_action"):  # User explicitly interacted
            context_boost += 0.1
        
        # Combined confidence
        confidence = base_confidence + weight_boost + entity_quality_boost + context_boost
        
        # Clamp to [0.0, 1.0]
        return max(0.0, min(1.0, confidence))
    
    def infer_citations_from_paper_content(
        self,
        paper_id: str,
        paper_content: str,
        known_papers: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Infer CITES relationships by finding paper mentions in content.
        
        Args:
            paper_id: ID of the citing paper
            paper_content: Full text or abstract of paper
            known_papers: List of known papers to search for
            
        Returns:
            List of inferred citation relationships
        """
        inferred_citations = []
        content_lower = paper_content.lower()
        
        for cited_paper in known_papers:
            # Skip self-citations
            if cited_paper["id"] == paper_id:
                continue
            
            # Check for mentions of paper title, authors, or DOI
            mentioned = False
            mention_signals = []
            
            # Title mention
            if cited_paper.get("title"):
                title_lower = cited_paper["title"].lower()
                if title_lower in content_lower:
                    mentioned = True
                    mention_signals.append("title")
            
            # Author mention (check if any author appears)
            if cited_paper.get("authors"):
                for author in cited_paper["authors"]:
                    author_lower = author.lower()
                    if author_lower in content_lower:
                        mentioned = True
                        mention_signals.append("author")
                        break
            
            # DOI mention
            if cited_paper.get("doi"):
                if cited_paper["doi"] in paper_content:
                    mentioned = True
                    mention_signals.append("doi")
            
            if mentioned:
                # Higher confidence if multiple signals
                base_confidence = 0.7
                if len(mention_signals) > 1:
                    base_confidence = 0.85
                if "doi" in mention_signals:
                    base_confidence = 0.95  # DOI is very specific
                
                inferred_citations.append({
                    "from_id": paper_id,
                    "from_type": "PAPER",
                    "to_id": cited_paper["id"],
                    "to_type": "PAPER",
                    "rel_type": "CITES",
                    "weight": 1.0,
                    "confidence": base_confidence,
                    "source": ["inference"],
                    "inference_method": "mention",
                    "mention_signals": mention_signals,
                    "inferred": True
                })
                
                inference_relationships_total.labels(
                    relationship_type="CITES",
                    inference_method="mention"
                ).inc()
        
        return inferred_citations
    
    def infer_tool_usage_from_text(
        self,
        entity_id: str,
        entity_type: str,
        text_content: str,
        known_tools: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Infer USES relationships by detecting tool/dataset mentions in text.
        
        Args:
            entity_id: ID of entity (PAPER or PROJECT)
            entity_type: Type of entity
            text_content: Text content to analyze
            known_tools: List of known tools/datasets
            
        Returns:
            List of inferred USES relationships
        """
        inferred_uses = []
        content_lower = text_content.lower()
        
        for tool in known_tools:
            tool_name = tool.get("canonical_name", tool.get("name", "")).lower()
            
            if not tool_name:
                continue
            
            # Check for exact or fuzzy matches
            if tool_name in content_lower:
                # Determine if primary or secondary usage
                is_primary = False
                if any(keyword in content_lower for keyword in [
                    f"primarily {tool_name}",
                    f"mainly {tool_name}",
                    f"using {tool_name}",
                    f"built with {tool_name}"
                ]):
                    is_primary = True
                
                weight = 2.0 if is_primary else 1.0
                confidence = 0.75  # Mention-based inference
                
                inferred_uses.append({
                    "from_id": entity_id,
                    "from_type": entity_type,
                    "to_id": tool["id"],
                    "to_type": tool["type"],
                    "rel_type": "USES",
                    "weight": weight,
                    "confidence": confidence,
                    "source": ["inference"],
                    "inference_method": "mention",
                    "primary": is_primary,
                    "inferred": True
                })
                
                inference_relationships_total.labels(
                    relationship_type="USES",
                    inference_method="mention"
                ).inc()
        
        return inferred_uses
    
    def batch_infer_from_activity_log(
        self,
        user_id: str,
        activity_log: List[Dict[str, Any]],
        lookback_days: int = 90
    ) -> Dict[str, Any]:
        """
        Batch infer relationships from user activity log.
        
        Analyzes activity patterns to find entity co-occurrences and infer relationships.
        
        Args:
            user_id: User ID
            activity_log: List of user activities with entity mentions
            lookback_days: How far back to analyze
            
        Returns:
            Summary of inferred relationships
        """
        import time
        start_time = time.time()
        
        # Track co-occurrences
        co_occurrence_tracker = defaultdict(lambda: {
            "count": 0,
            "timestamps": [],
            "contexts": []
        })
        
        # Filter recent activities
        cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
        recent_activities = [
            act for act in activity_log
            if datetime.fromisoformat(act.get("timestamp", "2000-01-01")) > cutoff_date
        ]
        
        # Entity type index: pair_key → (type_a, type_b)
        entity_type_index: Dict[Tuple[str, str], Tuple[str, str]] = {}

        # Extract co-occurrences from each activity
        for activity in recent_activities:
            entities = activity.get("entities", [])
            timestamp = datetime.fromisoformat(activity.get("timestamp", datetime.utcnow().isoformat()))
            act_type = activity.get("type", "app_focus")

            # Context strength based on activity type (reading/coding = stronger signal)
            act_strength_map = {
                'reading_analytics': 1.2,
                'code_edit': 1.3,
                'document_edit': 1.2,
                'app_focus': 1.0,
                'video_watching': 0.9,
                'social_media': 0.7,
                'search_query': 1.1,
            }
            context_strength = act_strength_map.get(act_type, 1.0) * activity.get('importance', 1.0)

            # Record all entity pairs in this activity
            for i in range(len(entities)):
                for j in range(i + 1, len(entities)):
                    entity_a = entities[i]
                    entity_b = entities[j]
                    entity_a_id = entity_a.get("id") or entity_a.get("name", f"ent_{i}")
                    entity_b_id = entity_b.get("id") or entity_b.get("name", f"ent_{j}")

                    # Create canonical pair key (sorted)
                    pair_key = tuple(sorted([entity_a_id, entity_b_id]))

                    co_occurrence_tracker[pair_key]["count"] += 1
                    co_occurrence_tracker[pair_key]["timestamps"].append(timestamp)
                    co_occurrence_tracker[pair_key]["contexts"].append({
                        "activity_type": act_type,
                        "context_strength": context_strength,
                    })

                    # Store entity types for later inference
                    if pair_key not in entity_type_index:
                        type_a = entity_a.get("entity_type") or entity_a.get("type", "TOPIC")
                        type_b = entity_b.get("entity_type") or entity_b.get("type", "TOPIC")
                        entity_type_index[pair_key] = (type_a.upper(), type_b.upper())

        # Infer relationships from co-occurrences
        all_inferred = []

        for pair_key, occurrence_data in co_occurrence_tracker.items():
            # Skip if below minimum threshold
            if occurrence_data["count"] < self.min_co_occurrences:
                continue

            # Get most recent timestamp
            most_recent = max(occurrence_data["timestamps"])

            # Average context strength
            avg_context_strength = sum(
                ctx.get("context_strength", 1.0)
                for ctx in occurrence_data["contexts"]
            ) / len(occurrence_data["contexts"])

            # Use actual entity types from index
            types = entity_type_index.get(pair_key, ("TOPIC", "TOPIC"))
            entities_for_inference = [
                {"id": pair_key[0], "type": types[0]},
                {"id": pair_key[1], "type": types[1]}
            ]

            context = {
                "frequency": occurrence_data["count"],
                "timestamp": most_recent,
                "context_strength": avg_context_strength,
                "user_action": True
            }

            # Infer relationships
            inferred = self.infer_relationships_from_co_occurrence(
                user_id=user_id,
                entities=entities_for_inference,
                context=context
            )

            all_inferred.extend(inferred)
        
        elapsed = time.time() - start_time

        inference_execution_time.labels(
            inference_method='batch_activity_log'
        ).observe(elapsed)

        return {
            "inferred_count": len(all_inferred),
            "co_occurrence_pairs": len(co_occurrence_tracker),
            "activities_analyzed": len(recent_activities),
            "execution_time_sec": elapsed,
            "relationships": all_inferred
        }

    def infer_from_activity_sequence(
        self,
        user_id: str,
        activity_log: List[Dict[str, Any]],
        lookback_days: int = 30
    ) -> Dict[str, Any]:
        """
        Infer TEMPORAL relationships from activity sequence patterns.

        Detects:
        - USED_TOGETHER: entity pairs appearing in same session (within SESSION_GAP_MINUTES)
        - CLOSELY_RELATED: entity pairs that co-occur across multiple separate sessions

        Args:
            user_id: User ID
            activity_log: Time-ordered list of activities with entities
            lookback_days: Lookback window

        Returns:
            Dict with inferred relationships and stats
        """
        import time as _time
        start_time = _time.time()

        cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
        sorted_acts = sorted(
            [a for a in activity_log
             if datetime.fromisoformat(a.get('timestamp', '2000-01-01')) > cutoff_date],
            key=lambda a: a.get('timestamp', '')
        )

        # Build sessions: group activities within SESSION_GAP_MINUTES of each other
        sessions: List[List[Dict]] = []
        current_session: List[Dict] = []
        prev_ts: Optional[datetime] = None

        for act in sorted_acts:
            ts = datetime.fromisoformat(act.get('timestamp', datetime.utcnow().isoformat()))
            if prev_ts is None or (ts - prev_ts).total_seconds() <= SESSION_GAP_MINUTES * 60:
                current_session.append(act)
            else:
                if current_session:
                    sessions.append(current_session)
                current_session = [act]
            prev_ts = ts
        if current_session:
            sessions.append(current_session)

        # Track same-session pair occurrences
        session_pair_counts: Dict[Tuple, int] = defaultdict(int)
        session_pair_types: Dict[Tuple, Tuple[str, str]] = {}

        for session in sessions:
            session_entities: Dict[str, str] = {}  # id → type
            for act in session:
                for ent in act.get('entities', []):
                    eid = ent.get('id') or ent.get('name', '')
                    etype = (ent.get('entity_type') or ent.get('type', 'TOPIC')).upper()
                    if eid:
                        session_entities[eid] = etype

            entity_ids = list(session_entities.keys())
            for i in range(len(entity_ids)):
                for j in range(i + 1, len(entity_ids)):
                    pair = tuple(sorted([entity_ids[i], entity_ids[j]]))
                    session_pair_counts[pair] += 1
                    if pair not in session_pair_types:
                        session_pair_types[pair] = (
                            session_entities[entity_ids[i]],
                            session_entities[entity_ids[j]]
                        )

        inferred = []
        now = datetime.utcnow()

        for pair, count in session_pair_counts.items():
            if count < 1:
                continue

            rel_type = 'RELATED_TO'  # Fallback; maps to RelationshipType.RELATED_TO
            confidence = min(0.9, 0.55 + count * 0.07)  # More sessions = higher confidence
            weight = min(3.0, 1.0 + count * 0.3)
            method = 'temporal_session'

            inferred.append({
                'from_id': pair[0],
                'from_type': session_pair_types.get(pair, ('TOPIC', 'TOPIC'))[0],
                'to_id': pair[1],
                'to_type': session_pair_types.get(pair, ('TOPIC', 'TOPIC'))[1],
                'rel_type': rel_type,
                'weight': weight,
                'confidence': confidence,
                'source': ['inference'],
                'inference_method': method,
                'inferred': True,
                'session_count': count,
                'timestamp': now.isoformat(),
            })

            inference_relationships_total.labels(
                relationship_type=rel_type,
                inference_method=method
            ).inc()

        elapsed = _time.time() - start_time
        inference_execution_time.labels(inference_method='temporal_sequence').observe(elapsed)

        return {
            'inferred_count': len(inferred),
            'sessions_analyzed': len(sessions),
            'execution_time_sec': elapsed,
            'relationships': inferred,
        }

    def infer_learning_relationships(
        self,
        user_id: str,
        activity_log: List[Dict[str, Any]],
        lookback_days: int = 90
    ) -> Dict[str, Any]:
        """
        Infer LEARNED_FROM, CONTRIBUTED_TO, and WORKED_ON relationships
        using activity type as a signal for the nature of the interaction.

        - LEARNED_FROM: reading/research activities on educational orgs
        - CONTRIBUTED_TO: coding/editing activities on repos or projects
        - WORKED_ON: repeated app_focus on the same project/tool

        Args:
            user_id: User ID
            activity_log: Activity list with entities and type fields
            lookback_days: Lookback window

        Returns:
            Dict with inferred relationships and stats
        """
        import time as _time
        start_time = _time.time()

        cutoff_date = datetime.utcnow() - timedelta(days=lookback_days)
        recent = [
            a for a in activity_log
            if datetime.fromisoformat(a.get('timestamp', '2000-01-01')) > cutoff_date
        ]

        # Buckets: (entity_id, rel_type) → {count, confidence_sum, timestamps}
        learning_buckets: Dict[str, Dict] = defaultdict(lambda: {
            'count': 0, 'confidence_sum': 0.0, 'timestamps': [],
            'entity_type': 'INSTITUTION', 'rel_type': 'RELATED_TO'
        })

        for act in recent:
            act_type = act.get('type', 'app_focus')
            signal = ACTIVITY_TYPE_SIGNALS.get(act_type, 'usage')
            ts = act.get('timestamp', datetime.utcnow().isoformat())

            for ent in act.get('entities', []):
                eid = ent.get('id') or ent.get('name', '')
                if not eid:
                    continue
                etype = (ent.get('entity_type') or ent.get('type', '')).lower()
                org_type = ent.get('org_type', '')

                # LEARNED_FROM: reading/research on educational orgs or research domains
                if signal in ('reading', 'research') and etype in ('organization', 'institution'):
                    conf = 0.72 if org_type in ('educational', 'open_source', 'community') else 0.55
                    key = f"{eid}|LEARNED_FROM"
                    learning_buckets[key]['count'] += 1
                    learning_buckets[key]['confidence_sum'] += conf
                    learning_buckets[key]['timestamps'].append(ts)
                    learning_buckets[key]['entity_id'] = eid
                    learning_buckets[key]['entity_type'] = 'INSTITUTION'
                    learning_buckets[key]['rel_type'] = 'RELATED_TO'  # Semantic: learned from
                    learning_buckets[key]['label'] = 'LEARNED_FROM'

                # CONTRIBUTED_TO: coding on a project/repo
                elif signal in ('coding', 'editing') and etype in ('project', 'artifact'):
                    key = f"{eid}|CONTRIBUTED_TO"
                    learning_buckets[key]['count'] += 1
                    learning_buckets[key]['confidence_sum'] += 0.75
                    learning_buckets[key]['timestamps'].append(ts)
                    learning_buckets[key]['entity_id'] = eid
                    learning_buckets[key]['entity_type'] = 'PROJECT'
                    learning_buckets[key]['rel_type'] = 'CONTRIBUTES_TO'
                    learning_buckets[key]['label'] = 'CONTRIBUTED_TO'

                # WORKED_ON: repeated app_focus on same tool/project
                elif signal == 'usage' and etype in ('project', 'artifact', 'skill'):
                    key = f"{eid}|WORKED_ON"
                    learning_buckets[key]['count'] += 1
                    learning_buckets[key]['confidence_sum'] += 0.60
                    learning_buckets[key]['timestamps'].append(ts)
                    learning_buckets[key]['entity_id'] = eid
                    learning_buckets[key]['entity_type'] = 'PROJECT'
                    learning_buckets[key]['rel_type'] = 'WORKS_ON'
                    learning_buckets[key]['label'] = 'WORKED_ON'

        inferred = []
        now = datetime.utcnow()

        for key, data in learning_buckets.items():
            if data['count'] < self.min_co_occurrences:
                continue

            avg_conf = min(0.92, data['confidence_sum'] / data['count'])
            # Boost confidence for higher frequency
            frequency_boost = min(0.15, data['count'] * 0.02)
            final_conf = min(0.95, avg_conf + frequency_boost)

            rel = {
                'from_id': user_id,
                'from_type': 'PERSON',
                'to_id': data.get('entity_id', ''),
                'to_type': data.get('entity_type', 'INSTITUTION'),
                'rel_type': data.get('rel_type', 'RELATED_TO'),
                'weight': min(3.0, 1.0 + math.log10(max(1, data['count']))),
                'confidence': final_conf,
                'source': ['inference'],
                'inference_method': 'activity_signal',
                'inferred': True,
                'activity_count': data['count'],
                'label': data.get('label', ''),
                'timestamp': now.isoformat(),
            }
            inferred.append(rel)

            inference_relationships_total.labels(
                relationship_type=rel['rel_type'],
                inference_method='activity_signal'
            ).inc()

        elapsed = _time.time() - start_time
        inference_execution_time.labels(inference_method='learning_relationships').observe(elapsed)

        return {
            'inferred_count': len(inferred),
            'activities_analyzed': len(recent),
            'execution_time_sec': elapsed,
            'relationships': inferred,
        }
    
    def apply_confidence_thresholds(
        self,
        inferred_relationships: List[Dict[str, Any]],
        thresholds: Dict[str, float] = None
    ) -> List[Dict[str, Any]]:
        """
        Filter inferred relationships by confidence thresholds.
        
        Args:
            inferred_relationships: List of inferred relationships
            thresholds: Custom thresholds per relationship type
            
        Returns:
            Filtered relationships above threshold
        """
        if thresholds is None:
            thresholds = {
                "AUTHORED": 0.7,
                "CITES": 0.6,
                "COLLABORATES_WITH": 0.65,
                "USES": 0.6,
                "WORKS_ON": 0.6,
                "AFFILIATED_WITH": 0.6,
                "CONTRIBUTES_TO": 0.55,
                "ON_TOPIC": 0.55,
                "RELATED_TO": 0.5,
                "USED_TOGETHER": 0.55,
                "LEARNED_FROM": 0.6,
                "default": self.min_confidence
            }
        
        filtered = []
        for rel in inferred_relationships:
            rel_type = rel["rel_type"]
            threshold = thresholds.get(rel_type, thresholds["default"])
            
            if rel["confidence"] >= threshold:
                filtered.append(rel)
        
        self.logger.info(
            f"Applied confidence thresholds: {len(inferred_relationships)} -> {len(filtered)} relationships"
        )
        
        return filtered

    def infer_from_activity_sequence(
        self,
        user_id: str,
        activities: List[Dict[str, Any]],
        session_gap_minutes: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Infer temporal relationships from activity sequences.

        Detects entity pairs that repeatedly co-occur within the same session
        (within `session_gap_minutes` of each other) and creates USED_TOGETHER
        relationships. Entities that *always* appear before another get a
        PRECEDES relationship with decay-adjusted weight.

        Args:
            user_id: User ID for multi-tenancy
            activities: Time-ordered list of activities, each with 'entities'
                        and 'timestamp'.
            session_gap_minutes: Max gap between activities to consider same session.

        Returns:
            List of inferred relationship dicts.
        """
        from collections import defaultdict

        session_pairs: Dict[tuple, List[datetime]] = defaultdict(list)
        before_counts: Dict[tuple, int] = defaultdict(int)  # (a,b) → a appeared before b

        sorted_acts = sorted(
            [a for a in activities if a.get("entities") and a.get("timestamp")],
            key=lambda a: a["timestamp"]
        )

        # Sliding window to find session pairs
        for i, act in enumerate(sorted_acts):
            t_i = act["timestamp"]
            if isinstance(t_i, str):
                try:
                    t_i = datetime.fromisoformat(t_i)
                except ValueError:
                    continue

            for j in range(i + 1, len(sorted_acts)):
                act_j = sorted_acts[j]
                t_j = act_j["timestamp"]
                if isinstance(t_j, str):
                    try:
                        t_j = datetime.fromisoformat(t_j)
                    except ValueError:
                        continue

                gap_min = (t_j - t_i).total_seconds() / 60
                if gap_min > session_gap_minutes:
                    break  # beyond session window

                for ea in (act.get("entities") or []):
                    for eb in (act_j.get("entities") or []):
                        if ea.get("id") == eb.get("id"):
                            continue
                        pair = (ea["id"], eb["id"])
                        session_pairs[pair].append(t_i)
                        before_counts[pair] += 1

        inferred = []
        now = datetime.utcnow()
        for (id_a, id_b), timestamps in session_pairs.items():
            if len(timestamps) < self.min_co_occurrences:
                continue
            most_recent = max(timestamps)
            weight = self._compute_weight(
                co_occurrence_count=len(timestamps),
                timestamp=most_recent,
                context_strength=0.9,
            )
            confidence = min(0.85, 0.55 + len(timestamps) * 0.05)
            inferred.append({
                "from_id": id_a,
                "to_id": id_b,
                "rel_type": "USED_TOGETHER",
                "weight": weight,
                "confidence": confidence,
                "inference_method": "session_co_occurrence",
                "inferred": True,
                "session_count": len(timestamps),
                "timestamp": most_recent.isoformat() if isinstance(most_recent, datetime) else most_recent,
            })
            inference_relationships_total.labels(
                relationship_type="USED_TOGETHER",
                inference_method="session_co_occurrence",
            ).inc()

        return inferred

    def infer_learning_relationships(
        self,
        user_id: str,
        activities: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Infer LEARNED_FROM, CONTRIBUTED_TO, and WORKED_ON relationships from
        activity context and organisation metadata.

        Rules:
        - LEARNED_FROM: user read content from an educational org/domain
          (org_type == 'educational' | 'open_source', activity type == reading_analytics)
        - CONTRIBUTED_TO: user had coding activity on a GitHub-hosted project
          (activity type involves github.com + project entity)
        - WORKED_ON: user had >= 3 app_focus sessions with a project entity

        Returns:
            List of inferred relationship dicts.
        """
        from collections import defaultdict

        learned_from: Dict[str, List] = defaultdict(list)    # org_name → timestamps
        contributed_to: Dict[str, List] = defaultdict(list)  # project_id → timestamps
        worked_on: Dict[str, List] = defaultdict(list)       # project_id → timestamps

        for act in activities:
            act_type = act.get("type", "")
            entities = act.get("entities") or []
            ts = act.get("timestamp", datetime.utcnow().isoformat())

            for ent in entities:
                etype = ent.get("entity_type", ent.get("type", ""))
                org_type = ent.get("org_type", "")

                # LEARNED_FROM: reading from educational/research org
                if act_type in ("reading_analytics", "web_browsing") and etype == "organization":
                    if org_type in ("educational", "open_source", "community", "media"):
                        learned_from[ent.get("id", ent.get("name", ""))].append(ts)

                # CONTRIBUTED_TO: code activity on GitHub
                if act_type in ("app_focus", "code_editing") and etype == "project":
                    if ent.get("source") == "url" or "github" in str(ent.get("name", "")).lower():
                        contributed_to[ent.get("id", ent.get("name", ""))].append(ts)

                # WORKED_ON: any focused project work
                if act_type == "app_focus" and etype == "project":
                    worked_on[ent.get("id", ent.get("name", ""))].append(ts)

        inferred = []

        for org_id, timestamps in learned_from.items():
            if len(timestamps) < 1:
                continue
            confidence = min(0.85, 0.60 + len(timestamps) * 0.04)
            inferred.append({
                "from_id": user_id,
                "to_id": org_id,
                "rel_type": "LEARNED_FROM",
                "weight": 1.0 + math.log10(max(1, len(timestamps))),
                "confidence": confidence,
                "inference_method": "reading_activity",
                "inferred": True,
                "session_count": len(timestamps),
            })
            inference_relationships_total.labels(
                relationship_type="LEARNED_FROM",
                inference_method="reading_activity",
            ).inc()

        for proj_id, timestamps in contributed_to.items():
            if len(timestamps) < 2:
                continue
            confidence = min(0.85, 0.60 + len(timestamps) * 0.05)
            inferred.append({
                "from_id": user_id,
                "to_id": proj_id,
                "rel_type": "CONTRIBUTED_TO",
                "weight": 1.0 + math.log10(max(1, len(timestamps))),
                "confidence": confidence,
                "inference_method": "coding_activity",
                "inferred": True,
                "session_count": len(timestamps),
            })
            inference_relationships_total.labels(
                relationship_type="CONTRIBUTED_TO",
                inference_method="coding_activity",
            ).inc()

        for proj_id, timestamps in worked_on.items():
            if len(timestamps) < 3:
                continue
            confidence = min(0.80, 0.55 + len(timestamps) * 0.04)
            inferred.append({
                "from_id": user_id,
                "to_id": proj_id,
                "rel_type": "WORKS_ON",
                "weight": 1.0 + math.log10(max(1, len(timestamps))),
                "confidence": confidence,
                "inference_method": "focus_activity",
                "inferred": True,
                "session_count": len(timestamps),
            })
            inference_relationships_total.labels(
                relationship_type="WORKS_ON",
                inference_method="focus_activity",
            ).inc()

        return inferred


# Global service instance
relationship_inference_service = RelationshipInferenceService()
