"""
Graph Intelligence Service — Postgres-Based Knowledge Graph Analytics.

5 features:
  1. Expertise discovery      — skill profiling from entity data
  2. Learning path recs       — gap analysis + suggested learning order
  3. Collaboration patterns   — PERSON entity co-occurrence analysis
  4. Cross-domain connections — bridge entities spanning multiple categories
  5. PageRank scoring         — iterative PageRank on entity co-occurrence graph

All methods are async and accept an AsyncSession from FastAPI endpoints.
No Neo4j dependency — everything uses Postgres Entity + ActivityEntityLink.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Dict, List, Optional, Any, Set, Tuple
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, text

from models import Entity, ActivityEntityLink, Activity
from services.expertise_discovery import expertise_discovery

logger = structlog.get_logger()


# ============================================================================
# Helper — build entity dicts for expertise_discovery consumption
# ============================================================================

def _entity_to_profile_dict(e: Entity) -> Dict[str, Any]:
    """Convert an ORM Entity to the dict shape expertise_discovery expects."""
    meta = e.entity_metadata or {}
    # Map our entity_type to the LABEL format expertise_discovery uses
    TYPE_MAP = {
        "skill": "SKILL",
        "artifact": "TOOL",
        "concept": "SKILL",
        "organization": "ORG",
        "person": "PERSON",
        "project": "PROJECT",
        "event": "EVENT",
        "interaction": "INTERACTION",
    }
    return {
        "text": e.name or "",
        "canonical_name": e.name or "",
        "type": TYPE_MAP.get(e.entity_type or "", "OTHER"),
        "label": TYPE_MAP.get(e.entity_type or "", "OTHER"),
        "frequency": e.occurrence_count or 1,
        "confidence": e.confidence or 0.5,
        "first_seen": e.first_seen.isoformat() if e.first_seen else "",
        "last_seen": e.last_seen.isoformat() if e.last_seen else "",
    }


# ============================================================================
# PageRank — pure iterative implementation
# ============================================================================

def _pagerank(
    adjacency: Dict[str, Dict[str, float]],
    damping: float = 0.85,
    iterations: int = 20,
) -> Dict[str, float]:
    """
    Iterative PageRank on a weighted adjacency dict.
    adjacency[a][b] = weight means an edge a→b with that weight.
    Returns {node_id: score} normalized to sum=1.
    """
    nodes = list(adjacency.keys())
    n = len(nodes)
    if n == 0:
        return {}

    # Initialize uniform
    scores: Dict[str, float] = {node: 1.0 / n for node in nodes}

    # Precompute out-weight sums
    out_weight: Dict[str, float] = {}
    for node in nodes:
        out_weight[node] = sum(adjacency[node].values()) if adjacency[node] else 0.0

    for _ in range(iterations):
        new_scores: Dict[str, float] = {}
        for node in nodes:
            rank_sum = 0.0
            # Sum contributions from all nodes that link TO this node
            for src in nodes:
                if node in adjacency.get(src, {}):
                    w = adjacency[src][node]
                    if out_weight[src] > 0:
                        rank_sum += scores[src] * (w / out_weight[src])
            new_scores[node] = (1.0 - damping) / n + damping * rank_sum
        scores = new_scores

    # Normalize
    total = sum(scores.values())
    if total > 0:
        scores = {k: v / total for k, v in scores.items()}

    return scores


# ============================================================================
# Main Service
# ============================================================================

class GraphIntelligenceService:
    """Postgres-based knowledge graph intelligence."""

    # ------------------------------------------------------------------
    # 1. Expertise Discovery
    # ------------------------------------------------------------------

    async def get_expertise_profile(
        self, user_id: UUID, db: AsyncSession
    ) -> Dict[str, Any]:
        """Build full expertise profile from entity data."""
        result = await db.execute(
            select(Entity).where(Entity.user_id == user_id)
        )
        entities = list(result.scalars().all())

        if not entities:
            return {
                "skills": {},
                "top_skills": [],
                "categories": {},
                "primary_category": "unknown",
                "total_skills": 0,
                "skill_diversity": 0,
                "rankings": [],
                "entity_count": 0,
            }

        profile_dicts = [_entity_to_profile_dict(e) for e in entities]

        # Build profile + rankings using existing service
        profile = expertise_discovery.build_skill_profile(profile_dicts)
        rankings = expertise_discovery.rank_expertise(profile_dicts)
        timeline = expertise_discovery.build_expertise_timeline(profile_dicts)

        return {
            **profile,
            "rankings": rankings[:20],
            "timeline": timeline,
            "entity_count": len(entities),
        }

    # ------------------------------------------------------------------
    # 2. Learning Path Recommendations
    # ------------------------------------------------------------------

    async def get_learning_paths(
        self, user_id: UUID, db: AsyncSession
    ) -> Dict[str, Any]:
        """Generate learning path recommendations from skill gaps."""
        result = await db.execute(
            select(Entity).where(Entity.user_id == user_id)
        )
        entities = list(result.scalars().all())
        profile_dicts = [_entity_to_profile_dict(e) for e in entities]

        # Build current profile
        profile = expertise_discovery.build_skill_profile(profile_dicts)
        current_skills = profile.get("skills", {})
        categories = profile.get("categories", {})

        # Find weak categories and suggest learning paths
        paths: List[Dict[str, Any]] = []

        # Identify the taxonomy categories the user is weakest in
        all_cats = expertise_discovery.get_all_categories()
        for cat in all_cats:
            cat_score = categories.get(cat, 0)
            cat_skills = expertise_discovery.skill_taxonomy.get(cat, [])

            # Find skills in this category NOT in user's skillset
            user_skill_names = set(current_skills.keys())
            missing = [s for s in cat_skills if s.lower() not in user_skill_names]
            known = [s for s in cat_skills if s.lower() in user_skill_names]
            known_scores = {
                s: current_skills.get(s.lower(), {}).get("score", 0)
                for s in known
            }

            # Only suggest if user has SOME presence (not totally unrelated)
            if not known and cat_score == 0:
                continue

            # Find weak skills (known but low score)
            weak = [
                s for s, score in known_scores.items() if score < 40
            ]

            if not missing and not weak:
                continue

            # Priority: high if they have skills in the domain but gaps remain
            priority = "high" if cat_score > 30 else "medium" if cat_score > 0 else "low"

            # Build ordered learning steps
            steps: List[Dict[str, str]] = []
            for s in weak[:3]:
                steps.append({
                    "skill": s,
                    "action": "deepen",
                    "reason": f"Current proficiency: {known_scores.get(s, 0):.0f}/100",
                })
            for s in missing[:3]:
                steps.append({
                    "skill": s,
                    "action": "learn",
                    "reason": f"Missing from your {cat.replace('_', ' ')} skillset",
                })

            if steps:
                paths.append({
                    "category": cat,
                    "category_label": cat.replace("_", " ").title(),
                    "current_score": cat_score,
                    "priority": priority,
                    "known_count": len(known),
                    "missing_count": len(missing),
                    "weak_count": len(weak),
                    "steps": steps,
                    "estimated_effort": f"{len(steps) * 2}-{len(steps) * 5} hours",
                })

        # Sort: high → medium → low, then highest existing score first
        prio_order = {"high": 0, "medium": 1, "low": 2}
        paths.sort(key=lambda p: (prio_order.get(p["priority"], 3), -p["current_score"]))

        return {
            "paths": paths[:8],
            "total_paths": len(paths),
            "current_skill_count": len(current_skills),
            "primary_category": profile.get("primary_category", "unknown"),
        }

    # ------------------------------------------------------------------
    # 3. Collaboration Patterns
    # ------------------------------------------------------------------

    async def get_collaboration_patterns(
        self, user_id: UUID, db: AsyncSession
    ) -> Dict[str, Any]:
        """
        Analyze collaboration patterns from entity co-occurrence.
        Two PERSON entities linked to the same Activity = collaboration.
        """
        # Get all PERSON entities for this user
        person_result = await db.execute(
            select(Entity).where(
                Entity.user_id == user_id,
                Entity.entity_type == "person",
            )
        )
        persons = {str(e.id): e for e in person_result.scalars().all()}

        if len(persons) < 2:
            return {
                "collaborators": [],
                "total_persons": len(persons),
                "collaboration_score": 0,
                "patterns": [],
            }

        # Find co-occurrences: activities that link ≥2 PERSON entities
        # Using raw SQL for efficiency: self-join on activity_entity_links
        person_ids = list(persons.keys())

        co_occ_query = text("""
            SELECT
                a.entity_id AS entity_a,
                b.entity_id AS entity_b,
                COUNT(DISTINCT a.activity_id) AS shared_count
            FROM activity_entity_links a
            JOIN activity_entity_links b
                ON a.activity_id = b.activity_id
                AND a.entity_id < b.entity_id
            WHERE a.entity_id = ANY(:person_ids)
              AND b.entity_id = ANY(:person_ids)
            GROUP BY a.entity_id, b.entity_id
            ORDER BY shared_count DESC
            LIMIT 50
        """)

        co_result = await db.execute(
            co_occ_query,
            {"person_ids": person_ids},
        )
        rows = co_result.fetchall()

        collaborators: List[Dict[str, Any]] = []
        max_shared = max((r[2] for r in rows), default=1)

        for row in rows:
            eid_a, eid_b, shared = str(row[0]), str(row[1]), int(row[2])
            ea = persons.get(eid_a)
            eb = persons.get(eid_b)
            if not ea or not eb:
                continue

            strength = round(shared / max(max_shared, 1), 4)
            collaborators.append({
                "entity_a": {"id": eid_a, "name": ea.name or ""},
                "entity_b": {"id": eid_b, "name": eb.name or ""},
                "shared_activities": shared,
                "strength": strength,
            })

        # Collaboration score: 0-10 scale
        total_pairs = len(collaborators)
        total_shared = sum(c["shared_activities"] for c in collaborators)
        collab_score = min(10, round(
            (min(total_pairs, 10) / 10 * 4) +
            (min(total_shared, 50) / 50 * 6),
            1
        ))

        # Detect patterns
        patterns: List[str] = []
        if total_pairs > 5:
            patterns.append("Broad collaboration network")
        if total_pairs > 0 and collaborators[0]["shared_activities"] > 10:
            patterns.append(f"Strong partnership with {collaborators[0]['entity_a']['name']} & {collaborators[0]['entity_b']['name']}")
        if len(persons) > 5 and total_pairs < 3:
            patterns.append("Many contacts but few strong collaborations")

        return {
            "collaborators": collaborators[:20],
            "total_persons": len(persons),
            "total_pairs": total_pairs,
            "collaboration_score": collab_score,
            "patterns": patterns,
        }

    # ------------------------------------------------------------------
    # 4. Cross-Domain Connections
    # ------------------------------------------------------------------

    async def get_cross_domain_connections(
        self, user_id: UUID, db: AsyncSession
    ) -> Dict[str, Any]:
        """Find entities that bridge multiple skill categories."""
        result = await db.execute(
            select(Entity).where(Entity.user_id == user_id)
        )
        entities = list(result.scalars().all())
        profile_dicts = [_entity_to_profile_dict(e) for e in entities]

        # Use existing service
        bridges = expertise_discovery.detect_cross_domain_bridges(profile_dicts)

        # Enrich with co-occurrence data: entities from different categories
        # that appear in the same activity
        cross_links: List[Dict[str, Any]] = []

        # Build entity_type → entity map
        type_groups: Dict[str, List[Entity]] = defaultdict(list)
        for e in entities:
            type_groups[e.entity_type or "unknown"].append(e)

        # If ≥2 types have entities, find cross-type co-occurrences
        if len(type_groups) >= 2:
            types = sorted(type_groups.keys())
            for i, t1 in enumerate(types):
                for t2 in types[i + 1:]:
                    ids_1 = [str(e.id) for e in type_groups[t1][:20]]
                    ids_2 = [str(e.id) for e in type_groups[t2][:20]]
                    if not ids_1 or not ids_2:
                        continue

                    cross_query = text("""
                        SELECT
                            a.entity_id AS eid_a,
                            b.entity_id AS eid_b,
                            COUNT(DISTINCT a.activity_id) AS cnt
                        FROM activity_entity_links a
                        JOIN activity_entity_links b
                            ON a.activity_id = b.activity_id
                        WHERE a.entity_id = ANY(:ids1)
                          AND b.entity_id = ANY(:ids2)
                        GROUP BY a.entity_id, b.entity_id
                        HAVING COUNT(DISTINCT a.activity_id) >= 2
                        ORDER BY cnt DESC
                        LIMIT 10
                    """)
                    cr = await db.execute(cross_query, {"ids1": ids_1, "ids2": ids_2})
                    for row in cr.fetchall():
                        eid_a_str, eid_b_str = str(row[0]), str(row[1])
                        ent_map = {str(e.id): e for e in entities}
                        ea = ent_map.get(eid_a_str)
                        eb = ent_map.get(eid_b_str)
                        if ea and eb:
                            cross_links.append({
                                "entity_a": {"id": eid_a_str, "name": ea.name, "type": ea.entity_type},
                                "entity_b": {"id": eid_b_str, "name": eb.name, "type": eb.entity_type},
                                "co_occurrences": int(row[2]),
                            })

        return {
            **bridges,
            "cross_type_connections": cross_links[:15],
        }

    # ------------------------------------------------------------------
    # 5. PageRank Scoring
    # ------------------------------------------------------------------

    async def compute_pagerank(
        self, user_id: UUID, db: AsyncSession
    ) -> Dict[str, Any]:
        """
        Compute PageRank for user entities using co-occurrence graph.
        Edge weight = number of shared activities between two entities.
        """
        # Get all entities
        ent_result = await db.execute(
            select(Entity).where(Entity.user_id == user_id)
        )
        entities = list(ent_result.scalars().all())
        ent_map = {str(e.id): e for e in entities}

        if not entities:
            return {"rankings": [], "total": 0, "stats": {}}

        entity_ids = list(ent_map.keys())

        # Build adjacency from co-occurrences
        co_query = text("""
            SELECT
                a.entity_id AS eid_a,
                b.entity_id AS eid_b,
                COUNT(DISTINCT a.activity_id) AS cnt
            FROM activity_entity_links a
            JOIN activity_entity_links b
                ON a.activity_id = b.activity_id
                AND a.entity_id < b.entity_id
            WHERE a.entity_id = ANY(:eids)
              AND b.entity_id = ANY(:eids)
            GROUP BY a.entity_id, b.entity_id
        """)
        co_result = await db.execute(co_query, {"eids": entity_ids})

        # Build symmetric adjacency
        adjacency: Dict[str, Dict[str, float]] = defaultdict(dict)
        for eid in entity_ids:
            adjacency[eid] = {}  # ensure all nodes are present

        for row in co_result.fetchall():
            a, b, w = str(row[0]), str(row[1]), float(row[2])
            adjacency[a][b] = w
            adjacency[b][a] = w

        # Run PageRank
        scores = _pagerank(dict(adjacency), damping=0.85, iterations=20)

        # Build ranked list
        rankings: List[Dict[str, Any]] = []
        for eid, score in scores.items():
            e = ent_map.get(eid)
            if not e:
                continue
            rankings.append({
                "id": eid,
                "name": e.name or "",
                "entity_type": e.entity_type or "",
                "pagerank": round(score, 6),
                "occurrence_count": e.occurrence_count or 0,
                "connections": len(adjacency.get(eid, {})),
            })

        rankings.sort(key=lambda r: -r["pagerank"])

        # Stats
        pr_values = [r["pagerank"] for r in rankings]
        max_pr = max(pr_values) if pr_values else 0
        avg_pr = sum(pr_values) / len(pr_values) if pr_values else 0

        return {
            "rankings": rankings[:50],
            "total": len(rankings),
            "stats": {
                "max_pagerank": round(max_pr, 6),
                "avg_pagerank": round(avg_pr, 6),
                "total_entities": len(entities),
                "connected_entities": sum(1 for eid in entity_ids if adjacency.get(eid)),
            },
        }

    # ------------------------------------------------------------------
    # 6. Graph-based Recommendations
    # ------------------------------------------------------------------

    async def get_recommendations(
        self, user_id: UUID, db: AsyncSession, limit: int = 15
    ) -> Dict[str, Any]:
        """
        Generate actionable recommendations by combining:
        - PageRank top entities → "Trending Topic"
        - Learning path gaps → "Deepen Expertise" / "Bridge Gap"
        - Cross-domain bridges → "Explore Connection"
        """
        recommendations: List[Dict[str, Any]] = []

        # 1. Get PageRank scores for trending topics
        try:
            pr_data = await self.compute_pagerank(user_id, db)
            top_entities = pr_data.get("rankings", [])[:5]
            for rank, entity in enumerate(top_entities):
                recommendations.append({
                    "id": f"trend_{entity['id'][:8]}",
                    "category": "trending_topic",
                    "icon": "🔥",
                    "title": f"Trending: {entity['name']}",
                    "description": f"High-impact entity with PageRank {entity['pagerank']:.4f} and {entity['connections']} connections. Consider deepening your engagement.",
                    "entity_name": entity["name"],
                    "entity_type": entity["entity_type"],
                    "score": round(entity["pagerank"] * 1000, 2),
                    "priority": "high" if rank < 2 else "medium",
                })
        except Exception as e:
            logger.warning("pagerank_recommendations_failed", error=str(e))

        # 2. Get learning path gaps
        try:
            learning = await self.get_learning_paths(user_id, db)
            for path in learning.get("paths", [])[:4]:
                for step in path.get("steps", [])[:2]:
                    action = step.get("action", "learn")
                    cat = "deepen_expertise" if action == "deepen" else "bridge_gap"
                    icon = "📈" if action == "deepen" else "🌉"
                    recommendations.append({
                        "id": f"learn_{step['skill'][:8].lower().replace(' ', '_')}",
                        "category": cat,
                        "icon": icon,
                        "title": f"{'Deepen' if action == 'deepen' else 'Learn'}: {step['skill']}",
                        "description": f"{step.get('reason', '')}. Category: {path['category_label']}.",
                        "entity_name": step["skill"],
                        "entity_type": "skill",
                        "score": path.get("current_score", 0),
                        "priority": path.get("priority", "medium"),
                    })
        except Exception as e:
            logger.warning("learning_recommendations_failed", error=str(e))

        # 3. Get cross-domain connections
        try:
            cross = await self.get_cross_domain_connections(user_id, db)
            for conn in cross.get("cross_type_connections", [])[:4]:
                ea = conn["entity_a"]
                eb = conn["entity_b"]
                recommendations.append({
                    "id": f"cross_{ea['id'][:4]}_{eb['id'][:4]}",
                    "category": "explore_connection",
                    "icon": "🔗",
                    "title": f"Explore: {ea['name']} ↔ {eb['name']}",
                    "description": f"Cross-domain link between {ea.get('type', 'unknown')} and {eb.get('type', 'unknown')} ({conn['co_occurrences']} shared activities). Unexplored synergy.",
                    "entity_name": f"{ea['name']} ↔ {eb['name']}",
                    "entity_type": "connection",
                    "score": conn["co_occurrences"],
                    "priority": "medium" if conn["co_occurrences"] >= 3 else "low",
                })
        except Exception as e:
            logger.warning("cross_domain_recommendations_failed", error=str(e))

        # Sort by priority then score
        prio_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda r: (prio_order.get(r.get("priority", "low"), 3), -r.get("score", 0)))

        return {
            "recommendations": recommendations[:limit],
            "total": len(recommendations),
            "categories": {
                "trending_topic": sum(1 for r in recommendations if r["category"] == "trending_topic"),
                "deepen_expertise": sum(1 for r in recommendations if r["category"] == "deepen_expertise"),
                "bridge_gap": sum(1 for r in recommendations if r["category"] == "bridge_gap"),
                "explore_connection": sum(1 for r in recommendations if r["category"] == "explore_connection"),
            },
        }

    # ------------------------------------------------------------------
    # 7. Community Detection (Label Propagation)
    # ------------------------------------------------------------------

    async def detect_communities(
        self, user_id: UUID, db: AsyncSession, max_iterations: int = 50
    ) -> Dict[str, Any]:
        """
        Detect communities in the entity co-occurrence graph using
        Label Propagation Algorithm.

        Each entity starts with a unique label; iteratively adopts the
        majority label of its neighbours until convergence.
        """
        import random

        # 1. Build adjacency list from co-occurrence
        entities_result = await db.execute(
            select(Entity.id, Entity.name, Entity.entity_type)
            .where(Entity.user_id == user_id)
        )
        entity_rows = entities_result.all()
        if len(entity_rows) < 3:
            return {"communities": [], "total_communities": 0, "modularity_score": 0}

        id_to_info: Dict[str, Dict[str, str]] = {}
        for eid, name, etype in entity_rows:
            id_to_info[str(eid)] = {"name": name, "type": etype}

        entity_ids = list(id_to_info.keys())

        # Get co-occurrence pairs
        links_result = await db.execute(
            select(
                ActivityEntityLink.activity_id,
                ActivityEntityLink.entity_id,
            )
            .where(ActivityEntityLink.entity_id.in_(
                [e[0] for e in entity_rows]
            ))
        )
        link_rows = links_result.all()

        # Group by activity → find entity pairs that share activities
        activity_entities: Dict[str, List[str]] = defaultdict(list)
        for aid, eid in link_rows:
            activity_entities[str(aid)].append(str(eid))

        # Build adjacency with edge weights
        adjacency: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        for act_id, ents in activity_entities.items():
            for i in range(len(ents)):
                for j in range(i + 1, len(ents)):
                    adjacency[ents[i]][ents[j]] += 1
                    adjacency[ents[j]][ents[i]] += 1

        # Only keep entities that have at least one edge
        connected_ids = [eid for eid in entity_ids if eid in adjacency]
        if len(connected_ids) < 3:
            return {"communities": [], "total_communities": 0, "modularity_score": 0}

        # 2. Label Propagation
        labels: Dict[str, int] = {eid: i for i, eid in enumerate(connected_ids)}

        for iteration in range(max_iterations):
            changed = False
            order = list(connected_ids)
            random.shuffle(order)

            for node in order:
                neighbors = adjacency.get(node, {})
                if not neighbors:
                    continue

                # Count weighted label votes from neighbours
                label_votes: Dict[int, int] = defaultdict(int)
                for nb, weight in neighbors.items():
                    if nb in labels:
                        label_votes[labels[nb]] += weight

                if label_votes:
                    max_votes = max(label_votes.values())
                    best_labels = [l for l, v in label_votes.items() if v == max_votes]
                    new_label = min(best_labels)  # deterministic tie-break
                    if labels[node] != new_label:
                        labels[node] = new_label
                        changed = True

            if not changed:
                break

        # 3. Group into communities
        community_map: Dict[int, List[str]] = defaultdict(list)
        for eid, label in labels.items():
            community_map[label].append(eid)

        # Total edges for modularity
        total_edges = sum(
            sum(w for w in neighbors.values())
            for neighbors in adjacency.values()
        ) // 2

        communities = []
        for cid, (label, members) in enumerate(
            sorted(community_map.items(), key=lambda x: -len(x[1]))
        ):
            if len(members) < 2:
                continue

            entities_in_community = []
            type_counts: Dict[str, int] = defaultdict(int)

            for eid in members:
                info = id_to_info.get(eid, {"name": "?", "type": "unknown"})
                entities_in_community.append({
                    "id": eid,
                    "name": info["name"],
                    "entity_type": info["type"],
                })
                type_counts[info["type"]] += 1

            # Internal density = actual internal edges / possible edges
            internal_edges = 0
            for i, m1 in enumerate(members):
                for m2 in members[i + 1:]:
                    internal_edges += adjacency.get(m1, {}).get(m2, 0)

            possible = len(members) * (len(members) - 1) // 2
            density = round(internal_edges / possible, 3) if possible > 0 else 0

            dominant_type = max(type_counts, key=type_counts.get) if type_counts else "mixed"

            communities.append({
                "id": cid,
                "label": f"{dominant_type.title()} Community ({len(members)} entities)",
                "size": len(members),
                "density": density,
                "dominant_type": dominant_type,
                "type_breakdown": dict(type_counts),
                "top_entity": entities_in_community[0]["name"] if entities_in_community else "",
                "entities": entities_in_community[:20],
            })

        # Simple modularity estimate
        modularity = 0.0
        if total_edges > 0:
            for label, members in community_map.items():
                internal = 0
                degree_sum = 0
                for m in members:
                    for m2 in members:
                        if m != m2:
                            internal += adjacency.get(m, {}).get(m2, 0)
                    degree_sum += sum(adjacency.get(m, {}).values())
                modularity += (internal / (2 * total_edges)) - (degree_sum / (2 * total_edges)) ** 2

        return {
            "communities": communities,
            "total_communities": len(communities),
            "total_connected_entities": len(connected_ids),
            "modularity_score": round(modularity, 4),
            "iterations_used": iteration + 1 if 'iteration' in dir() else 0,
        }


# Global singleton
graph_intelligence_service = GraphIntelligenceService()
