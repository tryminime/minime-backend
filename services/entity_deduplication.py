"""
Entity Deduplication Service — Full Graph-Clustering Implementation (Async).

Signals used:
  1. Embedding cosine similarity   (Qdrant, optional)
  2. External ID exact match       (deterministic, highest confidence)
  3. Levenshtein / SequenceMatcher (fuzzy name similarity)
  4. Token-set ratio               (token overlap after sorting)
  5. Alias cross-match             (intersection of alias sets)

Clustering:
  Union-Find (Disjoint Set Union) groups transitive duplicates so that if
  A ~ B and B ~ C, all three end up in the same cluster even without a
  direct A–C edge.

All DB methods are async and accept an AsyncSession passed from FastAPI endpoints.
"""

from __future__ import annotations

import re
import difflib
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
from uuid import UUID

import structlog
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from models import Entity, ActivityEntityLink

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------
AUTO_MERGE_THRESHOLD = 0.97
SUGGEST_THRESHOLD    = 0.80
MIN_PAIR_SCORE       = 0.75


# ===========================================================================
# Union-Find (Disjoint Set Union) for transitive clustering
# ===========================================================================

class UnionFind:
    """Path-compressed, union-by-rank Disjoint Set Union."""

    def __init__(self) -> None:
        self._parent: Dict[str, str] = {}
        self._rank:   Dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x]   = 0
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self._rank[ra] < self._rank[rb]:
            ra, rb = rb, ra
        self._parent[rb] = ra
        if self._rank[ra] == self._rank[rb]:
            self._rank[ra] += 1

    def clusters(self, all_ids: List[str]) -> Dict[str, List[str]]:
        """Return {root: [members]} for every id."""
        groups: Dict[str, List[str]] = defaultdict(list)
        for eid in all_ids:
            groups[self.find(eid)].append(eid)
        return dict(groups)


# ===========================================================================
# String helpers
# ===========================================================================

def _normalize(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _levenshtein_similarity(a: str, b: str) -> float:
    na, nb = _normalize(a), _normalize(b)
    if not na or not nb:
        return 0.0
    return difflib.SequenceMatcher(None, na, nb).ratio()


def _token_set_similarity(a: str, b: str) -> float:
    ta = set(_normalize(a).split())
    tb = set(_normalize(b).split())
    if not ta or not tb:
        return 0.0
    overlap = len(ta & tb) / max(len(ta), len(tb))
    if ta <= tb or tb <= ta:
        overlap = max(overlap, 0.90)
    return overlap


def _alias_similarity(meta_a: Optional[dict], meta_b: Optional[dict]) -> float:
    def _get_aliases(meta: Optional[dict]) -> set:
        if not meta:
            return set()
        raw = meta.get("aliases") or meta.get("alias") or []
        if isinstance(raw, list):
            return {_normalize(str(r)) for r in raw if r}
        return {_normalize(str(raw))} if raw else set()

    aa, ab = _get_aliases(meta_a), _get_aliases(meta_b)
    if not aa or not ab:
        return 0.0
    return len(aa & ab) / max(len(aa), len(ab))


def _combine_scores(scores: List[Tuple[float, float]]) -> float:
    if not scores:
        return 0.0
    total_w  = sum(w for w, _ in scores)
    total_ws = sum(w * s for w, s in scores)
    weighted_avg = total_ws / total_w if total_w else 0.0
    max_score = max(s for _, s in scores)
    return max(max_score, weighted_avg)


# ===========================================================================
# Pair scoring (pure, no DB needed)
# ===========================================================================

def _score_pair(a: Entity, b: Entity) -> Tuple[float, List[str]]:
    """Compute combined similarity score for two entities."""
    signals: List[Tuple[float, float]] = []
    reasons: List[str] = []

    # 1. External ID exact match
    if a.entity_metadata and b.entity_metadata:
        ea = a.entity_metadata.get("external_ids", {}) or {}
        eb = b.entity_metadata.get("external_ids", {}) or {}
        for k, v in ea.items():
            if k in eb and eb[k] == v:
                signals.append((5.0, 0.99))
                reasons.append("external_id")
                break

    # 2. Fuzzy name (Levenshtein)
    lev = _levenshtein_similarity(a.name or "", b.name or "")
    if lev >= 0.60:
        signals.append((2.0, lev))
        reasons.append("fuzzy_name")

    # 3. Token-set ratio
    tok = _token_set_similarity(a.name or "", b.name or "")
    if tok >= 0.60:
        signals.append((1.5, tok))
        reasons.append("token_set")

    # 4. Alias cross-match
    al = _alias_similarity(a.entity_metadata, b.entity_metadata)
    if al > 0:
        signals.append((2.0, al))
        reasons.append("alias_match")

    if not signals:
        return 0.0, []

    type_bonus = 0.05 if a.entity_type == b.entity_type else 0.0
    score = min(1.0, _combine_scores(signals) + type_bonus)
    return score, list(set(reasons))


# ===========================================================================
# Main service class (async)
# ===========================================================================

class EntityDeduplicationService:
    """
    Full graph-clustering entity deduplication service.
    All public methods are async and accept an AsyncSession from FastAPI.
    """

    AUTO_MERGE_THRESHOLD = AUTO_MERGE_THRESHOLD
    SUGGEST_THRESHOLD    = SUGGEST_THRESHOLD

    # ------------------------------------------------------------------
    # Batch scan — main new feature
    # ------------------------------------------------------------------

    async def scan_all_for_user(self, user_id: UUID, db: AsyncSession) -> Dict:
        """
        Scan ALL entities for a user and return duplicate clusters.

        Uses Union-Find to group transitively related duplicates.
        """
        result = await db.execute(
            select(Entity).where(Entity.user_id == user_id)
        )
        entities: List[Entity] = list(result.scalars().all())

        if not entities:
            return {
                "entities_scanned": 0,
                "duplicate_pairs":  0,
                "clusters":         [],
                "auto_merge_count": 0,
                "stats":            {"by_type": {}},
            }

        uf = UnionFind()
        pair_scores: Dict[Tuple[str, str], Dict] = {}

        ent_map = {str(e.id): e for e in entities}

        # Group by type — only compare same-type pairs (O(n²) per type)
        by_type: Dict[str, List[Entity]] = defaultdict(list)
        for e in entities:
            by_type[e.entity_type or "unknown"].append(e)

        type_counts = {k: len(v) for k, v in by_type.items()}
        total_pairs = 0

        for _etype, group in by_type.items():
            for i, a in enumerate(group):
                for b in group[i + 1:]:
                    score, reasons = _score_pair(a, b)
                    if score >= MIN_PAIR_SCORE:
                        total_pairs += 1
                        key = (str(a.id), str(b.id))
                        pair_scores[key] = {"score": score, "reasons": reasons}
                        uf.union(str(a.id), str(b.id))

        # Build clusters from Union-Find
        all_ids = [str(e.id) for e in entities]
        cluster_map = uf.clusters(all_ids)

        clusters = []
        auto_count = 0

        for root, members in cluster_map.items():
            if len(members) < 2:
                continue

            cluster_scores: List[float] = []
            cluster_reasons: set = set()
            for i, a in enumerate(members):
                for b in members[i + 1:]:
                    ps = pair_scores.get((a, b)) or pair_scores.get((b, a))
                    if ps:
                        cluster_scores.append(ps["score"])
                        cluster_reasons.update(ps["reasons"])

            max_conf = max(cluster_scores) if cluster_scores else MIN_PAIR_SCORE
            avg_conf = sum(cluster_scores) / len(cluster_scores) if cluster_scores else MIN_PAIR_SCORE
            rec = self._recommendation(max_conf)
            if rec == "auto_merge":
                auto_count += 1

            member_dicts = []
            for mid in members:
                ent = ent_map.get(mid)
                if ent:
                    member_dicts.append({
                        "id":               str(ent.id),
                        "name":             ent.name or "",
                        "entity_type":      ent.entity_type or "",
                        "occurrence_count": ent.occurrence_count or 0,
                        "first_seen":       ent.first_seen.isoformat() if ent.first_seen else None,
                        "last_seen":        ent.last_seen.isoformat()  if ent.last_seen  else None,
                        "confidence":       ent.confidence,
                        "metadata":         ent.entity_metadata,
                    })

            canonical = max(member_dicts, key=lambda m: m["occurrence_count"])

            clusters.append({
                "cluster_id":     root,
                "members":        member_dicts,
                "canonical_id":   canonical["id"],
                "canonical_name": canonical["name"],
                "entity_type":    member_dicts[0]["entity_type"],
                "max_confidence": round(max_conf, 4),
                "avg_confidence": round(avg_conf, 4),
                "match_reasons":  sorted(cluster_reasons),
                "recommendation": rec,
                "size":           len(members),
            })

        clusters.sort(key=lambda c: (-int(c["recommendation"] == "auto_merge"), -c["max_confidence"]))

        return {
            "entities_scanned": len(entities),
            "duplicate_pairs":  total_pairs,
            "clusters":         clusters,
            "auto_merge_count": auto_count,
            "stats":            {"by_type": type_counts},
        }

    # ------------------------------------------------------------------
    # N-way cluster merge
    # ------------------------------------------------------------------

    async def merge_cluster(
        self,
        entity_ids: List[UUID],
        canonical_id: Optional[UUID],
        user_id: UUID,
        db: AsyncSession,
    ) -> Optional[Dict]:
        """Merge a cluster of N entities into one canonical entity."""
        if len(entity_ids) < 2:
            return None

        result = await db.execute(
            select(Entity).where(
                Entity.id.in_(entity_ids),
                Entity.user_id == user_id,
            )
        )
        ents = {str(e.id): e for e in result.scalars().all()}

        if not ents:
            return None

        # Pick canonical — highest occurrence_count, or user-specified
        if canonical_id and str(canonical_id) in ents:
            target_id = canonical_id
        else:
            best = max(ents.values(), key=lambda e: e.occurrence_count or 0)
            target_id = best.id

        sources = [UUID(str(eid)) for eid in ents if str(eid) != str(target_id)]

        for src_id in sources:
            await self.merge_entities(src_id, target_id, user_id, db)

        # Re-fetch target (fresh state after merges)
        tgt_result = await db.execute(
            select(Entity).where(Entity.id == target_id)
        )
        tgt = tgt_result.scalar_one_or_none()
        return tgt.to_dict() if tgt else None

    # ------------------------------------------------------------------
    # 2-way merge
    # ------------------------------------------------------------------

    async def merge_entities(
        self,
        source_id: UUID,
        target_id: UUID,
        user_id: UUID,
        db: AsyncSession,
    ) -> Optional[Entity]:
        """Merge source into target. Redirects activity links, carries aliases/ext-IDs."""
        src_result = await db.execute(
            select(Entity).where(Entity.id == source_id, Entity.user_id == user_id)
        )
        source = src_result.scalar_one_or_none()

        tgt_result = await db.execute(
            select(Entity).where(Entity.id == target_id, Entity.user_id == user_id)
        )
        target = tgt_result.scalar_one_or_none()

        if not source or not target:
            logger.error("Entity not found for merge",
                         source_id=str(source_id), target_id=str(target_id))
            return None

        # Redirect activity links
        links_result = await db.execute(
            select(ActivityEntityLink).where(ActivityEntityLink.entity_id == source_id)
        )
        for link in links_result.scalars().all():
            link.entity_id = target_id

        # Merge metadata
        t_meta = dict(target.entity_metadata or {})
        s_meta = dict(source.entity_metadata or {})

        t_ext = dict(t_meta.get("external_ids") or {})
        t_ext.update(s_meta.get("external_ids") or {})
        t_meta["external_ids"] = t_ext

        t_aliases = set(t_meta.get("aliases") or [])
        t_aliases.update(s_meta.get("aliases") or [])
        t_aliases.add(source.name or "")
        t_meta["aliases"] = sorted(filter(None, t_aliases))

        target.entity_metadata = t_meta
        target.occurrence_count = (target.occurrence_count or 1) + (source.occurrence_count or 1)

        await db.delete(source)
        await db.flush()

        logger.info("Entities merged",
                    source=str(source_id), target=str(target_id),
                    new_count=target.occurrence_count)

        # Try to delete from Qdrant (non-fatal)
        try:
            from services.qdrant_entity_service import qdrant_entity_service
            qdrant_entity_service.delete_entity(source_id)
        except Exception as qe:
            logger.warning("Qdrant delete failed (non-fatal)", error=str(qe))

        return target

    # ------------------------------------------------------------------
    # Per-entity duplicate detection (sync text-signal only, no DB)
    # ------------------------------------------------------------------

    def find_candidates_for_entity(
        self, entity: Entity, all_entities: List[Entity], limit: int = 20
    ) -> List[Dict]:
        """
        Find duplicates for a single entity against a pre-fetched list.
        Pure computation — no DB calls.
        """
        candidates = []
        for other in all_entities:
            if str(other.id) == str(entity.id):
                continue
            score, reasons = _score_pair(entity, other)
            if score < MIN_PAIR_SCORE:
                continue
            candidates.append({
                "entity_id":    str(other.id),
                "name":         other.name or "",
                "entity_type":  other.entity_type or "",
                "confidence":   round(score, 4),
                "match_reasons": reasons,
                "recommendation": self._recommendation(score),
            })

        candidates.sort(key=lambda c: -c["confidence"])
        return candidates[:limit]

    def should_auto_merge(self, confidence: float) -> bool:
        return confidence >= AUTO_MERGE_THRESHOLD

    def _recommendation(self, confidence: float) -> str:
        if confidence >= AUTO_MERGE_THRESHOLD:
            return "auto_merge"
        if confidence >= SUGGEST_THRESHOLD:
            return "suggest"
        return "review"


# Global singleton
deduplication_service = EntityDeduplicationService()
