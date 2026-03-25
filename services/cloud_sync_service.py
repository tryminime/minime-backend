"""
CloudSyncService — Pushes local DB changes to cloud counterparts.

Local-first design: this service only PUSHES data upstream.  The app always
reads from and writes to local databases.  If the cloud is unreachable the
sync simply fails and retries on the next cycle.

Sync strategy per target:
  PostgreSQL → Supabase : incremental via ``synced_at`` watermark
  Redis → Upstash       : mirror selected key patterns
  Neo4j → AuraDB        : full user-scoped subgraph merge
  Qdrant → Qdrant Cloud : scroll local → batch upsert cloud
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from config import settings
from database.cloud_db_clients import (
    CloudSyncNotConfigured,
    get_cloud_pg_engine,
    get_cloud_redis,
    get_cloud_neo4j,
    get_cloud_qdrant,
)
from database.postgres import async_session_factory
from models import SyncHistory

logger = structlog.get_logger()

# Tables to sync to Supabase (order matters for FK constraints)
# Users MUST come first since other tables have FK references to users.id
PG_SYNC_TABLES = [
    "users",
    "activities",
    "entities",
    "activity_entity_links",
    "user_goals",
    "content_items",
    "daily_metrics",
    "daily_summaries",
    "weekly_reports",
    "integrations",
]


# ── Dataclass-style result ────────────────────────────────────────────────────

class SyncResult:
    def __init__(self):
        self.targets: Dict[str, Dict[str, Any]] = {}
        self.total_records = 0
        self.errors: List[str] = []

    @property
    def ok(self) -> bool:
        return len(self.errors) == 0

    def to_dict(self) -> dict:
        return {
            "targets": self.targets,
            "total_records": self.total_records,
            "errors": self.errors,
            "ok": self.ok,
        }


# ── Main service ──────────────────────────────────────────────────────────────

class CloudSyncService:
    """Orchestrates local→cloud sync for a single user."""

    async def sync_all(
        self,
        user_id: str,
        trigger: str = "manual",
    ) -> SyncResult:
        """Run all 4 syncs concurrently.  Record result in sync_history."""
        result = SyncResult()
        history_id = uuid.uuid4()
        started_at = datetime.now(timezone.utc)

        # Clean up stale "running" entries (older than 10 minutes)
        async with async_session_factory() as session:
            await session.execute(
                text("""
                    UPDATE sync_history
                    SET status = 'failed',
                        completed_at = NOW(),
                        error = 'Sync did not complete within timeout'
                    WHERE status = 'running'
                      AND started_at < NOW() - INTERVAL '10 minutes'
                """),
            )
            await session.commit()

        # Record sync start
        async with async_session_factory() as session:
            session.add(SyncHistory(
                id=history_id,
                user_id=uuid.UUID(user_id),
                started_at=started_at,
                status="running",
                trigger=trigger,
            ))
            await session.commit()

        # Run syncs in parallel — each returns (target_name, result_dict | error)
        tasks = [
            self._safe_sync("postgresql", self.sync_postgres, user_id, history_id=history_id),
            self._safe_sync("redis", self.sync_redis, user_id),
            self._safe_sync("neo4j", self.sync_neo4j, user_id),
            self._safe_sync("qdrant", self.sync_qdrant, user_id),
        ]
        outcomes = await asyncio.gather(*tasks)

        # Non-critical targets: their failure won't mark sync as "failed"
        # (Remove targets from this set once they're expected to be always-on)
        non_critical_targets: set = set()

        for name, outcome in outcomes:
            if isinstance(outcome, dict):
                result.targets[name] = outcome
                result.total_records += outcome.get("records", 0)
            else:
                result.targets[name] = {"error": str(outcome), "records": 0}
                if name not in non_critical_targets:
                    result.errors.append(f"{name}: {outcome}")
                else:
                    logger.warning("non_critical_sync_skip", target=name, error=str(outcome))

        # Update history record
        completed_at = datetime.now(timezone.utc)
        try:
            async with async_session_factory() as session:
                await session.execute(
                    text("""
                        UPDATE sync_history
                        SET completed_at = :completed,
                            status = :status,
                            results = CAST(:results AS json),
                            records_synced = :records,
                            error = :error
                        WHERE id = :hid
                    """),
                    {
                        "completed": completed_at,
                        "status": "completed" if result.ok else "failed",
                        "results": json.dumps(result.to_dict()["targets"]),
                        "records": result.total_records,
                        "error": "; ".join(result.errors) if result.errors else None,
                        "hid": history_id,
                    },
                )
                await session.commit()
            logger.info("sync_history_updated", history_id=str(history_id), status="completed" if result.ok else "failed")
        except Exception as e:
            logger.error("sync_history_update_failed", error=str(e), history_id=str(history_id))

        # Update user preferences with last_synced_at
        try:
            async with async_session_factory() as session:
                await session.execute(
                    text("""
                        UPDATE users
                        SET preferences = jsonb_set(
                            COALESCE(preferences, '{}'::jsonb),
                            '{last_synced_at}',
                            to_jsonb(CAST(:ts AS text))
                        )
                        WHERE id = :uid
                    """),
                    {"ts": completed_at.isoformat(), "uid": uuid.UUID(user_id)},
                )
                await session.commit()
        except Exception as e:
            logger.error("sync_prefs_update_failed", error=str(e))

        logger.info(
            "cloud_sync_complete",
            user_id=user_id,
            trigger=trigger,
            total_records=result.total_records,
            ok=result.ok,
            duration_s=round((completed_at - started_at).total_seconds(), 2),
        )
        return result

    # ── Per-target sync methods ───────────────────────────────────────────────

    async def sync_postgres(self, user_id: str, history_id=None) -> dict:
        """Incremental sync from local PG to Supabase PG.

        Loops through ALL unsynced records in batches of 1000 per table.
        Each batch is inserted via a single transaction for speed.
        One call fully syncs everything.
        """
        BATCH_SIZE = 1000
        engine = await get_cloud_pg_engine()
        total = 0

        from sqlalchemy.ext.asyncio import AsyncSession as CloudSession
        from sqlalchemy.ext.asyncio import async_sessionmaker
        cloud_sf = async_sessionmaker(engine, class_=CloudSession, expire_on_commit=False)

        async with async_session_factory() as local_session:
            for table in PG_SYNC_TABLES:
                table_total = 0
                batch_num = 0

                # Ensure cloud table exists (once per table)
                await self._ensure_cloud_table(engine, table, local_session)

                # Get cloud columns (once per table)
                async with cloud_sf() as cloud_session:
                    cloud_cols_result = await cloud_session.execute(
                        text("""
                            SELECT column_name FROM information_schema.columns
                            WHERE table_name = :t AND table_schema = 'public'
                        """),
                        {"t": table},
                    )
                    cloud_columns = {r[0] for r in cloud_cols_result.fetchall()}

                if not cloud_columns:
                    logger.warning("cloud_pg_no_columns", table=table)
                    continue

                # Loop: fetch batch → upsert → mark synced → repeat
                while True:
                    batch_num += 1

                    # Fetch next batch of unsynced rows
                    if table == "users":
                        rows = (await local_session.execute(
                            text("SELECT * FROM users WHERE id = :uid"),
                            {"uid": user_id},
                        )).mappings().all()
                    elif table == "activity_entity_links":
                        rows = (await local_session.execute(
                            text(f"""
                                SELECT ael.* FROM activity_entity_links ael
                                JOIN activities a ON a.id = ael.activity_id
                                WHERE a.user_id = :uid
                                  AND ael.synced_at IS NULL
                                LIMIT {BATCH_SIZE}
                            """),
                            {"uid": user_id},
                        )).mappings().all()
                    else:
                        rows = (await local_session.execute(
                            text(f"""
                                SELECT * FROM {table}
                                WHERE user_id = :uid
                                  AND (synced_at IS NULL OR updated_at > synced_at)
                                LIMIT {BATCH_SIZE}
                            """),
                            {"uid": user_id},
                        )).mappings().all()

                    if not rows:
                        break  # No more unsynced rows for this table

                    row_dicts = [dict(r) for r in rows]

                    # Build upsert SQL (first batch only needs column detection)
                    if batch_num == 1:
                        local_cols = set(row_dicts[0].keys())
                        cols = sorted(local_cols & cloud_columns)
                        if not cols:
                            logger.warning("cloud_pg_no_matching_columns", table=table)
                            break

                        col_list = ", ".join(cols)
                        val_placeholders = ", ".join(f":{c}" for c in cols)
                        pk_cols = "activity_id, entity_id" if table == "activity_entity_links" else "id"
                        update_set = ", ".join(
                            f"{c} = EXCLUDED.{c}" for c in cols if c not in pk_cols.split(", ")
                        )
                        upsert_sql = text(f"""
                            INSERT INTO {table} ({col_list})
                            VALUES ({val_placeholders})
                            ON CONFLICT ({pk_cols}) DO UPDATE SET {update_set}
                        """)

                    # Prepare filtered rows
                    filtered_rows = []
                    for row in row_dicts:
                        sanitized = self._sanitize_row(row)
                        filtered_rows.append({k: sanitized[k] for k in cols if k in sanitized})

                    # Batch insert: all rows in ONE transaction
                    synced_count = 0
                    try:
                        async with cloud_sf() as cloud_session:
                            if table == "users":
                                for fr in filtered_rows:
                                    if "email" in fr:
                                        await cloud_session.execute(
                                            text("DELETE FROM users WHERE email = :email AND id != :id"),
                                            {"email": fr["email"], "id": fr["id"]},
                                        )
                            for fr in filtered_rows:
                                await cloud_session.execute(upsert_sql, fr)
                            await cloud_session.commit()
                        synced_count = len(filtered_rows)
                    except Exception as e:
                        logger.warning("cloud_pg_batch_error", table=table, batch=batch_num, error=str(e)[:200])
                        # Fallback: try individually, skip failures
                        for fr in filtered_rows:
                            try:
                                async with cloud_sf() as cs:
                                    if table == "users" and "email" in fr:
                                        await cs.execute(
                                            text("DELETE FROM users WHERE email = :email AND id != :id"),
                                            {"email": fr["email"], "id": fr["id"]},
                                        )
                                    await cs.execute(upsert_sql, fr)
                                    await cs.commit()
                                synced_count += 1
                            except Exception as ie:
                                logger.warning("cloud_pg_row_skip", table=table, error=str(ie)[:120])

                    # Mark synced locally
                    if table != "users" and synced_count > 0:
                        if table == "activity_entity_links":
                            for r in row_dicts[:synced_count]:
                                await local_session.execute(
                                    text("""
                                        UPDATE activity_entity_links
                                        SET synced_at = NOW()
                                        WHERE activity_id = :aid AND entity_id = :eid
                                    """),
                                    {"aid": r["activity_id"], "eid": r["entity_id"]},
                                )
                        else:
                            synced_ids = [r["id"] for r in row_dicts[:synced_count]]
                            await local_session.execute(
                                text(f"UPDATE {table} SET synced_at = NOW() WHERE id = ANY(:ids)"),
                                {"ids": synced_ids},
                            )
                        await local_session.commit()

                    table_total += synced_count
                    logger.info("pg_sync_batch", table=table, batch=batch_num, rows=synced_count)

                    # Update sync_history with progress so the UI can show live counts
                    if history_id is not None:
                        try:
                            async with async_session_factory() as progress_session:
                                await progress_session.execute(
                                    text("UPDATE sync_history SET records_synced = :n WHERE id = :hid"),
                                    {"n": total + table_total, "hid": history_id},
                                )
                                await progress_session.commit()
                        except Exception:
                            pass  # Non-critical: progress update failure shouldn't stop sync

                    # Users table: only one pass needed
                    if table == "users":
                        break

                total += table_total
                logger.info("pg_sync_table_done", table=table, total_rows=table_total)

        return {"records": total, "tables": PG_SYNC_TABLES}

    async def sync_redis(self, user_id: str) -> dict:
        """Mirror select key patterns from local Redis to Upstash."""
        from database.redis_client import get_redis_client
        local = get_redis_client()
        cloud = await get_cloud_redis()

        patterns = [
            f"preferences:{user_id}",
            f"sync:state:{user_id}",
            f"user:settings:{user_id}",
        ]
        count = 0
        for pattern in patterns:
            keys = []
            async for key in local.scan_iter(match=pattern):
                keys.append(key)

            for key in keys:
                ttl = await local.ttl(key)
                val = await local.get(key)
                if val is not None:
                    if ttl > 0:
                        await cloud.setex(key, ttl, val)
                    else:
                        await cloud.set(key, val)
                    count += 1

        return {"records": count, "keys_synced": count}

    async def sync_neo4j(self, user_id: str) -> dict:
        """Export user's subgraph from local Neo4j → merge into AuraDB."""
        from database.neo4j_client import get_neo4j_driver
        local_driver = get_neo4j_driver()
        cloud_driver = await get_cloud_neo4j()
        db = settings.CLOUD_NEO4J_DATABASE or "neo4j"

        # Export nodes
        async with local_driver.session() as local_session:
            result = await local_session.run(
                "MATCH (n {user_id: $uid}) RETURN n, labels(n) as labels",
                {"uid": user_id},
            )
            nodes = await result.data()

        # Export relationships
        async with local_driver.session() as local_session:
            result = await local_session.run(
                """MATCH (a {user_id: $uid})-[r]->(b)
                   RETURN a.id as from_id, b.id as to_id,
                          type(r) as rel_type, properties(r) as props""",
                {"uid": user_id},
            )
            rels = await result.data()

        node_count = 0
        rel_count = 0

        # Merge nodes into cloud
        async with cloud_driver.session(database=db) as cloud_session:
            for node_data in nodes:
                n = node_data["n"]
                labels = node_data.get("labels", ["Node"])
                label_str = ":".join(labels)
                props = {k: v for k, v in n.items() if v is not None}
                try:
                    await cloud_session.run(
                        f"MERGE (n:{label_str} {{id: $id}}) SET n += $props",
                        {"id": n.get("id", ""), "props": props},
                    )
                    node_count += 1
                except Exception as e:
                    logger.warning("cloud_neo4j_node_skip", error=str(e)[:200])

        # Merge relationships into cloud
        async with cloud_driver.session(database=db) as cloud_session:
            for rel in rels:
                try:
                    await cloud_session.run(
                        f"""MATCH (a {{id: $from_id}}), (b {{id: $to_id}})
                            MERGE (a)-[r:{rel['rel_type']}]->(b)
                            SET r += $props""",
                        {
                            "from_id": rel["from_id"],
                            "to_id": rel["to_id"],
                            "props": rel.get("props", {}),
                        },
                    )
                    rel_count += 1
                except Exception as e:
                    logger.warning("cloud_neo4j_rel_skip", error=str(e)[:200])

        return {"records": node_count + rel_count, "nodes": node_count, "relationships": rel_count}

    async def sync_qdrant(self, user_id: str) -> dict:
        """Scroll local Qdrant points → batch upsert into Qdrant Cloud."""
        from database.qdrant_client import get_qdrant_client
        from qdrant_client.models import Filter, FieldCondition, MatchValue, VectorParams, Distance

        local = get_qdrant_client()
        cloud = await get_cloud_qdrant()

        collections = ["activities", "entities"]
        total = 0

        for coll_name in collections:
            # Ensure cloud collection exists — handle 409 "already exists" gracefully
            try:
                await cloud.get_collection(coll_name)
            except Exception:
                try:
                    await cloud.create_collection(
                        collection_name=coll_name,
                        vectors_config=VectorParams(size=384, distance=Distance.COSINE),
                    )
                except Exception as ce:
                    # 409 Conflict = collection already exists, which is fine
                    if "already exists" not in str(ce).lower():
                        logger.warning("qdrant_create_coll_error", collection=coll_name, error=str(ce)[:200])
                        continue

            # Scroll local points for this user
            offset = None
            batch_points = []
            while True:
                try:
                    result = await local.scroll(
                        collection_name=coll_name,
                        scroll_filter=Filter(
                            must=[FieldCondition(key="user_id", match=MatchValue(value=user_id))]
                        ),
                        limit=100,
                        offset=offset,
                        with_vectors=True,
                        with_payload=True,
                    )
                    points, next_offset = result
                except Exception as e:
                    logger.warning("qdrant_scroll_error", collection=coll_name, error=str(e)[:200])
                    break

                if not points:
                    break

                from qdrant_client.models import PointStruct
                for pt in points:
                    batch_points.append(PointStruct(
                        id=pt.id,
                        vector=pt.vector,
                        payload=pt.payload,
                    ))

                if next_offset is None:
                    break
                offset = next_offset

            # Batch upsert to cloud
            if batch_points:
                for i in range(0, len(batch_points), 100):
                    batch = batch_points[i:i + 100]
                    await cloud.upsert(collection_name=coll_name, points=batch)
                total += len(batch_points)
                logger.info("qdrant_sync_collection", collection=coll_name, points=len(batch_points))

        return {"records": total, "collections": collections}

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _safe_sync(self, name: str, fn, user_id: str, **kwargs):
        """Run a sync function and catch all exceptions."""
        try:
            result = await fn(user_id, **kwargs)
            return (name, result)
        except CloudSyncNotConfigured as e:
            logger.info("cloud_sync_skipped", target=name, reason=str(e))
            return (name, {"skipped": True, "reason": str(e), "records": 0})
        except Exception as e:
            logger.error("cloud_sync_error", target=name, error=str(e))
            return (name, e)

    async def _ensure_cloud_table(self, engine, table_name: str, local_session):
        """Create table in Supabase if it doesn't exist (DDL mirror)."""
        from sqlalchemy.ext.asyncio import AsyncSession as _S, async_sessionmaker
        sf = async_sessionmaker(engine, class_=_S, expire_on_commit=False)
        async with sf() as cloud_session:
            # Check if table exists
            exists = await cloud_session.execute(
                text("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = :t)"),
                {"t": table_name},
            )
            if exists.scalar():
                return

            # Detect actual PK columns from local DB
            pk_result = await local_session.execute(
                text("""
                    SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = CAST(:t AS regclass) AND i.indisprimary
                """),
                {"t": table_name},
            )
            pk_cols = pk_result.scalar() or "id"

            # Get DDL from local and replay on cloud
            ddl_result = await local_session.execute(
                text("""
                    SELECT 'CREATE TABLE IF NOT EXISTS ' || :t || ' (' ||
                    string_agg(
                        column_name || ' ' ||
                        CASE WHEN data_type = 'uuid' THEN 'UUID'
                             WHEN data_type = 'character varying' THEN 'VARCHAR(' || character_maximum_length || ')'
                             WHEN data_type = 'text' THEN 'TEXT'
                             WHEN data_type = 'integer' THEN 'INTEGER'
                             WHEN data_type = 'double precision' THEN 'DOUBLE PRECISION'
                             WHEN data_type = 'boolean' THEN 'BOOLEAN'
                             WHEN data_type = 'jsonb' THEN 'JSONB'
                             WHEN data_type = 'json' THEN 'JSONB'
                             WHEN data_type LIKE 'timestamp%' THEN 'TIMESTAMPTZ'
                             ELSE data_type
                        END ||
                        CASE WHEN is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END,
                        ', '
                    ) || ', PRIMARY KEY (' || :pk || '))' as ddl
                    FROM information_schema.columns
                    WHERE table_name = :t AND table_schema = 'public'
                """),
                {"t": table_name, "pk": pk_cols},
            )
            ddl = ddl_result.scalar()
            if ddl:
                try:
                    await cloud_session.execute(text(ddl))
                    await cloud_session.commit()
                    logger.info("cloud_table_created", table=table_name)
                except Exception as e:
                    logger.warning("cloud_table_create_failed", table=table_name, error=str(e)[:200])

    def _sanitize_row(self, row: dict) -> dict:
        """Prepare a row dict for SQL insertion — handle special types."""
        import json
        sanitized = {}
        for k, v in row.items():
            if isinstance(v, (dict, list)):
                sanitized[k] = json.dumps(v)
            elif isinstance(v, uuid.UUID):
                sanitized[k] = str(v)
            elif v is None:
                sanitized[k] = None
            else:
                sanitized[k] = v
        return sanitized

    # ── Restore (cloud → local) ───────────────────────────────────────────────

    async def restore_all(self, user_id: str) -> SyncResult:
        """Pull all cloud data → local DBs.  Records result in sync_history."""
        result = SyncResult()
        history_id = uuid.uuid4()
        started_at = datetime.now(timezone.utc)

        # Record restore start
        async with async_session_factory() as session:
            session.add(SyncHistory(
                id=history_id,
                user_id=uuid.UUID(user_id),
                started_at=started_at,
                status="running",
                trigger="restore",
            ))
            await session.commit()

        # Run restores in parallel
        tasks = [
            self._safe_sync("postgresql", self.restore_postgres, user_id),
            self._safe_sync("redis", self.restore_redis, user_id),
            self._safe_sync("neo4j", self.restore_neo4j, user_id),
            self._safe_sync("qdrant", self.restore_qdrant, user_id),
        ]
        outcomes = await asyncio.gather(*tasks)

        for name, outcome in outcomes:
            if isinstance(outcome, dict):
                result.targets[name] = outcome
                result.total_records += outcome.get("records", 0)
            else:
                result.targets[name] = {"error": str(outcome), "records": 0}
                result.errors.append(f"{name}: {outcome}")

        # Update history
        completed_at = datetime.now(timezone.utc)
        try:
            async with async_session_factory() as session:
                await session.execute(
                    text("""
                        UPDATE sync_history
                        SET completed_at = :completed,
                            status = :status,
                            results = CAST(:results AS json),
                            records_synced = :records,
                            error = :error
                        WHERE id = :hid
                    """),
                    {
                        "completed": completed_at,
                        "status": "completed" if result.ok else "failed",
                        "results": json.dumps(result.to_dict()["targets"]),
                        "records": result.total_records,
                        "error": "; ".join(result.errors) if result.errors else None,
                        "hid": history_id,
                    },
                )
                await session.commit()
        except Exception as e:
            logger.error("restore_history_update_failed", error=str(e))

        logger.info(
            "cloud_restore_complete",
            user_id=user_id,
            total_records=result.total_records,
            ok=result.ok,
            duration_s=round((completed_at - started_at).total_seconds(), 2),
        )
        return result

    async def restore_postgres(self, user_id: str, **kwargs) -> dict:
        """Pull from Supabase PG → local PG.  INSERT ON CONFLICT DO NOTHING (local-wins)."""
        cloud_engine = await get_cloud_pg_engine()
        from sqlalchemy.ext.asyncio import AsyncSession as _S, async_sessionmaker
        cloud_sf = async_sessionmaker(cloud_engine, class_=_S, expire_on_commit=False)

        total = 0

        for table in PG_SYNC_TABLES:
            table_count = 0
            try:
                # Fetch all user's data from cloud
                if table == "users":
                    query = f"SELECT * FROM {table} WHERE id = :uid"
                elif table == "activity_entity_links":
                    query = f"""
                        SELECT ael.* FROM {table} ael
                        JOIN activities a ON ael.activity_id = a.id
                        WHERE a.user_id = :uid
                    """
                else:
                    query = f"SELECT * FROM {table} WHERE user_id = :uid"

                async with cloud_sf() as cloud_session:
                    result = await cloud_session.execute(text(query), {"uid": user_id})
                    cloud_rows = [dict(r) for r in result.mappings().all()]

                if not cloud_rows:
                    logger.info("restore_pg_table_empty", table=table)
                    continue

                # Get column names and PK from local
                async with async_session_factory() as local_session:
                    pk_result = await local_session.execute(
                        text("""
                            SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                            FROM pg_index i
                            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                            WHERE i.indrelid = CAST(:t AS regclass) AND i.indisprimary
                        """),
                        {"t": table},
                    )
                    pk_cols = (pk_result.scalar() or "id").split(", ")

                # Insert into local — ON CONFLICT DO NOTHING (local-wins)
                cols = list(cloud_rows[0].keys())
                col_list = ", ".join(cols)
                val_list = ", ".join([f":{c}" for c in cols])
                pk_str = ", ".join(pk_cols)

                insert_sql = f"""
                    INSERT INTO {table} ({col_list})
                    VALUES ({val_list})
                    ON CONFLICT ({pk_str}) DO NOTHING
                """

                BATCH = 500
                for i in range(0, len(cloud_rows), BATCH):
                    batch = cloud_rows[i:i + BATCH]
                    async with async_session_factory() as local_session:
                        for row in batch:
                            try:
                                sanitized = self._sanitize_row(row)
                                await local_session.execute(text(insert_sql), sanitized)
                                table_count += 1
                            except Exception as e:
                                logger.warning("restore_pg_row_skip", table=table, error=str(e)[:200])
                        await local_session.commit()

                total += table_count
                logger.info("restore_pg_table", table=table, rows=table_count)

            except Exception as e:
                logger.error("restore_pg_table_error", table=table, error=str(e)[:300])

        return {"records": total}

    async def restore_redis(self, user_id: str, **kwargs) -> dict:
        """Pull from Upstash → local Redis."""
        from database.redis_client import get_redis_client
        local = get_redis_client()
        cloud = await get_cloud_redis()

        patterns = [
            f"preferences:{user_id}",
            f"sync:state:{user_id}",
            f"user:settings:{user_id}",
        ]
        count = 0
        for pattern in patterns:
            keys = []
            async for key in cloud.scan_iter(match=pattern):
                keys.append(key)

            for key in keys:
                ttl = await cloud.ttl(key)
                val = await cloud.get(key)
                if val is not None:
                    # Only set locally if key doesn't already exist (local-wins)
                    exists = await local.exists(key)
                    if not exists:
                        if ttl > 0:
                            await local.setex(key, ttl, val)
                        else:
                            await local.set(key, val)
                        count += 1

        return {"records": count, "keys_restored": count}

    async def restore_neo4j(self, user_id: str, **kwargs) -> dict:
        """Pull from AuraDB → local Neo4j."""
        from database.neo4j_client import get_neo4j_driver
        local_driver = get_neo4j_driver()
        cloud_driver = await get_cloud_neo4j()
        db = settings.CLOUD_NEO4J_DATABASE or "neo4j"

        # Export nodes from cloud
        async with cloud_driver.session(database=db) as cloud_session:
            result = await cloud_session.run(
                "MATCH (n {user_id: $uid}) RETURN n, labels(n) as labels",
                {"uid": user_id},
            )
            nodes = await result.data()

        # Export relationships from cloud
        async with cloud_driver.session(database=db) as cloud_session:
            result = await cloud_session.run(
                """MATCH (a {user_id: $uid})-[r]->(b)
                   RETURN a.id as from_id, b.id as to_id,
                          type(r) as rel_type, properties(r) as props""",
                {"uid": user_id},
            )
            rels = await result.data()

        node_count = 0
        rel_count = 0

        # Merge nodes into local
        async with local_driver.session() as local_session:
            for node_data in nodes:
                n = node_data["n"]
                labels = node_data.get("labels", ["Node"])
                label_str = ":".join(labels)
                props = {k: v for k, v in n.items() if v is not None}
                try:
                    await local_session.run(
                        f"MERGE (n:{label_str} {{id: $id}}) SET n += $props",
                        {"id": n.get("id", ""), "props": props},
                    )
                    node_count += 1
                except Exception as e:
                    logger.warning("restore_neo4j_node_skip", error=str(e)[:200])

        # Merge relationships into local
        async with local_driver.session() as local_session:
            for rel in rels:
                try:
                    await local_session.run(
                        f"""MATCH (a {{id: $from_id}}), (b {{id: $to_id}})
                            MERGE (a)-[r:{rel['rel_type']}]->(b)
                            SET r += $props""",
                        {
                            "from_id": rel["from_id"],
                            "to_id": rel["to_id"],
                            "props": rel.get("props", {}),
                        },
                    )
                    rel_count += 1
                except Exception as e:
                    logger.warning("restore_neo4j_rel_skip", error=str(e)[:200])

        return {"records": node_count + rel_count, "nodes": node_count, "relationships": rel_count}

    async def restore_qdrant(self, user_id: str, **kwargs) -> dict:
        """Pull from Qdrant Cloud → local Qdrant."""
        from database.qdrant_client import get_qdrant_client
        from qdrant_client.models import Filter, FieldCondition, MatchValue, VectorParams, Distance

        local = get_qdrant_client()
        cloud = await get_cloud_qdrant()

        collections = ["activities", "entities"]
        total = 0

        for coll_name in collections:
            # Ensure local collection exists
            try:
                await local.get_collection(coll_name)
            except Exception:
                try:
                    await local.create_collection(
                        collection_name=coll_name,
                        vectors_config=VectorParams(size=384, distance=Distance.COSINE),
                    )
                except Exception:
                    pass  # Already exists

            # Scroll cloud points for this user
            offset = None
            batch_points = []
            while True:
                try:
                    result = await cloud.scroll(
                        collection_name=coll_name,
                        scroll_filter=Filter(
                            must=[FieldCondition(key="user_id", match=MatchValue(value=user_id))]
                        ),
                        limit=100,
                        offset=offset,
                        with_vectors=True,
                        with_payload=True,
                    )
                    points, next_offset = result
                except Exception as e:
                    logger.warning("restore_qdrant_scroll_error", collection=coll_name, error=str(e)[:200])
                    break

                if not points:
                    break

                from qdrant_client.models import PointStruct
                for pt in points:
                    batch_points.append(PointStruct(
                        id=pt.id,
                        vector=pt.vector,
                        payload=pt.payload,
                    ))

                if next_offset is None:
                    break
                offset = next_offset

            # Batch upsert to local
            if batch_points:
                for i in range(0, len(batch_points), 100):
                    batch = batch_points[i:i + 100]
                    await local.upsert(collection_name=coll_name, points=batch)
                total += len(batch_points)
                logger.info("restore_qdrant_collection", collection=coll_name, points=len(batch_points))

        return {"records": total, "collections": collections}

