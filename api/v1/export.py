"""
Export/Import API — Phase 4b.

Endpoints:
  POST /api/v1/export/download  — Export all user data as encrypted .mmexport
  POST /api/v1/export/upload    — Import data from encrypted .mmexport file
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from fastapi.responses import Response
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import structlog

from auth.jwt_handler import get_current_user
from database.postgres import get_db, async_session_factory
from middleware.feature_gate import FeatureGate
from models import User
from services.cloud_sync import encrypt_payload, decrypt_payload, derive_key_from_password

logger = structlog.get_logger()
router = APIRouter(prefix="/api/v1/export", tags=["Data Export"])

# Version of the export format — increment on schema changes
EXPORT_VERSION = "1.0"
SCHEMA_VERSION = "2026.03"

# Tables to include in export (order matters for FK constraints on import)
EXPORT_PG_TABLES = [
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


# ── Auth helper ────────────────────────────────────────────────────────────────

async def _require_pro_user(
    user_data: dict = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Validate JWT and load User ORM for tier check."""
    user_id = user_data.get("user_id") or user_data.get("sub")
    if not user_id:
        raise HTTPException(401, "Invalid token")
    result = await db.execute(
        text("SELECT * FROM users WHERE id = :uid"),
        {"uid": user_id},
    )
    row = result.mappings().first()
    if not row:
        raise HTTPException(404, "User not found")

    user = User()
    for col in row.keys():
        if hasattr(user, col):
            setattr(user, col, row[col])

    FeatureGate(user).require("cloud_sync")
    return user


# ── Export Download ────────────────────────────────────────────────────────────

@router.post("/download")
async def export_download(
    password: str = Form(...),
    user: User = Depends(_require_pro_user),
    db: AsyncSession = Depends(get_db),
):
    """Export all user data as an AES-256-GCM encrypted .mmexport file.

    The encryption key is derived from the user's password + user_id salt.
    The user must provide their password as re-authentication.
    """
    from auth.password import verify_password

    # Re-authenticate
    if not user.password_hash or not verify_password(password, user.password_hash):
        raise HTTPException(403, "Incorrect password")

    user_id = str(user.id)
    logger.info("export_download_started", user_id=user_id)

    # Derive encryption key from password
    salt = uuid.UUID(user_id).bytes
    key = derive_key_from_password(password, salt)

    # Collect all PG data
    tables_data = {}
    for table in EXPORT_PG_TABLES:
        try:
            if table == "activity_entity_links":
                query = f"""
                    SELECT ael.* FROM {table} ael
                    JOIN activities a ON ael.activity_id = a.id
                    WHERE a.user_id = :uid
                """
            else:
                query = f"SELECT * FROM {table} WHERE user_id = :uid"

            result = await db.execute(text(query), {"uid": user_id})
            rows = result.mappings().all()
            tables_data[table] = [_row_to_serializable(dict(r)) for r in rows]
        except Exception as e:
            logger.warning("export_table_error", table=table, error=str(e)[:200])
            tables_data[table] = []

    # Collect Neo4j data
    neo4j_data = {"nodes": [], "relationships": []}
    try:
        from database.neo4j_client import get_neo4j_driver
        driver = get_neo4j_driver()
        async with driver.session() as session:
            result = await session.run(
                "MATCH (n {user_id: $uid}) RETURN n, labels(n) as labels",
                {"uid": user_id},
            )
            nodes = await result.data()
            neo4j_data["nodes"] = [
                {"properties": _row_to_serializable(dict(n["n"])), "labels": n.get("labels", [])}
                for n in nodes
            ]

            result = await session.run(
                """MATCH (a {user_id: $uid})-[r]->(b)
                   RETURN a.id as from_id, b.id as to_id,
                          type(r) as rel_type, properties(r) as props""",
                {"uid": user_id},
            )
            rels = await result.data()
            neo4j_data["relationships"] = [_row_to_serializable(dict(r)) for r in rels]
    except Exception as e:
        logger.warning("export_neo4j_error", error=str(e)[:200])

    # Collect Qdrant data
    qdrant_data = {}
    try:
        from database.qdrant_client import get_qdrant_client
        from qdrant_client.models import Filter, FieldCondition, MatchValue
        client = get_qdrant_client()

        for coll_name in ["activities", "entities"]:
            points_list = []
            offset = None
            while True:
                try:
                    result = await client.scroll(
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
                    if not points:
                        break
                    for pt in points:
                        points_list.append({
                            "id": str(pt.id),
                            "vector": pt.vector if isinstance(pt.vector, list) else list(pt.vector),
                            "payload": _row_to_serializable(pt.payload or {}),
                        })
                    if next_offset is None:
                        break
                    offset = next_offset
                except Exception:
                    break
            qdrant_data[coll_name] = points_list
    except Exception as e:
        logger.warning("export_qdrant_error", error=str(e)[:200])

    # Build export payload
    payload = {
        "version": EXPORT_VERSION,
        "format": "mmexport",
        "user_id": user_id,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "schema_version": SCHEMA_VERSION,
        "tables": tables_data,
        "neo4j": neo4j_data,
        "qdrant": qdrant_data,
    }

    # Calculate stats
    total_records = sum(len(v) for v in tables_data.values())
    total_records += len(neo4j_data.get("nodes", []))
    total_records += sum(len(v) for v in qdrant_data.values())

    # Encrypt
    payload_bytes = json.dumps(payload, default=str).encode("utf-8")
    encrypted = encrypt_payload(payload_bytes, key)

    logger.info(
        "export_download_complete",
        user_id=user_id,
        total_records=total_records,
        encrypted_size=len(encrypted),
    )

    # Return as downloadable file
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filename = f"minime_export_{ts}.mmexport"

    return Response(
        content=encrypted,
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Export-Records": str(total_records),
        },
    )


# ── Export Upload (Import) ─────────────────────────────────────────────────────

@router.post("/upload")
async def export_upload(
    password: str = Form(...),
    file: UploadFile = File(...),
    user: User = Depends(_require_pro_user),
    db: AsyncSession = Depends(get_db),
):
    """Import data from an AES-256-GCM encrypted .mmexport file.

    Re-authenticates via password to derive the decryption key.
    Uses ON CONFLICT DO UPDATE (cloud-wins) for PG tables and
    MERGE for Neo4j to ensure imported data takes precedence.
    """
    from auth.password import verify_password

    # Re-authenticate
    if not user.password_hash or not verify_password(password, user.password_hash):
        raise HTTPException(403, "Incorrect password")

    user_id = str(user.id)
    logger.info("export_upload_started", user_id=user_id)

    # Derive encryption key from password
    salt = uuid.UUID(user_id).bytes
    key = derive_key_from_password(password, salt)

    # Read and decrypt
    encrypted_data = await file.read()
    try:
        decrypted = decrypt_payload(encrypted_data, key)
    except Exception:
        raise HTTPException(400, "Decryption failed — wrong password or corrupted file")

    try:
        payload = json.loads(decrypted.decode("utf-8"))
    except Exception:
        raise HTTPException(400, "Invalid export file format")

    # Validate
    if payload.get("format") != "mmexport":
        raise HTTPException(400, "Not a valid .mmexport file")

    if payload.get("user_id") != user_id:
        raise HTTPException(403, "This export belongs to a different user")

    # Import PG tables
    tables_data = payload.get("tables", {})
    import_stats = {}

    for table in EXPORT_PG_TABLES:
        rows = tables_data.get(table, [])
        if not rows:
            import_stats[table] = 0
            continue

        count = 0
        try:
            # Get PK columns
            pk_result = await db.execute(
                text("""
                    SELECT string_agg(a.attname, ', ' ORDER BY array_position(i.indkey, a.attnum))
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = CAST(:t AS regclass) AND i.indisprimary
                """),
                {"t": table},
            )
            pk_cols = (pk_result.scalar() or "id").split(", ")

            cols = list(rows[0].keys())
            col_list = ", ".join(cols)
            val_list = ", ".join([f":{c}" for c in cols])
            pk_str = ", ".join(pk_cols)
            update_cols = [c for c in cols if c not in pk_cols]
            update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

            if update_clause:
                insert_sql = f"""
                    INSERT INTO {table} ({col_list})
                    VALUES ({val_list})
                    ON CONFLICT ({pk_str}) DO UPDATE SET {update_clause}
                """
            else:
                insert_sql = f"""
                    INSERT INTO {table} ({col_list})
                    VALUES ({val_list})
                    ON CONFLICT ({pk_str}) DO NOTHING
                """

            for row in rows:
                try:
                    sanitized = _sanitize_for_insert(row)
                    await db.execute(text(insert_sql), sanitized)
                    count += 1
                except Exception as e:
                    logger.warning("import_row_skip", table=table, error=str(e)[:200])

            await db.commit()
        except Exception as e:
            logger.error("import_table_error", table=table, error=str(e)[:300])

        import_stats[table] = count

    # Import Neo4j data
    neo4j_data = payload.get("neo4j", {})
    neo4j_imported = 0
    try:
        from database.neo4j_client import get_neo4j_driver
        driver = get_neo4j_driver()

        # Import nodes
        for node in neo4j_data.get("nodes", []):
            try:
                labels = node.get("labels", ["Node"])
                label_str = ":".join(labels)
                props = node.get("properties", {})
                async with driver.session() as session:
                    await session.run(
                        f"MERGE (n:{label_str} {{id: $id}}) SET n += $props",
                        {"id": props.get("id", ""), "props": props},
                    )
                    neo4j_imported += 1
            except Exception as e:
                logger.warning("import_neo4j_node_skip", error=str(e)[:200])

        # Import relationships
        for rel in neo4j_data.get("relationships", []):
            try:
                async with driver.session() as session:
                    await session.run(
                        f"""MATCH (a {{id: $from_id}}), (b {{id: $to_id}})
                            MERGE (a)-[r:{rel['rel_type']}]->(b)
                            SET r += $props""",
                        {
                            "from_id": rel["from_id"],
                            "to_id": rel["to_id"],
                            "props": rel.get("props", {}),
                        },
                    )
                    neo4j_imported += 1
            except Exception as e:
                logger.warning("import_neo4j_rel_skip", error=str(e)[:200])
    except Exception as e:
        logger.warning("import_neo4j_error", error=str(e)[:200])

    # Import Qdrant data
    qdrant_data = payload.get("qdrant", {})
    qdrant_imported = 0
    try:
        from database.qdrant_client import get_qdrant_client
        from qdrant_client.models import PointStruct, VectorParams, Distance
        client = get_qdrant_client()

        for coll_name, points in qdrant_data.items():
            if not points:
                continue

            # Ensure collection exists
            try:
                await client.get_collection(coll_name)
            except Exception:
                try:
                    await client.create_collection(
                        collection_name=coll_name,
                        vectors_config=VectorParams(size=384, distance=Distance.COSINE),
                    )
                except Exception:
                    pass

            # Batch upsert
            batch = []
            for pt in points:
                batch.append(PointStruct(
                    id=pt["id"],
                    vector=pt["vector"],
                    payload=pt.get("payload", {}),
                ))
                if len(batch) >= 100:
                    await client.upsert(collection_name=coll_name, points=batch)
                    qdrant_imported += len(batch)
                    batch = []
            if batch:
                await client.upsert(collection_name=coll_name, points=batch)
                qdrant_imported += len(batch)
    except Exception as e:
        logger.warning("import_qdrant_error", error=str(e)[:200])

    total_imported = sum(import_stats.values()) + neo4j_imported + qdrant_imported

    logger.info(
        "export_upload_complete",
        user_id=user_id,
        total_imported=total_imported,
        tables=import_stats,
    )

    return {
        "success": True,
        "total_imported": total_imported,
        "tables": import_stats,
        "neo4j_imported": neo4j_imported,
        "qdrant_imported": qdrant_imported,
        "exported_at": payload.get("exported_at"),
        "schema_version": payload.get("schema_version"),
    }


# ── Helpers ────────────────────────────────────────────────────────────────────

def _row_to_serializable(row: dict) -> dict:
    """Convert a DB row dict to JSON-serializable form."""
    import uuid as uuid_mod
    clean = {}
    for k, v in row.items():
        if isinstance(v, uuid_mod.UUID):
            clean[k] = str(v)
        elif isinstance(v, datetime):
            clean[k] = v.isoformat()
        elif isinstance(v, bytes):
            import base64
            clean[k] = base64.b64encode(v).decode()
        else:
            clean[k] = v
    return clean


def _sanitize_for_insert(row: dict) -> dict:
    """Prepare a row dict from export file for SQL insertion."""
    sanitized = {}
    for k, v in row.items():
        if isinstance(v, (dict, list)):
            sanitized[k] = json.dumps(v)
        elif v is None:
            sanitized[k] = None
        else:
            sanitized[k] = v
    return sanitized
