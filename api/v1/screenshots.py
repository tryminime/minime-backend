"""
Screenshot API endpoints.
Handles screenshot upload, listing, retrieval, and deletion.
"""

from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, File, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import Response
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Optional
from datetime import datetime
from uuid import UUID, uuid4
import structlog

from database.postgres import get_db
from auth.jwt_handler import get_current_user as get_current_user_from_token

logger = structlog.get_logger()
router = APIRouter()
security = HTTPBearer()

TABLE_INIT = """
CREATE TABLE IF NOT EXISTS screenshots (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    monitor_name TEXT DEFAULT 'primary',
    label TEXT,
    encrypted_data BYTEA NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    mime_type TEXT DEFAULT 'image/png',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_screenshots_user_id ON screenshots(user_id);
CREATE INDEX IF NOT EXISTS idx_screenshots_created_at ON screenshots(created_at);
"""


# =====================================================
# REQUEST/RESPONSE MODELS
# =====================================================


class ScreenshotMeta(BaseModel):
    id: str
    width: int
    height: int
    monitor_name: str
    label: Optional[str] = None
    file_size_bytes: int
    created_at: str


class ScreenshotUploadResponse(BaseModel):
    id: str
    message: str
    file_size_bytes: int


class ScreenshotListResponse(BaseModel):
    screenshots: List[ScreenshotMeta]
    total: int
    limit: int
    offset: int


# =====================================================
# HELPER: ensure table exists
# =====================================================


async def _ensure_table(db: AsyncSession):
    """Create screenshots table if it doesn't exist."""
    try:
        await db.execute(text(TABLE_INIT))
        await db.commit()
    except Exception:
        await db.rollback()


# =====================================================
# ENDPOINTS
# =====================================================


@router.post("/upload", response_model=ScreenshotUploadResponse, status_code=status.HTTP_201_CREATED)
async def upload_screenshot(
    file: UploadFile = File(...),
    width: int = Query(0, ge=0),
    height: int = Query(0, ge=0),
    monitor_name: str = Query("primary"),
    label: Optional[str] = None,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Upload an encrypted screenshot from the desktop client.

    The file is stored as-is (already encrypted on the client side).
    Only metadata is readable by the server.
    """
    await _ensure_table(db)

    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    # Read the encrypted file data
    file_data = await file.read()
    file_size = len(file_data)

    if file_size > 10 * 1024 * 1024:  # 10MB max
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail="Screenshot exceeds 10MB limit",
        )

    screenshot_id = str(uuid4())

    await db.execute(
        text(
            """INSERT INTO screenshots (id, user_id, width, height, monitor_name, label, encrypted_data, file_size_bytes)
               VALUES (:id, :user_id, :width, :height, :monitor_name, :label, :data, :size)"""
        ),
        {
            "id": screenshot_id,
            "user_id": str(user_id),
            "width": width,
            "height": height,
            "monitor_name": monitor_name,
            "label": label,
            "data": file_data,
            "size": file_size,
        },
    )
    await db.commit()

    logger.info("screenshot_uploaded", user_id=str(user_id), size=file_size, id=screenshot_id)

    return ScreenshotUploadResponse(
        id=screenshot_id,
        message="Screenshot uploaded successfully",
        file_size_bytes=file_size,
    )


@router.get("/", response_model=ScreenshotListResponse)
async def list_screenshots(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """List screenshot metadata for the current user (without image data)."""
    await _ensure_table(db)

    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    # Get total count
    count_result = await db.execute(
        text("SELECT COUNT(*) FROM screenshots WHERE user_id = :uid"),
        {"uid": str(user_id)},
    )
    total = count_result.scalar() or 0

    # Get metadata (no data column for performance)
    result = await db.execute(
        text(
            """SELECT id, width, height, monitor_name, label, file_size_bytes, created_at
               FROM screenshots
               WHERE user_id = :uid
               ORDER BY created_at DESC
               LIMIT :lim OFFSET :off"""
        ),
        {"uid": str(user_id), "lim": limit, "off": offset},
    )

    screenshots = []
    for row in result:
        screenshots.append(
            ScreenshotMeta(
                id=str(row[0]),
                width=row[1],
                height=row[2],
                monitor_name=row[3] or "primary",
                label=row[4],
                file_size_bytes=row[5],
                created_at=row[6].isoformat() if row[6] else "",
            )
        )

    return ScreenshotListResponse(
        screenshots=screenshots,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get("/{screenshot_id}")
async def get_screenshot(
    screenshot_id: str,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """
    Download a screenshot's encrypted data.

    Returns the raw encrypted bytes — the client must decrypt locally.
    """
    await _ensure_table(db)

    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    result = await db.execute(
        text(
            "SELECT encrypted_data, mime_type FROM screenshots WHERE id = :sid AND user_id = :uid"
        ),
        {"sid": screenshot_id, "uid": str(user_id)},
    )
    row = result.first()

    if not row:
        raise HTTPException(status_code=404, detail="Screenshot not found")

    return Response(
        content=row[0],
        media_type=row[1] or "application/octet-stream",
        headers={"Content-Disposition": f'attachment; filename="screenshot-{screenshot_id}.enc"'},
    )


@router.delete("/{screenshot_id}")
async def delete_screenshot(
    screenshot_id: str,
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Delete a screenshot (GDPR compliance)."""
    await _ensure_table(db)

    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    result = await db.execute(
        text("DELETE FROM screenshots WHERE id = :sid AND user_id = :uid"),
        {"sid": screenshot_id, "uid": str(user_id)},
    )
    await db.commit()

    if result.rowcount == 0:
        raise HTTPException(status_code=404, detail="Screenshot not found")

    logger.info("screenshot_deleted", user_id=str(user_id), id=screenshot_id)

    return {"message": "Screenshot deleted", "id": screenshot_id}


@router.delete("/")
async def delete_all_screenshots(
    current_user=Depends(get_current_user_from_token),
    db: AsyncSession = Depends(get_db),
):
    """Purge all screenshots for current user (privacy action)."""
    await _ensure_table(db)

    user_id = UUID(current_user["id"]) if isinstance(current_user, dict) else current_user.id

    result = await db.execute(
        text("DELETE FROM screenshots WHERE user_id = :uid"),
        {"uid": str(user_id)},
    )
    await db.commit()

    count = result.rowcount

    logger.info("screenshots_purged", user_id=str(user_id), count=count)

    return {"message": f"Deleted {count} screenshots", "count": count}
