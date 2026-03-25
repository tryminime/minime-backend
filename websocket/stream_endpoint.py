"""
WebSocket endpoint for real-time activity streaming.
Provides /v1/stream endpoint for clients to receive live updates.
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from uuid import UUID
import asyncio
import structlog

from auth.jwt_handler import decode_token, verify_token_type
from websocket.activity_stream import activity_stream_manager

logger = structlog.get_logger()
router = APIRouter()


async def get_current_user_from_websocket(token: str):
    """Authenticate user from websocket token."""
    payload = decode_token(token)
    
    if not payload or not verify_token_type(payload, "access"):
        raise Exception("Invalid access token")
    
    return {"id": payload.get("sub"), "email": payload.get("email")}


@router.websocket("/stream")
async def activity_stream_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="JWT authentication token")
):
    """
    WebSocket endpoint for real-time activity and entity updates.
    
    URL: ws://localhost:8000/v1/stream?token=<JWT>
    
    Events sent to clients:
    - connection.established: Initial connection success
    - activity.created: New activity ingested
    - entity.created: New entity extracted
    - entity.merged: Entities merged
    - ping: Heartbeat check
    
    Client can send:
    - pong: Heartbeat response
    - subscribe: Subscribe to specific event types
    - unsubscribe: Unsubscribe from event types
    """
    connection_id = None
    user_id = None
    
    try:
        # Authenticate user from token
        user = await get_current_user_from_websocket(token)
        user_id = UUID(user["id"]) if isinstance(user, dict) else user.id
        connection_id = f"ws_{user_id}_{asyncio.current_task().get_name()}"
        
        # Accept connection
        await activity_stream_manager.connect(user_id, websocket, connection_id)
        
        # Start heartbeat task
        heartbeat_task = asyncio.create_task(
            send_heartbeats(websocket, user_id)
        )
        
        # Listen for client messages
        try:
            while True:
                # Receive message from client
                data = await websocket.receive_json()
                
                # Handle message
                await activity_stream_manager.handle_client_message(
                    user_id, websocket, data
                )
                
        except WebSocketDisconnect:
            logger.info(
                "websocket_client_disconnected",
                user_id=str(user_id),
                connection_id=connection_id
            )
        finally:
            # Cancel heartbeat
            heartbeat_task.cancel()
            
            # Cleanup connection
            await activity_stream_manager.disconnect(user_id, websocket, connection_id)
    
    except Exception as e:
        logger.error(
            "websocket_error",
            user_id=str(user_id) if user_id else None,
            error=str(e),
            error_type=type(e).__name__
        )
        
        if connection_id and user_id:
            await activity_stream_manager.disconnect(user_id, websocket, connection_id)


async def send_heartbeats(websocket: WebSocket, user_id: UUID):
    """
    Send periodic heartbeat pings to keep connection alive.
    
    Args:
        websocket: WebSocket connection
        user_id: User UUID for logging
    """
    try:
        while True:
            await asyncio.sleep(30)  # Heartbeat every 30 seconds
            await activity_stream_manager.send_heartbeat(websocket)
            
    except asyncio.CancelledError:
        logger.debug("heartbeat_cancelled", user_id=str(user_id))
    except Exception as e:
        logger.warning(
            "heartbeat_error",
            user_id=str(user_id),
            error=str(e)
        )


@router.get("/stream/stats")
async def get_stream_stats():
    """
    Get WebSocket connection statistics.
    
    Admin endpoint for monitoring active connections.
    """
    return activity_stream_manager.get_stats()
