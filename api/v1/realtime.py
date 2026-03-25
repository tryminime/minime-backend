"""
WebSocket endpoint for real-time updates.
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends, Query
from websocket.manager import manager
from auth.jwt_handler import decode_token
import structlog

logger = structlog.get_logger()
router = APIRouter()


@router.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket,
    token: str = Query(..., description="JWT access token for authentication")
):
    """
    WebSocket endpoint for real-time updates.
    
    Usage:
        ws://localhost:8000/api/v1/realtime/ws?token=YOUR_ACCESS_TOKEN
    
    Message Format:
        Client -> Server: JSON commands (future: ping, subscribe to topics)
        Server -> Client: JSON events (activities, entities, sync status)
    
    Connection Flow:
        1. Client connects with JWT token in query param
        2. Server validates token and extracts user_id
        3. Connection registered in manager
        4. Client starts receiving real-time updates
        5. On disconnect, connection cleaned up
    """
    
    # Validate token and extract user ID
    payload = decode_token(token)
    
    if not payload:
        await websocket.close(code=1008, reason="Invalid token")
        return
    
    user_id = payload.get("sub")
    
    if not user_id:
        await websocket.close(code=1008, reason="Invalid token payload")
        return
    
    # Register connection
    await manager.connect(websocket, user_id)
    
    try:
        # Keep connection alive and handle incoming messages
        while True:
            data = await websocket.receive_text()
            
            # TODO: Phase 1 - Handle client commands
            # Examples:
            # - {"type": "ping"} -> respond with pong
            # - {"type": "subscribe", "topic": "analytics"} -> subscribe to specific updates
            # - {"type": "unsubscribe", "topic": "activities"} -> unsubscribe
            
            logger.debug("WebSocket message received", user_id=user_id, message=data)
            
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected", user_id=user_id)
    except Exception as e:
        logger.error("WebSocket error", user_id=user_id, error=str(e))
    finally:
        await manager.disconnect(websocket, user_id)
