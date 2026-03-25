"""
WebSocket manager for real-time updates.
Handles client connections, broadcasting, and pub/sub coordination.
"""

from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, Set, List
import structlog
import json
import asyncio
from datetime import datetime

logger = structlog.get_logger()


class ConnectionManager:
    """
    Manages WebSocket connections for real-time updates.
    
    Features:
    - Per-user connection tracking
    - Broadcasting to specific users
    - Group/room support for team features (Phase 2)
    - Message queuing and delivery guarantees
    """
    
    def __init__(self):
        # user_id -> Set of WebSocket connections
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        
        # Room-based connections for Phase 2 (team features)
        self.room_connections: Dict[str, Set[WebSocket]] = {}
        
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, user_id: str):
        """Accept and register a new WebSocket connection."""
        await websocket.accept()
        
        async with self._lock:
            if user_id not in self.active_connections:
                self.active_connections[user_id] = set()
            self.active_connections[user_id].add(websocket)
        
        logger.info("WebSocket connected", user_id=user_id, 
                   total_connections=len(self.active_connections[user_id]))
        
        # Send connection acknowledgment
        await self.send_personal_message(user_id, {
            "type": "connection",
            "status": "connected",
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def disconnect(self, websocket: WebSocket, user_id: str):
        """Remove a WebSocket connection."""
        async with self._lock:
            if user_id in self.active_connections:
                self.active_connections[user_id].discard(websocket)
                
                # Clean up empty user entries
                if not self.active_connections[user_id]:
                    del self.active_connections[user_id]
        
        logger.info("WebSocket disconnected", user_id=user_id)
    
    async def send_personal_message(self, user_id: str, message: dict):
        """
        Send a message to all connections for a specific user.
        Supports multiple devices (desktop + web + mobile).
        """
        if user_id not in self.active_connections:
            logger.debug("No active connections for user", user_id=user_id)
            return
        
        # Add timestamp if not present
        if "timestamp" not in message:
            message["timestamp"] = datetime.utcnow().isoformat()
        
        message_json = json.dumps(message)
        dead_connections = set()
        
        for connection in self.active_connections[user_id]:
            try:
                await connection.send_text(message_json)
            except Exception as e:
                logger.error("Failed to send message", user_id=user_id, error=str(e))
                dead_connections.add(connection)
        
        # Clean up dead connections
        if dead_connections:
            async with self._lock:
                self.active_connections[user_id] -= dead_connections
    
    async def broadcast_to_room(self, room_id: str, message: dict):
        """
        Broadcast message to all users in a room.
        
        TODO: Implement in Phase 2 for team features
        - Team activity feeds
        - Shared workspace updates
        - Real-time collaboration notifications
        """
        logger.info("Room broadcast (stub)", room_id=room_id)
        pass
    
    async def get_active_users(self) -> List[str]:
        """Get list of currently connected user IDs."""
        return list(self.active_connections.keys())
    
    async def get_connection_count(self, user_id: str) -> int:
        """Get number of active connections for a user."""
        return len(self.active_connections.get(user_id, set()))


# Global connection manager instance
manager = ConnectionManager()


# Message types for real-time updates
class MessageType:
    """Standard message types for WebSocket communication."""
    
    # Activity updates
    ACTIVITY_CREATED = "activity.created"
    ACTIVITY_SYNCED = "activity.synced"
    
    # Entity updates (Phase 1 Month 4)
    ENTITY_EXTRACTED = "entity.extracted"
    ENTITY_MERGED = "entity.merged"
    
    # Graph updates (Phase 1 Month 5)
    GRAPH_UPDATED = "graph.updated"
    RELATIONSHIP_CREATED = "relationship.created"
    
    # Analytics updates (Phase 1 Month 6)
    METRICS_UPDATED = "metrics.updated"
    SUMMARY_GENERATED = "summary.generated"
    
    # System messages
    SYNC_STATUS = "sync.status"
    NOTIFICATION = "notification"
    ERROR = "error"
    
    # Phase 2: Team features
    TEAM_ACTIVITY = "team.activity"
    TEAM_ALERT = "team.alert"


async def notify_activity_created(user_id: str, activity_data: dict):
    """Helper to notify user of new activity creation."""
    await manager.send_personal_message(user_id, {
        "type": MessageType.ACTIVITY_CREATED,
        "data": activity_data
    })


async def notify_entity_extracted(user_id: str, entity_data: dict):
    """Helper to notify user of new entity extraction (Phase 1 Month 4)."""
    await manager.send_personal_message(user_id, {
        "type": MessageType.ENTITY_EXTRACTED,
        "data": entity_data
    })


async def notify_sync_status(user_id: str, status: str, details: dict = None):
    """Helper to notify user of sync status updates."""
    await manager.send_personal_message(user_id, {
        "type": MessageType.SYNC_STATUS,
        "status": status,
        "details": details or {}
    })
