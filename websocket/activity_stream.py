"""
WebSocket Activity Stream Manager.
Manages WebSocket connections and broadcasts activity events in real-time.
"""

from fastapi import WebSocket, WebSocketDisconnect
from typing import Dict, List, Set
from uuid import UUID
import asyncio
import json
import structlog
from datetime import datetime

logger = structlog.get_logger()


class ActivityStreamManager:
    """Manages WebSocket connections for real-time activity streaming."""
    
    def __init__(self):
        # Map of user_id -> list of active WebSocket connections
        self.active_connections: Dict[UUID, List[WebSocket]] = {}
        
        # Set of all connected socket IDs for quick lookup
        self.connection_ids: Set[str] = set()
        
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()
    
    async def connect(self, user_id: UUID, websocket: WebSocket, connection_id: str):
        """
        Add a new WebSocket connection for a user.
        
        Args:
            user_id: User UUID
            websocket: WebSocket connection
            connection_id: Unique connection identifier
        """
        await websocket.accept()
        
        async with self._lock:
            if user_id not in self.active_connections:
                self.active_connections[user_id] = []
            
            self.active_connections[user_id].append(websocket)
            self.connection_ids.add(connection_id)
        
        logger.info(
            "websocket_connected",
            user_id=str(user_id),
            connection_id=connection_id,
            total_connections=len(self.connection_ids)
        )
        
        # Send welcome message
        await websocket.send_json({
            "type": "connection.established",
            "user_id": str(user_id),
            "connection_id": connection_id,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def disconnect(self, user_id: UUID, websocket: WebSocket, connection_id: str):
        """
        Remove a WebSocket connection.
        
        Args:
            user_id: User UUID
            websocket: WebSocket connection to remove
            connection_id: Connection identifier
        """
        async with self._lock:
            if user_id in self.active_connections:
                try:
                    self.active_connections[user_id].remove(websocket)
                    
                    # Clean up empty lists
                    if not self.active_connections[user_id]:
                        del self.active_connections[user_id]
                except ValueError:
                    pass  # Connection already removed
            
            self.connection_ids.discard(connection_id)
        
        logger.info(
            "websocket_disconnected",
            user_id=str(user_id),
            connection_id=connection_id,
            remaining_connections=len(self.connection_ids)
        )
    
    async def broadcast_activity(
        self,
        user_id: UUID,
        activity: Dict,
        event_type: str = "activity.created"
    ):
        """
        Broadcast activity event to all user's connected clients.
        
        Args:
            user_id: Target user UUID
            activity: Activity data dictionary
            event_type: Event type string
        """
        if user_id not in self.active_connections:
            return  # No active connections for this user
        
        message = {
            "type": event_type,
            "user_id": str(user_id),
            "activity": activity,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Get connections for this user
        connections = self.active_connections.get(user_id, [])
        
        # Track failed sends
        failed_connections = []
        
        for websocket in connections:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.warning(
                    "broadcast_failed",
                    user_id=str(user_id),
                    error=str(e)
                )
                failed_connections.append(websocket)
        
        # Clean up failed connections
        if failed_connections:
            async with self._lock:
                for ws in failed_connections:
                    try:
                        self.active_connections[user_id].remove(ws)
                    except (ValueError, KeyError):
                        pass
        
        logger.debug(
            "activity_broadcasted",
            user_id=str(user_id),
            event_type=event_type,
            sent_to=len(connections) - len(failed_connections),
            failed=len(failed_connections)
        )
    
    async def broadcast_entity(
        self,
        user_id: UUID,
        entity: Dict,
        event_type: str = "entity.created"
    ):
        """
        Broadcast entity event to all user's connected clients.
        
        Args:
            user_id: Target user UUID
            entity: Entity data dictionary
            event_type: Event type string
        """
        if user_id not in self.active_connections:
            return
        
        message = {
            "type": event_type,
            "user_id": str(user_id),
            "entity": entity,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        connections = self.active_connections.get(user_id, [])
        
        for websocket in connections:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.warning(
                    "entity_broadcast_failed",
                    user_id=str(user_id),
                    error=str(e)
                )
    
    async def send_heartbeat(self, websocket: WebSocket):
        """
        Send heartbeat ping to check connection health.
        
        Args:
            websocket: WebSocket connection
        """
        try:
            await websocket.send_json({
                "type": "ping",
                "timestamp": datetime.utcnow().isoformat()
            })
        except Exception as e:
            logger.warning("heartbeat_failed", error=str(e))
    
    async def handle_client_message(
        self,
        user_id: UUID,
        websocket: WebSocket,
        message: Dict
    ):
        """
        Handle incoming messages from clients.
        
        Supports:
        - pong: Heartbeat response
        - subscribe: Subscribe to specific event types
        - unsubscribe: Unsubscribe from event types
        
        Args:
            user_id: User UUID
            websocket: WebSocket connection
            message: Parsed JSON message
        """
        msg_type = message.get("type")
        
        if msg_type == "pong":
            logger.debug("heartbeat_received", user_id=str(user_id))
            
        elif msg_type == "subscribe":
            # Future: Implement selective event subscriptions
            await websocket.send_json({
                "type": "subscribed",
                "event_types": message.get("event_types", []),
                "timestamp": datetime.utcnow().isoformat()
            })
            
        elif msg_type == "unsubscribe":
            # Future: Handle unsubscribe
            await websocket.send_json({
                "type": "unsubscribed",
                "event_types": message.get("event_types", []),
                "timestamp": datetime.utcnow().isoformat()
            })
        
        else:
            logger.warning(
                "unknown_message_type",
                user_id=str(user_id),
                type=msg_type
            )
    
    def get_connection_count(self) -> int:
        """Get total number of active connections."""
        return len(self.connection_ids)
    
    def get_user_connection_count(self, user_id: UUID) -> int:
        """Get number of connections for a specific user."""
        return len(self.active_connections.get(user_id, []))
    
    def get_stats(self) -> Dict:
        """Get connection statistics."""
        return {
            "total_connections": len(self.connection_ids),
            "unique_users": len(self.active_connections),
            "avg_connections_per_user": (
                len(self.connection_ids) / len(self.active_connections)
                if self.active_connections else 0
            )
        }


# Global instance
activity_stream_manager = ActivityStreamManager()
