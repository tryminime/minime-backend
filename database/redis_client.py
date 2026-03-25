"""
Redis client for caching and real-time coordination.
"""

import redis.asyncio as redis
from typing import Optional, Any
import json
import structlog

from config import settings

logger = structlog.get_logger()

# Global Redis client
client: Optional[redis.Redis] = None


async def init_redis():
    """Initialize Redis connection pool."""
    global client
    
    client = await redis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True,
        max_connections=50
    )
    
    # Test connection
    await client.ping()
    logger.info("Redis client initialized")


async def close_redis():
    """Close Redis connections."""
    global client
    if client:
        await client.close()
        logger.info("Redis connections closed")


def get_redis_client() -> redis.Redis:
    """Get Redis client instance."""
    if client is None:
        raise RuntimeError("Redis client not initialized")
    return client


async def cache_set(key: str, value: Any, expire: int = 3600) -> bool:
    """
    Set a value in cache with expiration.
    
    Args:
        key: Cache key
        value: Value to cache (will be JSON serialized)
        expire: Expiration time in seconds (default 1 hour)
        
    Returns:
        Success status
    """
    serialized = json.dumps(value) if not isinstance(value, str) else value
    return await client.setex(key, expire, serialized)


async def cache_get(key: str) -> Optional[Any]:
    """
    Get a value from cache.
    
    Args:
        key: Cache key
        
    Returns:
        Cached value or None if not found
    """
    value = await client.get(key)
    if value is None:
        return None
    
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


async def cache_delete(key: str) -> bool:
    """
    Delete a key from cache.
    
    Args:
        key: Cache key
        
    Returns:
        Success status
    """
    return await client.delete(key) > 0


async def cache_exists(key: str) -> bool:
    """Check if a key exists in cache."""
    return await client.exists(key) > 0


async def increment_counter(key: str, amount: int = 1) -> int:
    """
    Increment a counter.
    
    Args:
        key: Counter key
        amount: Amount to increment by
        
    Returns:
        New counter value
    """
    return await client.incrby(key, amount)


async def rate_limit_check(
    identifier: str,
    limit: int,
    window: int = 60
) -> bool:
    """
    Check if identifier is within rate limit.
    
    Args:
        identifier: Unique identifier (e.g., user_id, IP address)
        limit: Maximum requests allowed
        window: Time window in seconds
        
    Returns:
        True if within limit, False if exceeded
    """
    key = f"rate_limit:{identifier}"
    
    # Use sliding window with sorted sets
    import time
    now = time.time()
    window_start = now - window
    
    # Remove old entries
    await client.zremrangebyscore(key, 0, window_start)
    
    # Count requests in window
    count = await client.zcard(key)
    
    if count >= limit:
        return False
    
    # Add new request
    await client.zadd(key, {str(now): now})
    await client.expire(key, window)
    
    return True


async def publish_message(channel: str, message: dict) -> int:
    """
    Publish a message to a Redis pub/sub channel.
    
    Args:
        channel: Channel name
        message: Message to publish (will be JSON serialized)
        
    Returns:
        Number of subscribers that received the message
    """
    serialized = json.dumps(message)
    return await client.publish(channel, serialized)


async def subscribe_channel(channel: str):
    """
    Subscribe to a Redis pub/sub channel.
    
    Args:
        channel: Channel name
        
    Returns:
        Async iterator of messages
    """
    pubsub = client.pubsub()
    await pubsub.subscribe(channel)
    
    async for message in pubsub.listen():
        if message["type"] == "message":
            try:
                yield json.loads(message["data"])
            except json.JSONDecodeError:
                yield message["data"]
