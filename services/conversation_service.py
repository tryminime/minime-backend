"""
Conversation Service

Manages conversation persistence, memory, and history:
- CRUD operations for conversations
- Message persistence (user + assistant)
- Sliding window memory with summarization
- Auto-title generation from first message
- Conversation archiving and deletion
"""

from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from collections import defaultdict
import uuid
import structlog

logger = structlog.get_logger()


# ============================================================================
# CONVERSATION MODELS (in-memory, DB-agnostic)
# ============================================================================

class Message:
    """Represents a single chat message."""

    __slots__ = ('id', 'conversation_id', 'role', 'content', 'model',
                 'tokens', 'context_used', 'citations', 'created_at')

    def __init__(
        self,
        conversation_id: str,
        role: str,
        content: str,
        model: Optional[str] = None,
        tokens: int = 0,
        context_used: Optional[Dict[str, Any]] = None,
        citations: Optional[List[Dict[str, Any]]] = None,
        message_id: Optional[str] = None,
        created_at: Optional[str] = None,
    ):
        self.id = message_id or str(uuid.uuid4())
        self.conversation_id = conversation_id
        self.role = role
        self.content = content
        self.model = model
        self.tokens = tokens
        self.context_used = context_used
        self.citations = citations or []
        self.created_at = created_at or datetime.now(tz=None).isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'conversation_id': self.conversation_id,
            'role': self.role,
            'content': self.content,
            'model': self.model,
            'tokens': self.tokens,
            'context_used': self.context_used,
            'citations': self.citations,
            'created_at': self.created_at,
        }

    def to_llm_message(self) -> Dict[str, str]:
        """Convert to LLM-compatible format."""
        return {'role': self.role, 'content': self.content}


class Conversation:
    """Represents a conversation with metadata."""

    __slots__ = ('id', 'user_id', 'title', 'summary', 'context_enabled',
                 'archived', 'created_at', 'updated_at', 'last_message_at',
                 'messages', 'message_count')

    def __init__(
        self,
        user_id: str,
        title: Optional[str] = None,
        conversation_id: Optional[str] = None,
        context_enabled: bool = True,
        archived: bool = False,
        created_at: Optional[str] = None,
    ):
        self.id = conversation_id or str(uuid.uuid4())
        self.user_id = user_id
        self.title = title or 'New Conversation'
        self.summary = ''
        self.context_enabled = context_enabled
        self.archived = archived
        now = created_at or datetime.now(tz=None).isoformat()
        self.created_at = now
        self.updated_at = now
        self.last_message_at = now
        self.messages: List[Message] = []
        self.message_count = 0

    def to_dict(self, include_messages: bool = False) -> Dict[str, Any]:
        result = {
            'id': self.id,
            'user_id': self.user_id,
            'title': self.title,
            'summary': self.summary,
            'context_enabled': self.context_enabled,
            'archived': self.archived,
            'message_count': self.message_count,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'last_message_at': self.last_message_at,
        }
        if include_messages:
            result['messages'] = [m.to_dict() for m in self.messages]
        return result


class ConversationService:
    """
    Service for managing conversations and messages.

    Uses an in-memory store by default (swap for DB adapter in production).
    """

    # Memory config
    MEMORY_WINDOW_SIZE = 20      # Keep last N messages in context
    SUMMARY_THRESHOLD = 30       # Summarize when messages exceed this
    MAX_TITLE_LENGTH = 80        # Max auto-generated title length

    def __init__(self):
        # In-memory stores (keyed by user_id → conversation_id)
        self._conversations: Dict[str, Dict[str, Conversation]] = defaultdict(dict)

    # ========================================================================
    # CONVERSATION CRUD
    # ========================================================================

    def create_conversation(
        self,
        user_id: str,
        title: Optional[str] = None,
        context_enabled: bool = True,
    ) -> Dict[str, Any]:
        """Create a new conversation."""
        conv = Conversation(
            user_id=user_id,
            title=title,
            context_enabled=context_enabled,
        )
        self._conversations[user_id][conv.id] = conv

        logger.info("conversation_created", user_id=user_id, conversation_id=conv.id)
        return conv.to_dict()

    def get_conversation(
        self,
        user_id: str,
        conversation_id: str,
        include_messages: bool = True,
    ) -> Optional[Dict[str, Any]]:
        """Get a conversation by ID."""
        conv = self._conversations.get(user_id, {}).get(conversation_id)
        if conv is None:
            return None
        return conv.to_dict(include_messages=include_messages)

    def list_conversations(
        self,
        user_id: str,
        include_archived: bool = False,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """List all conversations for a user."""
        user_convs = self._conversations.get(user_id, {})

        conversations = []
        for conv in user_convs.values():
            if not include_archived and conv.archived:
                continue
            conversations.append(conv.to_dict(include_messages=False))

        # Sort by last_message_at descending
        conversations.sort(key=lambda x: x['last_message_at'], reverse=True)

        total = len(conversations)
        paginated = conversations[offset:offset + limit]

        return {
            'conversations': paginated,
            'total': total,
            'limit': limit,
            'offset': offset,
        }

    def archive_conversation(
        self,
        user_id: str,
        conversation_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Archive a conversation."""
        conv = self._conversations.get(user_id, {}).get(conversation_id)
        if conv is None:
            return None

        conv.archived = True
        conv.updated_at = datetime.now(tz=None).isoformat()
        return conv.to_dict()

    def delete_conversation(
        self,
        user_id: str,
        conversation_id: str,
    ) -> bool:
        """Delete a conversation and all its messages."""
        user_convs = self._conversations.get(user_id, {})
        if conversation_id in user_convs:
            del user_convs[conversation_id]
            logger.info("conversation_deleted", user_id=user_id, conversation_id=conversation_id)
            return True
        return False

    def update_conversation(
        self,
        user_id: str,
        conversation_id: str,
        title: Optional[str] = None,
        context_enabled: Optional[bool] = None,
    ) -> Optional[Dict[str, Any]]:
        """Update conversation metadata."""
        conv = self._conversations.get(user_id, {}).get(conversation_id)
        if conv is None:
            return None

        if title is not None:
            conv.title = title
        if context_enabled is not None:
            conv.context_enabled = context_enabled
        conv.updated_at = datetime.now(tz=None).isoformat()

        return conv.to_dict()

    # ========================================================================
    # MESSAGE MANAGEMENT
    # ========================================================================

    def add_message(
        self,
        user_id: str,
        conversation_id: str,
        role: str,
        content: str,
        model: Optional[str] = None,
        tokens: int = 0,
        citations: Optional[List[Dict[str, Any]]] = None,
        context_used: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Add a message to a conversation."""
        conv = self._conversations.get(user_id, {}).get(conversation_id)
        if conv is None:
            # Auto-create conversation
            self.create_conversation(user_id)
            self._conversations[user_id][conversation_id] = Conversation(
                user_id=user_id,
                conversation_id=conversation_id,
            )
            conv = self._conversations[user_id][conversation_id]

        msg = Message(
            conversation_id=conversation_id,
            role=role,
            content=content,
            model=model,
            tokens=tokens,
            citations=citations,
            context_used=context_used,
        )

        conv.messages.append(msg)
        conv.message_count += 1
        conv.last_message_at = msg.created_at
        conv.updated_at = msg.created_at

        # Auto-title from first user message
        if conv.message_count == 1 and role == 'user':
            conv.title = self._generate_title(content)

        return msg.to_dict()

    def get_messages(
        self,
        user_id: str,
        conversation_id: str,
        limit: int = 100,
        before: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Get messages from a conversation."""
        conv = self._conversations.get(user_id, {}).get(conversation_id)
        if conv is None:
            return []

        messages = conv.messages
        if before:
            messages = [m for m in messages if m.created_at < before]

        # Return most recent `limit` messages
        return [m.to_dict() for m in messages[-limit:]]

    # ========================================================================
    # CONVERSATION MEMORY
    # ========================================================================

    def get_memory_context(
        self,
        user_id: str,
        conversation_id: str,
        window_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Get conversation memory for LLM context.

        Returns a sliding window of recent messages plus an optional
        summary of older messages.

        Args:
            user_id: User ID
            conversation_id: Conversation ID
            window_size: Number of recent messages to include

        Returns:
            Memory context with messages and optional summary
        """
        window_size = window_size or self.MEMORY_WINDOW_SIZE
        conv = self._conversations.get(user_id, {}).get(conversation_id)

        if conv is None or not conv.messages:
            return {
                'messages': [],
                'summary': '',
                'total_messages': 0,
                'window_size': window_size,
                'has_summary': False,
            }

        total = len(conv.messages)
        recent_messages = conv.messages[-window_size:]

        # Build summary for older messages
        summary = ''
        has_summary = False
        if total > window_size:
            older_messages = conv.messages[:total - window_size]
            summary = self._summarize_messages(older_messages)
            has_summary = True

        return {
            'messages': [m.to_llm_message() for m in recent_messages],
            'summary': summary,
            'total_messages': total,
            'window_size': window_size,
            'has_summary': has_summary,
        }

    def build_llm_messages(
        self,
        user_id: str,
        conversation_id: str,
        system_prompt: str,
        current_message: str,
        window_size: Optional[int] = None,
    ) -> List[Dict[str, str]]:
        """
        Build the full message list for an LLM call, including system prompt,
        memory summary, conversation history, and current message.

        Returns:
            List of LLM-format messages ready for API call
        """
        memory = self.get_memory_context(user_id, conversation_id, window_size)
        messages = [{'role': 'system', 'content': system_prompt}]

        # Include summary of older messages if available
        if memory['has_summary']:
            messages.append({
                'role': 'system',
                'content': f"Previous conversation summary:\n{memory['summary']}",
            })

        # Add recent conversation history
        messages.extend(memory['messages'])

        # Add the current user message
        messages.append({'role': 'user', 'content': current_message})

        return messages

    # ========================================================================
    # STATISTICS
    # ========================================================================

    def get_conversation_stats(
        self,
        user_id: str,
    ) -> Dict[str, Any]:
        """Get conversation statistics for a user."""
        user_convs = self._conversations.get(user_id, {})

        total_conversations = len(user_convs)
        total_messages = sum(c.message_count for c in user_convs.values())
        archived_count = sum(1 for c in user_convs.values() if c.archived)
        active_count = total_conversations - archived_count

        return {
            'total_conversations': total_conversations,
            'active_conversations': active_count,
            'archived_conversations': archived_count,
            'total_messages': total_messages,
            'avg_messages_per_conversation': round(
                total_messages / max(total_conversations, 1), 1
            ),
        }

    # ========================================================================
    # INTERNAL HELPERS
    # ========================================================================

    def _generate_title(self, first_message: str) -> str:
        """Generate a conversation title from the first message."""
        # Take first sentence or first N chars
        title = first_message.strip()

        # Try to get first sentence
        for sep in ['. ', '? ', '! ', '\n']:
            idx = title.find(sep)
            if idx > 0 and idx < self.MAX_TITLE_LENGTH:
                title = title[:idx + 1]
                break

        if len(title) > self.MAX_TITLE_LENGTH:
            title = title[:self.MAX_TITLE_LENGTH - 3] + '...'

        return title

    def _summarize_messages(self, messages: List[Message]) -> str:
        """
        Create a text summary of older messages.

        For now uses extractive summarization (key points).
        Can be replaced with LLM-based summarization.
        """
        if not messages:
            return ''

        # Extract key topics from user messages
        user_topics = []
        assistant_points = []

        for msg in messages:
            text = msg.content.strip()
            if msg.role == 'user':
                # Take first 100 chars of each user message
                snippet = text[:100] + ('...' if len(text) > 100 else '')
                user_topics.append(snippet)
            elif msg.role == 'assistant':
                # Take first line of each assistant response
                first_line = text.split('\n')[0][:120]
                assistant_points.append(first_line)

        parts = []
        if user_topics:
            parts.append(f"User asked about: {'; '.join(user_topics[:5])}")
        if assistant_points:
            parts.append(f"Key responses: {'; '.join(assistant_points[:5])}")

        return ' | '.join(parts)


# Global instance
conversation_service = ConversationService()
