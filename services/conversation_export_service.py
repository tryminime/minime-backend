"""
Conversation Export Service

Exports AI chat conversations to multiple formats:
- Markdown export
- JSON export
- PDF export (via Jinja2 template)
- Bulk export (all conversations)
"""

from typing import Dict, List, Optional, Any
from datetime import datetime
import json
import structlog

logger = structlog.get_logger()


class ConversationExportService:
    """
    Service for exporting conversations to various formats.
    """

    SUPPORTED_FORMATS = ['markdown', 'json', 'pdf', 'txt']

    def __init__(self):
        pass

    # ========================================================================
    # SINGLE CONVERSATION EXPORT
    # ========================================================================

    def export_conversation(
        self,
        conversation: Dict[str, Any],
        format: str = 'markdown',
    ) -> Dict[str, Any]:
        """
        Export a single conversation to the specified format.

        Args:
            conversation: Conversation dict with messages
            format: Output format ('markdown', 'json', 'pdf', 'txt')

        Returns:
            Dict with 'content' (string), 'format', and 'filename'
        """
        if format not in self.SUPPORTED_FORMATS:
            return {
                'error': f'Unsupported format: {format}. '
                         f'Supported: {", ".join(self.SUPPORTED_FORMATS)}',
                'content': '',
                'format': format,
            }

        title = conversation.get('title', 'Conversation')
        conv_id = conversation.get('id', 'unknown')
        messages = conversation.get('messages', [])

        if format == 'markdown':
            content = self._to_markdown(conversation)
        elif format == 'json':
            content = self._to_json(conversation)
        elif format == 'pdf':
            content = self._to_pdf_html(conversation)
        elif format == 'txt':
            content = self._to_plaintext(conversation)
        else:
            content = ''

        # Generate filename
        safe_title = ''.join(c if c.isalnum() or c in ' -_' else '' for c in title)
        safe_title = safe_title.strip().replace(' ', '_')[:50]
        ext = 'html' if format == 'pdf' else ('md' if format == 'markdown' else format)
        filename = f"conversation_{safe_title}_{conv_id[:8]}.{ext}"

        return {
            'content': content,
            'format': format,
            'filename': filename,
            'conversation_id': conv_id,
            'message_count': len(messages),
            'exported_at': datetime.now(tz=None).isoformat(),
        }

    # ========================================================================
    # BULK EXPORT
    # ========================================================================

    def export_all(
        self,
        conversations: List[Dict[str, Any]],
        format: str = 'markdown',
    ) -> Dict[str, Any]:
        """
        Export all conversations.

        Args:
            conversations: List of conversation dicts
            format: Output format

        Returns:
            Dict with list of exported conversations
        """
        exports = []
        for conv in conversations:
            export = self.export_conversation(conv, format=format)
            exports.append(export)

        return {
            'exports': exports,
            'total': len(exports),
            'format': format,
            'exported_at': datetime.now(tz=None).isoformat(),
        }

    def export_combined(
        self,
        conversations: List[Dict[str, Any]],
        format: str = 'markdown',
    ) -> Dict[str, Any]:
        """Export all conversations into a single combined document."""
        if format == 'json':
            content = json.dumps({
                'conversations': conversations,
                'exported_at': datetime.now(tz=None).isoformat(),
                'total_conversations': len(conversations),
            }, indent=2, default=str)
        else:
            parts = []
            for conv in conversations:
                if format == 'markdown':
                    parts.append(self._to_markdown(conv))
                else:
                    parts.append(self._to_plaintext(conv))
                parts.append('\n---\n\n')

            content = ''.join(parts)

        return {
            'content': content,
            'format': format,
            'filename': f"all_conversations.{'json' if format == 'json' else 'md'}",
            'conversation_count': len(conversations),
            'exported_at': datetime.now(tz=None).isoformat(),
        }

    # ========================================================================
    # FORMAT CONVERTERS
    # ========================================================================

    def _to_markdown(self, conversation: Dict[str, Any]) -> str:
        """Convert conversation to Markdown."""
        title = conversation.get('title', 'Conversation')
        created = conversation.get('created_at', '')[:10]
        messages = conversation.get('messages', [])

        lines = [
            f"# {title}",
            f"",
            f"**Date:** {created}  ",
            f"**Messages:** {len(messages)}  ",
            f"**ID:** `{conversation.get('id', 'N/A')}`",
            f"",
            "---",
            "",
        ]

        for msg in messages:
            role = msg.get('role', 'unknown')
            content = msg.get('content', '')
            timestamp = msg.get('created_at', '')[:19]

            if role == 'user':
                lines.append(f"### 🧑 You ({timestamp})")
            else:
                model = msg.get('model', '')
                model_info = f" — {model}" if model else ''
                lines.append(f"### 🤖 MiniMe AI ({timestamp}{model_info})")

            lines.append("")
            lines.append(content)
            lines.append("")

            # Citations
            citations = msg.get('citations', [])
            if citations:
                lines.append("**Sources:**")
                for i, cite in enumerate(citations, 1):
                    lines.append(f"  [{i}] {cite.get('title', 'Source')} — {cite.get('snippet', '')[:80]}")
                lines.append("")

        return '\n'.join(lines)

    def _to_json(self, conversation: Dict[str, Any]) -> str:
        """Convert conversation to JSON."""
        export = {
            'id': conversation.get('id'),
            'title': conversation.get('title'),
            'created_at': conversation.get('created_at'),
            'message_count': len(conversation.get('messages', [])),
            'messages': conversation.get('messages', []),
            'exported_at': datetime.now(tz=None).isoformat(),
        }
        return json.dumps(export, indent=2, default=str)

    def _to_plaintext(self, conversation: Dict[str, Any]) -> str:
        """Convert conversation to plain text."""
        title = conversation.get('title', 'Conversation')
        messages = conversation.get('messages', [])

        lines = [
            title,
            '=' * len(title),
            '',
        ]

        for msg in messages:
            role = 'You' if msg.get('role') == 'user' else 'MiniMe AI'
            content = msg.get('content', '')
            timestamp = msg.get('created_at', '')[:19]

            lines.append(f"[{timestamp}] {role}:")
            lines.append(content)
            lines.append('')

        return '\n'.join(lines)

    def _to_pdf_html(self, conversation: Dict[str, Any]) -> str:
        """
        Convert conversation to styled HTML (for PDF generation).
        Uses inline CSS for email/PDF compatibility.
        """
        title = conversation.get('title', 'Conversation')
        created = conversation.get('created_at', '')[:10]
        messages = conversation.get('messages', [])

        html_parts = [
            '<!DOCTYPE html>',
            '<html><head>',
            '<meta charset="utf-8">',
            f'<title>{title}</title>',
            '<style>',
            'body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; '
            'max-width: 800px; margin: 0 auto; padding: 20px; color: #1a1a1a; }',
            'h1 { color: #6366F1; border-bottom: 2px solid #6366F1; padding-bottom: 10px; }',
            '.meta { color: #666; font-size: 14px; margin-bottom: 20px; }',
            '.message { margin: 16px 0; padding: 12px 16px; border-radius: 8px; }',
            '.user { background: #F0F0FF; border-left: 3px solid #6366F1; }',
            '.assistant { background: #F0FFF0; border-left: 3px solid #10B981; }',
            '.role { font-weight: 600; font-size: 13px; color: #444; margin-bottom: 6px; }',
            '.content { white-space: pre-wrap; line-height: 1.6; }',
            '.timestamp { font-size: 11px; color: #999; }',
            '.citations { font-size: 12px; color: #666; margin-top: 8px; padding-top: 8px; border-top: 1px solid #ddd; }',
            '</style>',
            '</head><body>',
            f'<h1>{title}</h1>',
            f'<div class="meta">Date: {created} | Messages: {len(messages)}</div>',
        ]

        for msg in messages:
            role = msg.get('role', 'unknown')
            content = msg.get('content', '').replace('<', '&lt;').replace('>', '&gt;')
            timestamp = msg.get('created_at', '')[:19]
            css_class = 'user' if role == 'user' else 'assistant'
            role_label = '🧑 You' if role == 'user' else '🤖 MiniMe AI'

            html_parts.append(f'<div class="message {css_class}">')
            html_parts.append(f'<div class="role">{role_label} <span class="timestamp">{timestamp}</span></div>')
            html_parts.append(f'<div class="content">{content}</div>')

            citations = msg.get('citations', [])
            if citations:
                html_parts.append('<div class="citations"><strong>Sources:</strong><br>')
                for i, cite in enumerate(citations, 1):
                    html_parts.append(f'[{i}] {cite.get("title", "Source")}<br>')
                html_parts.append('</div>')

            html_parts.append('</div>')

        html_parts.append('</body></html>')
        return '\n'.join(html_parts)

    # ========================================================================
    # STATS
    # ========================================================================

    def get_export_stats(
        self,
        conversations: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Get statistics about exportable data."""
        total_messages = sum(len(c.get('messages', [])) for c in conversations)
        total_citations = 0
        for c in conversations:
            for m in c.get('messages', []):
                total_citations += len(m.get('citations', []))

        return {
            'total_conversations': len(conversations),
            'total_messages': total_messages,
            'total_citations': total_citations,
            'supported_formats': self.SUPPORTED_FORMATS,
        }


# Global instance
conversation_export_service = ConversationExportService()
