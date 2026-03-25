"""
Plugin/Extension System Service.

Provides an extensible framework for adding custom AI capabilities
via prompt-based plugins (no arbitrary code execution).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional


# =====================================================
# BUILT-IN PLUGINS
# =====================================================

BUILTIN_PLUGINS: List[Dict[str, Any]] = [
    {
        "id": "builtin_productivity_coach",
        "name": "Productivity Coach",
        "description": "Provides actionable productivity tips based on your work patterns. Analyzes focus sessions, context switches, and break habits.",
        "version": "1.0.0",
        "author": "MiniMe",
        "icon": "🏋️",
        "category": "productivity",
        "builtin": True,
        "enabled": True,
        "hooks": ["pre_chat"],
        "system_prompt": (
            "You are also a productivity coach. When the user asks about productivity, work habits, "
            "or time management, provide specific, data-driven advice based on their activity patterns. "
            "Suggest techniques like Pomodoro, time-blocking, and deep work scheduling. "
            "Reference their actual metrics when available."
        ),
        "config": {},
    },
    {
        "id": "builtin_code_reviewer",
        "name": "Code Reviewer",
        "description": "Reviews code snippets shared in chat with best practices, security considerations, and performance tips.",
        "version": "1.0.0",
        "author": "MiniMe",
        "icon": "🔍",
        "category": "development",
        "builtin": True,
        "enabled": False,
        "hooks": ["pre_chat"],
        "system_prompt": (
            "When the user shares code snippets, act as an expert code reviewer. "
            "Check for: security vulnerabilities, performance issues, code style, "
            "error handling, and suggest improvements. Be specific and constructive."
        ),
        "config": {},
    },
    {
        "id": "builtin_meeting_summarizer",
        "name": "Meeting Summarizer",
        "description": "Summarizes meeting notes and extracts action items, decisions, and follow-ups.",
        "version": "1.0.0",
        "author": "MiniMe",
        "icon": "📋",
        "category": "communication",
        "builtin": True,
        "enabled": False,
        "hooks": ["pre_chat"],
        "system_prompt": (
            "When the user shares meeting notes or asks about meetings, structure the response as: "
            "1. Key Decisions, 2. Action Items (with owners if mentioned), 3. Follow-ups, "
            "4. Summary. Be concise and actionable."
        ),
        "config": {},
    },
    {
        "id": "builtin_learning_tracker",
        "name": "Learning Tracker",
        "description": "Tracks learning progress, suggests study plans, and provides spaced repetition reminders.",
        "version": "1.0.0",
        "author": "MiniMe",
        "icon": "📚",
        "category": "learning",
        "builtin": True,
        "enabled": True,
        "hooks": ["pre_chat"],
        "system_prompt": (
            "You are also a learning coach. When discussing skills or learning topics, "
            "suggest structured learning paths, recommend spaced repetition for retention, "
            "and track the user's progress across skills in their knowledge graph. "
            "Reference their entity data for personalized recommendations."
        ),
        "config": {},
    },
]


class PluginManager:
    """
    Manages AI plugins. Plugins modify AI behavior via prompt injection.
    No arbitrary code execution — strictly prompt-based for security.

    Storage: User plugins persisted in user preferences JSON.
    Built-in plugins are always available.
    """

    def __init__(self):
        """Initialize with built-in plugins."""
        # In-memory cache: user_id → list of custom plugins
        self._user_plugins: Dict[str, List[Dict[str, Any]]] = {}

    def _get_user_plugins(self, user_id: str) -> List[Dict[str, Any]]:
        """Get user's custom plugins from cache."""
        return self._user_plugins.get(user_id, [])

    def list_plugins(self, user_id: str) -> List[Dict[str, Any]]:
        """List all plugins (built-in + user custom)."""
        builtins = [dict(p) for p in BUILTIN_PLUGINS]
        customs = self._get_user_plugins(user_id)
        return builtins + customs

    def get_enabled_plugins(self, user_id: str) -> List[Dict[str, Any]]:
        """Get only enabled plugins."""
        return [p for p in self.list_plugins(user_id) if p.get("enabled", False)]

    def get_enabled_system_prompts(self, user_id: str) -> str:
        """Build combined system prompt from enabled plugins."""
        enabled = self.get_enabled_plugins(user_id)
        prompts = [p["system_prompt"] for p in enabled if p.get("system_prompt")]
        if not prompts:
            return ""
        return "\n\n--- Plugin Context ---\n" + "\n\n".join(prompts)

    def get_plugin(self, user_id: str, plugin_id: str) -> Optional[Dict[str, Any]]:
        """Get a specific plugin by ID."""
        for p in self.list_plugins(user_id):
            if p["id"] == plugin_id:
                return p
        return None

    def create_plugin(
        self,
        user_id: str,
        name: str,
        description: str,
        system_prompt: str,
        icon: str = "🔌",
        category: str = "custom",
        hooks: Optional[List[str]] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a custom user plugin."""
        plugin = {
            "id": f"custom_{uuid.uuid4().hex[:12]}",
            "name": name,
            "description": description,
            "version": "1.0.0",
            "author": "User",
            "icon": icon,
            "category": category,
            "builtin": False,
            "enabled": True,
            "hooks": hooks or ["pre_chat"],
            "system_prompt": system_prompt,
            "config": config or {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        if user_id not in self._user_plugins:
            self._user_plugins[user_id] = []
        self._user_plugins[user_id].append(plugin)

        return plugin

    def toggle_plugin(self, user_id: str, plugin_id: str) -> Optional[Dict[str, Any]]:
        """Toggle a plugin's enabled state."""
        # Check builtins first (stored in class-level list)
        for p in BUILTIN_PLUGINS:
            if p["id"] == plugin_id:
                p["enabled"] = not p.get("enabled", False)
                return dict(p)

        # Check user plugins
        for p in self._get_user_plugins(user_id):
            if p["id"] == plugin_id:
                p["enabled"] = not p.get("enabled", False)
                return p

        return None

    def delete_plugin(self, user_id: str, plugin_id: str) -> bool:
        """Delete a custom plugin (cannot delete builtins)."""
        plugins = self._get_user_plugins(user_id)
        for i, p in enumerate(plugins):
            if p["id"] == plugin_id and not p.get("builtin", False):
                plugins.pop(i)
                return True
        return False

    def update_plugin(
        self, user_id: str, plugin_id: str, updates: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Update a custom plugin's fields."""
        for p in self._get_user_plugins(user_id):
            if p["id"] == plugin_id and not p.get("builtin", False):
                allowed = {"name", "description", "system_prompt", "icon", "category", "config", "hooks"}
                for key, val in updates.items():
                    if key in allowed:
                        p[key] = val
                return p
        return None


# Global singleton
plugin_manager = PluginManager()
