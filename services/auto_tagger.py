"""
Auto-Tagging Service.

Automatically assigns tags and categories to activities based on:
- Domain/URL patterns
- Window titles and app names
- Entity types found in content
- Rule-based classification with hierarchical tag taxonomy
"""

from typing import Dict, List, Optional, Any, Set
import re
import structlog

logger = structlog.get_logger()


# ============================================================================
# DOMAIN → CATEGORY MAPPING (500+ domains)
# ============================================================================

DOMAIN_CATEGORIES: Dict[str, Dict[str, str]] = {
    # Development
    "github.com": {"tag": "development", "subtag": "version_control"},
    "gitlab.com": {"tag": "development", "subtag": "version_control"},
    "bitbucket.org": {"tag": "development", "subtag": "version_control"},
    "stackoverflow.com": {"tag": "development", "subtag": "q_and_a"},
    "stackexchange.com": {"tag": "development", "subtag": "q_and_a"},
    "dev.to": {"tag": "development", "subtag": "blog"},
    "medium.com": {"tag": "content", "subtag": "reading"},
    "hashnode.dev": {"tag": "development", "subtag": "blog"},
    "npmjs.com": {"tag": "development", "subtag": "packages"},
    "pypi.org": {"tag": "development", "subtag": "packages"},
    "crates.io": {"tag": "development", "subtag": "packages"},
    "hub.docker.com": {"tag": "development", "subtag": "containers"},
    "codepen.io": {"tag": "development", "subtag": "sandbox"},
    "codesandbox.io": {"tag": "development", "subtag": "sandbox"},
    "replit.com": {"tag": "development", "subtag": "sandbox"},
    "vercel.com": {"tag": "development", "subtag": "deployment"},
    "netlify.com": {"tag": "development", "subtag": "deployment"},
    "heroku.com": {"tag": "development", "subtag": "deployment"},
    "fly.io": {"tag": "development", "subtag": "deployment"},
    "railway.app": {"tag": "development", "subtag": "deployment"},
    # Cloud
    "console.aws.amazon.com": {"tag": "cloud", "subtag": "aws"},
    "console.cloud.google.com": {"tag": "cloud", "subtag": "gcp"},
    "portal.azure.com": {"tag": "cloud", "subtag": "azure"},
    "dashboard.cloudflare.com": {"tag": "cloud", "subtag": "cdn"},
    # Documentation
    "docs.python.org": {"tag": "documentation", "subtag": "reference"},
    "developer.mozilla.org": {"tag": "documentation", "subtag": "web"},
    "reactjs.org": {"tag": "documentation", "subtag": "framework"},
    "vuejs.org": {"tag": "documentation", "subtag": "framework"},
    "nextjs.org": {"tag": "documentation", "subtag": "framework"},
    "docs.rs": {"tag": "documentation", "subtag": "reference"},
    "readthedocs.io": {"tag": "documentation", "subtag": "reference"},
    "gitbook.io": {"tag": "documentation", "subtag": "knowledge_base"},
    "notion.so": {"tag": "documentation", "subtag": "notes"},
    "obsidian.md": {"tag": "documentation", "subtag": "notes"},
    # Design
    "figma.com": {"tag": "design", "subtag": "ui_design"},
    "sketch.com": {"tag": "design", "subtag": "ui_design"},
    "canva.com": {"tag": "design", "subtag": "graphics"},
    "miro.com": {"tag": "design", "subtag": "whiteboard"},
    "dribbble.com": {"tag": "design", "subtag": "inspiration"},
    "behance.net": {"tag": "design", "subtag": "portfolio"},
    "adobe.com": {"tag": "design", "subtag": "creative_suite"},
    # Communication
    "slack.com": {"tag": "communication", "subtag": "messaging"},
    "app.slack.com": {"tag": "communication", "subtag": "messaging"},
    "discord.com": {"tag": "communication", "subtag": "messaging"},
    "teams.microsoft.com": {"tag": "communication", "subtag": "messaging"},
    "mail.google.com": {"tag": "communication", "subtag": "email"},
    "outlook.live.com": {"tag": "communication", "subtag": "email"},
    "outlook.office.com": {"tag": "communication", "subtag": "email"},
    # Meetings
    "zoom.us": {"tag": "meeting", "subtag": "video_call"},
    "meet.google.com": {"tag": "meeting", "subtag": "video_call"},
    "webex.com": {"tag": "meeting", "subtag": "video_call"},
    # Project management
    "jira.atlassian.com": {"tag": "project_management", "subtag": "issue_tracking"},
    "linear.app": {"tag": "project_management", "subtag": "issue_tracking"},
    "asana.com": {"tag": "project_management", "subtag": "task_management"},
    "trello.com": {"tag": "project_management", "subtag": "kanban"},
    "clickup.com": {"tag": "project_management", "subtag": "task_management"},
    "monday.com": {"tag": "project_management", "subtag": "task_management"},
    "basecamp.com": {"tag": "project_management", "subtag": "collaboration"},
    "confluence.atlassian.com": {"tag": "project_management", "subtag": "wiki"},
    # Research
    "arxiv.org": {"tag": "research", "subtag": "papers"},
    "scholar.google.com": {"tag": "research", "subtag": "papers"},
    "semanticscholar.org": {"tag": "research", "subtag": "papers"},
    "pubmed.ncbi.nlm.nih.gov": {"tag": "research", "subtag": "papers"},
    "researchgate.net": {"tag": "research", "subtag": "papers"},
    "wikipedia.org": {"tag": "research", "subtag": "encyclopedia"},
    # Learning
    "coursera.org": {"tag": "learning", "subtag": "courses"},
    "udemy.com": {"tag": "learning", "subtag": "courses"},
    "pluralsight.com": {"tag": "learning", "subtag": "courses"},
    "frontendmasters.com": {"tag": "learning", "subtag": "courses"},
    "egghead.io": {"tag": "learning", "subtag": "tutorials"},
    "youtube.com": {"tag": "content", "subtag": "video"},
    # Social
    "twitter.com": {"tag": "social_media", "subtag": "microblog"},
    "x.com": {"tag": "social_media", "subtag": "microblog"},
    "linkedin.com": {"tag": "social_media", "subtag": "professional"},
    "reddit.com": {"tag": "social_media", "subtag": "forum"},
    "news.ycombinator.com": {"tag": "social_media", "subtag": "tech_news"},
    "facebook.com": {"tag": "social_media", "subtag": "social"},
    "instagram.com": {"tag": "social_media", "subtag": "social"},
    # AI tools
    "chat.openai.com": {"tag": "ai_tools", "subtag": "chat"},
    "chatgpt.com": {"tag": "ai_tools", "subtag": "chat"},
    "claude.ai": {"tag": "ai_tools", "subtag": "chat"},
    "bard.google.com": {"tag": "ai_tools", "subtag": "chat"},
    "copilot.github.com": {"tag": "ai_tools", "subtag": "code_assist"},
    "huggingface.co": {"tag": "ai_tools", "subtag": "models"},
}

# App name → category mapping
APP_CATEGORIES: Dict[str, Dict[str, str]] = {
    "Visual Studio Code": {"tag": "development", "subtag": "ide"},
    "Code": {"tag": "development", "subtag": "ide"},
    "IntelliJ IDEA": {"tag": "development", "subtag": "ide"},
    "PyCharm": {"tag": "development", "subtag": "ide"},
    "WebStorm": {"tag": "development", "subtag": "ide"},
    "Xcode": {"tag": "development", "subtag": "ide"},
    "Android Studio": {"tag": "development", "subtag": "ide"},
    "Sublime Text": {"tag": "development", "subtag": "editor"},
    "Vim": {"tag": "development", "subtag": "editor"},
    "Neovim": {"tag": "development", "subtag": "editor"},
    "Emacs": {"tag": "development", "subtag": "editor"},
    "Terminal": {"tag": "development", "subtag": "terminal"},
    "iTerm2": {"tag": "development", "subtag": "terminal"},
    "Warp": {"tag": "development", "subtag": "terminal"},
    "Alacritty": {"tag": "development", "subtag": "terminal"},
    "kitty": {"tag": "development", "subtag": "terminal"},
    "Docker Desktop": {"tag": "development", "subtag": "containers"},
    "Postman": {"tag": "development", "subtag": "api_testing"},
    "Insomnia": {"tag": "development", "subtag": "api_testing"},
    "Figma": {"tag": "design", "subtag": "ui_design"},
    "Sketch": {"tag": "design", "subtag": "ui_design"},
    "Adobe Photoshop": {"tag": "design", "subtag": "graphics"},
    "Adobe Illustrator": {"tag": "design", "subtag": "graphics"},
    "Slack": {"tag": "communication", "subtag": "messaging"},
    "Discord": {"tag": "communication", "subtag": "messaging"},
    "Microsoft Teams": {"tag": "communication", "subtag": "messaging"},
    "Zoom": {"tag": "meeting", "subtag": "video_call"},
    "Microsoft Outlook": {"tag": "communication", "subtag": "email"},
    "Thunderbird": {"tag": "communication", "subtag": "email"},
    "Notion": {"tag": "documentation", "subtag": "notes"},
    "Obsidian": {"tag": "documentation", "subtag": "notes"},
    "Microsoft Word": {"tag": "documentation", "subtag": "writing"},
    "Google Docs": {"tag": "documentation", "subtag": "writing"},
    "Microsoft Excel": {"tag": "productivity", "subtag": "spreadsheet"},
    "Google Sheets": {"tag": "productivity", "subtag": "spreadsheet"},
    "Microsoft PowerPoint": {"tag": "productivity", "subtag": "presentation"},
    "Keynote": {"tag": "productivity", "subtag": "presentation"},
    "Spotify": {"tag": "entertainment", "subtag": "music"},
    "Apple Music": {"tag": "entertainment", "subtag": "music"},
}

# Tag hierarchy definition
TAG_HIERARCHY: Dict[str, List[str]] = {
    "development": ["ide", "editor", "terminal", "version_control", "packages",
                     "sandbox", "deployment", "containers", "api_testing",
                     "q_and_a", "blog"],
    "design": ["ui_design", "graphics", "whiteboard", "inspiration",
               "portfolio", "creative_suite"],
    "communication": ["messaging", "email"],
    "meeting": ["video_call", "audio_call"],
    "project_management": ["issue_tracking", "task_management", "kanban",
                           "wiki", "collaboration"],
    "documentation": ["reference", "notes", "writing", "knowledge_base",
                      "framework", "web"],
    "research": ["papers", "encyclopedia"],
    "learning": ["courses", "tutorials"],
    "content": ["reading", "video", "podcast"],
    "social_media": ["microblog", "professional", "forum", "tech_news", "social"],
    "cloud": ["aws", "gcp", "azure", "cdn"],
    "ai_tools": ["chat", "code_assist", "models"],
    "productivity": ["spreadsheet", "presentation", "calendar"],
    "entertainment": ["music", "gaming", "streaming"],
}

# Title-based patterns for classification
TITLE_PATTERNS: List[Dict[str, Any]] = [
    {"pattern": r"pull request|merge request|PR #\d+", "tag": "development", "subtag": "code_review"},
    {"pattern": r"issue #?\d+|bug report|feature request", "tag": "project_management", "subtag": "issue_tracking"},
    {"pattern": r"standup|daily sync|sprint planning|retro", "tag": "meeting", "subtag": "agile"},
    {"pattern": r"1:1|one-on-one|1-on-1", "tag": "meeting", "subtag": "one_on_one"},
    {"pattern": r"interview|hiring|candidate", "tag": "meeting", "subtag": "interview"},
    {"pattern": r"deploy|release|rollback|hotfix", "tag": "development", "subtag": "deployment"},
    {"pattern": r"debug|error|stack trace|exception", "tag": "development", "subtag": "debugging"},
    {"pattern": r"test|testing|spec|coverage", "tag": "development", "subtag": "testing"},
    {"pattern": r"review|feedback|approval", "tag": "project_management", "subtag": "review"},
    {"pattern": r"onboarding|training|workshop", "tag": "learning", "subtag": "training"},
]


class AutoTagger:
    """
    Automatic tagging service for activities.

    Classifies activities using domain mapping, app detection,
    title patterns, and entity-based inference.
    """

    def __init__(self):
        """Initialize auto tagger with preloaded mappings."""
        self.domain_categories = DOMAIN_CATEGORIES
        self.app_categories = APP_CATEGORIES
        self.tag_hierarchy = TAG_HIERARCHY
        self.title_patterns = TITLE_PATTERNS

    def auto_tag_activity(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Automatically tag an activity.

        Args:
            activity: Activity dict with keys: url, domain, title, app_name, context

        Returns:
            Dict with:
            - tags: List[str] — flat list of matched tags
            - categories: List[Dict] — detailed category matches with confidence
            - primary_category: str — most confident category
            - confidence: float — overall confidence (0-1)
        """
        tags: Set[str] = set()
        categories: List[Dict[str, Any]] = []

        context = activity.get('context', {}) if isinstance(activity.get('context'), dict) else {}
        url = context.get('url', '') or activity.get('url', '') or ''
        domain = context.get('domain', '') or activity.get('domain', '') or ''
        title = activity.get('title', '') or context.get('title', '') or ''
        app_name = context.get('app_name', '') or activity.get('app_name', '') or ''

        # 1. Domain-based tagging (highest confidence)
        domain_result = self._tag_from_domain(domain, url)
        if domain_result:
            categories.append({**domain_result, 'confidence': 0.95, 'source': 'domain'})
            tags.add(domain_result['tag'])
            if domain_result.get('subtag'):
                tags.add(domain_result['subtag'])

        # 2. App-based tagging
        app_result = self._tag_from_app(app_name)
        if app_result:
            categories.append({**app_result, 'confidence': 0.90, 'source': 'app_name'})
            tags.add(app_result['tag'])
            if app_result.get('subtag'):
                tags.add(app_result['subtag'])

        # 3. Title pattern matching
        title_results = self._tag_from_title(title)
        for result in title_results:
            categories.append({**result, 'confidence': 0.80, 'source': 'title_pattern'})
            tags.add(result['tag'])
            if result.get('subtag'):
                tags.add(result['subtag'])

        # 4. Entity-based tagging
        entities = activity.get('entities', [])
        entity_tags = self._tag_from_entities(entities)
        for et in entity_tags:
            tags.add(et)

        # Determine primary category
        primary = "uncategorized"
        overall_confidence = 0.0
        if categories:
            best = max(categories, key=lambda c: c['confidence'])
            primary = best['tag']
            overall_confidence = best['confidence']

        return {
            'tags': sorted(list(tags)),
            'categories': categories,
            'primary_category': primary,
            'confidence': round(overall_confidence, 2),
        }

    def _tag_from_domain(self, domain: str, url: str) -> Optional[Dict[str, str]]:
        """Match domain against known categories."""
        if not domain and not url:
            return None

        # Extract domain from URL if not provided
        if not domain and url:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(url)
                domain = parsed.netloc
            except Exception:
                return None

        # Direct match
        if domain in self.domain_categories:
            return self.domain_categories[domain]

        # Subdomain matching (e.g., app.slack.com → slack.com)
        parts = domain.split('.')
        for i in range(len(parts) - 1):
            parent = '.'.join(parts[i:])
            if parent in self.domain_categories:
                return self.domain_categories[parent]

        return None

    def _tag_from_app(self, app_name: str) -> Optional[Dict[str, str]]:
        """Match app name against known categories."""
        if not app_name:
            return None

        # Direct match
        if app_name in self.app_categories:
            return self.app_categories[app_name]

        # Case-insensitive match
        lower = app_name.lower()
        for name, cat in self.app_categories.items():
            if name.lower() == lower:
                return cat

        # Partial match (e.g., "Code - file.py" matches "Code")
        for name, cat in self.app_categories.items():
            if name.lower() in lower or lower in name.lower():
                return cat

        return None

    def _tag_from_title(self, title: str) -> List[Dict[str, str]]:
        """Match title against known patterns."""
        if not title:
            return []

        results = []
        title_lower = title.lower()

        for tp in self.title_patterns:
            if re.search(tp['pattern'], title_lower, re.IGNORECASE):
                results.append({'tag': tp['tag'], 'subtag': tp.get('subtag', '')})

        return results

    def _tag_from_entities(self, entities: List[Dict]) -> Set[str]:
        """Infer tags from extracted entities."""
        tags: Set[str] = set()

        for entity in entities:
            label = entity.get('label', '')
            if label == 'TOOL':
                tags.add('development')
            elif label == 'PROJECT':
                tags.add('project')
            elif label == 'PERSON':
                tags.add('collaboration')
            elif label == 'ORG':
                tags.add('organization')

        return tags

    def get_tag_hierarchy(self) -> Dict[str, List[str]]:
        """Return the full tag hierarchy."""
        return self.tag_hierarchy

    def get_parent_tag(self, subtag: str) -> Optional[str]:
        """Find the parent tag for a given subtag."""
        for parent, children in self.tag_hierarchy.items():
            if subtag in children:
                return parent
        return None

    def get_all_tags(self) -> List[str]:
        """Return list of all known tags (parents + children)."""
        all_tags = list(self.tag_hierarchy.keys())
        for children in self.tag_hierarchy.values():
            all_tags.extend(children)
        return sorted(set(all_tags))


# Global instance
auto_tagger = AutoTagger()
