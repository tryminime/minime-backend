"""
App Categorization Configuration for Productivity Metrics.

Categorizes applications and domains into:
- productive: IDEs, terminals, docs, research tools
- meetings: Video conferencing, calls
- distractive: Social media, entertainment
- neutral: Email, file managers, system apps
"""

from typing import Dict, List, Set
from enum import Enum


class AppCategory(str, Enum):
    """Application category for productivity metrics."""
    PRODUCTIVE = "productive"
    MEETINGS = "meetings"
    DISTRACTIVE = "distractive"
    NEUTRAL = "neutral"
    UNKNOWN = "unknown"


# Application name patterns (lowercase for case-insensitive matching)
PRODUCTIVE_APPS: Set[str] = {
    # IDEs & Code Editors
    "vscode", "code", "visual studio code",
    "pycharm", "intellij", "idea",
    "vim", "nvim", "neovim", "emacs",
    "sublime", "sublimetext",
    "atom", "brackets",
    "eclipse", "netbeans",
    "android studio",
    "xcode",
    "rider", "goland", "webstorm", "phpstorm",
    
    # Terminals
    "terminal", "iterm", "iterm2", "warp",
    "alacritty", "kitty", "hyper",
    "gnome-terminal", "konsole", "terminator",
    "powershell", "cmd", "command prompt",
    "windows terminal",
    
    # Research & Writing
    "overleaf", "latex", "texstudio", "texmaker",
    "jupyter", "jupyterlab", "jupyter notebook",
    "rstudio", "spyder",
    "zotero", "mendeley", "papers",
    "notion", "obsidian", "roam",
    "logseq", "remnote",
    "bear", "ulysses",
    "scrivener", "ia writer",
    
    # Documentation & Technical Writing
    "confluence", "readme",
    "gitbook", "docusaurus",
    "sphinx", "mkdocs",
    "typora", "marktext",
    
    # Design & Creative (productive context)
    "figma", "sketch", "adobe xd",
    "blender", "maya", "3ds max",
    "photoshop", "illustrator", "indesign",
    "gimp", "inkscape", "krita",
    
    # Data Analysis
    "tableau", "power bi", "looker",
    "excel", "google sheets", "libreoffice calc",
    "matlab", "octave",
    "mathematica", "maple",
    
    # Database Tools
    "dbeaver", "datagrip", "tableplus",
    "pgadmin", "mysql workbench",
    "mongodb compass", "redis desktop manager",
    
    # DevOps & Cloud
    "docker desktop", "kubernetes dashboard",
    "aws console", "azure portal", "gcp console",
    "postman", "insomnia", "paw",
    "github desktop", "sourcetree", "gitkraken",
}


MEETING_APPS: Set[str] = {
    # Video Conferencing
    "zoom", "zoom.us",
    "meet", "google meet",
    "teams", "microsoft teams",
    "webex", "cisco webex",
    "gotomeeting", "goto meeting",
    "bluejeans",
    "whereby",
    
    # Communication (call state)
    "slack", "slack call",
    "discord", "discord call",
    "skype",
    "facetime",
    "whatsapp", "whatsapp call",
    "telegram", "telegram call",
    "signal", "signal call",
    
    # Meeting rooms
    "calendar", "google calendar", "outlook calendar",
}


DISTRACTIVE_APPS: Set[str] = {
    # Social Media
    "facebook", "fb",
    "twitter", "x.com",
    "instagram", "ig",
    "reddit",
    "tiktok",
    "snapchat",
    "linkedin", "linkedin feed",  # browsing, not networking
    "pinterest",
    "tumblr",
    
    # Entertainment
    "youtube", "yt",
    "netflix",
    "twitch",
    "spotify", "apple music", "itunes",
    "hulu", "disney+", "amazon prime video",
    "hbo max", "paramount+",
    
    # Gaming
    "steam",
    "epic games",
    "gog galaxy",
    "battle.net", "blizzard",
    "origin", "ea desktop",
    "playstation", "xbox",
    "minecraft", "fortnite", "valorant",
    "league of legends", "dota",
    
    # News (passive consumption)
    "feedly", "flipboard",
    "pocket", "instapaper",
}


NEUTRAL_APPS: Set[str] = {
    # Email
    "mail", "apple mail",
    "outlook", "microsoft outlook",
    "gmail", "google mail",
    "thunderbird",
    "mailspring", "spark mail",
    
    # File Managers
    "finder", "explorer", "file explorer",
    "nautilus", "dolphin", "thunar",
    
    # System Apps
    "settings", "system preferences", "control panel",
    "activity monitor", "task manager", "system monitor",
    "alfred", "spotlight", "raycast",
    "1password", "lastpass", "bitwarden",
    
    # Browsers (context-dependent, but default neutral)
    "chrome", "google chrome",
    "firefox", "mozilla firefox",
    "safari",
    "edge", "microsoft edge",
    "brave", "brave browser",
    "arc", "arc browser",
    "opera",
    
    # Note-taking (light use)
    "notes", "apple notes",
    "simplenote", "standard notes",
    "evernote", "onenote",
}


# Domain patterns for web browsing categorization
PRODUCTIVE_DOMAINS: Set[str] = {
    # Documentation
    "docs.python.org", "docs.djangoproject.com", "docs.rs",
    "developer.mozilla.org", "mdn",
    "nodejs.org", "reactjs.org", "vuejs.org",
    "stackoverflow.com", "stackexchange.com",
    "github.com", "gitlab.com", "bitbucket.org",
    
    # Learning
    "coursera.org", "udemy.com", "edx.org",
    "leetcode.com", "hackerrank.com", "codewars.com",
    "freecodecamp.org", "codecademy.com",
    "arxiv.org", "scholar.google.com",
    "wikipedia.org", "wikiwand.com",
    
    # Work Tools
    "notion.so", "obsidian.md",
    "figma.com", "canva.com", 
    "trello.com", "asana.com", "linear.app",
    "jira.atlassian.com", "monday.com",
}


DISTRACTIVE_DOMAINS: Set[str] = {
    # Social Media
    "facebook.com", "fb.com", "instagram.com",
    "twitter.com", "x.com",
    "reddit.com", "tiktok.com",
    "snapchat.com", "pinterest.com",
    
    # Entertainment
    "youtube.com", "youtu.be",
    "netflix.com", "twitch.tv",
    "spotify.com", "soundcloud.com",
   
    # News (passive)
    "cnn.com", "bbc.com", "nytimes.com",
    "buzzfeed.com", "upworthy.com",
}


def categorize_app(app_name: str) -> AppCategory:
    """
    Categorize an application by name.
    
    Args:
        app_name: Application name (case-insensitive)
    
    Returns:
        AppCategory enum value
    """
    app_lower = app_name.lower().strip()
    
    if any(prod in app_lower for prod in PRODUCTIVE_APPS):
        return AppCategory.PRODUCTIVE
    
    if any(meet in app_lower for meet in MEETING_APPS):
        return AppCategory.MEETINGS
    
    if any(dist in app_lower for dist in DISTRACTIVE_APPS):
        return AppCategory.DISTRACTIVE
    
    if any(neut in app_lower for neut in NEUTRAL_APPS):
        return AppCategory.NEUTRAL
    
    return AppCategory.UNKNOWN


def categorize_domain(domain: str) -> AppCategory:
    """
    Categorize a web domain.
    
    Args:
        domain: Domain name (e.g., 'github.com')
    
    Returns:
        AppCategory enum value
    """
    domain_lower = domain.lower().strip()
    
    if any(prod in domain_lower for prod in PRODUCTIVE_DOMAINS):
        return AppCategory.PRODUCTIVE
    
    if any(dist in domain_lower for dist in DISTRACTIVE_DOMAINS):
        return AppCategory.DISTRACTIVE
    
    return AppCategory.NEUTRAL


def get_category_weight(category: AppCategory) -> float:
    """
    Get weight for focus score calculation.
    
    Productive apps contribute positively, distractive apps negatively.
    
    Returns:
        Weight value: 1.0 (productive), 0.0 (neutral), -0.5 (distractive)
    """
    weights = {
        AppCategory.PRODUCTIVE: 1.0,
        AppCategory.MEETINGS: 0.0,  # neutral in focus score
        AppCategory.NEUTRAL: 0.0,
        AppCategory.DISTRACTIVE: -0.5,
        AppCategory.UNKNOWN: 0.0,
    }
    return weights.get(category, 0.0)


# Export for easy access
__all__ = [
    "AppCategory",
    "categorize_app",
    "categorize_domain",
    "get_category_weight",
    "PRODUCTIVE_APPS",
    "MEETING_APPS",
    "DISTRACTIVE_APPS",
    "NEUTRAL_APPS",
    "PRODUCTIVE_DOMAINS",
    "DISTRACTIVE_DOMAINS",
]
