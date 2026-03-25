"""
Lightweight NER — Pure-Python entity extraction via regex + heuristics.

No external dependencies (no spaCy, no Celery, no Redis).
Extracts entities from activity titles, domains, apps, and context fields.

Entity types extracted:
- artifact: apps, files, documents, tools
- organization: companies, domains, educational institutions
- skill: programming languages, frameworks, technologies
- concept: topics, subjects, categories
- person: author names from documents
- project: from file paths, repositories
"""

import re
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse


# ─── Organization metadata: (name, entity_type, org_type, industry, confidence)
# org_type: company | educational | government | open_source | media |
#            community | cloud | developer_tools | productivity | social_media
# industry: tech | finance | education | social | content | infrastructure |
#            productivity | gaming | legal | health | news | research
DOMAIN_ORG_MAP: Dict[str, Tuple[str, str, str, str, float]] = {
    # Social media
    'facebook.com':         ('Facebook',       'organization', 'social_media',      'social',         0.95),
    'www.facebook.com':     ('Facebook',       'organization', 'social_media',      'social',         0.95),
    'instagram.com':        ('Instagram',      'organization', 'social_media',      'social',         0.95),
    'www.instagram.com':    ('Instagram',      'organization', 'social_media',      'social',         0.95),
    'twitter.com':          ('Twitter/X',      'organization', 'social_media',      'social',         0.95),
    'x.com':                ('Twitter/X',      'organization', 'social_media',      'social',         0.95),
    'linkedin.com':         ('LinkedIn',       'organization', 'social_media',      'social',         0.95),
    'www.linkedin.com':     ('LinkedIn',       'organization', 'social_media',      'social',         0.95),
    'reddit.com':           ('Reddit',         'organization', 'community',         'social',         0.95),
    'www.reddit.com':       ('Reddit',         'organization', 'community',         'social',         0.95),
    'tiktok.com':           ('TikTok',         'organization', 'social_media',      'social',         0.95),
    'www.tiktok.com':       ('TikTok',         'organization', 'social_media',      'social',         0.95),
    'discord.com':          ('Discord',        'organization', 'community',         'social',         0.95),
    'snapchat.com':         ('Snapchat',       'organization', 'social_media',      'social',         0.95),
    'threads.net':          ('Threads',        'organization', 'social_media',      'social',         0.95),
    'mastodon.social':      ('Mastodon',       'organization', 'open_source',       'social',         0.90),
    'bsky.app':             ('Bluesky',        'organization', 'social_media',      'social',         0.90),
    'whatsapp.com':         ('WhatsApp',       'organization', 'social_media',      'social',         0.95),
    'web.whatsapp.com':     ('WhatsApp',       'organization', 'social_media',      'social',         0.95),
    # Video / media
    'youtube.com':          ('YouTube',        'organization', 'media',             'content',        0.95),
    'www.youtube.com':      ('YouTube',        'organization', 'media',             'content',        0.95),
    'twitch.tv':            ('Twitch',         'organization', 'media',             'gaming',         0.95),
    'www.twitch.tv':        ('Twitch',         'organization', 'media',             'gaming',         0.95),
    'netflix.com':          ('Netflix',        'organization', 'media',             'content',        0.95),
    'www.netflix.com':      ('Netflix',        'organization', 'media',             'content',        0.95),
    'disneyplus.com':       ('Disney+',        'organization', 'media',             'content',        0.95),
    'primevideo.com':       ('Prime Video',    'organization', 'media',             'content',        0.95),
    'hulu.com':             ('Hulu',           'organization', 'media',             'content',        0.95),
    'vimeo.com':            ('Vimeo',          'organization', 'media',             'content',        0.90),
    'dailymotion.com':      ('Dailymotion',    'organization', 'media',             'content',        0.90),
    # Developer tools & code hosting
    'github.com':           ('GitHub',         'organization', 'developer_tools',   'tech',           0.95),
    'gitlab.com':           ('GitLab',         'organization', 'developer_tools',   'tech',           0.95),
    'bitbucket.org':        ('Bitbucket',      'organization', 'developer_tools',   'tech',           0.95),
    'stackoverflow.com':    ('Stack Overflow', 'organization', 'community',         'tech',           0.95),
    'npmjs.com':            ('npm',            'organization', 'open_source',       'tech',           0.95),
    'www.npmjs.com':        ('npm',            'organization', 'open_source',       'tech',           0.95),
    'pypi.org':             ('PyPI',           'organization', 'open_source',       'tech',           0.95),
    'crates.io':            ('Crates.io',      'organization', 'open_source',       'tech',           0.95),
    'hub.docker.com':       ('Docker Hub',     'organization', 'developer_tools',   'tech',           0.95),
    'dev.to':               ('DEV Community',  'organization', 'community',         'tech',           0.90),
    'hackernews.com':       ('Hacker News',    'organization', 'community',         'tech',           0.90),
    'news.ycombinator.com': ('Hacker News',    'organization', 'community',         'tech',           0.90),
    'codepen.io':           ('CodePen',        'organization', 'developer_tools',   'tech',           0.90),
    'replit.com':           ('Replit',         'organization', 'developer_tools',   'tech',           0.90),
    'jsfiddle.net':         ('JSFiddle',       'organization', 'developer_tools',   'tech',           0.88),
    # Cloud providers
    'aws.amazon.com':          ('AWS',            'organization', 'cloud',          'infrastructure', 0.95),
    'azure.microsoft.com':     ('Microsoft Azure','organization', 'cloud',          'infrastructure', 0.95),
    'console.cloud.google.com':('Google Cloud',   'organization', 'cloud',          'infrastructure', 0.95),
    'cloud.google.com':        ('Google Cloud',   'organization', 'cloud',          'infrastructure', 0.95),
    'vercel.com':              ('Vercel',          'organization', 'cloud',          'infrastructure', 0.95),
    'netlify.com':             ('Netlify',         'organization', 'cloud',          'infrastructure', 0.95),
    'digitalocean.com':        ('DigitalOcean',    'organization', 'cloud',          'infrastructure', 0.95),
    'heroku.com':              ('Heroku',           'organization', 'cloud',         'infrastructure', 0.95),
    'cloudflare.com':          ('Cloudflare',      'organization', 'cloud',          'infrastructure', 0.95),
    # Google (artifact-level services)
    'google.com':           ('Google',         'organization', 'company',           'tech',           0.95),
    'www.google.com':       ('Google',         'organization', 'company',           'tech',           0.95),
    'docs.google.com':      ('Google Docs',    'artifact',     'productivity',      'productivity',   0.95),
    'drive.google.com':     ('Google Drive',   'artifact',     'productivity',      'productivity',   0.95),
    'mail.google.com':      ('Gmail',          'artifact',     'productivity',      'productivity',   0.95),
    'meet.google.com':      ('Google Meet',    'artifact',     'productivity',      'productivity',   0.95),
    'calendar.google.com':  ('Google Calendar','artifact',     'productivity',      'productivity',   0.95),
    'sheets.google.com':    ('Google Sheets',  'artifact',     'productivity',      'productivity',   0.95),
    'slides.google.com':    ('Google Slides',  'artifact',     'productivity',      'productivity',   0.95),
    # Microsoft
    'microsoft.com':        ('Microsoft',      'organization', 'company',           'tech',           0.95),
    'office.com':           ('Microsoft Office','organization', 'productivity',      'productivity',   0.95),
    'teams.microsoft.com':  ('Microsoft Teams','organization', 'productivity',      'productivity',   0.95),
    'outlook.live.com':     ('Outlook',        'artifact',     'productivity',      'productivity',   0.95),
    # Productivity / knowledge tools
    'slack.com':            ('Slack',          'organization', 'productivity',      'productivity',   0.95),
    'notion.so':            ('Notion',          'organization', 'productivity',      'productivity',   0.95),
    'figma.com':            ('Figma',           'organization', 'developer_tools',   'tech',           0.95),
    'trello.com':           ('Trello',          'organization', 'productivity',      'productivity',   0.95),
    'jira.atlassian.com':   ('Jira',            'organization', 'developer_tools',   'tech',           0.95),
    'confluente.atlassian.com':('Confluence',   'organization', 'productivity',      'productivity',   0.90),
    'airtable.com':         ('Airtable',        'organization', 'productivity',      'productivity',   0.90),
    'asana.com':            ('Asana',           'organization', 'productivity',      'productivity',   0.90),
    'linear.app':           ('Linear',          'organization', 'developer_tools',   'tech',           0.90),
    'obsidian.md':          ('Obsidian',        'organization', 'productivity',      'productivity',   0.90),
    'roamresearch.com':     ('Roam Research',   'organization', 'productivity',      'productivity',   0.88),
    'clickup.com':          ('ClickUp',         'organization', 'productivity',      'productivity',   0.90),
    'miro.com':             ('Miro',            'organization', 'productivity',      'productivity',   0.90),
    'zoom.us':              ('Zoom',            'organization', 'productivity',      'productivity',   0.95),
    # News / media
    'medium.com':           ('Medium',          'organization', 'media',            'content',         0.90),
    'substack.com':         ('Substack',        'organization', 'media',            'content',         0.90),
    'wikipedia.org':        ('Wikipedia',       'organization', 'open_source',      'research',        0.95),
    'en.wikipedia.org':     ('Wikipedia',       'organization', 'open_source',      'research',        0.95),
    'arxiv.org':            ('arXiv',           'organization', 'educational',      'research',        0.95),
    'semanticscholar.org':  ('Semantic Scholar','organization', 'educational',      'research',        0.92),
    'scholar.google.com':   ('Google Scholar',  'organization', 'educational',      'research',        0.95),
    'pubmed.ncbi.nlm.nih.gov':('PubMed',        'organization', 'government',       'health',          0.95),
    # Finance
    'stripe.com':           ('Stripe',          'organization', 'company',          'finance',         0.95),
    'paypal.com':           ('PayPal',          'organization', 'company',          'finance',         0.95),
}

# ─── App Name → Canonical Name mapping ──────────────────────────────────────
APP_CANONICAL_MAP = {
    'firefox': 'Firefox',
    'google-chrome': 'Google Chrome',
    'google chrome': 'Google Chrome',
    'chromium': 'Chromium',
    'brave-browser': 'Brave Browser',
    'code': 'VS Code',
    'code-oss': 'VS Code',
    'visual studio code': 'VS Code',
    'spyder': 'Spyder IDE',
    'pycharm': 'PyCharm',
    'intellij': 'IntelliJ IDEA',
    'webstorm': 'WebStorm',
    'sublime_text': 'Sublime Text',
    'sublime text': 'Sublime Text',
    'atom': 'Atom',
    'vim': 'Vim',
    'nvim': 'Neovim',
    'emacs': 'Emacs',
    'terminal': 'Terminal',
    'gnome-terminal': 'GNOME Terminal',
    'konsole': 'Konsole',
    'alacritty': 'Alacritty',
    'kitty': 'Kitty',
    'wezterm': 'WezTerm',
    'nautilus': 'Files',
    'nemo': 'Nemo Files',
    'thunar': 'Thunar',
    'dolphin': 'Dolphin',
    'evince': 'Document Viewer',
    'document viewer': 'Document Viewer',
    'eog': 'Image Viewer',
    'gimp': 'GIMP',
    'inkscape': 'Inkscape',
    'libreoffice': 'LibreOffice',
    'libreoffice writer': 'LibreOffice Writer',
    'libreoffice calc': 'LibreOffice Calc',
    'libreoffice impress': 'LibreOffice Impress',
    'obs': 'OBS Studio',
    'vlc': 'VLC',
    'slack': 'Slack',
    'discord': 'Discord',
    'thunderbird': 'Thunderbird',
    'postman': 'Postman',
    'insomnia': 'Insomnia',
    'dbeaver': 'DBeaver',
    'pgadmin': 'pgAdmin',
    'docker': 'Docker Desktop',
    'minime': 'MiniMe',
    'antigravity': 'Antigravity',
    'gnome-text-editor': 'Text Editor',
    'text editor': 'Text Editor',
    'gedit': 'gedit',
    'mousepad': 'Mousepad',
    'kate': 'Kate',
}

# ─── Tech skills patterns ────────────────────────────────────────────────────
TECH_SKILLS = {
    # Languages
    'python', 'javascript', 'typescript', 'java', 'rust', 'go', 'golang',
    'c++', 'cpp', 'c#', 'csharp', 'ruby', 'php', 'swift', 'kotlin',
    'scala', 'r', 'matlab', 'julia', 'lua', 'perl', 'haskell', 'elixir',
    'dart', 'clojure', 'shell', 'bash', 'zsh', 'powershell', 'sql',
    # Frontend frameworks
    'react', 'reactjs', 'vue', 'vuejs', 'angular', 'svelte', 'nextjs',
    'next.js', 'nuxtjs', 'nuxt.js', 'gatsby', 'remix',
    # Backend frameworks
    'fastapi', 'django', 'flask', 'express', 'expressjs', 'nestjs',
    'spring', 'rails', 'laravel', 'phoenix',
    # Tools & platforms
    'docker', 'kubernetes', 'k8s', 'terraform', 'ansible', 'jenkins',
    'git', 'github', 'gitlab', 'npm', 'yarn', 'pnpm', 'pip', 'conda',
    'webpack', 'vite', 'rollup', 'parcel', 'esbuild',
    # Databases
    'postgresql', 'postgres', 'mysql', 'mongodb', 'redis', 'sqlite',
    'dynamodb', 'cassandra', 'elasticsearch', 'neo4j', 'qdrant',
    # Cloud
    'aws', 'azure', 'gcp', 'vercel', 'netlify', 'heroku',
    'cloudflare', 'digitalocean', 'linode',
    # AI/ML
    'tensorflow', 'pytorch', 'scikit-learn', 'sklearn', 'pandas',
    'numpy', 'keras', 'opencv', 'spacy', 'huggingface', 'langchain',
    'openai', 'llm', 'chatgpt', 'gpt',
    # CSS/UI
    'tailwindcss', 'tailwind', 'css', 'sass', 'scss', 'bootstrap',
    'material-ui', 'mui', 'chakra-ui', 'styled-components',
    # Other
    'graphql', 'rest', 'api', 'oauth', 'jwt', 'websocket',
    'html', 'json', 'yaml', 'toml', 'markdown',
    'linux', 'ubuntu', 'debian', 'fedora', 'centos',
    'macos', 'windows', 'android', 'ios',
}

# File extension → type mapping
FILE_TYPE_MAP = {
    '.pdf': 'document', '.doc': 'document', '.docx': 'document',
    '.xls': 'spreadsheet', '.xlsx': 'spreadsheet', '.csv': 'spreadsheet',
    '.ppt': 'presentation', '.pptx': 'presentation',
    '.py': 'code', '.js': 'code', '.ts': 'code', '.tsx': 'code',
    '.jsx': 'code', '.rs': 'code', '.go': 'code', '.java': 'code',
    '.c': 'code', '.cpp': 'code', '.h': 'code', '.hpp': 'code',
    '.rb': 'code', '.php': 'code', '.swift': 'code', '.kt': 'code',
    '.html': 'code', '.css': 'code', '.scss': 'code',
    '.json': 'config', '.yaml': 'config', '.yml': 'config',
    '.toml': 'config', '.ini': 'config', '.env': 'config',
    '.md': 'document', '.txt': 'document', '.rst': 'document',
    '.png': 'image', '.jpg': 'image', '.jpeg': 'image',
    '.gif': 'image', '.svg': 'image', '.webp': 'image',
    '.mp4': 'video', '.mov': 'video', '.avi': 'video',
    '.mp3': 'audio', '.wav': 'audio', '.flac': 'audio',
    '.zip': 'archive', '.tar': 'archive', '.gz': 'archive',
    '.sh': 'script', '.bash': 'script',
}

# Educational domain patterns (for fallback classification)
EDU_PATTERNS = re.compile(
    r'(umich\.edu|umdearborn\.edu|mit\.edu|stanford\.edu|harvard\.edu|'
    r'berkeley\.edu|cmu\.edu|gatech\.edu|[a-z]+\.edu)',
    re.IGNORECASE,
)

# Government TLD / domain patterns
GOV_PATTERNS = re.compile(
    r'\.(gov|mil)(\.[a-z]{2})?$|'
    r'(cdc\.gov|nih\.gov|nasa\.gov|fbi\.gov|cia\.gov|whitehouse\.gov|'
    r'congress\.gov|senate\.gov|house\.gov)',
    re.IGNORECASE,
)

# News / media subdomain and TLD patterns
NEWS_PATTERNS = re.compile(
    r'^(news|bbc|cnn|nytimes|theguardian|techcrunch|wired|arstechnica|'
    r'theverge|engadget|reuters|bloomberg|ft\.com|wsj|forbes|businessinsider)',
    re.IGNORECASE,
)

# Finance patterns
FINANCE_PATTERNS = re.compile(
    r'(bank|finance|invest|capital|trading|crypto|btc|eth|coinbase|'
    r'robinhood|etrade|fidelity|schwab|vanguard|blackrock)',
    re.IGNORECASE,
)

# Research / academic patterns
RESEARCH_PATTERNS = re.compile(
    r'(research|journal|review|pubmed|arxiv|scholar|ieee|acm|springer|'
    r'elsevier|nature\.com|science\.org)',
    re.IGNORECASE,
)


def classify_domain_org(domain: str) -> Dict[str, Any]:
    """
    Classify an unknown domain into org_type, industry, and confidence.
    Used as fallback when domain is not in DOMAIN_ORG_MAP.

    Returns:
        Dict with keys: name, org_type, industry, confidence
    """
    d = domain.lower().strip()
    parts = d.split('.')
    if len(parts) < 2:
        return {'org_type': 'company', 'industry': 'tech', 'confidence': 0.45}

    tld = parts[-1]
    sld = parts[-2]  # second-level domain (the brand name)

    # Handle compound TLDs like .ac.uk, .co.uk, .com.au
    # For example: research-lab.ac.uk → sld='ac', we want 'research-lab'
    compound_tlds = {'ac', 'co', 'com', 'org', 'gov', 'net', 'edu'}
    if sld in compound_tlds and len(parts) >= 3:
        sld = parts[-3]  # Use the part before the compound TLD

    name = sld.replace('-', ' ').title()

    # Normalise cc-TLD compound suffixes (co.uk, com.au etc.)
    compound = '.'.join(parts[-2:]) if len(parts) >= 2 else ''

    # 1. Educational
    if EDU_PATTERNS.search(d) or tld == 'edu':
        inst_name = f"{name} University" if 'university' not in name.lower() else name
        return {'name': inst_name, 'org_type': 'educational', 'industry': 'education', 'confidence': 0.80}

    # 2. Government
    if GOV_PATTERNS.search(d) or tld in ('gov', 'mil'):
        return {'name': name, 'org_type': 'government', 'industry': 'government', 'confidence': 0.85}

    # 3. News / media
    if NEWS_PATTERNS.search(sld) or tld in ('news', 'press', 'media'):
        return {'name': name, 'org_type': 'media', 'industry': 'news', 'confidence': 0.70}

    # 4. Research / academic
    if RESEARCH_PATTERNS.search(d) or tld == 'ac':
        return {'name': name, 'org_type': 'educational', 'industry': 'research', 'confidence': 0.72}

    # 5. Finance
    if FINANCE_PATTERNS.search(sld):
        return {'name': name, 'org_type': 'company', 'industry': 'finance', 'confidence': 0.68}

    # 6. Open-source / community TLDs
    if tld in ('org', 'io', 'dev', 'app'):
        return {'name': name, 'org_type': 'open_source', 'industry': 'tech', 'confidence': 0.60}

    # 7. Generic fallback
    return {'name': name, 'org_type': 'company', 'industry': 'tech', 'confidence': 0.55}


def extract_entities(activity: Dict[str, Any]) -> List[Dict[str, Any]]:

    """
    Extract entities from a single activity record.

    Args:
        activity: Activity dict with keys: title, app, domain, url, type,
                  context, data, duration_seconds, etc.

    Returns:
        List of entity dicts. For organization entities, extra keys are included:
        - org_type: company|educational|government|open_source|media|community|cloud|developer_tools
        - industry: tech|finance|education|social|content|infrastructure|productivity|gaming|research
    """
    entities: List[Dict[str, Any]] = []
    seen: set = set()  # (name_lower, type) dedup

    def add(name: str, entity_type: str, confidence: float = 0.8, source: str = 'title',
             extra: Optional[Dict[str, Any]] = None):
        key = (name.lower().strip(), entity_type)
        if key not in seen and len(name.strip()) > 1:
            seen.add(key)
            ent: Dict[str, Any] = {
                'name': name.strip(),
                'entity_type': entity_type,
                'confidence': confidence,
                'source': source,
            }
            if extra:
                ent.update(extra)
            entities.append(ent)

    title = activity.get('title') or ''
    app = activity.get('app') or ''
    domain = activity.get('domain') or ''
    url = activity.get('url') or ''
    act_type = activity.get('type') or ''
    context = activity.get('context') or {}
    data = activity.get('data') or {}

    if isinstance(context, str):
        context = {}
    if isinstance(data, str):
        data = {}

    # ── 1. App name → artifact entity ──────────────────────────────────────
    if app:
        canonical = APP_CANONICAL_MAP.get(app.lower(), app)
        if canonical and canonical.lower() not in ('unknown', 'untitled', ''):
            add(canonical, 'artifact', 0.95, 'app')

    # ── 2. Domain → organization entity ────────────────────────────────────
    if domain:
        domain_lower = domain.lower()
        # Check known domain map (5-tuple: name, entity_type, org_type, industry, confidence)
        lookup = DOMAIN_ORG_MAP.get(domain_lower)
        if lookup:
            name_, etype, org_type, industry, conf = lookup
            extra = {'org_type': org_type, 'industry': industry} if etype == 'organization' else None
            add(name_, etype, conf, 'domain', extra=extra)
        elif domain_lower not in ('localhost', '127.0.0.1', '', 'unknown'):
            # Pattern-based classification for unknown domains
            parts = domain_lower.split('.')
            if len(parts) >= 2:
                compound = '.'.join(parts[-2:])
                if compound not in ('co.uk', 'com.au', 'co.in', 'co.nz', 'com.br'):
                    classification = classify_domain_org(domain_lower)
                    org_name = classification.get('name') or parts[-2].replace('-', ' ').title()
                    if org_name.lower() not in ('www', 'com', 'org', 'net', 'io', ''):
                        add(
                            org_name,
                            'organization',
                            classification['confidence'],
                            'domain',
                            extra={
                                'org_type': classification['org_type'],
                                'industry': classification['industry'],
                            }
                        )


    # ── 3. File names → artifact entity ────────────────────────────────────
    file_name = data.get('file_name') or context.get('file_name') or ''
    file_path = data.get('file_path') or context.get('file_path') or ''

    # Extract from title (e.g., "Document Viewer — LinearModelsForBiologicalData.pdf")
    title_file_match = re.search(r'[—–-]\s*(.+?\.\w{2,5})\s*$', title)
    if title_file_match:
        file_name = file_name or title_file_match.group(1).strip()

    if file_name:
        add(file_name, 'artifact', 0.85, 'file')

        # Detect file type → skill/concept
        ext = '.' + file_name.rsplit('.', 1)[-1].lower() if '.' in file_name else ''
        file_type = FILE_TYPE_MAP.get(ext)
        if file_type == 'code':
            # Infer language from extension
            lang_map = {
                '.py': 'Python', '.js': 'JavaScript', '.ts': 'TypeScript',
                '.tsx': 'TypeScript', '.jsx': 'JavaScript', '.rs': 'Rust',
                '.go': 'Go', '.java': 'Java', '.rb': 'Ruby', '.php': 'PHP',
                '.swift': 'Swift', '.kt': 'Kotlin', '.cpp': 'C++', '.c': 'C',
                '.html': 'HTML', '.css': 'CSS', '.scss': 'Sass/SCSS',
            }
            lang = lang_map.get(ext)
            if lang:
                add(lang, 'skill', 0.7, 'file_ext')

    # ── 4. Working directory → project entity ──────────────────────────────
    work_dir = data.get('working_directory') or context.get('working_directory') or ''
    if work_dir:
        # Extract project name from path (last 1-2 components after common roots)
        path_parts = work_dir.rstrip('/').split('/')
        # Skip common roots: /home/user/Documents, /home/user/Projects, etc.
        if len(path_parts) >= 4:
            project_name = path_parts[-1]
            if project_name and project_name.lower() not in ('src', 'lib', 'bin', 'home', 'documents', 'desktop', 'downloads', ''):
                add(project_name, 'project', 0.65, 'working_dir')

    # ── 5. Tech skills from title + URL ────────────────────────────────────
    text_blob = f"{title} {url} {app}".lower()
    # Word boundary matching for skills
    words = set(re.findall(r'[a-z0-9#+.]+', text_blob))
    for skill in TECH_SKILLS:
        if skill in words or skill.replace('.', '') in words:
            add(skill.capitalize() if len(skill) > 3 else skill.upper(),
                'skill', 0.6, 'pattern')

    # ── 6. URL path analysis ───────────────────────────────────────────────
    if url:
        try:
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            if path:
                # GitHub repo detection: github.com/user/repo
                if 'github.com' in (parsed.netloc or ''):
                    path_parts = path.split('/')
                    if len(path_parts) >= 2:
                        repo = f"{path_parts[0]}/{path_parts[1]}"
                        add(repo, 'project', 0.8, 'url')
        except Exception:
            pass

    # ── 7. Author/person extraction from document titles ───────────────────
    # Pattern: "Title - Author Name" or "Author_Name_1234.pdf"
    author_match = re.search(
        r'(?:by|author[:\s]+)([A-Z][a-z]+ [A-Z][a-z]+)', title, re.IGNORECASE
    )
    if author_match:
        add(author_match.group(1), 'person', 0.6, 'title')

    # Pattern: "Author Last - Title.pdf" e.g. "Kenneth Laudon"
    # or multiple authors like "Firstname Lastname & Firstname Lastname"
    name_in_title = re.findall(
        r'\b([A-Z][a-z]{2,15}\s+[A-Z][a-z]{2,15})\b', title
    )
    for name in name_in_title[:2]:
        # Skip common false positives
        lower = name.lower()
        if lower not in ('document viewer', 'text editor', 'image viewer',
                         'file manager', 'system monitor', 'activity monitor',
                         'google chrome', 'visual studio', 'sublime text',
                         'microsoft word', 'libre office'):
            add(name, 'person', 0.5, 'title_name')

    return entities


def extract_entities_batch(activities: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Extract entities from a batch of activities.

    Returns:
        Dict mapping activity_id → list of extracted entities
    """
    result = {}
    for activity in activities:
        activity_id = activity.get('id') or activity.get('activity_id')
        if activity_id:
            result[str(activity_id)] = extract_entities(activity)
    return result
