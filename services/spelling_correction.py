"""
Spelling Correction Service.

Tech-aware spelling correction for entity names and activity text.
Preserves technical terms (kubectl, npm, FastAPI, etc.) while fixing
common typos in natural language.
"""

from typing import Dict, List, Optional, Tuple, Set, Any
import re
import structlog

logger = structlog.get_logger()


# ============================================================================
# TECHNICAL TERM WHITELIST (do NOT correct these)
# ============================================================================

TECH_WHITELIST: Set[str] = {
    # Programming languages
    "python", "javascript", "typescript", "golang", "kotlin", "swift",
    "rust", "elixir", "clojure", "haskell", "scala", "erlang", "nim",
    "zig", "lua", "perl", "fortran", "cobol", "matlab", "julia",
    # Frameworks & libraries
    "react", "vue", "angular", "svelte", "nextjs", "nuxtjs", "gatsby",
    "remix", "astro", "django", "flask", "fastapi", "express", "nestjs",
    "spring", "rails", "laravel", "phoenix", "actix", "axum", "gin",
    "fiber", "sinatra", "koa", "hapi", "rocket", "tauri", "electron",
    "pytorch", "tensorflow", "keras", "sklearn", "spacy", "nltk",
    "gensim", "pandas", "numpy", "scipy", "matplotlib", "seaborn",
    "huggingface", "langchain", "llamaindex", "celery", "sqlalchemy",
    "pydantic", "fastify", "deno", "bun", "htmx", "alpine",
    # Cloud & infra
    "aws", "gcp", "azure", "kubernetes", "k8s", "docker", "terraform",
    "ansible", "pulumi", "cloudflare", "vercel", "netlify", "heroku",
    "digitalocean", "flyio", "nginx", "caddy", "traefik",
    # Databases
    "postgresql", "postgres", "mysql", "mongodb", "redis", "sqlite",
    "elasticsearch", "opensearch", "cassandra", "dynamodb", "neo4j",
    "qdrant", "pinecone", "weaviate", "milvus", "mariadb", "cockroachdb",
    "supabase", "firebase", "couchdb", "influxdb", "clickhouse",
    "timescaledb", "memcached",
    # DevOps / tools
    "jenkins", "circleci", "travisci", "gitlab", "github", "bitbucket",
    "jira", "confluence", "grafana", "prometheus", "datadog", "sentry",
    "pagerduty", "opsgenie", "newrelic", "splunk", "kibana", "logstash",
    "fluentd", "jaeger", "kubectl", "helm", "minikube", "argocd",
    "tekton", "kustomize", "istio", "envoy", "linkerd",
    # Package managers / build tools
    "npm", "yarn", "pnpm", "pip", "pipenv", "poetry", "cargo",
    "maven", "gradle", "webpack", "vite", "esbuild", "rollup", "parcel",
    "turbopack", "bazel", "cmake", "makefile", "homebrew", "apt",
    "dnf", "pacman", "nix", "guix",
    # Editors / IDEs
    "vscode", "vim", "neovim", "nvim", "emacs", "nano", "intellij",
    "pycharm", "webstorm", "goland", "clion", "phpstorm", "rubymine",
    "xcode", "sublime",
    # CLI tools
    "grep", "awk", "sed", "curl", "wget", "htop", "tmux", "zsh",
    "bash", "fish", "fzf", "ripgrep", "fd", "exa", "bat", "jq",
    "yq", "ssh", "scp", "rsync",
    # Common acronyms
    "api", "rest", "graphql", "grpc", "tcp", "udp", "http", "https",
    "ws", "wss", "mqtt", "amqp", "csrf", "cors", "jwt", "oauth",
    "saml", "oidc", "tls", "ssl", "dns", "cdn", "vpn", "vpc",
    "iam", "rbac", "cicd", "mlops", "devops", "sre", "sla", "slo",
    "html", "css", "json", "yaml", "toml", "xml", "csv", "sql",
    "nosql", "cli", "gui", "ui", "ux", "dom", "svg", "png", "jpg",
    "webp", "wasm", "sse", "sdk", "ide",
    # AI/ML terms
    "llm", "rag", "bert", "gpt", "rlhf", "lora", "qlora", "peft",
    "mlflow", "wandb", "dvc", "onnx", "tensorrt", "cuda", "rocm",
    "triton", "ollama", "anthropic", "openai", "gemini", "llama",
    "mistral", "mixtral",
    # Collaboration
    "slack", "discord", "zoom", "figma", "miro", "notion", "obsidian",
    "linear", "asana", "trello", "monday", "clickup", "basecamp",
    "airtable", "webex",
}

# Common entity name corrections
COMMON_CORRECTIONS: Dict[str, str] = {
    "Gogle": "Google",
    "Gooogle": "Google",
    "Googl": "Google",
    "Mircosoft": "Microsoft",
    "Microsft": "Microsoft",
    "Micorsoft": "Microsoft",
    "Amazn": "Amazon",
    "Amzon": "Amazon",
    "Facebok": "Facebook",
    "Facbook": "Facebook",
    "Twtter": "Twitter",
    "Twiter": "Twitter",
    "Linkdin": "LinkedIn",
    "Linkedn": "LinkedIn",
    "Javascipt": "JavaScript",
    "Javscript": "JavaScript",
    "Typescipt": "TypeScript",
    "Typscript": "TypeScript",
    "Kubernets": "Kubernetes",
    "Kuberentes": "Kubernetes",
    "Kubernetse": "Kubernetes",
    "Postgrsql": "PostgreSQL",
    "Postgressql": "PostgreSQL",
    "Elasticserch": "Elasticsearch",
    "Elastcsearch": "Elasticsearch",
    "Promethus": "Prometheus",
    "Promethesu": "Prometheus",
    "Terrafrom": "Terraform",
    "Teraform": "Terraform",
}


class SpellingCorrector:
    """
    Tech-aware spelling correction service.

    Corrects common typos in entity names and text while preserving
    technical terms, acronyms, and domain-specific vocabulary.
    """

    def __init__(self):
        """Initialize with tech whitelist and common corrections."""
        self.whitelist = TECH_WHITELIST
        self.corrections = COMMON_CORRECTIONS

        # Build case-insensitive lookup
        self._corrections_lower = {k.lower(): v for k, v in self.corrections.items()}
        self._whitelist_lower = {w.lower() for w in self.whitelist}

    def correct_entity_name(self, text: str) -> Dict[str, Any]:
        """
        Correct potential spelling issues in an entity name.

        Args:
            text: Entity name to check

        Returns:
            Dict with:
            - original: str — original text
            - corrected: str — corrected text (same if no changes)
            - was_corrected: bool — whether any correction was applied
            - confidence: float — correction confidence (0-1)
            - corrections: List[Dict] — individual corrections applied
        """
        if not text or len(text.strip()) == 0:
            return {
                'original': text,
                'corrected': text,
                'was_corrected': False,
                'confidence': 1.0,
                'corrections': []
            }

        corrections_applied = []
        corrected = text

        # 1. Check against known corrections dictionary
        dict_result = self._apply_dictionary_corrections(corrected)
        if dict_result['was_corrected']:
            corrected = dict_result['corrected']
            corrections_applied.extend(dict_result['corrections'])

        # 2. Fix common casing issues
        case_result = self._fix_casing(corrected)
        if case_result['was_corrected']:
            corrected = case_result['corrected']
            corrections_applied.extend(case_result['corrections'])

        # 3. Remove trailing/leading artifacts
        cleaned = self._clean_artifacts(corrected)
        if cleaned != corrected:
            corrections_applied.append({
                'type': 'cleanup',
                'original': corrected,
                'corrected': cleaned,
            })
            corrected = cleaned

        was_corrected = len(corrections_applied) > 0
        confidence = 0.95 if was_corrected else 1.0

        return {
            'original': text,
            'corrected': corrected,
            'was_corrected': was_corrected,
            'confidence': confidence,
            'corrections': corrections_applied,
        }

    def correct_text(self, text: str, preserve_tech: bool = True) -> Dict[str, Any]:
        """
        Correct spelling in general text while preserving technical terms.

        Args:
            text: Text to correct
            preserve_tech: If True, skip words in the tech whitelist

        Returns:
            Dict with original, corrected, was_corrected, corrections
        """
        if not text:
            return {
                'original': text,
                'corrected': text,
                'was_corrected': False,
                'corrections': []
            }

        words = text.split()
        corrected_words = []
        corrections = []

        for word in words:
            # Skip if in whitelist
            clean_word = re.sub(r'[^\w]', '', word).lower()
            if preserve_tech and clean_word in self._whitelist_lower:
                corrected_words.append(word)
                continue

            # Check dictionary corrections
            if word in self.corrections:
                corrected_words.append(self.corrections[word])
                corrections.append({
                    'type': 'dictionary',
                    'original': word,
                    'corrected': self.corrections[word],
                })
            elif word.lower() in self._corrections_lower:
                corrected_words.append(self._corrections_lower[word.lower()])
                corrections.append({
                    'type': 'dictionary',
                    'original': word,
                    'corrected': self._corrections_lower[word.lower()],
                })
            else:
                corrected_words.append(word)

        corrected = ' '.join(corrected_words)

        return {
            'original': text,
            'corrected': corrected,
            'was_corrected': len(corrections) > 0,
            'corrections': corrections,
        }

    def is_tech_term(self, word: str) -> bool:
        """Check if a word is a known technical term."""
        return word.lower().strip() in self._whitelist_lower

    def add_to_whitelist(self, term: str):
        """Add a custom term to the tech whitelist."""
        self.whitelist.add(term)
        self._whitelist_lower.add(term.lower())

    def add_correction(self, wrong: str, correct: str):
        """Add a custom correction rule."""
        self.corrections[wrong] = correct
        self._corrections_lower[wrong.lower()] = correct

    def _apply_dictionary_corrections(self, text: str) -> Dict[str, Any]:
        """Apply known dictionary corrections using whole-word matching."""
        corrections = []
        corrected = text

        for wrong, right in self.corrections.items():
            # Use word-boundary regex to avoid substring corruption
            pattern = re.compile(re.escape(wrong), re.IGNORECASE)
            # Only match if the wrong word is the entire text or bounded by non-alpha
            if wrong == corrected or wrong.lower() == corrected.lower():
                corrected = right
                corrections.append({
                    'type': 'dictionary',
                    'original': wrong,
                    'corrected': right,
                })
                break
            elif re.search(r'\b' + re.escape(wrong) + r'\b', corrected):
                corrected = re.sub(r'\b' + re.escape(wrong) + r'\b', right, corrected)
                corrections.append({
                    'type': 'dictionary',
                    'original': wrong,
                    'corrected': right,
                })

        return {
            'corrected': corrected,
            'was_corrected': len(corrections) > 0,
            'corrections': corrections,
        }

    def _fix_casing(self, text: str) -> Dict[str, Any]:
        """Fix common casing issues (e.g., 'javascript' → 'JavaScript')."""
        casing_map = {
            "javascript": "JavaScript",
            "typescript": "TypeScript",
            "postgresql": "PostgreSQL",
            "mongodb": "MongoDB",
            "elasticsearch": "Elasticsearch",
            "graphql": "GraphQL",
            "github": "GitHub",
            "gitlab": "GitLab",
            "bitbucket": "Bitbucket",
            "kubernetes": "Kubernetes",
            "terraform": "Terraform",
            "fastapi": "FastAPI",
            "nodejs": "Node.js",
            "reactjs": "React.js",
            "vuejs": "Vue.js",
            "nextjs": "Next.js",
            "nuxtjs": "Nuxt.js",
            "openai": "OpenAI",
            "chatgpt": "ChatGPT",
            "pytorch": "PyTorch",
            "tensorflow": "TensorFlow",
            "linkedin": "LinkedIn",
            "youtube": "YouTube",
            "stackoverflow": "StackOverflow",
            "macos": "macOS",
            "ios": "iOS",
            "iphone": "iPhone",
            "ipad": "iPad",
            "devops": "DevOps",
            "mysql": "MySQL",
            "sqlite": "SQLite",
            "sqlalchemy": "SQLAlchemy",
            "redis": "Redis",
            "neo4j": "Neo4j",
            "qdrant": "Qdrant",
        }

        corrections = []
        corrected = text

        # Only fix if entire text matches (for entity names)
        text_lower = text.lower().strip()
        if text_lower in casing_map:
            proper = casing_map[text_lower]
            if text.strip() != proper:
                corrections.append({
                    'type': 'casing',
                    'original': text.strip(),
                    'corrected': proper,
                })
                corrected = proper

        return {
            'corrected': corrected,
            'was_corrected': len(corrections) > 0,
            'corrections': corrections,
        }

    def _clean_artifacts(self, text: str) -> str:
        """Remove trailing/leading artifacts from entity names."""
        # Remove trailing dots, commas, colons
        cleaned = text.strip().rstrip('.,;:!?')
        # Remove enclosing quotes
        if len(cleaned) >= 2 and cleaned[0] in ('"', "'") and cleaned[-1] == cleaned[0]:
            cleaned = cleaned[1:-1]
        # Remove enclosing brackets/parens
        if len(cleaned) >= 2 and cleaned[0] in ('(', '[', '{') and cleaned[-1] in (')', ']', '}'):
            cleaned = cleaned[1:-1]
        return cleaned.strip()


# Global instance
spelling_corrector = SpellingCorrector()
