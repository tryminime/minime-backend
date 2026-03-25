"""
Application Settings - MiniMe Backend
Centralized configuration using environment variables.
"""

import os
from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings from environment variables."""
    
    # Application
    PROJECT_NAME: str = "MiniMe API"
    APP_NAME: str = "MiniMe Backend"
    VERSION: str = "1.0.0"
    DEBUG: bool = True  # Set to False in production
    ENVIRONMENT: str = "development"  # development, staging, production
    
    # API
    API_V1_PREFIX: str = "/api/v1"
    SECRET_KEY: str = "your-secret-key-change-in-production"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24 hours
    
    # JWT Configuration
    JWT_SECRET_KEY: str = "dev_secret_key_change_in_production_12345"
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 15
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    JWT_REMEMBER_DEVICE_EXPIRE_DAYS: int = 90  # Long-lived token when "remember this device" checked
    
    # Database - PostgreSQL
    POSTGRES_HOST: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = "minime"
    POSTGRES_PASSWORD: str = "minime_dev_password"
    POSTGRES_DB: str = "minime"
    DATABASE_URL_OVERRIDE: Optional[str] = None  # Render sets DATABASE_URL directly
    
    @property
    def DATABASE_URL(self) -> str:
        # Use direct DATABASE_URL if provided (e.g. by Render)
        override = self.DATABASE_URL_OVERRIDE or os.environ.get("DATABASE_URL")
        if override:
            return override
        return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
    
    # Database - Neo4j (Knowledge Graph)
    NEO4J_URI: str = "bolt://localhost:7687"
    NEO4J_USERNAME: str = "neo4j"
    NEO4J_PASSWORD: str = "minime_dev_password"
    NEO4J_DATABASE: str = "neo4j"
    
    # Database - Redis (Caching & Queues)
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: Optional[str] = "minime_dev_password"
    
    @property
    def REDIS_URL(self) -> str:
        if self.REDIS_PASSWORD:
            return f"redis://:{self.REDIS_PASSWORD}@{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"
    
    # Database - Qdrant (Vector Embeddings)
    QDRANT_HOST: str = "localhost"
    QDRANT_PORT: int = 6333
    QDRANT_API_KEY: Optional[str] = None
    
    @property
    def QDRANT_URL(self) -> str:
        return f"http://{self.QDRANT_HOST}:{self.QDRANT_PORT}"
    
    # Cloud Sync Databases (Pro/Enterprise — background sync targets)
    SUPABASE_DB_URL: str = ""
    UPSTASH_REDIS_URL: str = ""
    CLOUD_NEO4J_URI: str = ""
    CLOUD_NEO4J_USERNAME: str = ""
    CLOUD_NEO4J_PASSWORD: str = ""
    CLOUD_NEO4J_DATABASE: str = ""
    CLOUD_QDRANT_URL: str = ""
    CLOUD_QDRANT_API_KEY: str = ""
    
    # Celery
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"
    
    # NLP & Embeddings
    SPACY_MODEL: str = "en_core_web_lg"
    EMBEDDING_MODEL: str = "all-MiniLM-L6-v2"  # Sentence-BERT
    EMBEDDING_DIMENSION: int = 384
    
    # Graph Settings
    GRAPH_NODE2VEC_DIM: int = 128
    GRAPH_NODE2VEC_WALK_LENGTH: int = 30
    GRAPH_NODE2VEC_NUM_WALKS: int = 10
    GRAPH_CENTRALITY_SCHEDULE: str = "0 2 * * 0"  # Sunday 2 AM (cron)
    
    # CORS
    CORS_ORIGINS: str = "http://localhost:3000,http://localhost:8000,http://localhost:5173,http://localhost:1420,https://www.tryminime.com,https://tryminime.com"
    
    # OAuth Integrations
    GITHUB_CLIENT_ID: str = ""
    GITHUB_CLIENT_SECRET: str = ""
    GITHUB_REDIRECT_URI: str = "http://localhost:1420/oauth/callback"
    
    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_REDIRECT_URI: str = "http://localhost:1420/oauth/callback"
    
    NOTION_CLIENT_ID: str = ""
    NOTION_CLIENT_SECRET: str = ""
    NOTION_REDIRECT_URI: str = "http://localhost:1420/oauth/callback"
    
    @property
    def cors_origins_list(self):
        """Split CORS origins into a list."""
        return [origin.strip() for origin in self.CORS_ORIGINS.split(",")]
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json or text
    
    # Security
    BCRYPT_ROUNDS: int = 12
    
    # File Uploads
    MAX_UPLOAD_SIZE: int = 10 * 1024 * 1024  # 10 MB
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
        extra = "allow"  # Allow extra fields from .env file


@lru_cache()
def get_settings() -> Settings:
    """
    Get cached settings instance.
    Uses lru_cache to ensure single instance.
    """
    return Settings()


# Global settings instance
settings = get_settings()
