FROM python:3.12-slim

# System dependencies for weasyprint, psycopg, etc.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libffi-dev \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libgdk-pixbuf-2.0-0 \
    libcairo2 \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies
COPY requirements.txt requirements-ai.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir asyncpg && \
    pip install --no-cache-dir -r requirements.txt || true

# Fix karateclub forcing old numpy — install with --no-deps and pin scipy
RUN pip install --no-cache-dir --no-deps "karateclub>=1.3.3" "gensim>=4.3.0" "scipy<1.12.0" || true

# Download lightweight spaCy model for production
RUN python -m spacy download en_core_web_sm || true

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run with uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
