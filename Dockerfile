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

# Copy only requirements first (Docker layer caching)
COPY requirements.txt ./

# Install Python dependencies
# numpy<2 is pinned in requirements.txt to avoid NumPy 2.x issues
# karateclub is NOT installed (its numpy<1.23 pin is irreconcilable)
# — the import is wrapped in try/except in node2vec_service.py
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Download lightweight spaCy model for production
RUN python -m spacy download en_core_web_sm

# Verify critical imports work at build time
RUN python -c "import fastapi, uvicorn, sqlalchemy, asyncpg, psycopg2; print('Core deps OK')"
RUN python -c "import numpy; print(f'numpy {numpy.__version__}')"
RUN python -c "import spacy; print('spacy OK')"

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:${PORT:-10000}/health || exit 1

# Run with uvicorn — use PORT env var from Render (default 10000), fallback to 8000 for local
CMD uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --workers 1
