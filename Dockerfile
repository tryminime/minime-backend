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

# Install Python dependencies — numpy<2 is pinned in requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir asyncpg && \
    pip install --no-cache-dir -r requirements.txt

# Download lightweight spaCy model for production
RUN python -m spacy download en_core_web_sm

# Verify critical imports work
RUN python -c "import fastapi; import uvicorn; import sqlalchemy; print('Core imports OK')"
RUN python -c "import numpy; print(f'numpy {numpy.__version__}')"
RUN python -c "import karateclub; print('karateclub OK')" || echo "WARN: karateclub not available (non-fatal)"

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run with uvicorn (single worker for free tier memory)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
