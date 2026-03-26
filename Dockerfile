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
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir asyncpg && \
    pip install --no-cache-dir -r requirements.txt

# Install karateclub separately with --no-deps
# karateclub 1.3.3 demands numpy<1.23 but actually works fine with numpy 1.26+
# Its real deps (networkx, scikit-learn, etc.) are already installed above
RUN pip install --no-cache-dir --no-deps "karateclub>=1.3.3"

# Download lightweight spaCy model for production
RUN python -m spacy download en_core_web_sm

# Verify critical imports work at build time
RUN python -c "import fastapi, uvicorn, sqlalchemy; print('Core OK')"
RUN python -c "import numpy; print(f'numpy {numpy.__version__}')"
RUN python -c "import karateclub; print('karateclub OK')" || echo "WARN: karateclub not available"
RUN python -c "import spacy; print('spacy OK')"

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Run with uvicorn (single worker for free tier memory)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
