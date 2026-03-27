FROM python:3.12-slim

# System dependencies — minimal set for production API
# (no pango/cairo/gdk since weasyprint is not used on Render)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    libffi-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy slim production requirements
COPY requirements.render.txt ./

# Install only what the API server needs on Render (~150MB, not ~2GB)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.render.txt

# Verify critical imports work at build time
RUN python -c "import fastapi, uvicorn, sqlalchemy, asyncpg, psycopg2, stripe; print('Core deps OK')"

# Copy application code
COPY . .

# Expose port (default Render PORT is 10000)
EXPOSE 10000

# Health check using Render's PORT env var
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:${PORT:-10000}/health || exit 1

# Make startup script executable
RUN chmod +x start.sh

# Run migrations then start server
CMD ["sh", "start.sh"]
