#!/bin/sh
# Startup script for MiniMe backend (Render production)
# Creates DB tables via SQLAlchemy create_all(), then starts the API server.

set -e

echo "==> Initializing database tables..."
python scripts/init_db_render.py

echo "==> Starting API server on port ${PORT:-10000}..."
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --workers 1
