#!/bin/sh
# Startup script for MiniMe backend (Render production)
# Runs DB migrations first, then starts the API server.

set -e

echo "==> Running database migrations..."
alembic upgrade head
echo "==> Migrations complete. Starting API server..."

exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-10000} --workers 1
