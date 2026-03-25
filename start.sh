#!/bin/bash
# Start the MiniMe backend server

cd "$(dirname "$0")/.."
source backend/venv/bin/activate
python -m uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
