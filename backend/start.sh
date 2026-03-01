#!/usr/bin/env sh
set -e
alembic upgrade head || true
exec uvicorn main:app --host 0.0.0.0 --port ${PORT:-8080}
