#!/usr/bin/env bash
# Simple launcher for FastAPI (serves API + frontend) for WSL/macOS/Linux
# Usage:
#   chmod +x scripts/dev_start_simple.sh
#   ./scripts/dev_start_simple.sh [port] [venv_path]
# Example:
#   ./scripts/dev_start_simple.sh 8000 .venv/bin/activate

BACKEND_PORT=${1:-8000}
VENV_ACTIVATE=${2:-"./venv/bin/activate"}

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

if [ -f "$VENV_ACTIVATE" ]; then
  # shellcheck source=/dev/null
  source "$VENV_ACTIVATE"
  echo "Activated venv from $VENV_ACTIVATE"
else
  echo "No venv activate script found at $VENV_ACTIVATE — running with system Python"
fi

echo "Starting backend on port $BACKEND_PORT (logs -> backend.log)"
nohup python -m uvicorn api.main:app --reload --host 0.0.0.0 --port "$BACKEND_PORT" > backend.log 2>&1 &
BACKEND_PID=$!

echo "Backend PID: $BACKEND_PID"
echo "Logs: backend.log"
