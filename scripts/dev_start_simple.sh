#!/usr/bin/env bash
# Simple launcher for backend (uvicorn) and frontend (spa_server.py) for WSL/macOS/Linux
# Usage:
#   chmod +x scripts/dev_start_simple.sh
#   ./scripts/dev_start_simple.sh [backend_port] [frontend_port] [venv_path]
# Example:
#   ./scripts/dev_start_simple.sh 8000 8001 .venv/bin/activate

BACKEND_PORT=${1:-8000}
FRONTEND_PORT=${2:-8001}
VENV_ACTIVATE=${3:-"./venv/bin/activate"}

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
sleep 0.5
echo "Starting frontend on port $FRONTEND_PORT (logs -> frontend.log)"
nohup python frontend/spa_server.py "$FRONTEND_PORT" > frontend.log 2>&1 &

echo "Backend PID: $(jobs -rp | sed -n '1p')"
echo "Frontend PID: $(jobs -rp | sed -n '2p')"
echo "Logs: backend.log, frontend.log"
