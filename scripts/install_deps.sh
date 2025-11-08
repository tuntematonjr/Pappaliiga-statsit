#!/usr/bin/env bash
# Install Python dependencies into a virtualenv (WSL / macOS / Linux)
# Usage:
#   chmod +x scripts/install_deps.sh
#   ./scripts/install_deps.sh [venv_dir] [python_executable]
# Example:
#   ./scripts/install_deps.sh .venv python3

VENV_DIR=${1:-"./venv"}
PYTHON=${2:-python3}
REQUIREMENTS=${3:-requirements.txt}

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR" || exit 1

if [ ! -f "$REQUIREMENTS" ]; then
  echo "Requirements file '$REQUIREMENTS' not found. Aborting." >&2
  exit 1
fi

if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtualenv at $VENV_DIR using $PYTHON..."
  $PYTHON -m venv "$VENV_DIR" || { echo "Failed to create venv"; exit 2; }
else
  echo "Virtualenv exists at $VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing requirements from $REQUIREMENTS..."
pip install -r "$REQUIREMENTS"

if [ $? -eq 0 ]; then
  echo "Dependencies installed successfully. Virtualenv active.";
  echo "To deactivate: deactivate"
else
  echo "pip finished with errors." >&2; exit 3
fi
