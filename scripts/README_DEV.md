# Dev Scripts

## Start App (API + Frontend)
- Windows (PowerShell): `.\scripts\dev_start_simple.ps1 -Port 8000 -VenvPath ".\venv\Scripts\Activate.ps1"`
- WSL/macOS/Linux: `./scripts/dev_start_simple.sh 8000 .venv/bin/activate`

## DB Diagnostics
- `python scripts/db_diag.py --season 11`

## Manual Start
- `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`

## Legacy Static Server (optional)
- `python frontend/spa_server.py 8080`

Use your project virtualenv before running scripts.
