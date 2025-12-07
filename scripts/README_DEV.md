Development helper scripts

dev_start_simple.ps1 / dev_start_simple.sh
- Start backend (uvicorn) and frontend (spa_server.py) for local development.
- Windows (PowerShell): `.\scripts\dev_start_simple.ps1 -BackendPort 8000 -FrontendPort 8001 -VenvPath ".\venv\Scripts\Activate.ps1"`
- WSL/macOS/Linux: `./scripts/dev_start_simple.sh 8000 8001 .venv/bin/activate`
- Optional live-reload for frontend (PowerShell only): `-FrontendLiveReload` uses `scripts/lr_frontend.py`.

db_diag.py
- Connectivity + quick season counts.
- Example: `python scripts/db_diag.py --season 11`

Manual alternatives
- Backend only: `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`
- Frontend only: `python frontend/spa_server.py 8080`

Notes
- Activate your virtualenv before running the scripts so the correct interpreter is used.
