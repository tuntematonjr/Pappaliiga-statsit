Development helper scripts

dev_start_simple.ps1 / dev_start_simple.sh
- Start FastAPI via uvicorn; it serves both API and frontend assets.
- Windows (PowerShell): `.\scripts\dev_start_simple.ps1 -Port 8000 -VenvPath ".\venv\Scripts\Activate.ps1"`
- WSL/macOS/Linux: `./scripts/dev_start_simple.sh 8000 .venv/bin/activate`

db_diag.py
- Connectivity + quick season counts.
- Example: `python scripts/db_diag.py --season 11`

Manual alternatives
- API + frontend together: `python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000`
- Legacy frontend-only static server (not needed when uvicorn is running): `python frontend/spa_server.py 8080`

Notes
- Activate your virtualenv before running the scripts so the correct interpreter is used.
