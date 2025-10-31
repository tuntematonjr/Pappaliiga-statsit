Development helper scripts

dev_start.ps1
- Starts backend (uvicorn) and frontend (serve_frontend.py) in separate PowerShell windows so you can view logs interactively.

Usage examples (PowerShell):

```powershell
# From repo root
.\scripts\dev_start.ps1

# With custom ports
.\scripts\dev_start.ps1 -BackendPort 8000 -FrontendPort 8001
```

Manual alternatives
- Start backend only:

```powershell
python -m uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- Start frontend only:

```powershell
python serve_frontend.py 8080
```

Notes
- The script uses `Start-Process` to open new terminals for each service so logs are visible. If you'd rather have both logs in the same terminal, run the above manual commands in separate tabs or use a multiplexer.
- Ensure your Python environment has the project dependencies installed (`requirements.txt`).
- If you use a virtualenv, activate it before running the script so the correct Python interpreter is used.
