<#
.SYNOPSIS
Start backend (FastAPI/uvicorn) and frontend (static SPA server) concurrently for local development.

.DESCRIPTION
This script opens two new PowerShell windows (so logs are visible) and runs the backend and frontend dev servers.
It uses the repo-local Python to run uvicorn for the backend and the included `serve_frontend.py` for the frontend.

.EXAMPLES
.
    # Start both with defaults (backend:8000, frontend:8080)
    .\dev_start.ps1

    # Start with custom ports
    .\dev_start.ps1 -BackendPort 8000 -FrontendPort 8001

#>

param(
    [int]$BackendPort = 8000,
    [int]$FrontendPort = 8080
)

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
# Repo root is parent of scripts directory
$Root = Split-Path -Parent $ScriptDir
Set-Location $Root

Write-Host "Repo root: $Root"

# Commands to run
# Build raw commands (these are plain strings so they are easy to inspect)
$BackendCmd = "python -m uvicorn api.main:app --reload --host 0.0.0.0 --port $BackendPort"
$FrontendCmd = "python serve_frontend.py $FrontendPort"

Write-Host "Starting backend on port $BackendPort..."

# Wrap commands into a single -Command argument so Start-Process runs the full sequence in the new shell.
# Use single quotes around $Root to allow paths with spaces, and expand $BackendCmd/$FrontendCmd here so the final string is valid.
$BackendCommandArg = "& { Set-Location -Path '$Root'; $BackendCmd }"
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", $BackendCommandArg -WindowStyle Normal

Start-Sleep -Seconds 1

Write-Host "Starting frontend on port $FrontendPort..."
$FrontendCommandArg = "& { Set-Location -Path '$Root'; $FrontendCmd }"
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", $FrontendCommandArg -WindowStyle Normal

Write-Host ""
Write-Host "Started backend and frontend in separate PowerShell windows. Close those windows or press Ctrl+C in them to stop the servers."
Write-Host "If you prefer logs in this window, comment out the Start-Process calls and use Start-Job / Receive-Job (jobs do not stream live logs by default)."
