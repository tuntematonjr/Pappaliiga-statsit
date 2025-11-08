<#
Simple launcher for backend (uvicorn) and frontend (spa_server.py) on Windows PowerShell.

Usage:
    # From repository root
    .\scripts\dev_start_simple.ps1

    # With custom ports or venv path
    .\scripts\dev_start_simple.ps1 -BackendPort 8000 -FrontendPort 8001 -VenvPath ".\\venv\\Scripts\\Activate.ps1"

What it does:
- Optionally activates a virtualenv if the given Activate.ps1 exists.
- Opens two new PowerShell windows (keeps them open) running backend and frontend.
- Falls back to calling `python` directly when no venv activation script is found.
#>
param(
    [int]$BackendPort = 8000,
    [int]$FrontendPort = 8001,
    [string]$VenvPath = ".\\venv\\Scripts\\Activate.ps1"
)

Set-Location -Path (Split-Path -Path $MyInvocation.MyCommand.Path -Parent) | Out-Null
$repoRoot = Resolve-Path -Path ".." | Select-Object -ExpandProperty Path
Push-Location $repoRoot

$activateExists = Test-Path $VenvPath

if ($activateExists) {
    # Use a command string that activates venv then runs the command
    $backendCmd = "& { `"$VenvPath`"; python -m uvicorn api.main:app --reload --host 0.0.0.0 --port $BackendPort }"
    $frontendCmd = "& { `"$VenvPath`"; python .\\frontend\\spa_server.py $FrontendPort }"
} else {
    $backendCmd = "python -m uvicorn api.main:app --reload --host 0.0.0.0 --port $BackendPort"
    $frontendCmd = "python .\\frontend\\spa_server.py $FrontendPort"
}

Write-Host "Launching backend on port $BackendPort and frontend on port $FrontendPort..."

# Start backend in a new PowerShell window and keep it open
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", $backendCmd

# Start frontend in a new PowerShell window and keep it open
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", $frontendCmd

Write-Host "Started processes. Check the new windows for logs."

Pop-Location
