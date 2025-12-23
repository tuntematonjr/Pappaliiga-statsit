<#
Simple launcher for FastAPI (serves API + frontend) on Windows PowerShell.

Usage:
    # From repository root
    .\scripts\dev_start_simple.ps1

    # With a custom port or venv path
    .\scripts\dev_start_simple.ps1 -Port 8000 -VenvPath ".\\venv\\Scripts\\Activate.ps1"

What it does:
- Optionally activates a virtualenv if the given Activate.ps1 exists.
- Opens a new PowerShell window (keeps it open) running uvicorn.
- FastAPI serves both the API and the static SPA assets, so no separate frontend server is needed.
#>
param(
    [int]$Port = 8000,
    [string]$VenvPath = ".\\venv\\Scripts\\Activate.ps1"
)

Set-Location -Path (Split-Path -Path $MyInvocation.MyCommand.Path -Parent) | Out-Null
$repoRoot = Resolve-Path -Path ".." | Select-Object -ExpandProperty Path
Push-Location $repoRoot

$activateExists = Test-Path $VenvPath

if ($activateExists) {
    # Use a command string that activates venv then runs the command
    $backendCmd = "& { `"$VenvPath`"; python -m uvicorn api.main:app --reload --host 0.0.0.0 --port $Port }"
} else {
    $backendCmd = "python -m uvicorn api.main:app --reload --host 0.0.0.0 --port $Port"
}

Write-Host "Launching backend on port $Port..."

# Start backend in a new PowerShell window and keep it open
Start-Process -FilePath "powershell.exe" -ArgumentList "-NoExit", "-Command", $backendCmd

Write-Host "Started process. Check the new window for logs."

Pop-Location
