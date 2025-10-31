#!/usr/bin/env pwsh
<#
Creates a local virtualenv in .venv, installs requirements.txt, and runs a small
import smoke-test to ensure required packages can be imported.

Usage (PowerShell):
  powershell -ExecutionPolicy Bypass -File .\scripts\run_in_venv.ps1
#>

$ErrorActionPreference = 'Stop'

$venv = Join-Path $PSScriptRoot '..' -Resolve | Join-Path -ChildPath '.venv'
if (-not (Test-Path $venv)) {
    Write-Host "Creating virtualenv at $venv"
    python -m venv $venv
} else {
    Write-Host "Re-using virtualenv at $venv"
}

$activate = Join-Path $venv 'Scripts\Activate.ps1'
if (-not (Test-Path $activate)) {
    Write-Error "Activate script not found at $activate"
    exit 2
}

Write-Host "Activating virtualenv"
. $activate

Write-Host "Upgrading pip and installing requirements"
python -m pip install --upgrade pip setuptools wheel
python -m pip install --no-cache-dir -r requirements.txt

Write-Host "Running import smoke-test"
$tmp = [System.IO.Path]::GetTempFileName()
$py = "$tmp.py"
[System.IO.File]::WriteAllText($py, @'
import sys
try:
    import asyncmy, httpx, requests, fastapi, uvicorn, tenacity, aiofiles, dateutil
except Exception as e:
    print('IMPORTS FAILED', e, file=sys.stderr)
    raise
else:
    print('IMPORTS OK', sys.version)
'@)

$proc = Start-Process -FilePath python -ArgumentList $py -NoNewWindow -Wait -PassThru -RedirectStandardError "$tmp.err" -RedirectStandardOutput "$tmp.out"
if ($proc.ExitCode -eq 0) {
    Get-Content "$tmp.out" | ForEach-Object { Write-Host $_ }
    Write-Host "Smoke-test passed"
    Remove-Item -Force $py, "$tmp.out", "$tmp.err" -ErrorAction SilentlyContinue
    exit 0
} else {
    if (Test-Path "$tmp.err") { Get-Content "$tmp.err" | ForEach-Object { Write-Error $_ } }
    if (Test-Path "$tmp.out") { Get-Content "$tmp.out" | ForEach-Object { Write-Host $_ } }
    Write-Error "Smoke-test failed (exit code $($proc.ExitCode))"
    Remove-Item -Force $py, "$tmp.out", "$tmp.err" -ErrorAction SilentlyContinue
    exit $proc.ExitCode
}
