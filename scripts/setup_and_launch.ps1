<#
Prompt for DATABASE_URL (or take via parameter), write a .env in repo root and launch the dev starter.

Usage (from repo root):
    # Interactive prompt
    .\scripts\setup_and_launch.ps1

    # Provide DATABASE_URL directly
    .\scripts\setup_and_launch.ps1 -DatabaseUrl 'mariadb://user:pass@127.0.0.1:3306/pappaliiga_stats'

This writes a minimal .env file containing DATABASE_URL and then runs the existing
`scripts\dev_start_simple.ps1` to start backend and frontend. The new processes will
read the .env file automatically.
#>
param(
    [string]$DatabaseUrl
)

function Get-RepoRoot {
    # script is located in scripts/; repo root is parent
    $scriptDir = Split-Path -Path $MyInvocation.MyCommand.Path -Parent
    return Resolve-Path -Path (Join-Path $scriptDir '..') | Select-Object -ExpandProperty Path
}

$repoRoot = Get-RepoRoot
Push-Location $repoRoot

if (-not $DatabaseUrl) {
    Write-Host "Enter your DATABASE_URL (example: mariadb://user:pass@127.0.0.1:3306/pappaliiga_stats)"
    $DatabaseUrl = Read-Host -Prompt 'DATABASE_URL'
}

if (-not $DatabaseUrl -or $DatabaseUrl.Trim() -eq '') {
    Write-Error "No DATABASE_URL provided; aborting."
    Pop-Location
    exit 1
}

$envPath = Join-Path $repoRoot '.env'
if (Test-Path $envPath) {
    $ok = Read-Host -Prompt ".env already exists. Overwrite? (y/N)"
    if ($ok.ToLower() -ne 'y') {
        Write-Host "Aborting without changes. Existing .env preserved at $envPath"
        Pop-Location
        exit 0
    }
}

Write-Host "Writing .env to $envPath"
Set-Content -Path $envPath -Value ("DATABASE_URL={0}" -f $DatabaseUrl) -Encoding UTF8

Write-Host "Launching development servers (backend + frontend)..."
# Call the existing dev starter; it will read the .env file from repo root
.
\
\scripts\dev_start_simple.ps1

Pop-Location
