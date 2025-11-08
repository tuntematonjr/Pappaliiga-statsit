<#
Install Python dependencies for the project into a virtualenv.

Usage (from repository root):
    # Create venv in .\venv and install requirements.txt
    .\scripts\install_deps.ps1

    # Custom venv dir and python executable
    .\scripts\install_deps.ps1 -VenvDir '.\\.venv' -PythonExe 'C:\\Python310\\python.exe'

This script will:
- Create a virtual environment if it doesn't exist
- Activate the venv in the current PowerShell session (unless -NoActivate)
- Upgrade pip and install packages from requirements.txt
#>
param(
    [string]$VenvDir = ".\\venv",
    [string]$Requirements = "requirements.txt",
    [string]$PythonExe = "python",
    [switch]$NoActivate = $false,
    [switch]$UpgradePip = $true
)

Set-Location -Path (Split-Path -Path $MyInvocation.MyCommand.Path -Parent) | Out-Null
$repoRoot = Resolve-Path -Path ".." | Select-Object -ExpandProperty Path
Push-Location $repoRoot

# Resolve python executable: prefer user-specified, then try common names
function Resolve-PythonExecutable {
    param([string[]]$candidates)
    foreach ($c in $candidates) {
        try {
            $cmd = Get-Command $c -ErrorAction Stop
            if ($cmd -and $cmd.Path) { return $cmd.Path }
        } catch {
            # ignore
        }
    }
    return $null
}

$pythonCandidates = @($PythonExe, 'python', 'py', 'python3') | Where-Object { $_ -and $_ -ne '' }
$ResolvedPython = Resolve-PythonExecutable -candidates $pythonCandidates
if (-not $ResolvedPython) {
    Write-Error "No suitable Python executable found. Tried: $($pythonCandidates -join ', '). Install Python from https://www.python.org/ or use 'winget install Python.Python.3', then re-run this script or pass -PythonExe 'C:\Path\To\python.exe'."
    Pop-Location
    exit 3
} else {
    # Detect Microsoft Store / App Execution Alias stub which commonly lives under WindowsApps
    if ($ResolvedPython -and ($ResolvedPython -like '*WindowsApps*' -or $ResolvedPython -like '*AppData*Microsoft\WindowsApps*')) {
        Write-Warning "Detected Python executable in WindowsApps (store stub): $ResolvedPython"
        # Try to prefer the py launcher or python3 if available
        $fallback = Resolve-PythonExecutable -candidates @('py','python3','python')
        if ($fallback -and ($fallback -notlike '*WindowsApps*')) {
            Write-Host "Preferring alternate Python executable: $fallback"
            $ResolvedPython = $fallback
        } else {
            Write-Warning "No alternate Python executable (py/python3) found. The WindowsApps python is a Store placeholder and can't create venvs."
            Write-Host "You can install Python from https://www.python.org/ or run this script with -PythonExe 'C:\Path\To\python.exe' or try the 'py' launcher: .\scripts\install_deps.ps1 -PythonExe py"
        }
    }
    Write-Host "Using Python executable: $ResolvedPython"
}

if (-not (Test-Path $Requirements)) {
    Write-Error "Requirements file '$Requirements' not found in project root."
    Pop-Location
    exit 1
}

if (-not (Test-Path $VenvDir)) {
    Write-Host "Creating virtual environment in $VenvDir..."
    & $ResolvedPython -m venv $VenvDir
    if ($LASTEXITCODE -ne 0) { Write-Error ("Failed to create venv with {0}" -f $ResolvedPython); Pop-Location; exit 2 }
} else {
    Write-Host "Virtual environment found at $VenvDir"
}

$activateScript = Join-Path $VenvDir 'Scripts\Activate.ps1'
if (-not (Test-Path $activateScript)) {
    # Use -f formatting to avoid quote/backslash parsing issues
    Write-Warning ("Activation script not found at {0} - continuing without activation. You can run {1}\Scripts\Activate.ps1 manually." -f $activateScript, $VenvDir)
    $activateScript = $null
}

if ($activateScript -and -not $NoActivate) {
    Write-Host "Activating virtual environment..."
    # Use the activation in the current session
    . $activateScript
} elseif ($NoActivate) {
    Write-Host "Skipping activation as requested (-NoActivate)."
}

if ($UpgradePip) {
    Write-Host "Upgrading pip..."
    & (Join-Path $VenvDir 'Scripts\pip.exe') install --upgrade pip 2>$null
}

Write-Host "Installing packages from $Requirements..."
& (Join-Path $VenvDir 'Scripts\pip.exe') install -r $Requirements

if ($LASTEXITCODE -eq 0) {
    Write-Host "Dependencies installed successfully."
    if (-not $NoActivate -and $activateScript) {
        Write-Host "Virtual environment is active in this session. To deactivate later, run: deactivate"
    } else {
        Write-Host ("Activate the virtualenv before using the project: .\{0}\Scripts\Activate.ps1" -f $VenvDir)
    }
} else {
    Write-Error "pip install finished with errors (exit code $LASTEXITCODE). Check output above."
}

Pop-Location
