@echo off
setlocal enabledelayedexpansion

REM Run from the folder where this .bat is located
cd /d "%~dp0"

REM Optional: activate venv
REM call venv\Scripts\activate

echo [1/2] Running sync.py...
python sync.py
if errorlevel 1 (
  echo [ERROR] sync.py failed. Aborting.
  exit /b 1
)

echo [2/2] Generating HTML...
python html_gen.py
if errorlevel 1 (
  echo [ERROR] html_gen.py failed.
  exit /b 1
)

echo [OK] Done.
pause
