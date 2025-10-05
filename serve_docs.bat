@echo off
cd /d "%~dp0docs"
python -m http.server 8000 --bind 192.168.0.13
pause
