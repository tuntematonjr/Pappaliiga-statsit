@echo off
REM Copy updated static assets from web_static to docs
xcopy /Y /D /S /R /C /Q web_static\* docs\
echo All files from web_static moved to docs\
