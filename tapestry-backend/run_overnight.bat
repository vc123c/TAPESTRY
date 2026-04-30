@echo off
setlocal
cd /d "%~dp0"
echo TAPESTRY overnight pipeline
echo ---------------------------
echo This will keep running and write reports under data\logs and data\overnight_summary.json.
echo Google Trends is skipped in this stable launcher because it is heavily rate limited.
echo Run ".venv\Scripts\python.exe overnight.py" manually if you want that slow step included.
echo.
".venv\Scripts\python.exe" overnight.py --skip-slow-trends
echo.
echo Overnight process exited. Check data\overnight_summary.json and data\logs for details.
pause
