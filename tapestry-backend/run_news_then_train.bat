@echo off
setlocal
cd /d "%~dp0"
echo TAPESTRY news refresh + model training
echo --------------------------------------
echo This runs race/local/source-intelligence news, backfills embeddings when available,
echo then trains and scores the model from the collected data.
echo.
".venv\Scripts\python.exe" overnight.py --news-only
echo.
echo Process exited. Check data\overnight_summary.json and data\logs for details.
pause
