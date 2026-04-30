@echo off
setlocal
cd /d "%~dp0"
echo TAPESTRY continuous source-seeking + training loop
echo ----------------------------------------------------
echo This keeps scraping news/intelligence, backfilling embeddings,
echo retraining the model, and writing reports until you press Ctrl+C.
echo.
echo Reports:
echo   data\continuous_train_summary.json
echo   data\logs\continuous_train_%date:~-4%-%date:~4,2%-%date:~7,2%.log
echo.
".venv\Scripts\python.exe" continuous_train.py --sleep-minutes 20 --race-thresholds 20,30,99
echo.
echo Continuous training stopped. Check data\continuous_train_summary.json.
pause
