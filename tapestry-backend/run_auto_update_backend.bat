@echo off
setlocal

cd /d "%~dp0"

set AUTO_UPDATE_ENABLED=1
if "%AUTO_UPDATE_INTERVAL_MINUTES%"=="" set AUTO_UPDATE_INTERVAL_MINUTES=60
if "%AUTO_UPDATE_FULL_RETRAIN_ENABLED%"=="" set AUTO_UPDATE_FULL_RETRAIN_ENABLED=1
if "%AUTO_UPDATE_FULL_RETRAIN_HOUR%"=="" set AUTO_UPDATE_FULL_RETRAIN_HOUR=3

echo TAPESTRY auto-update backend
echo ----------------------------
echo Hourly refresh: every %AUTO_UPDATE_INTERVAL_MINUTES% minutes
if "%AUTO_UPDATE_FULL_RETRAIN_ENABLED%"=="1" (
  echo Nightly full retrain: %AUTO_UPDATE_FULL_RETRAIN_HOUR%:00 Pacific
) else (
  echo Nightly full retrain: disabled
)
echo.
echo Starting backend on http://localhost:8000
echo Leave this window open to keep automatic updates running.
echo.

call .\.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000
