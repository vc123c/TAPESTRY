@echo off
setlocal

cd /d "%~dp0"

echo.
echo TAPESTRY Vercel deploy
echo ======================
echo.

where npm >nul 2>nul
if errorlevel 1 (
  echo ERROR: npm is not available in this Command Prompt.
  echo.
  echo Install Node.js LTS from:
  echo   https://nodejs.org
  echo.
  echo Then close and reopen Command Prompt and run this file again:
  echo   deploy_vercel.bat
  echo.
  pause
  exit /b 1
)

echo Installing frontend deploy dependencies...
call npm install
if errorlevel 1 exit /b 1

echo.
echo Building TAPESTRY frontend for Render backend...
set VITE_API_URL=https://tapestry-2iyf.onrender.com
call npm run build
if errorlevel 1 exit /b 1

echo.
echo Checking Vercel CLI...
where vercel >nul 2>nul
if errorlevel 1 (
  echo Installing Vercel CLI globally...
  call npm install -g vercel
  if errorlevel 1 exit /b 1
)

echo.
echo Deploying to Vercel production...
echo If prompted, log in and accept the defaults:
echo   Set up and deploy? Y
echo   Which scope? your account
echo   Link to existing project? N unless you already created one
echo   Project name? tapestry
echo   Directory? ./
echo   Override settings? N
echo.
vercel deploy --prod --build-env VITE_API_URL=https://tapestry-2iyf.onrender.com --env VITE_API_URL=https://tapestry-2iyf.onrender.com

echo.
echo Done. Copy the https://*.vercel.app URL above as your submission frontend URL.
pause
