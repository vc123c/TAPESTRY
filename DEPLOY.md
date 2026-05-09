# TAPESTRY Deployment

## Step 1: Export Demo Database

Run locally:

```bat
cd tapestry-backend
python scripts/export_demo_db.py
```

This creates `data/tapestry_demo.duckdb`. The demo database strips embedding BLOB columns that are only needed for training, while preserving forecast, roster, FEC, Census, article metadata, event, market, and morning-brief serving data.

Latest local export:

- Full DB: ~180 MB
- Demo DB: ~28 MB

## Step 2: Deploy Backend To Render

1. Go to [Render](https://render.com) and create a free account.
2. Click **New** -> **Web Service**.
3. Connect the GitHub repository.
4. Configure:
   - Root directory: `tapestry-backend`
   - Build command: `pip install -r requirements_serve.txt`
   - Start command: `uvicorn main:app --host 0.0.0.0 --port $PORT`
   - Instance type: Free
5. Add environment variables:
   - `DATABASE_PATH=./data/tapestry_demo.duckdb`
   - `ENVIRONMENT=production`
   - `AUTO_UPDATE_ENABLED=1`
   - `AUTO_UPDATE_INTERVAL_MINUTES=60`
   - `AUTO_UPDATE_FULL_RETRAIN_ENABLED=1`
   - `AUTO_UPDATE_FULL_RETRAIN_HOUR=3`
6. Database option for submission:
   - Simplest: commit `tapestry-backend/data/tapestry_demo.duckdb`.
   - If GitHub rejects the file, use Git LFS:

```bat
git lfs install
git lfs track "*.duckdb"
git add .gitattributes
git add -f tapestry-backend/data/tapestry_demo.duckdb
git commit -m "Add demo database for Render deployment"
```

7. Create the web service.
8. Note the backend URL, for example:

```text
https://tapestry-2iyf.onrender.com
```

Current Render URL supplied for this project:

```text
https://tapestry-2iyf.onrender.com
```

If this URL returns `404 Not Found` for `/`, `/docs`, or `/api/chambers`, the service is reachable but is not running the TAPESTRY FastAPI app. In Render, check:

- Root Directory must be `tapestry-backend`
- Build Command must be `pip install -r requirements_serve.txt`
- Start Command must be `uvicorn main:app --host 0.0.0.0 --port $PORT`
- Environment variables must include:
  - `DATABASE_PATH=./data/tapestry_demo.duckdb`
  - `ENVIRONMENT=production`
- The GitHub repo/deploy must include `tapestry-backend/data/tapestry_demo.duckdb`

After changing these settings, click **Manual Deploy** -> **Clear build cache & deploy**.

Render free tier may spin down after inactivity. First request after spin-down can take around 30 seconds.
Because of that spin-down behavior, the new hourly auto-update scheduler is perfect for local always-on use and acceptable for demos on Render, but it is not a true always-on hourly cloud worker unless the service is kept awake.

## Step 3: Configure Frontend API URL

For Vercel, set:

```text
VITE_API_URL=https://your-render-url.onrender.com
```

The checked-in `.env.production` currently uses:

```text
VITE_API_URL=https://tapestry-2iyf.onrender.com
```

Update this after Render gives the actual URL.

## Step 4: Deploy Frontend To Vercel

Command-line option:

```bat
deploy_vercel.bat
```

This script builds the frontend with:

```text
VITE_API_URL=https://tapestry-2iyf.onrender.com
```

and then runs:

```bat
vercel deploy --prod
```

If it says `npm is not available`, install Node.js LTS from `https://nodejs.org`, close and reopen Command Prompt, then run `deploy_vercel.bat` again.

Dashboard option:

1. Go to [Vercel](https://vercel.com).
2. Click **Add New** -> **Project**.
3. Import the GitHub repository.
4. Configure:
   - Root Directory: project root
   - Build Command: `npm run build`
   - Output Directory: `dist`
5. Add environment variable:
   - `VITE_API_URL=https://your-render-url.onrender.com`
6. Deploy.

The frontend can also be deployed with the Vercel plugin from this workspace once the backend URL is final.

## Step 5: Test Deployment

Open the Vercel URL and verify:

- Splash screen loads.
- Map renders with state colors.
- Zoom into a state shows congressional districts.
- Click AZ-06 -> Juan Ciscomani appears.
- Click CA-30 -> Laura Friedman appears.
- Chamber panel shows House D control around 77%.
- Morning brief shows `data_current_as_of: 2026-04-29`.

Check backend directly:

```text
https://tapestry-2iyf.onrender.com/api/chambers
https://tapestry-2iyf.onrender.com/api/districts/AZ-06
https://tapestry-2iyf.onrender.com/api/morning-brief
```

Expected:

- `/api/chambers` returns House D control around 0.773 and Polymarket 0.86.
- `/api/districts/AZ-06` returns Juan Ciscomani.
- `/api/morning-brief` returns `data_current_as_of: 2026-04-29`.

## Submission Note

Backend may take up to 30 seconds to respond on first visit due to Render free-tier spin-down. Refresh the page if the map appears empty on first load.
