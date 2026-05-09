# TAPESTRY Backend

Production-oriented local backend for the TAPESTRY political intelligence platform.

## Startup

1. `.venv\Scripts\python.exe -m pip install -r requirements.txt`
2. `.venv\Scripts\python.exe -m playwright install chromium`
3. `copy .env.example .env` and fill in market/data keys:
   - `POLYMARKET_API_KEY=...`
   - `KALSHI_ACCESS_KEY_ID=...`
   - `KALSHI_PRIVATE_KEY_PATH=.\secrets\kalshi_private_key.pem`
   - optional: `CENSUS_API_KEY=...`, `NEWSAPI_KEY=...`, `FRED_API_KEY=...`
4. `.venv\Scripts\python.exe db\connection.py`
5. `.venv\Scripts\python.exe data\historical\load_roster.py`
6. Place MIT Election Lab data at `data\historical\mit_house_results.csv`, then run `.venv\Scripts\python.exe data\historical\load_historical.py`
7. `.venv\Scripts\python.exe -m scrapers.census_scraper`
8. `.venv\Scripts\python.exe -m scrapers.fec_scraper`
9. `.venv\Scripts\python.exe -m scrapers.local_news_scraper`
10. `.venv\Scripts\python.exe -c "from model.retrainer import TapestryRetrainer; TapestryRetrainer().daily_update()"`
11. `.venv\Scripts\python.exe -m uvicorn main:app --port 8000 --reload`
12. In a separate terminal: `.venv\Scripts\python.exe -m streamlit run admin/admin_app.py --server.port 8502`

## Automatic Updates

When the FastAPI backend is running, TAPESTRY can now refresh itself automatically:

- Every 60 minutes:
  - local news scrape
  - race-specific news scrape
  - source intelligence scrape
  - Pape escalation scrape
  - event state backfill
  - embedding backfill
  - fast forecast refresh
- Every night at 3:00 AM Pacific:
  - full slow update and retrain

Environment variables:

- `AUTO_UPDATE_ENABLED=1`
- `AUTO_UPDATE_INTERVAL_MINUTES=60`
- `AUTO_UPDATE_FULL_RETRAIN_ENABLED=1`
- `AUTO_UPDATE_FULL_RETRAIN_HOUR=3`

If you want the backend to just sit there and keep itself fresh locally, run:

```bat
cd /d C:\Users\pdc\Documents\Codex\2026-04-24\build-a-full-screen-interactive-us\tapestry-backend
.\.venv\Scripts\python.exe -m uvicorn main:app --host 0.0.0.0 --port 8000
```

As long as that process stays running, the scheduler stays running too.

## Notes

- DuckDB lives at `data/tapestry.duckdb`.
- Raw and processed scraper outputs are Parquet under `data/raw/` and `data/processed/`.
- `data/morning_brief.json` is written atomically and is the frontend contract.
- Scrapers are failure-tolerant, but they do not fabricate candidate names, dollar amounts, vote percentages, or bios. Missing data is returned as null or empty.
- The chamber simulation is vectorized NumPy and seeded with `42`.
- On Render free tier, background jobs pause when the service spins down from inactivity. Hourly auto-refresh is reliable locally or on an always-on host. On free Render, it resumes when the service wakes back up.

## Polymarket Key

Put the Polymarket key directly in:

```bat
tapestry-backend\.env
```

Use this exact line:

```env
POLYMARKET_API_KEY=PASTE_YOUR_KEY_HERE
POLYMARKET_CLOB_URL=https://clob.polymarket.com
```

Then restart the backend server so the key is loaded.

MIT Election Lab data must be downloaded manually from:
https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/IG0UN2

## Overnight Pipeline

To run the full overnight data collection and model training:

```bat
cd /d C:\Users\pdc\Documents\Codex\2026-04-24\build-a-full-screen-interactive-us\tapestry-backend
.\.venv\Scripts\python.exe overnight.py --skip-slow-trends
```

Or double-click:

```bat
run_overnight.bat
```

This will:

- Collect data from the stable source set: FRED, EIA, BLS, Census, FEC, Ballotpedia, local/race news, incumbent status, 270toWin context, source intelligence, ideology texts, embeddings, Kalshi, Polymarket, DW-NOMINATE, polling, and Pape.
- Cache every completed step under `data/raw/`, `data/historical/`, DuckDB, and `data/overnight_state.json`.
- Train the district forecast model.
- Score all 435 districts.
- Run chamber simulation.
- Generate `data/morning_brief.json`.
- Write `data/overnight_summary.json` for the admin UI.

Safe to run while sleeping or while using the laptop. If Windows sleeps, the network drops, or a scraper times out, run the same command again and it will skip fresh completed cached steps. Failed scraper steps are marked partial and the model still trains on whatever data is available.

Morning report files:

- `data\overnight_summary.json`
- `data\morning_brief.json`
- `data\logs\overnight_YYYY-MM-DD.log`

The admin app also shows the latest overnight summary banner when it opens.

Estimated total time: 1-4 hours on the stable path, depending on news/source response times. Google Trends is skipped in the stable launcher because it is heavily rate-limited. To include it anyway, run `.\.venv\Scripts\python.exe overnight.py`.

Refresh only news/source intelligence, then train:

```bat
run_news_then_train.bat
```

Preview the planned steps without running them:

```bat
.\.venv\Scripts\python.exe overnight.py --dry-run --skip-slow-trends
```

Resume an interrupted run:

```bat
.\.venv\Scripts\python.exe overnight.py
```

Force a full re-run ignoring cache:

```bat
.\.venv\Scripts\python.exe overnight.py --force
```

Run only model training:

```bat
.\.venv\Scripts\python.exe overnight.py --model-only
```

Run only data collection:

```bat
.\.venv\Scripts\python.exe overnight.py --data-only
```

Run only news/source-intelligence collection:

```bat
.\.venv\Scripts\python.exe overnight.py --news-only --data-only
```
