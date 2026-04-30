# TAPESTRY
### The country, woven together.

A political intelligence dashboard for the 2026 US congressional midterms. Built with Codex.

**Data current as of: April 29, 2026**

---

## What It Does

TAPESTRY models the probability of Democratic or Republican victory in all 435 congressional districts and aggregates those into Senate and House chamber control forecasts. It compares its own structural model against Polymarket prediction market prices, flagging disagreements.

**Live model outputs:**
- House D control: ~77% (Polymarket: 86%)
- Senate D control: ~50%
- 435 district-level probability forecasts
- Brier score on 2024 competitive races: 0.135

---

## The Model

Two-stage forecasting model:

**Stage 1 - Linear fundamentals:** Presidential approval (35%), generic ballot (D+7), TAPESTRY PVI (custom partisan lean index from MIT Election Lab results), incumbency advantage, kitchen-table economic index (gas/grocery/egg/rent), special election swings as leading indicators, issue-specific approval (prices 26%, immigration 44%), and Iran war escalation stage via Pape's framework.

**Stage 2 - XGBoost residual correction:** Trained on Stage 1 residuals using Census ACS demographics, FEC fundraising ratios, medical debt, gini coefficient, AI displacement exposure, and ideology alignment scores from sentence-transformers embeddings of race-specific news articles.

**Validation:** Walk-forward backtesting on 2020, 2022, and 2024 holdout sets. No same-year outcome leakage. Brier 0.128-0.141 on competitive races.

**Chamber simulation:** 50,000 Monte Carlo runs with correlated national environment shocks capturing wave election dynamics. The House simulation uses a 5.5-point historical cycle shock based on generic ballot volatility from 2008-2024.

---

## Data Sources

All free and public:
- MIT Election Lab house results, 2008-2024
- FEC bulk candidate finance (`weball26.zip`)
- Census ACS 5-year district demographics
- House Clerk XML official 435-member roster
- UCSB American Presidency Project Gallup approval data
- Strength In Numbers / Verasight April 2026 poll
- FRED economic indicators
- EIA gas prices, BLS grocery and egg CPI
- Ballotpedia 2026 candidate declarations
- Polymarket House/Senate control prices
- Kalshi market discovery where available
- Robert Pape's Substack for Iran escalation signals
- 7,000+ race-specific news articles via GDELT, Google News, and NewsAPI
- 270toWin House race context
- Public special election and voter registration seed data

---

## Architecture

```text
React frontend (localhost:5173)
    <-> REST API
FastAPI + DuckDB backend (localhost:8000)
    |-- 20+ data scrapers
    |-- Two-stage XGBoost model
    |-- 50,000-run Monte Carlo chamber sim
    |-- Resumable overnight pipeline
    `-- Streamlit admin interface (localhost:8502)
```

---

## Key Endpoints

- `GET /api/chambers` - chamber control forecasts and prediction-market comparison
- `GET /api/districts/{district_id}` - full district profile, forecast, candidates, FEC, signals, and news
- `GET /api/districts/summary` - lightweight national district forecast layer for the map
- `GET /api/states` - state-level summaries for the national map
- `GET /api/morning-brief` - daily political intelligence brief
- `GET /api/market/gaps` - model-vs-market disagreement analysis

---

## Running Locally

Backend:

```bat
cd tapestry-backend
.venv\Scripts\python.exe -m uvicorn api.main:app --host 0.0.0.0 --port 8000
```

Frontend:

```bat
node server.js
```

Quick data/model refresh:

```bat
cd tapestry-backend
.venv\Scripts\python.exe -c "from model.retrainer import TapestryRetrainer; TapestryRetrainer().fast_update()"
```

Full overnight pipeline:

```bat
cd tapestry-backend
.venv\Scripts\python.exe overnight.py --force
```

---

## Guardrails

TAPESTRY avoids county proxy rendering for congressional districts, uses Census CD118 district boundaries, normalizes district IDs across data sources, and validates walk-forward features so same-year election outcomes do not leak into model training.

Known caveat: candidate declarations and special-election candidate lists are fast-moving. The dashboard prioritizes official roster and FEC/Ballotpedia/Secretary of State data where available, and flags open-seat status rather than inventing missing candidate data.
