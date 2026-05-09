from __future__ import annotations

import os
from datetime import date
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from api.routes import admin, chambers, conflicts, districts, market, national, scandals, states
from db.connection import ROOT, init_db
from model.retrainer import TapestryRetrainer
from scheduler import create_scheduler
from utils.logging import setup_logging

logger = setup_logging(__name__)
app_scheduler = None

app = FastAPI(title="TAPESTRY API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://localhost:3000",
        "http://127.0.0.1:5173",
        "*",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

app.include_router(national.router)
app.include_router(districts.router)
app.include_router(chambers.router)
app.include_router(conflicts.router)
app.include_router(scandals.router)
app.include_router(admin.router)
app.include_router(states.router)
app.include_router(market.router)


@app.on_event("startup")
async def startup() -> None:
    global app_scheduler
    init_db()
    auto_update_enabled = os.getenv("AUTO_UPDATE_ENABLED", "1").lower() in {"1", "true", "yes", "on"}
    if auto_update_enabled and app_scheduler is None:
        try:
            app_scheduler = create_scheduler()
            app_scheduler.start()
            logger.info("Background auto-update scheduler started")
        except Exception:
            logger.exception("Background auto-update scheduler failed to start")
    if os.getenv("ENVIRONMENT", "").lower() == "production":
        logger.info("Production environment detected; serving packaged TAPESTRY data without startup refresh")
        return
    brief = ROOT / "data" / "morning_brief.json"
    if not brief.exists() or date.fromtimestamp(brief.stat().st_mtime) != date.today():
        logger.info("No current morning brief; running daily update")
        try:
            await TapestryRetrainer().daily_update_async()
        except Exception:
            logger.exception("Startup daily update failed; API will serve any existing data")


@app.on_event("shutdown")
async def shutdown() -> None:
    global app_scheduler
    if app_scheduler is not None:
        try:
            app_scheduler.shutdown(wait=False)
            logger.info("Background auto-update scheduler stopped")
        except Exception:
            logger.exception("Background auto-update scheduler failed to stop cleanly")
        finally:
            app_scheduler = None


@app.get("/")
def root():
    return HTMLResponse(
        """
        <!doctype html>
        <html>
          <head>
            <title>TAPESTRY API</title>
            <style>
              body{margin:0;background:#080b10;color:#e2e8f0;font-family:system-ui;padding:32px}
              h1{letter-spacing:.16em;font-size:20px;margin:0 0 6px;text-shadow:0 0 18px #7c3aed}
              p{color:#94a3b8}
              a{display:block;color:#c4b5fd;margin:10px 0;text-decoration:none}
              a:hover{color:#fff}
              code{background:#111827;border:1px solid #1e2130;padding:2px 5px}
            </style>
          </head>
          <body>
            <h1>TAPESTRY API</h1>
            <p>Backend is running. Use these endpoints:</p>
            <a href="http://localhost:5173">TAPESTRY UI · http://localhost:5173</a>
            <a href="http://localhost:8000/api/morning-brief">Morning brief · /api/morning-brief</a>
            <a href="http://localhost:8000/api/districts">Districts · /api/districts</a>
            <a href="http://localhost:8000/api/chambers">Chambers · /api/chambers</a>
            <a href="http://localhost:8000/api/national">National · /api/national</a>
            <a href="http://localhost:8000/api/districts/AZ-06/news">Example race news · /api/districts/AZ-06/news</a>
            <a href="http://localhost:8000/docs">Interactive API docs · /docs</a>
            <p>React frontend remains at <a href="http://localhost:5173">http://localhost:5173</a>.</p>
          </body>
        </html>
        """
    )
