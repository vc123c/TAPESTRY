from __future__ import annotations

import json
import uuid
from datetime import datetime

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query

from api.schemas import ResolveQueueRequest, RetrainJobStatus
from db.connection import get_read_connection, table_count, write_connection
from model.retrainer import TapestryRetrainer
from scrapers.local_news_scraper import LocalNewsScraper

router = APIRouter(prefix="/api/admin", tags=["admin"])


def _run_retrain(job_id: str) -> None:
    TapestryRetrainer().full_retrain(job_id)


def _run_local_news() -> None:
    LocalNewsScraper().run()


@router.post("/retrain")
def trigger_retrain(background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())
    with write_connection() as con:
        con.execute("INSERT INTO retrain_jobs VALUES (?, ?, ?, ?, ?, ?)", [job_id, datetime.utcnow(), datetime.utcnow(), "queued", 0.0, "Queued"])
    background_tasks.add_task(_run_retrain, job_id)
    return {"job_id": job_id}


@router.post("/scrape-local-news")
def scrape_local_news(background_tasks: BackgroundTasks):
    background_tasks.add_task(_run_local_news)
    return {"status": "queued", "source": "local_news"}


@router.post("/scrape-local-news/{state}")
def scrape_local_news_state(state: str, fast: bool = Query(True)):
    try:
        return {"status": "complete", **LocalNewsScraper().run_state(state, fast=fast)}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/news-readiness")
def news_readiness():
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT SUBSTR(district_id, 1, 2) AS state, COUNT(*) AS articles
            FROM local_news
            GROUP BY state
            """
        ).fetchall()
    return {state: count for state, count in rows}


@router.get("/retrain/{job_id}", response_model=RetrainJobStatus)
def retrain_status(job_id: str):
    with get_read_connection() as con:
        row = con.execute("SELECT job_id,status,progress,message FROM retrain_jobs WHERE job_id=?", [job_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": row[0], "status": row[1], "progress": row[2], "message": row[3]}


@router.post("/flag/{district_id}")
def flag_district(district_id: str):
    with write_connection() as con:
        con.execute("UPDATE district_forecasts SET suspect_flag=true WHERE district_id=?", [district_id.upper()])
    return {"district_id": district_id.upper(), "suspect_flag": True}


@router.get("/queue")
def admin_queue():
    with get_read_connection() as con:
        rows = con.execute("SELECT * FROM admin_queue WHERE status='pending' ORDER BY queued_at DESC").fetchall()
    return [{"queue_id": r[0], "queued_at": r[1], "event_description": r[2], "affected_districts": r[3], "search_results": r[4], "suggested_token": r[5], "status": r[6]} for r in rows]


@router.post("/queue/{queue_id}/resolve")
def resolve_queue(queue_id: str, body: ResolveQueueRequest):
    status = "dismissed" if body.dismissed else "accepted"
    with write_connection() as con:
        con.execute("UPDATE admin_queue SET status=?, user_response=?, resolved_at=? WHERE queue_id=?", [status, json.dumps(body.model_dump()), datetime.utcnow(), queue_id])
    return {"queue_id": queue_id, "status": status}


@router.get("/performance")
def performance():
    with get_read_connection() as con:
        rows = con.execute("SELECT * FROM model_performance ORDER BY test_year").fetchall()
    return [{"evaluation_date": r[0], "train_years": r[1], "test_year": r[2], "brier_score": r[3], "cook_brier_score": r[4], "improvement": r[5], "n_races": r[6], "calibration_data": r[7]} for r in rows]


@router.get("/health")
def health():
    tables = ["district_forecasts", "chamber_forecasts", "national_factors", "conflict_states", "admin_queue", "scraper_runs"]
    with get_read_connection() as con:
        runs = con.execute("SELECT source_name, MAX(run_at), ANY_VALUE(status), ANY_VALUE(error) FROM scraper_runs GROUP BY source_name").fetchall()
    return {"status": "ok", "row_counts": {t: table_count(t) for t in tables}, "scrapers": [{"source": r[0], "last_run": r[1], "status": r[2], "error": r[3]} for r in runs]}
