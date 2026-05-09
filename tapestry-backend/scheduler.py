from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler

from db.connection import ROOT, init_db
from model.retrainer import TapestryRetrainer
from utils.logging import setup_logging

logger = setup_logging(__name__)


def _truthy(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _run_python_task(name: str, args: list[str], extra_env: dict[str, str] | None = None, timeout_seconds: int = 60 * 60) -> bool:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(ROOT) if not existing_pythonpath else f"{ROOT}{os.pathsep}{existing_pythonpath}"
    if extra_env:
        env.update(extra_env)

    logger.info("Scheduled task start: %s", name)
    try:
        completed = subprocess.run(
            [sys.executable, *args],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        output = ((exc.stdout or "") + (exc.stderr or "")).strip()
        if output:
            logger.warning("%s timed out tail:\n%s", name, output[-2000:])
        logger.warning("Scheduled task timed out: %s", name)
        return False
    except Exception:
        logger.exception("Scheduled task crashed before launch: %s", name)
        return False

    output = ((completed.stdout or "") + (completed.stderr or "")).strip()
    if output:
        logger.info("%s output tail:\n%s", name, output[-2000:])
    ok = completed.returncode == 0
    if ok:
        logger.info("Scheduled task complete: %s", name)
    else:
        logger.warning("Scheduled task partial/fail: %s (exit %s)", name, completed.returncode)
    return ok


def run_hourly_refresh() -> None:
    init_db()
    logger.info("TAPESTRY hourly refresh starting")

    _run_python_task(
        "local news scrape",
        ["-c", "from scrapers.local_news_scraper import LocalNewsScraper; raise SystemExit(0 if LocalNewsScraper().run() else 1)"],
        timeout_seconds=30 * 60,
    )
    _run_python_task(
        "race web scrape",
        ["-c", "from scrapers.race_web_scraper import RaceWebScraper; raise SystemExit(0 if RaceWebScraper().run() else 1)"],
        extra_env={
            "RACE_WEB_FORCE": "1",
            "RACE_WEB_PVI_THRESHOLD": os.getenv("RACE_WEB_PVI_THRESHOLD", "20"),
        },
        timeout_seconds=60 * 60,
    )
    _run_python_task(
        "source intelligence scrape",
        ["-c", "from scrapers.source_intel_scraper import SourceIntelScraper; raise SystemExit(0 if SourceIntelScraper().run() else 1)"],
        timeout_seconds=30 * 60,
    )
    _run_python_task(
        "Pape escalation scrape",
        ["-c", "from scrapers.pape_scraper import PapeScraper; raise SystemExit(0 if PapeScraper().run() else 1)"],
        timeout_seconds=10 * 60,
    )
    _run_python_task(
        "event state backfill",
        ["scripts/backfill_event_states.py"],
        timeout_seconds=5 * 60,
    )
    _run_python_task(
        "embedding backfill",
        ["-m", "utils.backfill_embeddings"],
        timeout_seconds=30 * 60,
    )

    try:
        report = TapestryRetrainer().fast_update()
        logger.info(
            "TAPESTRY hourly refresh complete; generated_at=%s",
            report.get("generated_at") if isinstance(report, dict) else None,
        )
    except Exception:
        logger.exception("TAPESTRY hourly refresh failed during fast_update")


def run_full_retrain() -> None:
    init_db()
    logger.info("TAPESTRY scheduled full retrain starting")
    try:
        report = TapestryRetrainer().slow_update()
        logger.info(
            "TAPESTRY full retrain complete; updated_at=%s",
            report.get("updated_at") if isinstance(report, dict) else None,
        )
    except Exception:
        logger.exception("TAPESTRY full retrain failed")


def create_scheduler() -> BackgroundScheduler:
    interval_minutes = max(15, int(os.getenv("AUTO_UPDATE_INTERVAL_MINUTES", "60")))
    full_retrain_hour = int(os.getenv("AUTO_UPDATE_FULL_RETRAIN_HOUR", "3")) % 24
    full_retrain_enabled = _truthy(os.getenv("AUTO_UPDATE_FULL_RETRAIN_ENABLED"), default=True)

    scheduler = BackgroundScheduler(
        timezone="America/Los_Angeles",
        job_defaults={
            "coalesce": True,
            "max_instances": 1,
            "misfire_grace_time": 30 * 60,
        },
    )

    scheduler.add_job(
        run_hourly_refresh,
        "interval",
        minutes=interval_minutes,
        id="hourly_refresh",
        replace_existing=True,
    )

    if full_retrain_enabled:
        scheduler.add_job(
            run_full_retrain,
            "cron",
            hour=full_retrain_hour,
            minute=0,
            id="nightly_full_retrain",
            replace_existing=True,
        )

    logger.info(
        "Scheduler configured: hourly refresh every %s minutes; nightly full retrain %s at %02d:00 PT",
        interval_minutes,
        "enabled" if full_retrain_enabled else "disabled",
        full_retrain_hour,
    )
    return scheduler


if __name__ == "__main__":
    init_db()
    sched = create_scheduler()
    sched.start()
    logger.info("TAPESTRY scheduler started at %s", datetime.now().isoformat())
    try:
        import time

        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sched.shutdown()
