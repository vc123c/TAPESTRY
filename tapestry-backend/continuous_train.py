from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from db.connection import ROOT, get_read_connection


LOG_DIR = ROOT / "data" / "logs"
SUMMARY_PATH = ROOT / "data" / "continuous_train_summary.json"


@dataclass
class Task:
    name: str
    args: list[str]
    timeout_seconds: int
    env: dict[str, str] = field(default_factory=dict)


class ContinuousTrainer:
    def __init__(self, sleep_minutes: int, max_cycles: int, race_thresholds: list[float]) -> None:
        self.sleep_minutes = sleep_minutes
        self.max_cycles = max_cycles
        self.race_thresholds = race_thresholds or [20.0, 30.0, 99.0]
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        self.log_path = LOG_DIR / f"continuous_train_{datetime.now(UTC).date().isoformat()}.log"
        self.cycles: list[dict] = []

    def log(self, message: str) -> None:
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
        print(line, flush=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _env(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        env = os.environ.copy()
        existing = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(ROOT) if not existing else f"{ROOT}{os.pathsep}{existing}"
        if extra:
            env.update(extra)
        return env

    def run_task(self, task: Task) -> dict:
        start = time.perf_counter()
        self.log(f"START {task.name}")
        try:
            proc = subprocess.run(
                task.args,
                cwd=ROOT,
                env=self._env(task.env),
                text=True,
                capture_output=True,
                timeout=task.timeout_seconds,
            )
            output = ((proc.stdout or "") + (proc.stderr or "")).strip()
            status = "ok" if proc.returncode == 0 else "partial"
        except subprocess.TimeoutExpired as exc:
            output = ((exc.stdout or "") + (exc.stderr or "")).strip()
            status = "timeout"
        except Exception as exc:
            output = f"{type(exc).__name__}: {exc}"
            status = "error"
        duration = round(time.perf_counter() - start, 1)
        if output:
            self.log(output[-1800:])
        self.log(f"{status.upper()} {task.name} in {duration:.1f}s")
        return {"name": task.name, "status": status, "duration_seconds": duration, "output_tail": output[-1800:]}

    def counts(self) -> dict:
        queries = {
            "race_web_articles": "SELECT COUNT(*) FROM race_web_articles",
            "race_news_districts": "SELECT COUNT(DISTINCT district_id) FROM race_web_articles",
            "local_news": "SELECT COUNT(*) FROM local_news",
            "local_news_districts": "SELECT COUNT(DISTINCT district_id) FROM local_news",
            "event_tokens": "SELECT COUNT(*) FROM event_tokens",
            "events_with_states": "SELECT COUNT(*) FROM event_tokens WHERE affected_states IS NOT NULL",
            "national_signals": "SELECT COUNT(*) FROM event_tokens WHERE is_national_signal = TRUE",
            "embedded_race_articles": "SELECT COUNT(*) FROM race_web_articles WHERE embedding IS NOT NULL",
            "gini_nulls": "SELECT COUNT(*) FROM district_features WHERE gini_coefficient IS NULL",
            "healthcare_nulls": "SELECT COUNT(*) FROM district_features WHERE healthcare_cost_burden IS NULL",
            "data_center_nulls": "SELECT COUNT(*) FROM district_features WHERE data_center_mw_planned IS NULL",
        }
        out = {}
        with get_read_connection() as con:
            for key, sql in queries.items():
                try:
                    out[key] = int(con.execute(sql).fetchone()[0] or 0)
                except Exception:
                    out[key] = None
        return out

    def tasks_for_cycle(self, cycle: int) -> list[Task]:
        threshold = self.race_thresholds[(cycle - 1) % len(self.race_thresholds)]
        race_env = {
            "RACE_WEB_FORCE": "1",
            "RACE_WEB_PVI_THRESHOLD": str(threshold),
        }
        py = sys.executable
        return [
            Task("local news scrape", [py, "-c", "from scrapers.local_news_scraper import LocalNewsScraper; raise SystemExit(0 if LocalNewsScraper().run() else 1)"], 30 * 60),
            Task("race-specific web scrape", [py, "-c", "from scrapers.race_web_scraper import RaceWebScraper; raise SystemExit(0 if RaceWebScraper().run() else 1)"], 60 * 60, race_env),
            Task("source intelligence scrape", [py, "-c", "from scrapers.source_intel_scraper import SourceIntelScraper; raise SystemExit(0 if SourceIntelScraper().run() else 1)"], 30 * 60),
            Task("backfill embeddings", [py, "-m", "utils.backfill_embeddings"], 30 * 60),
            Task("backfill event states", [py, "scripts/backfill_event_states.py"], 5 * 60),
            Task("fill static features", [py, "scripts/fill_static_features.py"], 5 * 60),
            Task("model-only training", [py, "overnight.py", "--model-only"], 45 * 60),
            Task("coverage report", [py, "scripts/review_coverage.py"], 3 * 60),
        ]

    def write_summary(self) -> None:
        summary = {
            "updated_at": datetime.now(UTC).isoformat(),
            "sleep_minutes": self.sleep_minutes,
            "max_cycles": self.max_cycles,
            "cycles": self.cycles,
            "latest_counts": self.counts(),
            "log_path": str(self.log_path),
        }
        tmp = SUMMARY_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, SUMMARY_PATH)

    def run(self) -> None:
        self.log("TAPESTRY CONTINUOUS TRAINING LOOP")
        self.log("Stop anytime with Ctrl+C. Progress is written after every task.")
        cycle = 0
        try:
            while self.max_cycles <= 0 or cycle < self.max_cycles:
                cycle += 1
                self.log(f"===== CYCLE {cycle} =====")
                before = self.counts()
                results = []
                for task in self.tasks_for_cycle(cycle):
                    results.append(self.run_task(task))
                    self.write_summary()
                after = self.counts()
                self.cycles.append({"cycle": cycle, "started_counts": before, "finished_counts": after, "tasks": results})
                self.write_summary()
                self.log(f"CYCLE {cycle} COMPLETE: {after}")
                if self.max_cycles > 0 and cycle >= self.max_cycles:
                    break
                self.log(f"Sleeping {self.sleep_minutes} minutes before next cycle...")
                time.sleep(self.sleep_minutes * 60)
        except KeyboardInterrupt:
            self.log("STOP requested by user. Writing final summary.")
            self.write_summary()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Continuously collect TAPESTRY news/intel data and retrain until stopped.")
    parser.add_argument("--sleep-minutes", type=int, default=20, help="Pause between cycles.")
    parser.add_argument("--max-cycles", type=int, default=0, help="0 means run until Ctrl+C.")
    parser.add_argument("--race-thresholds", default="20,30,99", help="Comma-separated PVI thresholds to rotate across cycles.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    thresholds = [float(x.strip()) for x in args.race_thresholds.split(",") if x.strip()]
    ContinuousTrainer(args.sleep_minutes, args.max_cycles, thresholds).run()
