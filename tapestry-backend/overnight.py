from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Callable

import httpx
import polars as pl

from db.connection import ROOT, init_db, table_count, write_connection
from model.overnight_trainer import OvernightTrainer


STATE_PATH = ROOT / "data" / "overnight_state.json"
SUMMARY_PATH = ROOT / "data" / "overnight_summary.json"
LOG_DIR = ROOT / "data" / "logs"
RAW_DIR = ROOT / "data" / "raw"
HISTORICAL_DIR = ROOT / "data" / "historical"
MAX_STEP_SECONDS = 90 * 60


@dataclass
class StepResult:
    name: str
    status: str
    rows_loaded: int = 0
    cache_hit: bool = False
    duration_seconds: float = 0.0
    warning: str | None = None
    details: dict = field(default_factory=dict)


@dataclass
class Step:
    key: str
    name: str
    cache_paths: list[Path]
    runner: Callable[[], StepResult]


class OvernightPipeline:
    def __init__(
        self,
        force: bool = False,
        model_only: bool = False,
        data_only: bool = False,
        news_only: bool = False,
        skip_slow_trends: bool = False,
        dry_run: bool = False,
    ) -> None:
        self.force = force
        self.model_only = model_only
        self.data_only = data_only
        self.news_only = news_only
        self.skip_slow_trends = skip_slow_trends
        self.dry_run = dry_run
        self.started_at = datetime.now(UTC)
        self.warnings: list[str] = []
        self.results: list[StepResult] = []
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)
        self.log_path = LOG_DIR / f"overnight_{date.today().isoformat()}.log"
        self.state = self._load_state()

    def _load_state(self) -> dict:
        if STATE_PATH.exists():
            try:
                return json.loads(STATE_PATH.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        return {"run_date": date.today().isoformat(), "steps": {}}

    def _save_state(self) -> None:
        tmp = STATE_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.state, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, STATE_PATH)

    def log(self, message: str) -> None:
        line = f"[{datetime.now().strftime('%H:%M:%S')}] {message}"
        print(line, flush=True)
        with self.log_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _fresh(self, paths: list[Path], max_age: timedelta = timedelta(hours=24)) -> bool:
        existing = [path for path in paths if path.exists()]
        if not existing:
            return False
        newest = max(datetime.fromtimestamp(path.stat().st_mtime, UTC) for path in existing)
        return datetime.now(UTC) - newest < max_age

    def _is_cached(self, step: Step) -> bool:
        if self.force:
            return False
        record = self.state.get("steps", {}).get(step.key, {})
        return record.get("status") == "complete" and self._fresh(step.cache_paths)

    def _record(self, step: Step, result: StepResult) -> None:
        self.results.append(result)
        self.state.setdefault("steps", {})[step.key] = {
            "name": step.name,
            "status": result.status,
            "rows_loaded": result.rows_loaded,
            "cache_hit": result.cache_hit,
            "duration_seconds": result.duration_seconds,
            "warning": result.warning,
            "completed_at": datetime.now(UTC).isoformat(),
        }
        self._save_state()

    def run_command(self, args: list[str], timeout: int = MAX_STEP_SECONDS) -> tuple[bool, str]:
        env = os.environ.copy()
        existing_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = str(ROOT) if not existing_pythonpath else f"{ROOT}{os.pathsep}{existing_pythonpath}"
        try:
            completed = subprocess.run(
                args,
                cwd=ROOT,
                env=env,
                text=True,
                capture_output=True,
                timeout=timeout,
            )
            output = (completed.stdout or "") + (completed.stderr or "")
            return completed.returncode == 0, output.strip()
        except subprocess.TimeoutExpired as exc:
            output = (exc.stdout or "") + (exc.stderr or "")
            return False, f"Timed out after {timeout}s\n{output}"

    def run_scraper(self, class_name: str, module: str, output_copy: Path | None = None, timeout: int = MAX_STEP_SECONDS) -> StepResult:
        code = f"from {module} import {class_name}; raise SystemExit(0 if {class_name}().run() else 1)"
        ok, output = self.run_command([sys.executable, "-c", code], timeout=timeout)
        if output:
            self.log(output[-1600:])
        rows = 0
        if output_copy and output_copy.exists():
            try:
                rows = pl.read_parquet(output_copy).height
            except Exception:
                rows = 0
        return StepResult(name=class_name, status="complete" if ok else "partial", rows_loaded=rows, warning=None if ok else output[-500:])

    def copy_latest(self, pattern: str, destination: Path) -> int:
        matches = sorted(RAW_DIR.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        if not matches:
            return 0
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(matches[0], destination)
        try:
            return pl.read_parquet(destination).height
        except Exception:
            return 0

    def step_fred(self) -> StepResult:
        result = self.run_scraper("FredScraper", "scrapers.fred_scraper")
        result.rows_loaded = self.copy_latest("fred_all_*.parquet", RAW_DIR / "fred_all.parquet") or result.rows_loaded
        return result

    def step_approval(self) -> StepResult:
        ok, output = self.run_command([sys.executable, "-m", "scrapers.approval_scraper"], timeout=5 * 60)
        if output:
            self.log(output[-1600:])
        return StepResult("Approval polling", "complete" if ok else "partial", table_count("approval_polls"), warning=None if ok else output[-500:])

    def step_eia(self) -> StepResult:
        result = self.run_scraper("EIAGasScraper", "scrapers.eia_gas_scraper", RAW_DIR / "eia_gas_latest.parquet")
        if (RAW_DIR / "eia_gas_latest.parquet").exists():
            shutil.copy2(RAW_DIR / "eia_gas_latest.parquet", RAW_DIR / "eia_gas.parquet")
            result.rows_loaded = pl.read_parquet(RAW_DIR / "eia_gas.parquet").height
        return result

    def step_bls(self) -> StepResult:
        result = self.run_scraper("BLSGroceryScraper", "scrapers.bls_grocery_scraper", RAW_DIR / "bls_grocery_latest.parquet")
        if (RAW_DIR / "bls_grocery_latest.parquet").exists():
            shutil.copy2(RAW_DIR / "bls_grocery_latest.parquet", RAW_DIR / "bls_grocery.parquet")
            result.rows_loaded = pl.read_parquet(RAW_DIR / "bls_grocery.parquet").height
        return result

    def step_census(self) -> StepResult:
        result = self.run_scraper("CensusScraper", "scrapers.census_scraper", RAW_DIR / "census_district_features_latest.parquet")
        result.rows_loaded = table_count("district_features")
        return result

    def step_fec(self) -> StepResult:
        zip_path = HISTORICAL_DIR / "weball26.zip"
        if not zip_path.exists():
            url = "https://www.fec.gov/files/bulk-downloads/2026/weball26.zip"
            try:
                self.log("Downloading public FEC weball26.zip ...")
                with httpx.stream("GET", url, timeout=300) as response:
                    response.raise_for_status()
                    tmp = zip_path.with_suffix(".zip.tmp")
                    with tmp.open("wb") as fh:
                        for chunk in response.iter_bytes():
                            fh.write(chunk)
                    os.replace(tmp, zip_path)
            except Exception as exc:
                warning = f"FEC bulk download failed: {type(exc).__name__}"
                self.warnings.append(warning)
                self.log(warning)
        ok, output = self.run_command([sys.executable, "data/historical/load_fec_weball.py"], timeout=20 * 60)
        if output:
            self.log(output[-1600:])
        api_totals = RAW_DIR / "fec_api_totals.json"
        try:
            params = {"cycle": 2026, "office": "H", "sort": "-receipts", "per_page": 100, "api_key": os.getenv("FEC_API_KEY", "DEMO_KEY")}
            response = httpx.get("https://api.open.fec.gov/v1/candidates/totals/", params=params, timeout=60)
            response.raise_for_status()
            api_totals.write_text(json.dumps(response.json(), indent=2), encoding="utf-8")
        except Exception as exc:
            warning = f"FEC API top totals failed: {type(exc).__name__}"
            self.warnings.append(warning)
        rows = table_count("fec_candidate_finance")
        return StepResult("FEC", "complete" if ok or rows else "partial", rows, warning=None if rows else "No FEC rows loaded")

    def step_candidate_sanity(self) -> StepResult:
        ok, output = self.run_command([sys.executable, "data/historical/audit_candidates.py", "--fix"], timeout=10 * 60)
        if output:
            self.log(output[-1600:])
        rows = table_count("candidate_roster_2026")
        report_path = ROOT / "data" / "candidate_sanity_report.json"
        details = {}
        if report_path.exists():
            try:
                details = json.loads(report_path.read_text(encoding="utf-8"))
            except Exception:
                details = {}
        issue_count = int(details.get("issue_count") or 0)
        warning = None if ok and issue_count == 0 else f"candidate sanity issues remaining: {issue_count}"
        return StepResult("Candidate sanity audit", "complete" if ok and issue_count == 0 else "partial", rows, warning=warning, details=details)

    def step_ballotpedia(self) -> StepResult:
        result = self.run_scraper("BallotpediaScraper", "scrapers.ballotpedia_scraper", RAW_DIR / "ballotpedia_2026_house.parquet")
        result.rows_loaded = table_count("candidate_roster_2026")
        return result

    def step_trends(self) -> StepResult:
        result = self.run_scraper("GoogleTrendsScraper", "scrapers.google_trends_scraper", RAW_DIR / "google_trends_latest.parquet", timeout=15 * 60)
        return result

    def step_news(self) -> StepResult:
        result = self.run_scraper("LocalNewsScraper", "scrapers.local_news_scraper", RAW_DIR / "local_news_latest.parquet", timeout=20 * 60)
        result.rows_loaded = table_count("local_news")
        return result

    def step_race_news(self) -> StepResult:
        result = self.run_scraper("RaceWebScraper", "scrapers.race_web_scraper", RAW_DIR / "race_web_latest.parquet", timeout=45 * 60)
        result.rows_loaded = table_count("race_web_articles")
        return result

    def step_incumbent_status(self) -> StepResult:
        result = self.run_scraper("IncumbentStatusScraper", "scrapers.incumbent_status_scraper", RAW_DIR / "incumbent_status_2026_latest.parquet", timeout=10 * 60)
        result.rows_loaded = table_count("incumbent_status_2026")
        return result

    def step_twoseventy_house(self) -> StepResult:
        result = self.run_scraper("TwoSeventyHouseScraper", "scrapers.twoseventy_house_scraper", RAW_DIR / "twoseventy_house_context_latest.parquet", timeout=10 * 60)
        result.rows_loaded = table_count("twoseventy_house_context")
        return result

    def step_source_intel(self) -> StepResult:
        result = self.run_scraper("SourceIntelScraper", "scrapers.source_intel_scraper", RAW_DIR / "source_intel_latest.parquet", timeout=20 * 60)
        result.rows_loaded = table_count("media_event_articles")
        try:
            result.details = {
                "signals": table_count("media_signal_summary"),
                "event_tokens": table_count("event_tokens"),
            }
        except Exception:
            pass
        return result

    def step_ideology_corpus(self) -> StepResult:
        result = self.run_scraper("IdeologyCorpusScraper", "scrapers.ideology_corpus_scraper", RAW_DIR / "ideology_corpus_latest.parquet", timeout=20 * 60)
        result.rows_loaded = table_count("ideology_corpus_chunks")
        return result

    def step_backfill_embeddings(self) -> StepResult:
        ok, output = self.run_command([sys.executable, "-m", "utils.backfill_embeddings"], timeout=30 * 60)
        if output:
            self.log(output[-1600:])
        rows = 0
        try:
            with write_connection() as con:
                rows = int(con.execute("SELECT COUNT(*) FROM race_web_articles WHERE embedding IS NOT NULL").fetchone()[0] or 0)
        except Exception:
            rows = 0
        return StepResult("Backfill embeddings", "complete" if ok else "partial", rows, warning=None if ok else output[-500:])

    def step_kalshi(self) -> StepResult:
        result = self.run_scraper("KalshiScraper", "scrapers.kalshi_scraper", RAW_DIR / "kalshi_markets_latest.parquet")
        if (RAW_DIR / "kalshi_markets_latest.parquet").exists():
            shutil.copy2(RAW_DIR / "kalshi_markets_latest.parquet", RAW_DIR / "kalshi_markets.parquet")
        result.rows_loaded = table_count("kalshi_market_mapping")
        return result

    def step_polymarket(self) -> StepResult:
        result = self.run_scraper("PolymarketScraper", "scrapers.polymarket_scraper", RAW_DIR / "polymarket_markets_latest.parquet", timeout=5 * 60)
        result.rows_loaded = table_count("polymarket_market_mapping")
        return result

    def _latest_national_insert_or_update(self, updates: dict[str, float]) -> None:
        from model.features import build_national_factors

        data = build_national_factors()
        data.update(updates)
        data["factor_date"] = date.today()
        with write_connection() as con:
            placeholders = ", ".join(["?"] * len(data))
            con.execute(f"INSERT OR REPLACE INTO national_factors VALUES ({placeholders})", list(data.values()))

    def step_dwnominate(self) -> StepResult:
        url = "https://voteview.com/static/data/out/members/HSall_members.csv"
        path = RAW_DIR / "dwnominate.csv"
        if not path.exists() or self.force:
            response = httpx.get(url, timeout=120)
            response.raise_for_status()
            path.write_bytes(response.content)
        df = pl.read_csv(path, infer_schema_length=10000, ignore_errors=True)
        chamber_col = "chamber" if "chamber" in df.columns else None
        party_col = "party_code" if "party_code" in df.columns else None
        cong_col = "congress"
        score_col = "nominate_dim1"
        recent = df
        if chamber_col:
            recent = recent.filter(pl.col(chamber_col) == "House")
        recent = recent.filter((pl.col(cong_col) >= 108) & (pl.col(cong_col) <= 119))
        if party_col:
            grouped = recent.group_by([cong_col, party_col]).agg(pl.col(score_col).mean().alias("mean_score"))
            rows = grouped.to_dicts()
            spreads = []
            for congress in sorted({row[cong_col] for row in rows}):
                d = next((row["mean_score"] for row in rows if row[cong_col] == congress and row[party_col] == 100), None)
                r = next((row["mean_score"] for row in rows if row[cong_col] == congress and row[party_col] == 200), None)
                if d is not None and r is not None:
                    spreads.append(float(r - d))
            spread = float(spreads[-1]) if spreads else 0.8
        else:
            spread = 0.8
        self._latest_national_insert_or_update({"dw_nominate_spread": spread})
        return StepResult("DW-NOMINATE", "complete", df.height, details={"latest_spread": spread})

    def step_polling(self) -> StepResult:
        urls = [
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/polls/generic-ballot-polls.csv",
            "https://raw.githubusercontent.com/fivethirtyeight/data/master/congress-generic-ballot/generic_ballot_averages.csv",
        ]
        path = RAW_DIR / "fte_generic_ballot.csv"
        warning = None
        for url in urls:
            try:
                response = httpx.get(url, timeout=60)
                response.raise_for_status()
                path.write_bytes(response.content)
                break
            except Exception as exc:
                warning = f"{url} failed: {type(exc).__name__}"
        rows = 0
        margin = None
        if path.exists():
            try:
                df = pl.read_csv(path, infer_schema_length=10000)
                rows = df.height
                numeric_cols = [col for col in df.columns if "dem" in col.lower() or "rep" in col.lower()]
                if len(numeric_cols) >= 2:
                    last = df.tail(1).to_dicts()[0]
                    margin = _safe_float(last.get(numeric_cols[0])) - _safe_float(last.get(numeric_cols[1]))
            except Exception as exc:
                warning = f"FTE parse failed: {type(exc).__name__}"
        if margin is not None:
            self._latest_national_insert_or_update({"generic_ballot_d_margin": margin})
        return StepResult("Polling archive", "complete" if rows else "partial", rows, warning=warning)

    def step_pape(self) -> StepResult:
        result = self.run_scraper("PapeScraper", "scrapers.pape_scraper", RAW_DIR / "pape_escalation_latest.parquet")
        if (RAW_DIR / "pape_escalation_latest.parquet").exists():
            shutil.copy2(RAW_DIR / "pape_escalation_latest.parquet", RAW_DIR / "pape_posts.parquet")
        return result

    def steps(self) -> list[Step]:
        steps = [
            Step("approval", "Approval polling", [RAW_DIR / "approval_polls_latest.json"], self.step_approval),
            Step("fred", "FRED economic data", [RAW_DIR / "fred_all.parquet"], self.step_fred),
            Step("eia", "EIA gas prices", [RAW_DIR / "eia_gas.parquet"], self.step_eia),
            Step("bls", "BLS grocery and egg prices", [RAW_DIR / "bls_grocery.parquet"], self.step_bls),
            Step("census", "Census ACS district demographics", [RAW_DIR / "census_district_features_latest.parquet"], self.step_census),
            Step("fec", "FEC 2026 candidate finance update", [HISTORICAL_DIR / "weball26.zip"], self.step_fec),
            Step("ballotpedia", "Ballotpedia candidate scrape", [RAW_DIR / "ballotpedia_2026_house.parquet"], self.step_ballotpedia),
            Step("candidate_sanity", "Candidate roster sanity audit", [ROOT / "data" / "candidate_sanity_report.json"], self.step_candidate_sanity),
            Step("trends", "Google Trends", [RAW_DIR / "google_trends_latest.parquet"], self.step_trends),
            Step("local_news", "Local news scrape", [RAW_DIR / "local_news_latest.parquet"], self.step_news),
            Step("race_news", "Race-specific web scrape", [RAW_DIR / "race_web_latest.parquet"], self.step_race_news),
            Step("incumbent_status", "Incumbent retirements and vacancies", [RAW_DIR / "incumbent_status_2026_latest.parquet"], self.step_incumbent_status),
            Step("twoseventy_house", "270toWin House race context", [RAW_DIR / "twoseventy_house_context_latest.parquet"], self.step_twoseventy_house),
            Step("source_intel", "Source intelligence and event tokenization", [RAW_DIR / "source_intel_latest.parquet"], self.step_source_intel),
            Step("ideology_corpus", "Ideology corpus scrape", [RAW_DIR / "ideology_corpus_latest.parquet"], self.step_ideology_corpus),
            Step("backfill_embeddings", "Backfill article and ideology embeddings", [ROOT / "data" / "models" / "embedding_backfill_latest.json"], self.step_backfill_embeddings),
            Step("kalshi", "Kalshi market data", [RAW_DIR / "kalshi_markets.parquet"], self.step_kalshi),
            Step("polymarket", "Polymarket market data", [RAW_DIR / "polymarket_markets_latest.parquet"], self.step_polymarket),
            Step("dwnominate", "DW-NOMINATE polarization data", [RAW_DIR / "dwnominate.csv"], self.step_dwnominate),
            Step("polling", "Historical polling archive", [RAW_DIR / "fte_generic_ballot.csv"], self.step_polling),
            Step("pape", "Pape Substack scrape", [RAW_DIR / "pape_posts.parquet"], self.step_pape),
        ]
        if self.news_only:
            keep = {"local_news", "race_news", "source_intel", "ideology_corpus", "backfill_embeddings", "pape"}
            steps = [step for step in steps if step.key in keep]
        if self.skip_slow_trends:
            steps = [step for step in steps if step.key != "trends"]
        return steps

    def run_data_steps(self) -> None:
        steps = self.steps()
        self.log("TAPESTRY OVERNIGHT PIPELINE")
        self.log("---------------------------")
        if self.dry_run:
            for index, step in enumerate(steps, start=1):
                self.log(f"[DRY RUN] Step {index}/{len(steps)}: {step.name}")
            return
        for index, step in enumerate(steps, start=1):
            if self._is_cached(step):
                result = StepResult(step.name, "complete", cache_hit=True)
                self.log(f"[CACHED] Step {index}/{len(steps)}: {step.name}")
                self._record(step, result)
                continue
            self.log(f"Step {index}/{len(steps)}: {step.name} ...")
            start = time.perf_counter()
            try:
                result = step.runner()
            except Exception as exc:
                result = StepResult(step.name, "partial", warning=f"{type(exc).__name__}: {exc}")
            result.name = step.name
            result.duration_seconds = round(time.perf_counter() - start, 2)
            if result.duration_seconds > MAX_STEP_SECONDS:
                result.status = "partial"
                result.warning = (result.warning or "") + " exceeded overnight step timeout"
            if result.warning:
                self.warnings.append(f"{step.name}: {result.warning}")
            marker = "OK" if result.status == "complete" else "PARTIAL"
            self.log(f"{marker} Step {index}/{len(steps)}: {step.name} - {result.rows_loaded} rows loaded in {result.duration_seconds:.1f}s")
            self._record(step, result)

    def run_model(self) -> dict:
        self.log("Training district forecast model ...")
        start = time.perf_counter()
        result = OvernightTrainer().train_and_score({"steps_completed": sum(1 for r in self.results if r.status == "complete"), "steps_cached": sum(1 for r in self.results if r.cache_hit)})
        elapsed = time.perf_counter() - start
        self.log(f"OK Model training complete in {elapsed / 60:.1f} min")
        self.log(f"Model trained on {result['training_examples']} historical races across {len(result['training_years'])} election cycles")
        return result

    def data_completeness(self) -> dict:
        try:
            with write_connection() as con:
                total = con.execute("SELECT COUNT(*) FROM house_roster").fetchone()[0] or 435
                census = con.execute("SELECT COUNT(DISTINCT district_id) FROM district_features").fetchone()[0]
                fec = con.execute("SELECT COUNT(DISTINCT district_id) FROM fec_candidate_finance").fetchone()[0]
                news = con.execute("SELECT COUNT(DISTINCT district_id) FROM local_news").fetchone()[0]
                race_news = con.execute("SELECT COUNT(DISTINCT district_id) FROM race_web_articles").fetchone()[0]
                kalshi = con.execute("SELECT COUNT(DISTINCT district_id) FROM kalshi_market_mapping WHERE district_id IS NOT NULL").fetchone()[0]
                full = con.execute(
                    """
                    SELECT COUNT(*)
                    FROM house_roster h
                    WHERE h.district_id IN (SELECT DISTINCT district_id FROM district_features)
                      AND h.district_id IN (SELECT DISTINCT district_id FROM fec_candidate_finance)
                    """
                ).fetchone()[0]
        except Exception as exc:
            self.warnings.append(f"Data completeness summary failed: {type(exc).__name__}: {exc}")
            total, census, fec, news, race_news, kalshi, full = 435, 0, 0, 0, 0, 0, 0
        return {
            "districts_with_census": int(census),
            "districts_with_fec": int(fec),
            "districts_with_news": int(news),
            "districts_with_race_news": int(race_news),
            "districts_with_kalshi": int(kalshi),
            "districts_with_full_data": int(full),
            "total_districts": int(total),
        }

    def write_summary(self, model_result: dict | None) -> dict:
        completed_at = datetime.now(UTC)
        validation = (model_result or {}).get("validation", {})
        v2024 = validation.get("2024", {})
        top_features = [name for name, _score in (model_result or {}).get("top_features", [])[:10]]
        summary = {
            "run_date": date.today().isoformat(),
            "started_at": self.started_at.isoformat(),
            "completed_at": completed_at.isoformat(),
            "duration_minutes": int((completed_at - self.started_at).total_seconds() / 60),
            "steps": [result.__dict__ for result in self.results],
            "model_performance": {
                "brier_2020": validation.get("2020", {}).get("brier_score"),
                "brier_2022": validation.get("2022", {}).get("brier_score"),
                "brier_2024": v2024.get("brier_score"),
                "baseline_brier_2024": v2024.get("baseline_brier"),
                "improvement_2024": v2024.get("improvement"),
                "optimal_decay_rate": (model_result or {}).get("optimal_decay_rate"),
                "top_features": top_features,
                "status": "complete" if model_result else "not_completed",
            },
            "data_completeness": self.data_completeness(),
            "warnings": self.warnings,
        }
        tmp = SUMMARY_PATH.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, SUMMARY_PATH)
        self.log("OK Overnight summary written")
        return summary

    def run(self) -> dict:
        init_db()
        try:
            if not self.model_only:
                self.run_data_steps()
        except Exception as exc:
            warning = f"Data pipeline stopped unexpectedly but summary will still be written: {type(exc).__name__}: {exc}"
            self.warnings.append(warning)
            self.log(warning)
        model_result = None
        if self.dry_run:
            self.log("Dry run complete; no data or model steps were executed.")
        elif not self.data_only:
            try:
                model_result = self.run_model()
            except Exception as exc:
                warning = f"Model training failed; old API forecasts remain in place: {type(exc).__name__}: {exc}"
                self.warnings.append(warning)
                self.log(warning)
        summary = self.write_summary(model_result)
        self.log("OK All requested overnight work complete")
        return summary


def _safe_float(value: object) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TAPESTRY's resumable overnight data and model pipeline.")
    parser.add_argument("--force", action="store_true", help="Ignore cache state and rerun all steps.")
    parser.add_argument("--model-only", action="store_true", help="Skip data collection and only train/score the model.")
    parser.add_argument("--data-only", action="store_true", help="Run data collection but skip model training.")
    parser.add_argument("--news-only", action="store_true", help="Only run news/source-intelligence steps, then train unless --data-only is also set.")
    parser.add_argument("--skip-slow-trends", action="store_true", help="Skip Google Trends, the slowest and most rate-limited step.")
    parser.add_argument("--dry-run", action="store_true", help="Print the planned steps without running them.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.model_only and args.data_only:
        raise SystemExit("--model-only and --data-only cannot be used together")
    raise SystemExit(
        0
        if OvernightPipeline(
            force=args.force,
            model_only=args.model_only,
            data_only=args.data_only,
            news_only=args.news_only,
            skip_slow_trends=args.skip_slow_trends,
            dry_run=args.dry_run,
        ).run()
        else 1
    )
