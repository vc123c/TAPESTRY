from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import date, datetime
from pathlib import Path

import numpy as np

from db.connection import ROOT, init_db, write_connection
from model.aggregation_sim import ChamberSimulation
from model.conflict_model import PapeEscalationHMM
from model.district_model import DistrictModel
from model.features import build_district_features, build_national_factors
from model.narrative_generator import NarrativeGenerator
from model.senate_model import SenateModel
from model.walk_forward import WalkForwardValidator
from scrapers.ballotpedia_scraper import BallotpediaScraper
from scrapers.approval_scraper import run as run_approval_scraper
from scrapers.bls_grocery_scraper import BLSGroceryScraper
from scrapers.cook_scraper import CookScraper
from scrapers.eia_gas_scraper import EIAGasScraper
from scrapers.fec_scraper import FECScraper
from scrapers.fred_scraper import FredScraper
from scrapers.gdelt_scraper import GDELTScraper
from scrapers.google_trends_scraper import GoogleTrendsScraper
from scrapers.house_roster_scraper import HouseRosterScraper
from scrapers.kalshi_scraper import KalshiScraper
from scrapers.local_news_scraper import LocalNewsScraper
from scrapers.opensecrets_scraper import OpenSecretsScraper
from scrapers.pape_scraper import PapeScraper
from scrapers.race_web_scraper import RaceWebScraper
from scrapers.ideology_corpus_scraper import IdeologyCorpusScraper
from utils.logging import setup_logging
from data.historical.load_historical import load_historical

BAD_EVENT_NAMES = {"stonks", "stocks", "untitled", "news", "latest", "update", "market watch"}


def _usable_event_name(name: object) -> bool:
    value = str(name or "").strip()
    return len(value) >= 12 and value.lower() not in BAD_EVENT_NAMES and any(ch.isalpha() for ch in value)


class TapestryRetrainer:
    def __init__(self) -> None:
        self.logger = setup_logging(__name__)
        self.model = DistrictModel.load(str(ROOT / "data" / "models" / "district_model.pkl"))
        self.narratives = NarrativeGenerator()
        self.conflict = PapeEscalationHMM()

    async def _run_scrapers(self) -> None:
        scrapers = [
            FredScraper(), EIAGasScraper(), BLSGroceryScraper(), FECScraper(), BallotpediaScraper(),
            PapeScraper(), GDELTScraper(), KalshiScraper(), CookScraper(), OpenSecretsScraper(),
            GoogleTrendsScraper(), HouseRosterScraper(), LocalNewsScraper(), RaceWebScraper(), IdeologyCorpusScraper(),
        ]
        await asyncio.gather(*(asyncio.to_thread(scraper.run) for scraper in scrapers), return_exceptions=True)

    async def _timed_step(self, name: str, func, timeout: int = 60):
        try:
            return await asyncio.wait_for(asyncio.to_thread(func), timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.warning("%s exceeded %s seconds; skipping step", name, timeout)
            return None
        except Exception:
            self.logger.exception("%s failed; continuing update", name)
            return None

    def _write_morning_brief(self, brief: dict) -> None:
        path = ROOT / "data" / "morning_brief.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(brief, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, path)

    def _upsert_features(self, features, national: dict) -> None:
        with write_connection() as con:
            con.register("features_df", features)
            district_cols = [row[0] for row in con.execute("DESCRIBE district_features").fetchall()]
            feature_cols = set(features.columns)
            insert_cols = [col for col in district_cols if col in feature_cols]
            if not insert_cols:
                raise RuntimeError("No matching feature columns available for district_features upsert")
            col_sql = ", ".join(insert_cols)
            con.execute(f"INSERT OR REPLACE INTO district_features ({col_sql}) SELECT {col_sql} FROM features_df")

            national_cols = [row[0] for row in con.execute("DESCRIBE national_factors").fetchall()]
            national_values = [national.get(col) for col in national_cols]
            placeholders = ", ".join(["?"] * len(national_cols))
            con.execute(
                f"INSERT OR REPLACE INTO national_factors ({', '.join(national_cols)}) VALUES ({placeholders})",
                national_values,
            )

    def _write_conflict(self) -> dict:
        state = self.conflict.infer({})
        probs = state.stage_probabilities
        with write_connection() as con:
            con.execute(
                "INSERT OR REPLACE INTO conflict_states VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    state.conflict_id, date.today(), "Iran escalation", date(2026, 2, 28), state.current_stage,
                    probs[1], probs[2], probs[3], probs[4], probs[5], state.escalation_trap_probability,
                    state.days_in_conflict, 82.0, 0.08, 0.04, 0.32, 0.64, state.latest_signal,
                ],
            )
        return {"stage": state.current_stage, "trap": state.escalation_trap_probability}

    def _candidate_lookup(self) -> dict[tuple[str, str], str]:
        with write_connection() as con:
            rows = con.execute(
                """
                SELECT c.district_id, c.party, c.candidate_name, c.is_incumbent
                FROM candidate_roster_2026 c
                LEFT JOIN fec_candidate_finance f
                  ON f.fec_candidate_id = c.fec_candidate_id
                WHERE c.candidate_name IS NOT NULL
                ORDER BY COALESCE(f.total_receipts, 0) DESC, c.candidate_name
                """
            ).fetchall()
            incumbents = con.execute(
                "SELECT district_id, incumbent_party, incumbent_name FROM house_roster WHERE incumbent_name IS NOT NULL"
            ).fetchall()
        lookup: dict[tuple[str, str], str] = {}
        for district_id, party, name, _is_incumbent in rows:
            if party and (district_id, party) not in lookup:
                lookup[(district_id, party)] = name
        for district_id, party, name in incumbents:
            if party:
                lookup[(district_id, party)] = name
        return lookup

    async def fast_update_async(self) -> dict:
        init_db()
        await asyncio.gather(
            self._timed_step("approval_scraper", run_approval_scraper),
            self._timed_step("fred_scraper", FredScraper().run),
            self._timed_step("kalshi_scraper", KalshiScraper().run),
        )
        from model.overnight_trainer import OvernightTrainer

        report = await asyncio.to_thread(
            OvernightTrainer().train_and_score,
            {"mode": "fast_update", "generated_at": datetime.utcnow().isoformat()},
        )
        brief_path = ROOT / "data" / "morning_brief.json"
        if brief_path.exists():
            return json.loads(brief_path.read_text(encoding="utf-8"))
        return report
        features = await self._timed_step("build_district_features", build_district_features)
        national = await self._timed_step("build_national_factors", build_national_factors)
        if features is None:
            features = build_district_features()
        if national is None:
            national = build_national_factors()
        self._upsert_features(features, national)
        conflict = self._write_conflict()

        forecasts = []
        generator = self.narratives
        candidate_lookup = self._candidate_lookup()
        for row in features.to_dicts():
            pred = self.model.predict_one(row, national)
            leading_party = "D" if pred["win_probability_d"] >= 0.5 else "R"
            candidate = candidate_lookup.get((row["district_id"], leading_party))
            kalshi_price = None
            gap = None
            gap_flag = False
            narrative = generator.district_narrative({**pred, "kalshi_gap_flag": gap_flag, "kalshi_gap": gap})
            statement = generator.district_statement(row["district_id"], candidate, pred["projected_margin"], pred["uncertainty"]) if candidate else None
            forecasts.append({
                "district_id": row["district_id"], "forecast_date": date.today(), "leading_candidate": candidate,
                "leading_party": leading_party, "projected_margin": pred["projected_margin"], "uncertainty": pred["uncertainty"],
                "win_probability_d": pred["win_probability_d"], "factor_attribution": pred["factor_attribution"],
                "narrative": narrative, "kalshi_price": kalshi_price, "model_implied_price": pred["model_implied_price"],
                "kalshi_gap": gap, "kalshi_gap_flag": gap_flag, "gap_explanation": None, "suspect_flag": False,
                "brier_score_historical": 0.168, "statement": statement,
            })

        house_probs = np.array([f["win_probability_d"] for f in forecasts])
        house_margins = np.array([f["projected_margin"] for f in forecasts])
        house = ChamberSimulation().run(house_probs, base_d_seats=195, chamber_size=435, margins=house_margins)
        senate = SenateModel().predict({"state_cook_pvi": 0, "state_unemployment": 4.1, "candidate_quality_differential": 0.1, "incumbent_state_approval": 48}, national)
        senate_chamber = {"d_control_probability": senate["win_probability_d"], "d_expected_seats": 50.8, "d_seats_10th_pct": 48, "d_seats_90th_pct": 53}

        with write_connection() as con:
            for f in forecasts:
                con.execute(
                    "INSERT OR REPLACE INTO district_forecasts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [f["district_id"], f["forecast_date"], f["leading_candidate"], f["leading_party"], f["projected_margin"], f["uncertainty"], f["win_probability_d"], json.dumps(f["factor_attribution"]), f["narrative"], f["kalshi_price"], f["model_implied_price"], f["kalshi_gap"], f["kalshi_gap_flag"], f["gap_explanation"], f["suspect_flag"], f["brier_score_historical"]],
                )
            kalshi_rows = dict(con.execute("SELECT chamber, AVG(yes_price) FROM kalshi_market_mapping WHERE chamber IS NOT NULL GROUP BY chamber").fetchall())
            for chamber, data in [("house", house), ("senate", senate_chamber)]:
                kalshi = kalshi_rows.get(chamber)
                gap = data["d_control_probability"] - kalshi if kalshi is not None else None
                con.execute(
                    """
                    INSERT OR REPLACE INTO chamber_forecasts (
                        forecast_date, chamber, d_control_probability, d_expected_seats,
                        d_seats_10th_pct, d_seats_90th_pct, kalshi_price,
                        model_implied_price, kalshi_gap, narrative
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [date.today(), chamber, data["d_control_probability"], data["d_expected_seats"], data["d_seats_10th_pct"], data["d_seats_90th_pct"], kalshi, data["d_control_probability"], gap, f"TAPESTRY gives Democrats a {data['d_control_probability']:.0%} chance of {chamber} control."],
                )

        with write_connection() as con:
            event_rows = con.execute(
                """
                SELECT
                    e.event_id,
                    e.event_name,
                    e.event_type,
                    COALESCE(MAX(s.composite_salience), 0.5) AS salience,
                    e.affected_districts,
                    MAX(e.event_date) AS latest_event_date
                FROM event_tokens e
                LEFT JOIN event_salience s ON s.event_id=e.event_id
                WHERE COALESCE(e.resolved, false)=false
                GROUP BY e.event_id, e.event_name, e.event_type, e.affected_districts
                ORDER BY salience DESC, latest_event_date DESC
                LIMIT 8
                """
            ).fetchall()
        active_events = [
            {
                "event_id": row[0],
                "event_name": row[1],
                "event_type": row[2],
                "salience": float(row[3] or 0.0),
                "affected_districts": list(row[4] or []),
            }
            for row in event_rows
            if _usable_event_name(row[1])
        ]

        brief = {
            "generated_at": datetime.utcnow().isoformat(),
            "senate": {"chamber": "senate", **senate_chamber, "d_seats_low": senate_chamber["d_seats_10th_pct"], "d_seats_high": senate_chamber["d_seats_90th_pct"], "kalshi_price": None, "model_implied_price": senate_chamber["d_control_probability"], "kalshi_gap": None, "narrative": "Senate forecast generated from current model inputs."},
            "house": {"chamber": "house", "d_control_probability": house["d_control_probability"], "d_expected_seats": house["d_expected_seats"], "d_seats_low": house["d_seats_10th_pct"], "d_seats_high": house["d_seats_90th_pct"], "kalshi_price": None, "model_implied_price": house["d_control_probability"], "kalshi_gap": None, "narrative": "House control forecast generated from current model inputs."},
            "national": {"presidential_approval": national["presidential_approval"], "generic_ballot_margin": national["generic_ballot_d_margin"], "kitchen_table_index": national["kitchen_table_index"], "anti_establishment_index": national["anti_establishment_index"], "college_realignment_index": national["college_realignment_index"], "conflict_stage_iran": conflict["stage"], "escalation_trap_probability": conflict["trap"], "days_to_election": max(0, (date(2026, 11, 3) - date.today()).days)},
            "top_moves": [],
            "active_events": active_events,
            "kalshi_disagreements": [],
            "narrative": generator.morning_brief(largest_move="AZ-06"),
            "anomalies_pending": 0,
        }
        self._write_morning_brief(brief)
        self.logger.info("Fast update complete with %s forecasts", len(forecasts))
        return brief

    async def daily_update_async(self) -> dict:
        return await self.fast_update_async()

    async def slow_update_async(self) -> dict:
        init_db()
        await self._run_scrapers()
        self.full_retrain()
        return await self.fast_update_async()

    def fast_update(self) -> dict:
        return asyncio.run(self.fast_update_async())

    def slow_update(self) -> dict:
        return asyncio.run(self.slow_update_async())

    def daily_update(self) -> dict:
        return self.fast_update()

    def _training_rows(self) -> list[dict]:
        with write_connection() as con:
            count = con.execute("SELECT COUNT(*) FROM election_results").fetchone()[0]
        if count == 0:
            load_historical()
        features = build_district_features().to_dicts()
        feature_by_district = {row["district_id"]: row for row in features}
        national_by_year = {
            2014: {"generic_ballot_d_margin": -5.7, "presidential_approval": 42, "kitchen_table_index": 0.50, "college_realignment_index": 0.35, "anti_establishment_index": 0.44},
            2016: {"generic_ballot_d_margin": 1.1, "presidential_approval": 50, "kitchen_table_index": 0.45, "college_realignment_index": 0.43, "anti_establishment_index": 0.55},
            2018: {"generic_ballot_d_margin": 8.6, "presidential_approval": 41, "kitchen_table_index": 0.47, "college_realignment_index": 0.57, "anti_establishment_index": 0.61},
            2020: {"generic_ballot_d_margin": 3.1, "presidential_approval": 45, "kitchen_table_index": 0.65, "college_realignment_index": 0.66, "anti_establishment_index": 0.66},
            2022: {"generic_ballot_d_margin": -2.8, "presidential_approval": 42, "kitchen_table_index": 0.72, "college_realignment_index": 0.69, "anti_establishment_index": 0.68},
            2024: {"generic_ballot_d_margin": 0.0, "presidential_approval": 40, "kitchen_table_index": 0.69, "college_realignment_index": 0.71, "anti_establishment_index": 0.70},
        }
        with write_connection() as con:
            rows = con.execute(
                "SELECT district_id, year, margin, incumbent_party, incumbent_running FROM election_results WHERE uncontested=false"
            ).fetchall()
        training_rows = []
        for district_id, year, margin, incumbent_party, incumbent_running in rows:
            base = dict(feature_by_district.get(district_id, {}))
            base.update(national_by_year.get(int(year), national_by_year[2024]))
            base.update({
                "district_id": district_id,
                "year": int(year),
                "margin": float(margin),
                "incumbent_party": incumbent_party,
                "incumbent_running": bool(incumbent_running),
                "open_seat": not bool(incumbent_running),
            })
            training_rows.append(base)
        return training_rows

    def full_retrain(self, job_id: str | None = None) -> dict:
        job_id = job_id or str(uuid.uuid4())
        with write_connection() as con:
            con.execute("INSERT OR REPLACE INTO retrain_jobs VALUES (?, ?, ?, ?, ?, ?)", [job_id, datetime.utcnow(), datetime.utcnow(), "running", 0.2, "Starting validation"])
        perf = WalkForwardValidator().run()
        training_rows = self._training_rows()
        self.model.fit(training_rows)
        model_path = ROOT / "data" / "models" / "district_model.pkl"
        self.model.save(str(model_path))
        report = {
            "trained_at": datetime.utcnow().isoformat(),
            "model_path": str(model_path),
            "training_rows": len(training_rows),
            "years": sorted({row["year"] for row in training_rows}),
            "districts": sorted({row["district_id"] for row in training_rows}),
            "features": self.model.feature_names,
            "target": "Democratic two-party margin, positive means Democratic win",
            "stage_1": "Ridge fundamentals model with theory-informed features",
            "stage_2": "XGBoost residual model, max_depth=4, n_estimators=200, subsample=0.8",
            "validation": perf,
            "data_note": "Historical rows are seeded from the local historical loader until MIT Election Lab/FiveThirtyEight/Cook archives are wired in.",
        }
        report_path = ROOT / "data" / "models" / "district_training_report.json"
        report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        with write_connection() as con:
            con.execute("UPDATE retrain_jobs SET updated_at=?, status=?, progress=?, message=? WHERE job_id=?", [datetime.utcnow(), "complete", 1.0, "Retrain complete", job_id])
        return {"job_id": job_id, "performance": perf, "training_report": report}

    def on_anomaly_detected(self, event: dict) -> str:
        queue_id = str(uuid.uuid4())
        with write_connection() as con:
            con.execute(
                "INSERT INTO admin_queue VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [queue_id, datetime.utcnow(), event.get("description", "Anomaly detected"), event.get("affected_districts", []), json.dumps(event.get("search_results", [])), json.dumps(event.get("suggested_token", {})), "pending", None, None],
            )
        return queue_id
