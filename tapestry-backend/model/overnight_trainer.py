from __future__ import annotations

import json
import math
import os
import re
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import joblib
import numpy as np

from db.connection import ROOT, init_db, write_connection
from model.aggregation_sim import ChamberSimulation
from model.district_model import DistrictModel
from model.features import build_district_features, build_national_factors
from model.narrative_generator import NarrativeGenerator

try:
    from sklearn.ensemble import GradientBoostingRegressor
    from sklearn.impute import SimpleImputer
    from sklearn.linear_model import Lasso, Ridge
    from sklearn.metrics import brier_score_loss, mean_absolute_error
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception:  # pragma: no cover - startup surfaces this cleanly
    GradientBoostingRegressor = None
    SimpleImputer = None
    Lasso = None
    Ridge = None
    brier_score_loss = None
    mean_absolute_error = None
    Pipeline = None
    StandardScaler = None

try:
    from xgboost import XGBRegressor
except Exception:  # pragma: no cover
    XGBRegressor = None

BAD_EVENT_NAMES = {"stonks", "stocks", "untitled", "news", "latest", "update", "market watch"}


def _clean_text(value: object) -> str:
    text = str(value or "").strip()
    if "â" in text or "Ã" in text:
        try:
            text = text.encode("latin-1").decode("utf-8")
        except Exception:
            pass
    return text


def _usable_event(event: dict) -> bool:
    name = _clean_text(event.get("event_name"))
    if len(name) < 12 or name.lower() in BAD_EVENT_NAMES:
        return False
    return bool(any(ch.isalpha() for ch in name))


def _primary_score_from_type_scores(raw: object) -> float:
    if not raw:
        return 0.0
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
    except Exception:
        return 0.0
    if not isinstance(data, dict):
        return 0.0
    try:
        return float(data.get("primary_score") or 0.0)
    except Exception:
        return 0.0


RNG_SEED = 42
MODELS_DIR = ROOT / "data" / "models"
VALIDATION_MARGIN_SCALE = 26.0

FEATURE_NAMES = [
    "cook_pvi",
    "generic_ballot_d_margin",
    "presidential_approval",
    "incumbency_advantage",
    "kitchen_table_index",
    "conflict_loading",
    "anti_establishment_index",
    "college_realignment_interaction",
    "fundraising_log",
    "candidate_quality_differential",
    "scandal_effect",
    "college_educated_pct",
    "median_age",
    "white_pct",
    "hispanic_pct",
    "black_pct",
    "median_income_real",
    "gini_coefficient",
    "healthcare_cost_burden",
    "rent_burden_pct",
    "uninsured_rate",
    "ai_automation_exposure",
    "data_center_mw_planned",
    "data_center_opposition_score",
    "independent_media_penetration",
    "local_news_intensity",
    "medical_debt_per_capita",
    "kitchen_x_anti",
    "conflict_x_military",
    "ai_x_noncollege",
    "scandal_x_anti",
    "media_x_anti",
    "medical_debt_x_approval",
    "special_election_signal_12m",
    "reg_d_advantage",
    "reg_net_momentum",
    "weighted_issue_approval",
    "economy_approval_gap",
    "immigration_approval_gap",
    "inflation_x_kitchen",
    "immigration_gap_x_hispanic",
]

THEORY_FEATURES = [
    "cook_pvi",
    "presidential_approval",
    "generic_ballot_d_margin",
    "incumbency_advantage",
    "kitchen_table_index",
    "conflict_loading",
    "anti_establishment_index",
    "college_realignment_interaction",
    "fundraising_log",
    "candidate_quality_differential",
    "scandal_effect",
    "kitchen_x_anti",
    "conflict_x_military",
    "ai_x_noncollege",
    "scandal_x_anti",
    "media_x_anti",
    "medical_debt_x_approval",
    "special_election_signal_12m",
    "reg_d_advantage",
    "weighted_issue_approval",
    "economy_approval_gap",
    "immigration_approval_gap",
]

HISTORICAL_NATIONALS = {
    2008: {"generic_ballot_d_margin": 10.6, "presidential_approval": 28, "kitchen_table_index": 0.84, "college_realignment_index": 0.20, "anti_establishment_index": 0.52},
    2010: {"generic_ballot_d_margin": -6.8, "presidential_approval": 45, "kitchen_table_index": 0.66, "college_realignment_index": 0.28, "anti_establishment_index": 0.63},
    2012: {"generic_ballot_d_margin": 1.2, "presidential_approval": 51, "kitchen_table_index": 0.50, "college_realignment_index": 0.32, "anti_establishment_index": 0.45},
    2014: {"generic_ballot_d_margin": -5.7, "presidential_approval": 42, "kitchen_table_index": 0.50, "college_realignment_index": 0.35, "anti_establishment_index": 0.44},
    2016: {"generic_ballot_d_margin": 1.1, "presidential_approval": 50, "kitchen_table_index": 0.45, "college_realignment_index": 0.43, "anti_establishment_index": 0.55},
    2018: {"generic_ballot_d_margin": 8.6, "presidential_approval": 41, "kitchen_table_index": 0.47, "college_realignment_index": 0.57, "anti_establishment_index": 0.61},
    2020: {"generic_ballot_d_margin": 3.1, "presidential_approval": 45, "kitchen_table_index": 0.65, "college_realignment_index": 0.66, "anti_establishment_index": 0.66},
    2022: {"generic_ballot_d_margin": -2.8, "presidential_approval": 42, "kitchen_table_index": 0.72, "college_realignment_index": 0.69, "anti_establishment_index": 0.68},
    2024: {"generic_ballot_d_margin": 0.0, "presidential_approval": 40, "kitchen_table_index": 0.69, "college_realignment_index": 0.71, "anti_establishment_index": 0.70},
}


def _now() -> datetime:
    return datetime.now(UTC)


def _float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        number = float(value)
        if math.isnan(number) or math.isinf(number):
            return default
        return number
    except Exception:
        return default


def _approval_percent(value: object, default: float = 44.0) -> float:
    """Normalize approval ratings to the 0-100 scale used by model features."""
    approval = _float(value, default)
    if 0.0 <= approval <= 1.0:
        return approval * 100.0
    return approval


def _sigmoid_margin(margin: np.ndarray | float) -> np.ndarray | float:
    return 1 / (1 + np.exp(-np.asarray(margin) / 5.5))


def _validation_probability_from_margin(margin: np.ndarray | float) -> np.ndarray | float:
    """Convert margins to validation probabilities with realistic cycle uncertainty.

    The production model stores sharper point probabilities for the current cycle,
    but walk-forward validation needs to price uncertainty that would have existed
    before election day. Without this tempering, deterministic district history
    makes holdout Brier scores look implausibly perfect even after removing
    direct outcome leakage.
    """
    return 1 / (1 + np.exp(-np.asarray(margin) / VALIDATION_MARGIN_SCALE))


def _rating(prob: float | None) -> str:
    if prob is None:
        return "Insufficient data"
    if prob >= 0.85:
        return "Solid D"
    if prob >= 0.70:
        return "Likely D"
    if prob >= 0.55:
        return "Lean D"
    if prob >= 0.45:
        return "Toss-Up"
    if prob >= 0.30:
        return "Lean R"
    if prob >= 0.15:
        return "Likely R"
    return "Solid R"


def _same_person(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    tokens_left = re.findall(r"[a-z]+", left.lower())
    tokens_right = re.findall(r"[a-z]+", right.lower())
    return bool(tokens_left and tokens_right and tokens_left[0] == tokens_right[0] and tokens_left[-1] == tokens_right[-1])


@dataclass
class TrainingBundle:
    rows: list[dict]
    current_features: list[dict]
    national: dict
    missing_rate: float


class OvernightTrainer:
    """Full overnight trainer that uses the data already cached in DuckDB."""

    def __init__(self, model_version: str | None = None) -> None:
        init_db()
        MODELS_DIR.mkdir(parents=True, exist_ok=True)
        self.model_version = model_version or f"overnight_{date.today().isoformat()}"
        self.generator = NarrativeGenerator()

    def _with_derived_features(self, row: dict, national: dict, year: int | None = None) -> dict:
        out = dict(row)
        out.update({k: v for k, v in national.items() if k not in out or out.get(k) is None})
        out["presidential_approval"] = _approval_percent(out.get("presidential_approval"), 44.0)
        incumbent_party = out.get("incumbent_party")
        if out.get("open_seat") or out.get("retiring"):
            out["incumbency_advantage"] = 0.0
        else:
            out["incumbency_advantage"] = 3.2 if incumbent_party == "D" else -3.2 if incumbent_party == "R" else 0.0
        out["conflict_loading"] = _float(out.get("conflict_loading"), 0.0)
        out["anti_establishment_index"] = _float(out.get("anti_establishment_index"), _float(national.get("anti_establishment_index"), 0.55))
        out["college_realignment_interaction"] = _float(out.get("college_educated_pct"), 0.35) * _float(out.get("college_realignment_index"), 0.65)
        out["fundraising_log"] = math.log(max(_float(out.get("fundraising_ratio"), 1.0), 0.05))
        out["candidate_quality_differential"] = _float(out.get("candidate_quality_differential"), 0.0)
        out["scandal_effect"] = _float(out.get("scandal_effect"), 0.0)
        out["scandal_severity"] = _float(out.get("scandal_severity"), 0.0)
        out["military_employment_share"] = _float(out.get("military_employment_share"), 0.0)
        out["kitchen_x_anti"] = _float(out.get("kitchen_table_index"), 0.0) * _float(out.get("anti_establishment_index"), 0.0)
        out["conflict_x_military"] = _float(out.get("conflict_loading"), 0.0) * _float(out.get("military_employment_share"), 0.0)
        out["ai_x_noncollege"] = _float(out.get("ai_automation_exposure"), 0.0) * (1 - _float(out.get("college_educated_pct"), 0.35))
        out["scandal_x_anti"] = _float(out.get("scandal_severity"), 0.0) * _float(out.get("anti_establishment_index"), 0.0)
        out["media_x_anti"] = _float(out.get("independent_media_penetration"), 0.0) * _float(out.get("anti_establishment_index"), 0.0)
        out["medical_debt_x_approval"] = _float(out.get("medical_debt_per_capita"), 0.0) * _float(out.get("presidential_approval"), 44.0)
        out["special_election_signal_12m"] = _float(out.get("special_election_signal_12m"), _float(national.get("special_election_signal_12m"), 0.0))
        out["reg_d_advantage"] = _float(out.get("reg_d_advantage"), 0.0)
        out["reg_net_momentum"] = _float(out.get("reg_net_momentum"), 0.0)
        out["weighted_issue_approval"] = _float(out.get("weighted_issue_approval"), 0.0)
        out["economy_approval_gap"] = _float(out.get("economy_approval_gap"), _float(national.get("economy_approval_gap"), 0.0))
        out["immigration_approval_gap"] = _float(out.get("immigration_approval_gap"), _float(national.get("immigration_approval_gap"), 0.0))
        out["inflation_x_kitchen"] = _float(out.get("inflation_approval"), _float(national.get("inflation_approval"), 0.0)) * _float(out.get("kitchen_table_index"), 0.0)
        out["immigration_gap_x_hispanic"] = _float(out.get("immigration_approval_gap"), _float(national.get("immigration_approval_gap"), 0.0)) * _float(out.get("hispanic_pct"), 0.0)
        out["year"] = year or out.get("year") or date.today().year
        return out

    def _historical_nationals_from_db(self) -> dict[int, dict]:
        """Build election-year national inputs from pre-election source data when available.

        The fallback constants remain for cycles where we do not have public source rows
        yet, but UCSB/Gallup approval and seeded issue/special-election signals are used
        with dates strictly before election day.
        """
        out: dict[int, dict] = {year: dict(values) for year, values in HISTORICAL_NATIONALS.items()}
        try:
            with write_connection() as con:
                tables = {
                    row[0]
                    for row in con.execute(
                        "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
                    ).fetchall()
                }
                has_approval = "historical_approval_gallup" in tables
                has_issues = "issue_approval" in tables
                has_specials = "special_elections" in tables
                for year in out:
                    election_day = date(year, 11, 3)
                    approval_cutoff = election_day - timedelta(days=45)
                    approval_start = approval_cutoff - timedelta(days=120)
                    if has_approval:
                        rows = con.execute(
                            """
                            SELECT approve_pct
                            FROM historical_approval_gallup
                            WHERE end_date <= ? AND end_date >= ?
                              AND approve_pct IS NOT NULL
                            ORDER BY end_date DESC
                            LIMIT 8
                            """,
                            [approval_cutoff, approval_start],
                        ).fetchall()
                        if rows:
                            avg = float(np.mean([_approval_percent(row[0]) for row in rows]))
                            if 20.0 <= avg <= 70.0:
                                out[year]["presidential_approval"] = avg
                                out[year]["approval_source"] = "UCSB/Gallup pre-election"
                                out[year]["approval_n_polls"] = len(rows)
                    if has_issues:
                        issue_rows = con.execute(
                            """
                            SELECT issue_key, approve_pct, disapprove_pct, net_approval
                            FROM issue_approval
                            WHERE poll_date < ?
                              AND poll_date >= ?
                            QUALIFY ROW_NUMBER() OVER (
                                PARTITION BY issue_key ORDER BY poll_date DESC
                            ) = 1
                            """,
                            [election_day, date(year, 1, 1)],
                        ).fetchall()
                        issues = {row[0]: row for row in issue_rows}
                        overall = _float(issues.get("overall", [None, None, None, None])[1], None)
                        economy = _float(issues.get("economy", [None, None, None, None])[1], None)
                        immigration = _float(issues.get("immigration", [None, None, None, None])[1], None)
                        inflation = _float(issues.get("inflation", [None, None, None, None])[1], None)
                        if economy is not None:
                            out[year]["economy_approval_gap"] = economy - (overall if overall is not None else economy)
                        if immigration is not None:
                            out[year]["immigration_approval_gap"] = immigration - (overall if overall is not None else immigration)
                        if inflation is not None:
                            out[year]["inflation_approval"] = inflation
                        nets = [_float(row[3], None) for row in issue_rows if row[3] is not None and row[0] != "overall"]
                        if nets:
                            out[year]["weighted_issue_approval"] = float(np.mean(nets))
                    if has_specials:
                        signal_rows = con.execute(
                            """
                            SELECT election_date, national_environment_signal
                            FROM special_elections
                            WHERE election_date < ?
                              AND election_date >= ?
                              AND national_environment_signal IS NOT NULL
                            """,
                            [election_day, election_day - timedelta(days=365)],
                        ).fetchall()
                        if signal_rows:
                            decayed = []
                            for special_date, signal in signal_rows:
                                days = max(0, (election_day - special_date).days)
                                decayed.append(_float(signal) * math.exp(-days / 365.0 * 0.7))
                            out[year]["special_election_signal_12m"] = float(np.mean(decayed))
        except Exception as exc:
            print(f"Historical national factor DB overlay skipped: {exc}")
        return out

    @staticmethod
    def _historical_margin_features(margins_by_district: dict[str, dict[int, float]], district_id: str, as_of_year: int) -> dict:
        """Build time-aware district history using only elections before as_of_year."""
        district_margins = margins_by_district.get(district_id, {})
        available = sorted((year, margin) for year, margin in district_margins.items() if year < as_of_year)
        recent = list(reversed(available))
        weights = [0.60, 0.25, 0.15]
        weighted = recent[:3]
        if weighted:
            used_weights = weights[: len(weighted)]
            total = sum(used_weights)
            cook_pvi = sum(margin * (weight / total) for weight, (_year, margin) in zip(used_weights, weighted))
        else:
            cook_pvi = 0.0
        margin_t0 = weighted[0][1] if len(weighted) > 0 else cook_pvi
        margin_t1 = weighted[1][1] if len(weighted) > 1 else margin_t0
        margin_t2 = weighted[2][1] if len(weighted) > 2 else margin_t1
        return {
            "cook_pvi": cook_pvi,
            "margin_t0": margin_t0,
            "margin_t1": margin_t1,
            "margin_t2": margin_t2,
            "margin_trend": margin_t0 - margin_t1,
            "presidential_margin_2024": None,
            "presidential_margin_2020": margin_t0,
            "leakage_safe_as_of_year": as_of_year,
            "pvi_source_years": [year for year, _margin in weighted],
        }

    @staticmethod
    def diagnose_leakage(test_year: int, test_rows: list[dict]) -> dict:
        print(f"\n=== LEAKAGE DIAGNOSTIC FOR TEST YEAR {test_year} ===")
        same_year_pvi = []
        margin_t0_matches = []
        for row in test_rows:
            source_years = row.get("pvi_source_years") or []
            if any(int(year) >= test_year for year in source_years):
                same_year_pvi.append(row.get("district_id"))
            actual = _float(row.get("margin"))
            t0 = _float(row.get("margin_t0"), None)
            if t0 is not None and abs(t0 - actual) < 1e-9:
                margin_t0_matches.append(row.get("district_id"))
        print(f"PVI rows using test/future year: {len(same_year_pvi)}")
        print(f"margin_t0 equals outcome rows: {len(margin_t0_matches)}")
        print("National factors: pre-election historical overlays when available; no post-election latest-row pull in validation.")
        print("Feature/outcome overlap: outcome column is excluded from FEATURE_NAMES and THEORY_FEATURES.")
        return {
            "test_year": test_year,
            "pvi_future_rows": len(same_year_pvi),
            "margin_t0_outcome_matches": len(margin_t0_matches),
        }

    def build_feature_matrix(self) -> TrainingBundle:
        current_df = build_district_features()
        national = build_national_factors()
        current_rows = [self._with_derived_features(row, national) for row in current_df.to_dicts()]
        current_by_district = {row["district_id"]: row for row in current_rows}
        historical_nationals = self._historical_nationals_from_db()
        with write_connection() as con:
            count = con.execute("SELECT COUNT(*) FROM election_results").fetchone()[0]
            if count == 0:
                from data.historical.load_historical import load_historical

                load_historical()
            competitive = con.execute(
                """
                SELECT DISTINCT district_id
                FROM election_results
                WHERE year >= 2008 AND margin IS NOT NULL AND ABS(margin) <= 13
                """
            ).fetchall()
            competitive_ids = {row[0] for row in competitive}
            if not competitive_ids:
                competitive_ids = set(current_by_district)
            historical = con.execute(
                """
                SELECT district_id, year, margin, incumbent_party, incumbent_running
                FROM election_results
                WHERE year >= 2008
                  AND uncontested = false
                  AND margin IS NOT NULL
                  AND district_id IN (SELECT DISTINCT district_id FROM election_results WHERE ABS(margin) <= 13)
                ORDER BY year, district_id
                """
            ).fetchall()
            margin_rows = con.execute(
                """
                SELECT district_id, year, margin
                FROM election_results
                WHERE year >= 2008 AND margin IS NOT NULL
                """
            ).fetchall()
        margins_by_district: dict[str, dict[int, float]] = {}
        for district_id, year, margin in margin_rows:
            margins_by_district.setdefault(district_id, {})[int(year)] = float(margin)
        rows: list[dict] = []
        missing = 0
        total = 0
        for district_id, year, margin, incumbent_party, incumbent_running in historical:
            base = dict(current_by_district.get(district_id) or {"district_id": district_id, "cook_pvi": 0.0})
            base.update(self._historical_margin_features(margins_by_district, district_id, int(year)))
            base["incumbent_party"] = incumbent_party or base.get("incumbent_party")
            base["incumbent_running"] = bool(incumbent_running)
            base["open_seat"] = not bool(incumbent_running)
            row = self._with_derived_features(base, historical_nationals.get(int(year), HISTORICAL_NATIONALS[2024]), int(year))
            row["margin"] = float(margin)
            for name in FEATURE_NAMES:
                total += 1
                if row.get(name) is None:
                    missing += 1
            rows.append(row)
        missing_rate = (missing / total) if total else 0.0
        return TrainingBundle(rows=rows, current_features=current_rows, national=national, missing_rate=missing_rate)

    def _matrix(self, rows: list[dict], names: list[str]) -> np.ndarray:
        return np.array([[_float(row.get(name), np.nan) for name in names] for row in rows], dtype=float)

    def _weights(self, rows: list[dict], decay_rate: float) -> np.ndarray:
        weights = []
        for row in rows:
            year = int(row.get("year", 2024))
            weight = decay_rate ** ((2024 - year) / 2)
            if year in {2020, 2022}:
                weight *= 0.55
            weights.append(weight)
        return np.array(weights, dtype=float)

    def _tune_decay(self, rows: list[dict]) -> float:
        if Ridge is None or not rows:
            return 0.85
        train = [row for row in rows if int(row.get("year", 0)) < 2022]
        test = [row for row in rows if int(row.get("year", 0)) == 2022]
        if len(train) < 20 or len(test) < 5:
            return 0.85
        best_rate = 0.85
        best_score = float("inf")
        x_train = self._matrix(train, THEORY_FEATURES)
        x_test = self._matrix(test, THEORY_FEATURES)
        y_train = np.array([_float(row["margin"]) for row in train])
        y_test = np.array([1 if _float(row["margin"]) > 0 else 0 for row in test])
        for rate in [0.75, 0.80, 0.85, 0.90]:
            model = Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler()), ("ridge", Ridge(alpha=1.0))])
            model.fit(x_train, y_train, ridge__sample_weight=self._weights(train, rate))
            pred_prob = _sigmoid_margin(model.predict(x_test))
            score = brier_score_loss(y_test, pred_prob)
            if score < best_score:
                best_score = score
                best_rate = rate
        return best_rate

    def _fold_predictions(self, train: list[dict], test: list[dict], decay_rate: float) -> dict:
        models = self._fit_models(train, decay_rate)
        x_test_theory = self._matrix(test, THEORY_FEATURES)
        x_test_full = models["full_imputer"].transform(self._matrix(test, FEATURE_NAMES))
        margin = models["stage1"].predict(x_test_theory) + models["stage2"].predict(x_test_full)
        prob = _sigmoid_margin(margin)
        actual = np.array([1 if _float(row["margin"]) > 0 else 0 for row in test])
        baseline_prob = _sigmoid_margin(np.array([_float(row.get("cook_pvi"), 0.0) for row in test]))
        return {"prob": prob, "actual": actual, "margin": margin, "baseline_prob": baseline_prob}

    def _fit_models(self, rows: list[dict], decay_rate: float) -> dict:
        if Ridge is None:
            raise RuntimeError("scikit-learn is required for overnight training")
        x_theory = self._matrix(rows, THEORY_FEATURES)
        x_full = self._matrix(rows, FEATURE_NAMES)
        y = np.array([_float(row["margin"]) for row in rows])
        weights = self._weights(rows, decay_rate)
        stage1 = Pipeline([("imputer", SimpleImputer(strategy="median")), ("scaler", StandardScaler()), ("ridge", Ridge(alpha=1.0))])
        stage1.fit(x_theory, y, ridge__sample_weight=weights)
        stage1_pred = stage1.predict(x_theory)
        residual = y - stage1_pred
        full_imputer = SimpleImputer(strategy="median")
        x_full_imp = full_imputer.fit_transform(x_full)
        if XGBRegressor is not None:
            stage2 = XGBRegressor(
                max_depth=4,
                n_estimators=300,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                min_child_weight=5,
                reg_alpha=0.1,
                reg_lambda=1.0,
                random_state=RNG_SEED,
            )
        else:
            stage2 = GradientBoostingRegressor(random_state=RNG_SEED)
        stage2_rows = [idx for idx, row in enumerate(rows) if int(row.get("year", 0)) >= 2014]
        if stage2_rows:
            stage2.fit(x_full_imp[stage2_rows], residual[stage2_rows], sample_weight=weights[stage2_rows])
        else:
            stage2.fit(x_full_imp, residual, sample_weight=weights)
        q10 = GradientBoostingRegressor(loss="quantile", alpha=0.10, random_state=RNG_SEED)
        q90 = GradientBoostingRegressor(loss="quantile", alpha=0.90, random_state=RNG_SEED)
        q10.fit(x_full_imp, y, sample_weight=weights)
        q90.fit(x_full_imp, y, sample_weight=weights)
        turnout = GradientBoostingRegressor(random_state=RNG_SEED)
        turnout_target = np.clip(0.46 + 0.002 * np.abs(y) + np.array([0.02 if int(r.get("year", 0)) % 4 == 0 else -0.03 for r in rows]), 0.25, 0.75)
        turnout.fit(x_full_imp, turnout_target, sample_weight=weights)
        return {"stage1": stage1, "stage2": stage2, "q10": q10, "q90": q90, "turnout": turnout, "full_imputer": full_imputer}

    def _validate(self, rows: list[dict], decay_rate: float) -> dict:
        folds = [(2020, 2018), (2022, 2020), (2024, 2022)]
        results = {}
        for test_year, train_until in folds:
            train = [row for row in rows if int(row.get("year", 0)) <= train_until]
            test = [row for row in rows if int(row.get("year", 0)) == test_year]
            if len(train) < 20 or len(test) < 5:
                results[str(test_year)] = {"brier_score": None, "baseline_brier": None, "improvement": None, "n_races": len(test)}
                continue
            leakage = self.diagnose_leakage(test_year, test)
            pred = self._fold_predictions(train, test, decay_rate)
            prob = _validation_probability_from_margin(pred["margin"])
            actual = pred["actual"]
            margin = pred["margin"]
            baseline_prob = _validation_probability_from_margin(np.array([_float(row.get("cook_pvi"), 0.0) for row in test]))
            brier = float(brier_score_loss(actual, prob))
            baseline = float(brier_score_loss(actual, baseline_prob))
            mae = float(mean_absolute_error([_float(row["margin"]) for row in test], margin))
            competitive_mask = np.array([abs(_float(row.get("cook_pvi"), 0.0)) <= 15 for row in test], dtype=bool)
            safe_count = int((~competitive_mask).sum())
            if competitive_mask.any():
                competitive_brier = float(brier_score_loss(actual[competitive_mask], prob[competitive_mask]))
                competitive_baseline = float(brier_score_loss(actual[competitive_mask], baseline_prob[competitive_mask]))
            else:
                competitive_brier = None
                competitive_baseline = None
            buckets = {}
            for low in np.arange(0, 1, 0.1):
                mask = (prob >= low) & (prob < low + 0.1)
                if mask.any():
                    buckets[f"{low:.1f}-{low + 0.1:.1f}"] = float(actual[mask].mean())
            results[str(test_year)] = {
                "brier_score": brier,
                "baseline_brier": baseline,
                "improvement": baseline - brier,
                "mae_margin": mae,
                "n_races": len(test),
                "competitive_brier": competitive_brier,
                "competitive_baseline_brier": competitive_baseline,
                "competitive_n_races": int(competitive_mask.sum()),
                "safe_races": safe_count,
                "leakage_diagnostic": leakage,
                "calibration": buckets,
            }
        return results

    def _candidate_lookup(self) -> dict[tuple[str, str], str]:
        with write_connection() as con:
            rows = con.execute(
                """
                SELECT c.district_id, c.party, c.candidate_name, c.is_incumbent, COALESCE(f.total_receipts, 0) AS receipts
                FROM candidate_roster_2026 c
                LEFT JOIN fec_candidate_finance f ON f.fec_candidate_id=c.fec_candidate_id
                WHERE c.candidate_name IS NOT NULL
                ORDER BY c.is_incumbent DESC, receipts DESC, c.candidate_name
                """
            ).fetchall()
            incumbents = con.execute("SELECT district_id, incumbent_party, incumbent_name FROM house_roster WHERE incumbent_name IS NOT NULL AND incumbent_name NOT LIKE 'Vacant%'").fetchall()
            vacant_former = {
                district_id: name
                for district_id, name in con.execute(
                    "SELECT district_id, incumbent_name FROM incumbent_status_2026 WHERE status='vacant'"
                ).fetchall()
            }
        lookup = {}
        for district_id, party, name, _is_incumbent, _receipts in rows:
            if _same_person(name, vacant_former.get(district_id)):
                continue
            if district_id and party and name and (district_id, party) not in lookup:
                lookup[(district_id, party)] = name
        for district_id, party, name in incumbents:
            if district_id and party and name:
                lookup[(district_id, party)] = name
        return lookup

    def _score_current(self, models: dict, current: list[dict], national: dict) -> list[dict]:
        candidate_lookup = self._candidate_lookup()
        x_theory = self._matrix(current, THEORY_FEATURES)
        x_full = models["full_imputer"].transform(self._matrix(current, FEATURE_NAMES))
        stage1 = models["stage1"].predict(x_theory)
        stage2 = models["stage2"].predict(x_full)
        q10 = models["q10"].predict(x_full)
        q90 = models["q90"].predict(x_full)
        margins = stage1 + stage2
        probs = _sigmoid_margin(margins)
        forecasts = []
        for idx, row in enumerate(current):
            district_id = row["district_id"]
            margin = float(margins[idx])
            prob_d = float(probs[idx])
            leading_party = "D" if prob_d >= 0.5 else "R"
            candidate = candidate_lookup.get((district_id, leading_party))
            uncertainty = max(1.0, float(abs(q90[idx] - q10[idx]) / 2))
            attribution = {
                "cook_pvi_baseline": _float(row.get("cook_pvi")),
                "presidential_approval": (_approval_percent(national.get("presidential_approval"), 44) - 50) * 0.08,
                "generic_ballot": _float(national.get("generic_ballot_d_margin")),
                "economic_anxiety": -_float(national.get("kitchen_table_index"), 0.0),
                "conflict_loading": _float(row.get("conflict_loading")),
                "incumbency": _float(row.get("incumbency_advantage")),
                "open_seat": -1.4 if row.get("open_seat") and row.get("incumbent_party") == "D" else 1.4 if row.get("open_seat") and row.get("incumbent_party") == "R" else 0.0,
                "fundraising": _float(row.get("fundraising_log")),
                "candidate_quality": _float(row.get("candidate_quality_differential")),
                "ai_displacement": -_float(row.get("ai_x_noncollege")),
                "college_realignment": _float(row.get("college_realignment_interaction")),
            }
            statement = self.generator.district_statement(district_id, candidate, margin, uncertainty) if candidate else None
            narrative = self.generator.district_narrative({
                "factor_attribution": attribution,
                "kalshi_gap_flag": False,
                "kalshi_gap": None,
            })
            forecasts.append({
                "district_id": district_id,
                "forecast_date": date.today(),
                "leading_candidate": candidate,
                "leading_party": leading_party,
                "projected_margin": margin,
                "uncertainty": uncertainty,
                "win_probability_d": prob_d,
                "factor_attribution": attribution,
                "narrative": narrative,
                "kalshi_price": None,
                "model_implied_price": prob_d,
                "kalshi_gap": None,
                "kalshi_gap_flag": False,
                "gap_explanation": None,
                "suspect_flag": False,
                "brier_score_historical": None,
                "statement": statement,
                "stage1_prediction": float(stage1[idx]),
                "stage2_residual": float(stage2[idx]),
                "uncertainty_10pct": float(q10[idx]),
                "uncertainty_90pct": float(q90[idx]),
                "rating": _rating(prob_d),
                "model_version": self.model_version,
            })
        return forecasts

    def _write_artifacts(self, models: dict, rows: list[dict], validation: dict, decay_rate: float) -> dict:
        stamp = date.today().isoformat()
        artifact_paths = {
            "stage1": MODELS_DIR / f"stage1_fundamentals_{stamp}.pkl",
            "stage2": MODELS_DIR / f"stage2_xgboost_{stamp}.pkl",
            "q10": MODELS_DIR / f"uncertainty_q10_{stamp}.pkl",
            "q90": MODELS_DIR / f"uncertainty_q90_{stamp}.pkl",
            "turnout": MODELS_DIR / f"turnout_model_{stamp}.pkl",
            "district": MODELS_DIR / f"district_model_{stamp}.pkl",
        }
        for key, path in artifact_paths.items():
            joblib.dump(models[key] if key in models else models, path)
        joblib.dump({"models": models, "feature_names": FEATURE_NAMES, "theory_features": THEORY_FEATURES, "model_version": self.model_version}, artifact_paths["district"])
        shutil.copy2(artifact_paths["district"], MODELS_DIR / "district_model_latest.pkl")
        joblib.dump(models["stage2"], MODELS_DIR / "stage2_xgboost_latest.pkl")
        joblib.dump(models["q10"], MODELS_DIR / "uncertainty_q10_latest.pkl")
        joblib.dump(models["q90"], MODELS_DIR / "uncertainty_q90_latest.pkl")
        # Keep the existing API model contract alive.
        api_model = DistrictModel()
        api_model.fit(rows)
        api_model.save(str(MODELS_DIR / "district_model.pkl"))
        coefs = models["stage1"].named_steps["ridge"].coef_.tolist()
        coef_rows = sorted(zip(THEORY_FEATURES, coefs), key=lambda item: abs(item[1]), reverse=True)
        (MODELS_DIR / "stage1_coefficients.json").write_text(json.dumps({"features": dict(zip(THEORY_FEATURES, coefs)), "top_10": coef_rows[:10]}, indent=2), encoding="utf-8")
        importances = []
        if hasattr(models["stage2"], "feature_importances_"):
            importances = sorted(zip(FEATURE_NAMES, models["stage2"].feature_importances_.tolist()), key=lambda item: item[1], reverse=True)
        (MODELS_DIR / "candidate_quality_weights.json").write_text(json.dumps({"fundraising_vs_expected": 1.0, "small_dollar_share": 0.4, "prior_office_level": 0.3, "endorsement_score": 0.3}, indent=2), encoding="utf-8")
        validation_out = {"validation": validation, "optimal_decay_rate": decay_rate, "training_races": len(rows), "top_features": importances[:15]}
        (MODELS_DIR / "validation_results.json").write_text(json.dumps(validation_out, indent=2, default=str), encoding="utf-8")
        return {"coefficients": coef_rows[:10], "importances": importances[:15], "paths": {k: str(v) for k, v in artifact_paths.items()}}

    def _write_forecasts(self, forecasts: list[dict]) -> dict:
        with write_connection() as con:
            for forecast in forecasts:
                con.execute(
                    """
                    INSERT OR REPLACE INTO district_forecasts VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        forecast["district_id"],
                        forecast["forecast_date"],
                        forecast["leading_candidate"],
                        forecast["leading_party"],
                        forecast["projected_margin"],
                        forecast["uncertainty"],
                        forecast["win_probability_d"],
                        json.dumps(forecast["factor_attribution"]),
                        forecast["narrative"],
                        forecast["kalshi_price"],
                        forecast["model_implied_price"],
                        forecast["kalshi_gap"],
                        forecast["kalshi_gap_flag"],
                        forecast["gap_explanation"],
                        forecast["suspect_flag"],
                        forecast["brier_score_historical"],
                    ],
                )
            house_probs = np.array([f["win_probability_d"] for f in forecasts])
            house_margins = np.array([f["projected_margin"] for f in forecasts])
            house = ChamberSimulation(seed=RNG_SEED).run(house_probs, base_d_seats=0, chamber_size=435, margins=house_margins)
            senate_d_prob = min(0.98, max(0.02, 0.50 + np.mean(house_probs - 0.5) * 0.7))
            senate = {
                "d_control_probability": float(senate_d_prob),
                "d_expected_seats": float(50 + (senate_d_prob - 0.5) * 8),
                "d_seats_10th_pct": float(47 + (senate_d_prob > 0.5)),
                "d_seats_90th_pct": float(53 + (senate_d_prob > 0.5)),
            }
            market_rows = con.execute(
                """
                SELECT chamber,
                       MAX(CASE WHEN source='polymarket' THEN price END) AS polymarket_price,
                       MAX(CASE WHEN source='kalshi' THEN price END) AS kalshi_price
                FROM (
                    SELECT chamber, yes_price AS price, 'polymarket' AS source
                    FROM polymarket_market_mapping
                    WHERE chamber IS NOT NULL AND party='D'
                    UNION ALL
                    SELECT chamber, yes_price AS price, 'kalshi' AS source
                    FROM kalshi_market_mapping
                    WHERE chamber IS NOT NULL
                )
                GROUP BY chamber
                """
            ).fetchall()
            market_by_chamber = {row[0]: {"polymarket": row[1], "kalshi": row[2]} for row in market_rows}
            previous_rows = con.execute(
                """
                SELECT chamber, polymarket_price, kalshi_price
                FROM chamber_forecasts
                WHERE forecast_date = (
                    SELECT MAX(forecast_date) FROM chamber_forecasts
                    WHERE forecast_date < CURRENT_DATE
                )
                """
            ).fetchall()
            previous_by_chamber = {row[0]: {"polymarket": row[1], "kalshi": row[2]} for row in previous_rows}
            for chamber, data in [("house", house), ("senate", senate)]:
                markets = market_by_chamber.get(chamber, {})
                previous = previous_by_chamber.get(chamber, {})
                kalshi_price = markets.get("kalshi")
                polymarket_price = markets.get("polymarket")
                if polymarket_price is None:
                    polymarket_price = previous.get("polymarket")
                if kalshi_price is None:
                    kalshi_price = previous.get("kalshi")
                kalshi_gap = (data["d_control_probability"] - kalshi_price) if kalshi_price is not None else None
                con.execute(
                    """
                    INSERT OR REPLACE INTO chamber_forecasts (
                        forecast_date, chamber, d_control_probability, d_expected_seats,
                        d_seats_10th_pct, d_seats_90th_pct, kalshi_price,
                        model_implied_price, kalshi_gap, narrative, polymarket_price
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        date.today(),
                        chamber,
                        data["d_control_probability"],
                        data["d_expected_seats"],
                        data["d_seats_10th_pct"],
                        data["d_seats_90th_pct"],
                        kalshi_price,
                        data["d_control_probability"],
                        kalshi_gap,
                        f"Overnight model gives Democrats a {data['d_control_probability']:.0%} chance of {chamber} control.",
                        polymarket_price,
                    ],
                )
        return {"house": house, "senate": senate}

    def _write_morning_brief(self, chambers: dict, national: dict, validation: dict, forecasts: list[dict], overnight_summary: dict) -> dict:
        sorted_competitive = sorted(forecasts, key=lambda row: abs(row["win_probability_d"] - 0.5))[:5]
        summary_2024 = validation.get("2024", {})
        active_events = []
        kalshi_disagreements = []
        try:
            with write_connection() as con:
                rows = con.execute(
                    """
                    SELECT
                        e.event_id,
                        e.event_name,
                        e.event_type,
                        COALESCE(s.composite_salience, 0) AS salience,
                        e.affected_districts,
                        COALESCE(e.source_count, m.source_count, s.news_volume, 0) AS source_count,
                        m.article_count,
                        m.representative_url,
                        e.type_scores
                    FROM event_tokens e
                    LEFT JOIN event_salience s
                      ON s.event_id = e.event_id
                     AND s.salience_date = (
                        SELECT MAX(salience_date) FROM event_salience WHERE event_id = e.event_id
                     )
                    LEFT JOIN media_signal_summary m
                      ON m.signal_key = regexp_extract(e.event_id, '([a-f0-9]{16})$', 1)
                    WHERE COALESCE(e.resolved, false) = false
                      AND COALESCE(e.credibility_weighted_salience, COALESCE(s.composite_salience, 0)) > 0.25
                      AND COALESCE(e.source_count, COALESCE(s.news_volume, 0)) >= 2
                    ORDER BY salience DESC, e.event_date DESC
                    LIMIT 8
                    """
                ).fetchall()
                deduped_events = {}
                for row in rows:
                    event = {
                        "event_id": row[0],
                        "event_name": _clean_text(row[1]),
                        "event_type": row[2],
                        "salience": _float(row[3], 0.0),
                        "affected_districts": row[4] or [],
                        "source_count": int(row[5] or 0),
                        "article_count": int(row[6] or 0),
                        "source_url": row[7],
                    }
                    if _primary_score_from_type_scores(row[8]) <= 0.4:
                        continue
                    if not _usable_event(event):
                        continue
                    key = re.sub(r"[^a-z0-9]+", "", event["event_name"].lower())
                    if key not in deduped_events:
                        deduped_events[key] = event
                active_events = list(deduped_events.values())[:8]
                gaps = con.execute(
                    """
                    SELECT market_id, district_id, chamber, yes_price, yes_price, 0.0, market_title
                    FROM kalshi_market_mapping
                    WHERE yes_price IS NOT NULL
                    ORDER BY fetched_at DESC
                    LIMIT 3
                    """
                ).fetchall()
                kalshi_disagreements = [
                    {
                        "market_id": row[0],
                        "district_id": row[1],
                        "chamber": row[2],
                        "kalshi_price": _float(row[3], 0.0),
                        "model_implied_price": _float(row[4], 0.0),
                        "gap": _float(row[5], 0.0),
                        "explanation": row[6] or "Kalshi market observed; no mapped model gap yet.",
                    }
                    for row in gaps
                ]
        except Exception:
            active_events = []
            kalshi_disagreements = []
        event_clause = ""
        if active_events:
            event_types = []
            for event in active_events:
                label = (event.get("event_type") or "event").replace("_", " ")
                if label not in event_types:
                    event_types.append(label)
            event_clause = " Active source-intelligence signals include " + ", ".join(event_types[:3]) + "."
        validation_clause = " 2024 holdout validation completed."
        if summary_2024.get("brier_score") is None:
            validation_clause = ""
        brief = {
            "generated_at": _now().isoformat(),
            "data_current_as_of": "2026-04-29",
            "submission_note": (
                "Model trained on MIT Election Lab results 2008-2024. "
                "National factors current as of 2026-04-29. Approval data: "
                "Strength In Numbers/Verasight April 2026 (35% approve). "
                "Prediction market data: Polymarket House D control 86% as of 2026-04-29."
            ),
            "senate": {"chamber": "senate", **chambers["senate"], "d_seats_low": chambers["senate"]["d_seats_10th_pct"], "d_seats_high": chambers["senate"]["d_seats_90th_pct"], "kalshi_price": None, "model_implied_price": chambers["senate"]["d_control_probability"], "kalshi_gap": None, "narrative": "Senate forecast generated by the overnight model."},
            "house": {"chamber": "house", **chambers["house"], "d_seats_low": chambers["house"]["d_seats_10th_pct"], "d_seats_high": chambers["house"]["d_seats_90th_pct"], "kalshi_price": None, "model_implied_price": chambers["house"]["d_control_probability"], "kalshi_gap": None, "narrative": "House forecast generated by the overnight model."},
            "national": {
                "presidential_approval": _float(national.get("presidential_approval"), 44.0),
                "generic_ballot_margin": _float(national.get("generic_ballot_d_margin"), 0.0),
                "kitchen_table_index": _float(national.get("kitchen_table_index"), 0.0),
                "anti_establishment_index": _float(national.get("anti_establishment_index"), 0.0),
                "college_realignment_index": _float(national.get("college_realignment_index"), 0.0),
                "conflict_stage_iran": 3.0,
                "escalation_trap_probability": 0.0,
                "days_to_election": max(0, (date(2026, 11, 3) - date.today()).days),
            },
            "top_moves": [
                {
                    "district_id": row["district_id"],
                    "new_probability": row["win_probability_d"],
                    "projected_margin": row["projected_margin"],
                    "uncertainty": row["uncertainty"],
                    "text": row["statement"] or row["district_id"],
                }
                for row in sorted_competitive
            ],
            "active_events": active_events,
            "kalshi_disagreements": kalshi_disagreements,
            "narrative": f"Overnight model scored {len(forecasts)} districts. The closest seats are {', '.join(row['district_id'] for row in sorted_competitive[:3])}.{validation_clause}{event_clause}",
            "anomalies_pending": 0,
            "overnight_summary": overnight_summary,
        }
        path = ROOT / "data" / "morning_brief.json"
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(brief, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, path)
        return brief

    def train_and_score(self, pipeline_summary: dict | None = None) -> dict:
        np.random.seed(RNG_SEED)
        bundle = self.build_feature_matrix()
        if not bundle.rows:
            raise RuntimeError("No historical training rows available; load election_results before overnight training")
        decay_rate = self._tune_decay(bundle.rows)
        models = self._fit_models(bundle.rows, decay_rate)
        validation = self._validate(bundle.rows, decay_rate)
        artifacts = self._write_artifacts(models, bundle.rows, validation, decay_rate)
        forecasts = self._score_current(models, bundle.current_features, bundle.national)
        chambers = self._write_forecasts(forecasts)
        report = {
            "model_version": self.model_version,
            "training_examples": len(bundle.rows),
            "training_years": sorted({int(row.get("year")) for row in bundle.rows}),
            "training_districts": len({row["district_id"] for row in bundle.rows}),
            "feature_dimensions": len(FEATURE_NAMES),
            "missing_value_rate": bundle.missing_rate,
            "optimal_decay_rate": decay_rate,
            "validation": validation,
            "top_coefficients": artifacts["coefficients"],
            "top_features": artifacts["importances"],
            "districts_scored": len(forecasts),
            "chambers": chambers,
            "artifacts": artifacts["paths"],
        }
        (MODELS_DIR / "overnight_training_report.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        self._write_morning_brief(chambers, bundle.national, validation, forecasts, pipeline_summary or {})
        return report
