from __future__ import annotations

import json
import math
from pathlib import Path

import joblib
import numpy as np
from sklearn.linear_model import Ridge
from sklearn.preprocessing import StandardScaler

try:
    from xgboost import XGBRegressor
except Exception:
    XGBRegressor = None

from model.candidate_quality import CandidateQualityScorer
from model.conflict_model import PapeEscalationHMM
from model.turnout_model import TurnoutModel


def _approval_percent(value: object, default: float = 44.0) -> float:
    try:
        approval = float(value if value is not None else default)
    except Exception:
        approval = default
    if 0.0 <= approval <= 1.0:
        return approval * 100.0
    return approval


class DistrictModel:
    feature_names = [
        "cook_pvi", "generic_ballot_d_margin", "presidential_approval", "kitchen_table_index",
        "fundraising_ratio", "college_educated_pct", "ai_automation_exposure", "independent_media_penetration",
        "unemployment_vs_national", "income_growth_2yr",
    ]

    def __init__(self) -> None:
        self.scaler = StandardScaler()
        self.stage1 = Ridge(alpha=2.0, random_state=42)
        self.stage2 = XGBRegressor(max_depth=4, n_estimators=200, subsample=0.8, random_state=42) if XGBRegressor else None
        self.turnout = TurnoutModel()
        self.quality = CandidateQualityScorer()
        self.conflict = PapeEscalationHMM()
        self.fitted = False

    def stage1_margin(self, row: dict, national: dict) -> tuple[float, dict]:
        pvi = float(row.get("cook_pvi", 0))
        generic = float(national.get("generic_ballot_d_margin", 0))
        approval_penalty = (50 - _approval_percent(national.get("presidential_approval", 44))) * -0.08
        incumbent = 3.2 if row.get("incumbent_party") == "D" else -3.2
        kitchen = -1.5 * float(national.get("kitchen_table_index", 0.6)) * (1 if row.get("incumbent_party") == "D" else -1)
        conflict_effect = self.conflict.district_conflict_effect(row, self.conflict.infer({}))
        anti = -0.04 * float(row.get("independent_media_penetration", 50)) * (1 if row.get("incumbent_party") == "D" else -1)
        college = float(row.get("college_educated_pct", 35)) * float(national.get("college_realignment_index", 0.7)) * 0.035
        fundraising = math.log(max(float(row.get("fundraising_ratio", 1)), 0.05)) * 1.4
        candidate_quality = self.quality.score({"fundraising_vs_expected": row.get("fundraising_ratio", 1.0)}) - 0.5
        turnout = (self.turnout.predict(row)["likely_voter_composition"]["D_share"] - 0.5) * 8
        ai = -0.02 * float(row.get("ai_automation_exposure", 50)) * (1 - float(row.get("college_educated_pct", 35)) / 100)
        interactions = -0.35 * float(national.get("kitchen_table_index", 0.6)) * float(national.get("anti_establishment_index", 0.5))
        margin = pvi + generic + approval_penalty + incumbent + kitchen + conflict_effect + anti + college + fundraising + candidate_quality + turnout + ai + interactions
        attribution = {
            "cook_pvi_baseline": pvi, "generic_ballot": generic, "presidential_approval": approval_penalty,
            "incumbency_advantage": incumbent, "kitchen_table_index": kitchen, "conflict_effect": conflict_effect,
            "independent_media": anti, "college_realignment": college, "fundraising_ratio": fundraising,
            "candidate_quality": candidate_quality, "turnout_adjustment": turnout, "ai_exposure": ai,
        }
        return float(margin), attribution

    def predict_one(self, row: dict, national: dict) -> dict:
        margin, attribution = self.stage1_margin(row, national)
        residual = 0.0
        if self.fitted and self.stage2:
            x = np.array([[float(row.get(name, national.get(name, 0))) for name in self.feature_names]])
            residual = float(self.stage2.predict(x)[0])
        final_margin = margin + residual
        uncertainty = 3.0 + min(4.0, abs(float(row.get("cook_pvi", 0))) * 0.18)
        prob_d = 1 / (1 + math.exp(-final_margin / 5.5))
        return {
            "projected_margin": final_margin, "uncertainty": uncertainty, "win_probability_d": prob_d,
            "factor_attribution": attribution, "model_implied_price": prob_d,
        }

    def fit(self, training_rows: list[dict]) -> None:
        if not training_rows:
            self.fitted = False
            return
        x = np.array([[float(row.get(name, 0)) for name in self.feature_names] for row in training_rows])
        y = np.array([float(row["margin"]) for row in training_rows])
        weights = np.array([0.85 ** ((2024 - int(row.get("year", 2024))) / 2) for row in training_rows])
        xs = self.scaler.fit_transform(x)
        self.stage1.fit(xs, y, sample_weight=weights)
        residuals = y - self.stage1.predict(xs)
        if self.stage2:
            self.stage2.fit(x, residuals, sample_weight=weights)
        self.fitted = True

    def save(self, path: str = "data/models/district_model.pkl") -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        joblib.dump({"model": self, "feature_names": self.feature_names}, path)

    @classmethod
    def load(cls, path: str = "data/models/district_model.pkl") -> "DistrictModel":
        if Path(path).exists():
            return joblib.load(path)["model"]
        return cls()
