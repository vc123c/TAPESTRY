from __future__ import annotations

from sklearn.ensemble import GradientBoostingRegressor


class TurnoutModel:
    def __init__(self) -> None:
        self.midterm_model = GradientBoostingRegressor(random_state=42)
        self.presidential_model = GradientBoostingRegressor(random_state=42)

    def predict(self, features: dict, cycle_type: str = "midterm") -> dict:
        base = 0.46 if cycle_type == "midterm" else 0.64
        age = float(features.get("median_age", 40))
        salience = 0.03 if features.get("abortion_measure") or features.get("min_wage_measure") else 0.0
        turnout = max(0.25, min(0.78, base + (age - 40) * 0.002 + salience))
        d_share = max(0.25, min(0.65, 0.48 + float(features.get("cook_pvi", 0)) / 100))
        return {"expected_turnout_pct": turnout, "likely_voter_composition": {"D_share": d_share, "R_share": 1 - d_share - 0.06, "ind_share": 0.06}}
