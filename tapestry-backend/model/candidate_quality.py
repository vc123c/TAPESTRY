from __future__ import annotations

import numpy as np
from sklearn.linear_model import Lasso


class CandidateQualityScorer:
    def __init__(self) -> None:
        self.weights = np.ones(6) / 6

    def score(self, features: dict) -> float:
        values = np.array([
            features.get("fundraising_vs_expected", 1.0),
            features.get("small_dollar_pct", 0.25),
            features.get("prior_office_level", 1) / 4,
            features.get("endorsement_score", 1) / 10,
            features.get("earned_media_ratio", 1.0),
            -features.get("scandal_severity", 0) / 5,
        ], dtype=float)
        return float(np.clip(np.dot(values, self.weights), -1, 2))

    def self_retrain(self, election_results) -> None:
        x = np.asarray(election_results.get("X", np.eye(6)), dtype=float)
        y = np.asarray(election_results.get("y", np.zeros(x.shape[0])), dtype=float)
        model = Lasso(alpha=0.05, random_state=42).fit(x, y)
        coef = np.abs(model.coef_)
        self.weights = coef / coef.sum() if coef.sum() else np.ones(6) / 6
