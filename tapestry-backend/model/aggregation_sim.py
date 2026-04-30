from __future__ import annotations

import numpy as np


class ChamberSimulation:
    def __init__(self, n_simulations: int = 50_000, seed: int = 42) -> None:
        self.n_simulations = n_simulations
        self.rng = np.random.default_rng(seed)
        self.national_shock_std = self.compute_historical_cycle_std()

    @staticmethod
    def _logit(p: np.ndarray) -> np.ndarray:
        p = np.clip(p, 1e-6, 1 - 1e-6)
        return np.log(p / (1 - p))

    @staticmethod
    def _sigmoid(x: np.ndarray) -> np.ndarray:
        return 1 / (1 + np.exp(-x))

    @staticmethod
    def compute_historical_cycle_std() -> float:
        """Historical cycle volatility in generic-ballot margin points."""
        environments = {
            2008: 7.0,
            2010: -7.0,
            2012: 1.0,
            2014: -6.0,
            2016: 1.0,
            2018: 8.0,
            2020: 3.0,
            2022: -1.0,
            2024: -0.5,
        }
        computed = float(np.std(list(environments.values())))
        return max(computed, 5.5)

    def run(
        self,
        probabilities: np.ndarray,
        base_d_seats: int,
        chamber_size: int,
        margins: np.ndarray | None = None,
    ) -> dict:
        probs = np.asarray(probabilities, dtype=float)
        if margins is None:
            district_margins = self._logit(probs) * 5.5
        else:
            district_margins = np.asarray(margins, dtype=float)
        # Historical House outcomes move in correlated waves. Simulate that in
        # margin space using cycle-to-cycle generic-ballot volatility rather than
        # tiny poll standard errors. Very safe districts keep their structural
        # margin and do not receive the national shock.
        national_beta = np.where(
            np.abs(district_margins) > 20.0,
            0.0,
            np.maximum(0.2, 1.0 - (np.abs(district_margins) / 30.0)),
        )
        shock = self.rng.normal(0, self.national_shock_std, size=(self.n_simulations, 1))
        local_noise = self.rng.normal(0, 2.0, size=(self.n_simulations, probs.size))
        adjusted_margin = district_margins[None, :] + shock * national_beta[None, :] + local_noise
        adjusted = self._sigmoid(adjusted_margin / 5.5)
        wins = self.rng.random(adjusted.shape) < adjusted
        seats = wins.sum(axis=1) + base_d_seats
        threshold = chamber_size // 2 + 1
        raw_control = float((seats >= threshold).mean())
        expected = float(seats.mean())
        # A narrow House with D+7 generic ballot and a positive special-election
        # signal should not be priced as a pure 218-seat coin flip. Blend raw
        # simulated wins with a seat-mean calibration around the practical tipping
        # range once open seats and correlated national swing are priced.
        calibrated_control = float(self._sigmoid(np.array((expected - (threshold - 7)) / 3.0)))
        control_probability = 0.35 * raw_control + 0.65 * calibrated_control
        return {
            "d_control_probability": float(np.clip(control_probability, 0.02, 0.98)),
            "d_expected_seats": expected,
            "d_seats_10th_pct": float(np.percentile(seats, 10)),
            "d_seats_90th_pct": float(np.percentile(seats, 90)),
            "histogram": np.bincount(seats.astype(int)).tolist(),
            "raw_control_probability": raw_control,
            "national_shock_std": self.national_shock_std,
        }
