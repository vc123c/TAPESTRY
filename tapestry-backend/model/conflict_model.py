from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import numpy as np


@dataclass
class ConflictState:
    conflict_id: str
    current_stage: float
    stage_probabilities: dict[int, float]
    escalation_trap_probability: float
    domestic_political_loadings: dict[str, float]
    latest_signal: str
    days_in_conflict: int


class PapeEscalationHMM:
    def __init__(self, seed: int = 42) -> None:
        np.random.seed(seed)
        self.transition_matrix = np.array([
            [0.72, 0.23, 0.04, 0.01, 0.00],
            [0.12, 0.60, 0.22, 0.05, 0.01],
            [0.04, 0.12, 0.55, 0.24, 0.05],
            [0.02, 0.04, 0.14, 0.55, 0.25],
            [0.00, 0.00, 0.02, 0.08, 0.90],
        ])

    def infer(self, observations: dict | None = None, conflict_id: str = "iran_2026") -> ConflictState:
        observations = observations or {}
        oil = float(observations.get("oil_price_change_7d", 0.08))
        pape_signal = float(observations.get("pape_stage_signal", 0.64))
        hormuz = float(observations.get("hormuz_shipping_disruption", 0.32))
        raw = np.array([0.02, 0.08, 0.48, 0.30, 0.12])
        raw[3] += max(0, oil) + hormuz * 0.15 + pape_signal * 0.1
        probs = raw / raw.sum()
        current_stage = float(np.dot(np.arange(1, 6), probs))
        return ConflictState(
            conflict_id=conflict_id,
            current_stage=current_stage,
            stage_probabilities={i + 1: float(p) for i, p in enumerate(probs)},
            escalation_trap_probability=float(probs[3] + probs[4] * 0.8),
            domestic_political_loadings={"military_heavy": -0.8, "anti_war_suburban": 1.4, "rural_non_military": -0.3, "urban": 0.9},
            latest_signal="Tanker seizures in Hormuz - Stage 3 to 4 transition signal",
            days_in_conflict=int(observations.get("days_in_conflict", 56)),
        )

    def district_conflict_effect(self, district_features: dict, state: ConflictState) -> float:
        density = str(district_features.get("urban_rural_class", "suburban"))
        exposure = float(district_features.get("manufacturing_share", 10)) / 100
        base = state.escalation_trap_probability
        if density == "urban":
            return base * 0.9
        if density == "rural":
            return -base * (0.3 + exposure)
        return base * 1.1
