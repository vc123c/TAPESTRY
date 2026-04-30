from __future__ import annotations


class SenateModel:
    def __init__(self, national_weight: float = 0.55) -> None:
        self.national_weight = national_weight

    def predict(self, state_features: dict, national: dict) -> dict:
        national_component = (
            national.get("generic_ballot_d_margin", 0) * 0.8
            + (national.get("presidential_approval", 44) - 45) * 0.12
            - national.get("kitchen_table_index", 0.6) * 1.4
        )
        state_component = (
            state_features.get("state_cook_pvi", 0)
            - state_features.get("state_unemployment", 4.2) * 0.2
            + state_features.get("candidate_quality_differential", 0) * 1.5
            + state_features.get("incumbent_state_approval", 47) * 0.04
        )
        margin = self.national_weight * national_component + (1 - self.national_weight) * state_component
        return {"projected_margin": margin, "win_probability_d": 1 / (1 + pow(2.71828, -margin / 5.5))}
