from __future__ import annotations

from datetime import date

from model.features import build_national_factors


def _approval_percent(value: object, default: float = 44.0) -> float:
    try:
        approval = float(value if value is not None else default)
    except Exception:
        approval = default
    if 0.0 <= approval <= 1.0:
        return approval * 100.0
    return approval


class NationalEnvironmentModel:
    def score(self, factor_date: date | None = None) -> dict:
        factors = build_national_factors(factor_date)
        approval = _approval_percent(factors.get("presidential_approval", 44.0))
        factors["national_environment_index"] = (
            factors["generic_ballot_d_margin"] * 0.25
            - (50 - approval) * 0.08
            - factors["kitchen_table_index"] * 1.5
        )
        return factors
