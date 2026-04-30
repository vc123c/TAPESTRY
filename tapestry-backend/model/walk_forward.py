from __future__ import annotations

from datetime import date

from db.connection import write_connection
from model.overnight_trainer import OvernightTrainer


class WalkForwardValidator:
    test_years = [2020, 2022, 2024]

    def run(self) -> dict:
        trainer = OvernightTrainer(model_version=f"walk_forward_{date.today().isoformat()}")
        bundle = trainer.build_feature_matrix()
        decay_rate = trainer._tune_decay(bundle.rows)
        validation = trainer._validate(bundle.rows, decay_rate)
        result = {
            "optimal_decay_rate": decay_rate,
            "calibration_curves": {},
        }
        with write_connection() as con:
            for year in self.test_years:
                row = validation.get(str(year), {})
                brier = row.get("brier_score")
                baseline = row.get("baseline_brier")
                improvement = None if brier is None or baseline is None else baseline - brier
                result[f"tapestry_brier_{year}"] = brier
                result[f"competitive_brier_{year}"] = row.get("competitive_brier")
                result[f"baseline_brier_{year}"] = baseline
                result[f"competitive_baseline_brier_{year}"] = row.get("competitive_baseline_brier")
                result[f"improvement_{year}"] = improvement
                result[f"n_races_{year}"] = row.get("n_races", 0)
                result[f"competitive_n_races_{year}"] = row.get("competitive_n_races", 0)
                result[f"safe_races_{year}"] = row.get("safe_races", 0)
                result[f"leakage_diagnostic_{year}"] = row.get("leakage_diagnostic", {})
                result["calibration_curves"][str(year)] = row.get("calibration", {})
                con.execute(
                    "INSERT OR REPLACE INTO model_performance VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        date.today(),
                        f"strict_pre_{year}",
                        year,
                        brier,
                        baseline,
                        improvement,
                        row.get("n_races", 0),
                        row.get("calibration", {}),
                    ],
                )
        print("CORRECTED WALK-FORWARD BRIER SCORES (no leakage):")
        for year in self.test_years:
            print(
                f" {year}: {result.get(f'tapestry_brier_{year}')} "
                f"(competitive races only: {result.get(f'competitive_brier_{year}')}; "
                f"n={result.get(f'competitive_n_races_{year}')}, safe={result.get(f'safe_races_{year}')})"
            )
        return result
