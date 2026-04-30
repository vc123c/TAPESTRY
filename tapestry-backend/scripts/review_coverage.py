from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.connection import get_read_connection


def scalar(con, sql: str, params: list[object] | None = None) -> object:
    try:
        row = con.execute(sql, params or []).fetchone()
        return row[0] if row else None
    except Exception as exc:
        return f"ERROR: {type(exc).__name__}: {exc}"


def main() -> None:
    with get_read_connection() as con:
        print("TAPESTRY COVERAGE REVIEW")
        print("========================")
        print(f"house_roster rows: {scalar(con, 'SELECT COUNT(*) FROM house_roster')}")
        print(f"candidate_roster_2026 rows: {scalar(con, 'SELECT COUNT(*) FROM candidate_roster_2026')}")
        print(f"district_forecasts rows: {scalar(con, 'SELECT COUNT(*) FROM district_forecasts')}")
        print(f"race_web_articles rows: {scalar(con, 'SELECT COUNT(*) FROM race_web_articles')}")
        print(f"districts with race articles: {scalar(con, 'SELECT COUNT(DISTINCT district_id) FROM race_web_articles')}")
        print(f"local_news rows: {scalar(con, 'SELECT COUNT(*) FROM local_news')}")
        print(f"districts with local news: {scalar(con, 'SELECT COUNT(DISTINCT district_id) FROM local_news')}")
        print(f"media_event_articles rows: {scalar(con, 'SELECT COUNT(*) FROM media_event_articles')}")
        print(f"event_tokens rows: {scalar(con, 'SELECT COUNT(*) FROM event_tokens')}")
        print(
            "events above threshold: "
            f"{scalar(con, '''SELECT COUNT(*) FROM event_tokens WHERE credibility_weighted_salience > 0.25 AND COALESCE(source_count, 1) >= 2''')}"
        )
        print(f"events with affected_states: {scalar(con, 'SELECT COUNT(*) FROM event_tokens WHERE affected_states IS NOT NULL')}")
        print(f"national signals: {scalar(con, 'SELECT COUNT(*) FROM event_tokens WHERE is_national_signal = TRUE')}")
        print()
        print("FEATURE COVERAGE")
        print("----------------")
        for col in ["gini_coefficient", "healthcare_cost_burden", "data_center_mw_planned", "incumbent_years"]:
            print(f"{col} nulls: {scalar(con, f'SELECT COUNT(*) FROM district_features WHERE {col} IS NULL')}")
            print(f"{col} distinct non-null: {scalar(con, f'SELECT COUNT(DISTINCT {col}) FROM district_features WHERE {col} IS NOT NULL')}")
        print()
        print("SANITY CANDIDATES")
        print("-----------------")
        for district_id in ["AZ-06", "CA-30"]:
            row = con.execute(
                """
                SELECT district_id, incumbent_name, incumbent_party
                FROM house_roster
                WHERE district_id = ?
                """,
                [district_id],
            ).fetchone()
            print(f"{district_id}: {row}")
        print()
        print("MODEL REPORT")
        print("------------")
        report_path = ROOT / "data" / "models" / "overnight_training_report.json"
        if report_path.exists():
            data = json.loads(report_path.read_text(encoding="utf-8"))
            print(f"model_version: {data.get('model_version')}")
            print(f"training_examples: {data.get('training_examples')}")
            print(f"missing_value_rate: {data.get('missing_value_rate')}")
            print("top_features:")
            for name, value in (data.get("top_features") or [])[:15]:
                print(f"  {name}: {value:.4f}")
        else:
            print("No overnight_training_report.json found")


if __name__ == "__main__":
    main()
