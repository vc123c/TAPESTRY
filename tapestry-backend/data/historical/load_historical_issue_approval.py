from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime

from db.connection import init_db, write_connection


HISTORICAL_ISSUE_APPROVALS = {
    2010: {"overall": 0.45, "economy": 0.37, "healthcare": 0.40, "immigration": 0.33, "foreign_affairs": 0.45, "president_party": "D", "source": "Gallup October 2010"},
    2014: {"overall": 0.40, "economy": 0.40, "healthcare": 0.38, "immigration": 0.31, "foreign_affairs": 0.36, "president_party": "D", "source": "Gallup October 2014"},
    2018: {"overall": 0.40, "economy": 0.51, "healthcare": 0.38, "immigration": 0.40, "foreign_affairs": 0.35, "president_party": "R", "source": "Gallup November 2018"},
    2022: {"overall": 0.41, "economy": 0.33, "healthcare": 0.40, "immigration": 0.29, "foreign_affairs": 0.40, "president_party": "D", "source": "Gallup October 2022"},
}

CURRENT_ISSUE_APPROVALS = {
    "economy": 0.31,
    "inflation": 0.26,
    "immigration": 0.44,
    "iran_war": 0.32,
    "gas_prices": 0.24,
    "healthcare": 0.30,
    "crime": 0.38,
    "tariffs": 0.29,
}


def _record_id(*parts: object) -> str:
    return hashlib.sha1(":".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def run() -> int:
    init_db()
    rows = []
    now = datetime.now(UTC)
    for year, data in HISTORICAL_ISSUE_APPROVALS.items():
        poll_date = date(year, 10, 15)
        for issue, approve in data.items():
            if issue in {"source", "president_party"}:
                continue
            disapprove = 1.0 - float(approve)
            rows.append([
                _record_id(year, issue, data["source"]),
                issue,
                data["source"],
                poll_date,
                float(approve),
                disapprove,
                float(approve) - disapprove,
                "adults",
                "Gallup historical issue approval",
                now,
            ])
    for issue, approve in CURRENT_ISSUE_APPROVALS.items():
        disapprove = 1.0 - float(approve)
        rows.append([
            _record_id("current", issue, date.today()),
            issue,
            "current_seed",
            date.today(),
            float(approve),
            disapprove,
            float(approve) - disapprove,
            "adults",
            "seeded current issue approval pending live scrape",
            now,
        ])

    with write_connection() as con:
        for row in rows:
            con.execute(
                """
                INSERT OR REPLACE INTO issue_approval VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        con.execute("DELETE FROM issue_approval_averages")
        con.execute(
            """
            INSERT INTO issue_approval_averages
            SELECT
                issue_key,
                AVG(approve_pct) AS approve_pct,
                AVG(disapprove_pct) AS disapprove_pct,
                AVG(net_approval) AS net_approval,
                COUNT(*) AS source_count,
                CURRENT_TIMESTAMP AS updated_at
            FROM issue_approval
            WHERE poll_date >= CURRENT_DATE - INTERVAL 45 DAYS
            GROUP BY issue_key
            """
        )
        overall = con.execute(
            "SELECT presidential_approval FROM national_factors ORDER BY factor_date DESC LIMIT 1"
        ).fetchone()
        overall_approval = float(overall[0]) if overall and overall[0] is not None else 0.37
        con.execute(
            """
            UPDATE national_factors
            SET economy_approval = ?,
                inflation_approval = ?,
                immigration_approval = ?,
                iran_war_approval = ?,
                healthcare_approval = ?,
                crime_approval = ?,
                tariffs_approval = ?,
                gas_prices_approval = ?,
                economy_approval_gap = ?,
                immigration_approval_gap = ?
            WHERE factor_date = (SELECT MAX(factor_date) FROM national_factors)
            """,
            [
                CURRENT_ISSUE_APPROVALS["economy"],
                CURRENT_ISSUE_APPROVALS["inflation"],
                CURRENT_ISSUE_APPROVALS["immigration"],
                CURRENT_ISSUE_APPROVALS["iran_war"],
                CURRENT_ISSUE_APPROVALS["healthcare"],
                CURRENT_ISSUE_APPROVALS["crime"],
                CURRENT_ISSUE_APPROVALS["tariffs"],
                CURRENT_ISSUE_APPROVALS["gas_prices"],
                CURRENT_ISSUE_APPROVALS["economy"] - overall_approval,
                CURRENT_ISSUE_APPROVALS["immigration"] - overall_approval,
            ],
        )
    print(f"Loaded issue approval records: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
