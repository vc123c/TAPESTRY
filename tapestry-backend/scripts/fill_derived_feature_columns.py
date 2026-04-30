from __future__ import annotations

from db.connection import init_db, write_connection


def main() -> int:
    init_db()
    with write_connection() as con:
        con.execute(
            """
            UPDATE district_features AS df
            SET
                reg_d_advantage = vr.d_share - vr.r_share,
                reg_d_r_ratio = vr.d_r_ratio,
                reg_d_trend_90d = COALESCE(vr.d_registration_trend, 0.0)
            FROM voter_registration vr
            WHERE LEFT(df.district_id, 2) = vr.state_abbr
              AND vr.report_date = (
                  SELECT MAX(report_date)
                  FROM voter_registration
                  WHERE state_abbr = vr.state_abbr
              )
            """
        )
        averages = con.execute(
            """
            SELECT issue_key, net_approval
            FROM issue_approval_averages
            """
        ).fetchall()
        issue = {key: float(value) for key, value in averages if value is not None}
        weighted = (
            0.25 * issue.get("economy", 0.0)
            + 0.25 * issue.get("inflation", 0.0)
            + 0.10 * issue.get("immigration", 0.0)
            + 0.10 * issue.get("iran_war", 0.0)
            + 0.10 * issue.get("healthcare", 0.0)
            + 0.10 * issue.get("gas_prices", 0.0)
            + 0.05 * issue.get("crime", 0.0)
            + 0.05 * issue.get("tariffs", 0.0)
        )
        con.execute(
            """
            UPDATE district_features
            SET weighted_issue_approval = ?,
                immigration_approval_relevance = COALESCE(immigration_approval_relevance, 0.10),
                economy_approval_relevance = COALESCE(economy_approval_relevance, 0.25),
                iran_war_approval_relevance = COALESCE(iran_war_approval_relevance, 0.10)
            """,
            [weighted],
        )
        counts = con.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE reg_d_advantage IS NOT NULL),
                COUNT(*) FILTER (WHERE weighted_issue_approval IS NOT NULL)
            FROM district_features
            """
        ).fetchone()
    print(f"Derived district feature columns populated: reg={counts[0]}, weighted_issue={counts[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
