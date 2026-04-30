from __future__ import annotations

import statistics

from db.connection import get_read_connection


def main() -> int:
    con = get_read_connection()
    print("SPECIAL ELECTION PREDICTIVENESS ANALYSIS")
    print("=" * 60)
    for general_year in [2018, 2020, 2022, 2024, 2026]:
        specials = con.execute(
            """
            SELECT district_id, election_date, swing_from_baseline,
                   national_environment_signal, notes
            FROM special_elections
            WHERE election_date >= DATE(? || '-11-01') - INTERVAL 365 DAYS
              AND election_date < DATE(? || '-11-01')
            ORDER BY election_date
            """,
            [general_year, general_year],
        ).fetchall()
        print(f"\n{general_year} GENERAL ELECTION")
        if not specials:
            print("  No specials found in prior 12 months")
            continue
        signals = [float(row[3]) for row in specials if row[3] is not None]
        avg_signal = statistics.mean(signals) if signals else 0.0
        print(f"  Specials in prior 12 months: {len(specials)}")
        for district_id, election_date, swing, _signal, _notes in specials:
            print(f"    {election_date} {district_id}: swing {float(swing):+.1f}pts")
        print(f"  Average special signal: {avg_signal:+.2f}pts")
        print(f"  Interpretation: {'BULLISH D' if avg_signal > 3 else 'BULLISH R' if avg_signal < -3 else 'NEUTRAL'}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
