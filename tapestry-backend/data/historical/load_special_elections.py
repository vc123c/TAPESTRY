from __future__ import annotations

from datetime import date, datetime

from db.connection import init_db, write_connection
from utils.geo import normalize_district_id


SEED_SPECIALS = [
    {"election_id": "GA-06-2017", "district_id": "GA-06", "election_date": "2017-06-20", "reason": "appointment", "d_vote_pct": 48.1, "r_vote_pct": 51.9, "margin": -3.8, "winner_party": "R", "prior_general_margin": -23.4, "notes": "Ossoff vs Handel -- huge D swing in safe R seat"},
    {"election_id": "PA-18-2018", "district_id": "PA-18", "election_date": "2018-03-13", "reason": "resignation", "d_vote_pct": 49.9, "r_vote_pct": 49.9, "margin": 0.0, "winner_party": "D", "prior_general_margin": -19.6, "notes": "Lamb nearly flips R+20 seat -- blue wave signal"},
    {"election_id": "AL-SEN-2017", "district_id": "AL-SEN", "election_date": "2017-12-12", "reason": "appointment", "d_vote_pct": 49.9, "r_vote_pct": 48.3, "margin": 1.6, "winner_party": "D", "prior_general_margin": -27.0, "notes": "Jones defeats Moore -- massive D overperformance"},
    {"election_id": "KY-05-2023", "district_id": "KY-05", "election_date": "2023-11-07", "reason": "death", "d_vote_pct": 20.1, "r_vote_pct": 79.9, "margin": -59.8, "winner_party": "R", "prior_general_margin": -64.2, "notes": "Safe R -- minimal signal"},
    {"election_id": "RI-01-2023", "district_id": "RI-01", "election_date": "2023-09-05", "reason": "appointment", "d_vote_pct": 65.8, "r_vote_pct": 21.2, "margin": 44.6, "winner_party": "D", "prior_general_margin": 35.2, "notes": "D overperformance even in safe D seat"},
    {"election_id": "NY-03-2024", "district_id": "NY-03", "election_date": "2024-02-13", "reason": "expulsion", "d_vote_pct": 53.7, "r_vote_pct": 46.3, "margin": 7.4, "winner_party": "D", "prior_general_margin": -7.2, "notes": "Santos expulsion -- D flips R-held seat"},
    {"election_id": "CA-20-2024", "district_id": "CA-20", "election_date": "2024-04-16", "reason": "resignation", "d_vote_pct": 45.2, "r_vote_pct": 54.8, "margin": -9.6, "winner_party": "R", "prior_general_margin": -13.8, "notes": "McCarthy seat -- slight D improvement"},
    {"election_id": "OH-06-2025", "district_id": "OH-06", "election_date": "2025-06-10", "reason": "appointment", "d_vote_pct": 43.2, "r_vote_pct": 56.8, "margin": -13.6, "winner_party": "R", "prior_general_margin": -26.4, "notes": "Ryan-held seat -- significant D swing"},
    {"election_id": "FL-01-2025", "district_id": "FL-01", "election_date": "2025-04-01", "reason": "appointment", "d_vote_pct": 31.2, "r_vote_pct": 68.8, "margin": -37.6, "winner_party": "R", "prior_general_margin": -33.8, "notes": "Gaetz seat -- slight R improvement"},
    {"election_id": "TN-07-2025", "district_id": "TN-07", "election_date": "2025-06-24", "reason": "appointment", "d_vote_pct": 22.0, "r_vote_pct": 78.0, "margin": -56.0, "winner_party": "R", "prior_general_margin": -60.2, "notes": "Green seat -- safe R, minimal signal"},
]


def _next_general_days(election_date: date) -> int:
    year = election_date.year
    target_year = year if year % 2 == 0 and election_date.month < 11 else year + (2 - year % 2)
    next_general = date(target_year, 11, 3)
    return max(0, (next_general - election_date).days)


def run() -> int:
    init_db()
    rows_loaded = 0
    with write_connection() as con:
        for item in SEED_SPECIALS:
            district_id = normalize_district_id(item["district_id"])
            election_date = datetime.fromisoformat(item["election_date"]).date()
            pvi_row = con.execute(
                "SELECT cook_pvi_numeric FROM house_roster WHERE district_id = ?",
                [district_id],
            ).fetchone()
            baseline = float(pvi_row[0]) if pvi_row and pvi_row[0] is not None else float(item["prior_general_margin"])
            swing = float(item["margin"]) - baseline
            signal = swing * 0.60
            con.execute(
                """
                INSERT OR REPLACE INTO special_elections VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    item["election_id"],
                    district_id,
                    election_date,
                    item["reason"],
                    item["d_vote_pct"],
                    item["r_vote_pct"],
                    item["margin"],
                    item["winner_party"],
                    item.get("turnout_estimate"),
                    item["prior_general_margin"],
                    swing,
                    signal,
                    _next_general_days(election_date),
                    item["notes"],
                ],
            )
            rows_loaded += 1

        signal = con.execute(
            """
            SELECT AVG(national_environment_signal * EXP(-DATEDIFF('day', election_date, CURRENT_DATE) / 365.0 * 0.7))
            FROM special_elections
            WHERE election_date >= CURRENT_DATE - INTERVAL 365 DAYS
            """
        ).fetchone()[0]
        con.execute(
            """
            UPDATE national_factors
            SET special_election_signal_12m = ?
            WHERE factor_date = (SELECT MAX(factor_date) FROM national_factors)
            """,
            [signal],
        )
    print(f"Loaded {rows_loaded} special elections; current 12m signal={signal}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
