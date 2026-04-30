from __future__ import annotations

from fastapi import APIRouter

from db.connection import get_read_connection

router = APIRouter(prefix="/api/states", tags=["states"])


@router.get("")
def list_states():
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT
                h.state_abbr,
                ANY_VALUE(h.state_name) AS state_name,
                AVG(f.win_probability_d) AS avg_win_probability_d,
                COUNT(f.district_id) AS forecast_count,
                COUNT(h.district_id) AS roster_count
            FROM house_roster h
            LEFT JOIN district_forecasts f
              ON f.district_id = h.district_id
             AND f.forecast_date = (SELECT MAX(forecast_date) FROM district_forecasts)
            GROUP BY h.state_abbr
            ORDER BY h.state_abbr
            """
        ).fetchall()
    return [
        {
            "state_abbr": row[0],
            "state_name": row[1],
            "avg_win_probability_d": row[2],
            "forecast_count": row[3],
            "roster_count": row[4],
        }
        for row in rows
    ]
