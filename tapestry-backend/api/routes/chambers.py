from __future__ import annotations

from fastapi import APIRouter

from api.schemas import ChamberForecast
from db.connection import get_read_connection

router = APIRouter(prefix="/api/chambers", tags=["chambers"])


@router.get("", response_model=list[ChamberForecast])
def get_chambers():
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT chamber,d_control_probability,d_expected_seats,d_seats_10th_pct,d_seats_90th_pct,
                   kalshi_price,polymarket_price,model_implied_price,kalshi_gap,narrative
            FROM chamber_forecasts
            WHERE forecast_date=(SELECT MAX(forecast_date) FROM chamber_forecasts)
            ORDER BY chamber
            """
        ).fetchall()
    return [
        ChamberForecast(
            chamber=r[0],
            d_control_probability=r[1],
            d_expected_seats=r[2],
            d_seats_low=r[3],
            d_seats_high=r[4],
            kalshi_price=r[5],
            polymarket_price=r[6],
            model_implied_price=r[7],
            kalshi_gap=r[8],
            model_vs_polymarket_gap=(r[1] - r[6]) if r[6] is not None else None,
            model_vs_kalshi_gap=(r[1] - r[5]) if r[5] is not None else None,
            narrative=r[9],
        )
        for r in rows
    ]
