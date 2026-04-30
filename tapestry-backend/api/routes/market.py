from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter

from api.schemas import MarketGapsResponse
from db.connection import get_read_connection

router = APIRouter(prefix="/api/market", tags=["market"])


@router.get("/gaps", response_model=MarketGapsResponse)
def get_market_gaps():
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT chamber, d_control_probability, polymarket_price, kalshi_price
            FROM chamber_forecasts
            WHERE forecast_date=(SELECT MAX(forecast_date) FROM chamber_forecasts)
            ORDER BY chamber
            """
        ).fetchall()
    chamber_gaps = []
    for chamber, model, poly, kalshi in rows:
        gaps = []
        if poly is not None:
            gaps.append(("polymarket", float(poly), abs(float(model) - float(poly))))
        if kalshi is not None:
            gaps.append(("kalshi", float(kalshi), abs(float(model) - float(kalshi))))
        if not gaps:
            continue
        source, market_price, gap = max(gaps, key=lambda item: item[2])
        chamber_gaps.append({
            "chamber": chamber,
            "tapestry_probability": float(model),
            "polymarket_price": float(poly) if poly is not None else None,
            "kalshi_price": float(kalshi) if kalshi is not None else None,
            "largest_gap": gap,
            "gap_direction": "market_higher" if market_price > float(model) else "model_higher",
            "source": source,
            "explanation": f"{source.title()} prices D {gap * 100:.0f}pts {'above' if market_price > float(model) else 'below'} TAPESTRY.",
        })
    return {"chamber_gaps": chamber_gaps, "district_gaps": [], "generated_at": datetime.now(timezone.utc).isoformat()}
