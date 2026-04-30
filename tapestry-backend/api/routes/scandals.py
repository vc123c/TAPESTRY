from __future__ import annotations

import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from db.connection import get_read_connection

router = APIRouter(tags=["events"])
BAD_EVENT_NAMES = {"stonks", "stocks", "untitled", "news", "latest", "update", "market watch"}


def _usable_event_name(name: object) -> bool:
    value = str(name or "").strip()
    return len(value) >= 12 and value.lower() not in BAD_EVENT_NAMES and any(ch.isalpha() for ch in value)


def _primary_score(raw: object) -> float:
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        return float((data or {}).get("primary_score") or 0.0)
    except Exception:
        return 0.0


@router.get("/api/events")
def get_events(
    state: Optional[str] = Query(default=None),
    district: Optional[str] = Query(default=None),
    limit: int = Query(default=50, ge=1, le=100),
):
    requested_state = (state or "").strip().upper() or None
    requested_district = (district or "").strip().upper() or None
    if requested_district and not requested_state and "-" in requested_district:
        requested_state = requested_district.split("-", 1)[0]
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT
                e.event_id,
                e.event_name,
                e.event_date,
                e.event_type,
                e.primary_target_party,
                e.half_life_days,
                e.affected_districts,
                e.resolved,
                e.notes,
                COALESCE(s.composite_salience, e.credibility_weighted_salience, 0) AS salience,
                COALESCE(e.source_count, m.source_count, s.news_volume, 0) AS source_count,
                m.article_count,
                m.representative_url,
                m.representative_headline,
                e.type_scores,
                e.affected_states,
                COALESCE(e.is_national_signal, false) AS is_national_signal
            FROM event_tokens e
            LEFT JOIN event_salience s
              ON s.event_id = e.event_id
             AND s.salience_date = (
                SELECT MAX(salience_date) FROM event_salience WHERE event_id = e.event_id
             )
            LEFT JOIN media_signal_summary m
              ON m.signal_key = regexp_extract(e.event_id, '([a-f0-9]{16})$', 1)
            WHERE COALESCE(e.resolved, false)=false
              AND COALESCE(e.source_count, COALESCE(s.news_volume, 0)) >= 2
            ORDER BY salience DESC, e.event_date DESC
            LIMIT 300
            """
        ).fetchall()
    results = []
    for r in rows:
        if not _usable_event_name(r[1]):
            continue
        affected_states = [str(s).upper() for s in (r[15] or [])]
        is_national = bool(r[16])
        state_specific = 1 if requested_state and requested_state in affected_states else 0
        passes_confidence = (float(r[9] or 0) > 0.25 and _primary_score(r[14]) > 0.4)
        passes_geo = bool(state_specific)
        if not (passes_confidence or passes_geo):
            continue
        if requested_state and not (is_national or requested_state in affected_states):
            continue
        if requested_district:
            affected_districts = [str(d).upper() for d in (r[6] or [])]
            if not (is_national or requested_district in affected_districts or requested_state in affected_states):
                continue
        results.append(
            {
            "event_id": r[0],
            "event_name": r[1],
            "event_date": r[2],
            "event_type": r[3],
            "target_party": r[4],
            "half_life_days": r[5],
            "affected_districts": r[6],
            "resolved": r[7],
            "notes": r[8],
            "salience": r[9],
            "source_count": r[10],
            "article_count": r[11],
            "source_url": r[12],
            "source_headline": r[13],
            "affected_states": affected_states,
            "is_national_signal": is_national,
            "_state_specific": state_specific,
            }
        )
    if requested_state:
        results.sort(key=lambda item: (item.pop("_state_specific", 0), item.get("salience") or 0), reverse=True)
    else:
        for item in results:
            item.pop("_state_specific", None)
    return results[:limit]


@router.get("/api/events/{event_id}")
def get_event(event_id: str):
    with get_read_connection() as con:
        event = con.execute("SELECT * FROM event_tokens WHERE event_id=?", [event_id]).fetchone()
        series = con.execute("SELECT salience_date, composite_salience FROM event_salience WHERE event_id=? ORDER BY salience_date", [event_id]).fetchall()
    if not event:
        raise HTTPException(status_code=404, detail="Event not found")
    return {"event": event, "salience_time_series": [{"date": r[0], "salience": r[1]} for r in series]}


@router.get("/api/kalshi/gaps")
def kalshi_gaps():
    with get_read_connection() as con:
        rows = con.execute("SELECT district_id, kalshi_price, model_implied_price, kalshi_gap, gap_explanation FROM district_forecasts WHERE kalshi_gap_flag=true ORDER BY ABS(kalshi_gap) DESC").fetchall()
    return [{"district_id": r[0], "kalshi_price": r[1], "model_implied_price": r[2], "gap": r[3], "explanation": r[4]} for r in rows]


@router.get("/api/market/gaps")
def market_gaps():
    with get_read_connection() as con:
        chambers = con.execute(
            """
            SELECT chamber,d_control_probability,polymarket_price,kalshi_price
            FROM chamber_forecasts
            WHERE forecast_date=(SELECT MAX(forecast_date) FROM chamber_forecasts)
            ORDER BY chamber
            """
        ).fetchall()
        district_rows = con.execute(
            "SELECT district_id, kalshi_price, model_implied_price, kalshi_gap FROM district_forecasts WHERE kalshi_gap_flag=true ORDER BY ABS(kalshi_gap) DESC"
        ).fetchall()
    chamber_gaps = []
    for chamber, tapestry, poly, kalshi in chambers:
        gaps = []
        if poly is not None:
            gaps.append(("polymarket", poly, abs(tapestry - poly)))
        if kalshi is not None:
            gaps.append(("kalshi", kalshi, abs(tapestry - kalshi)))
        if not gaps:
            continue
        source, market, largest = max(gaps, key=lambda item: item[2])
        direction = "market_higher" if market > tapestry else "model_higher"
        chamber_gaps.append({
            "chamber": chamber,
            "tapestry_probability": tapestry,
            "polymarket_price": poly,
            "kalshi_price": kalshi,
            "largest_gap": largest,
            "gap_direction": direction,
            "source": source,
            "explanation": f"{source.title()} prices D {largest * 100:.0f}pts {'above' if market > tapestry else 'below'} TAPESTRY.",
        })
    return {
        "chamber_gaps": chamber_gaps,
        "district_gaps": [{"district_id": r[0], "kalshi_price": r[1], "model_implied_price": r[2], "gap": r[3]} for r in district_rows],
    }
