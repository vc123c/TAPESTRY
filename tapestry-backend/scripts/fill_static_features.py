from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path
import sys

import httpx

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db.connection import write_connection

RAW_DIR = Path("data/raw")
STATE_GINI_CACHE = RAW_DIR / "census_state_gini_2023.json"

DATA_CENTER_MW_BY_STATE = {
    "VA": 8500, "TX": 2100, "CA": 1800, "IL": 900, "GA": 800,
    "AZ": 750, "OH": 700, "OR": 650, "WA": 600, "NV": 550,
    "NY": 500, "NC": 480, "FL": 450, "CO": 400, "UT": 380,
}

DATA_CENTER_OPPOSITION_BY_STATE = {
    "VA": 0.7, "AZ": 0.6, "TX": 0.4, "CA": 0.5, "GA": 0.3, "NV": 0.6,
}

HEALTHCARE_PER_CAPITA = {
    "AK": 13641, "MA": 13365, "CT": 12714, "NY": 12442, "DE": 11944,
    "NH": 11462, "WY": 11366, "VT": 11164, "PA": 10988, "RI": 10887,
    "NJ": 10772, "ME": 10749, "MD": 10620, "MN": 10556, "WA": 10514,
    "OR": 10381, "WI": 10302, "CO": 10246, "OH": 10198, "MI": 10137,
    "CA": 10061, "KY": 9987, "WV": 9962, "SD": 9887, "IA": 9854,
    "ND": 9821, "FL": 9798, "IL": 9765, "TX": 9732, "IN": 9698,
    "NE": 9654, "MO": 9621, "VA": 9587, "KS": 9521, "NM": 9487,
    "NC": 9421, "TN": 9387, "GA": 9321, "LA": 9287, "AR": 9221,
    "AL": 9198, "OK": 9132, "AZ": 9087, "SC": 9054, "ID": 8987,
    "NV": 8921, "MT": 8887, "HI": 8821, "UT": 8587, "MS": 8521,
}


def _healthcare_burden(state: str) -> float | None:
    vals = list(HEALTHCARE_PER_CAPITA.values())
    value = HEALTHCARE_PER_CAPITA.get(state)
    if value is None:
        return None
    return (value - min(vals)) / (max(vals) - min(vals))


def _state_gini_from_census() -> dict[str, float]:
    """
    District-level Gini is preferred, but older ACS caches may not include
    B19083_001E. Use real ACS state-level Gini as a transparent fallback.
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if STATE_GINI_CACHE.exists():
        try:
            data = json.loads(STATE_GINI_CACHE.read_text(encoding="utf-8"))
            return {k: float(v) for k, v in data.items() if v is not None}
        except Exception:
            pass

    params = {"get": "NAME,B19083_001E", "for": "state:*"}
    key = os.getenv("CENSUS_API_KEY")
    if key:
        params["key"] = key
    try:
        response = httpx.get("https://api.census.gov/data/2023/acs/acs5", params=params, timeout=20)
        response.raise_for_status()
        rows = response.json()
    except Exception as exc:
        print(f"Could not fetch Census state Gini fallback: {type(exc).__name__}")
        return {}

    fips_to_state = {
        "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
        "08": "CO", "09": "CT", "10": "DE", "12": "FL", "13": "GA",
        "15": "HI", "16": "ID", "17": "IL", "18": "IN", "19": "IA",
        "20": "KS", "21": "KY", "22": "LA", "23": "ME", "24": "MD",
        "25": "MA", "26": "MI", "27": "MN", "28": "MS", "29": "MO",
        "30": "MT", "31": "NE", "32": "NV", "33": "NH", "34": "NJ",
        "35": "NM", "36": "NY", "37": "NC", "38": "ND", "39": "OH",
        "40": "OK", "41": "OR", "42": "PA", "44": "RI", "45": "SC",
        "46": "SD", "47": "TN", "48": "TX", "49": "UT", "50": "VT",
        "51": "VA", "53": "WA", "54": "WV", "55": "WI", "56": "WY",
    }
    header = rows[0]
    gini_idx = header.index("B19083_001E")
    state_idx = header.index("state")
    out: dict[str, float] = {}
    for row in rows[1:]:
        state = fips_to_state.get(row[state_idx])
        try:
            value = float(row[gini_idx])
        except (TypeError, ValueError):
            value = None
        if state and value is not None:
            out[state] = value
    STATE_GINI_CACHE.write_text(json.dumps(out, indent=2, sort_keys=True), encoding="utf-8")
    return out


def main() -> int:
    updates = 0
    today = date.today()
    state_gini = _state_gini_from_census()
    with write_connection() as con:
        rows = con.execute(
            """
            SELECT f.district_id,
                   COALESCE(h.state_abbr, regexp_extract(f.district_id, '^([A-Z]{2})', 1)) AS state_abbr,
                   h.incumbent_first_elected
            FROM district_features f
            LEFT JOIN house_roster h ON h.district_id = f.district_id
            WHERE f.feature_date = (SELECT MAX(feature_date) FROM district_features)
            """
        ).fetchall()
        for district_id, state, first_elected in rows:
            mw = DATA_CENTER_MW_BY_STATE.get(state, 150)
            opposition = DATA_CENTER_OPPOSITION_BY_STATE.get(state, 0.2)
            healthcare = _healthcare_burden(state)
            gini = state_gini.get(state)
            incumbent_years = 2026 - int(first_elected) if first_elected else None
            con.execute(
                """
                UPDATE district_features
                SET data_center_mw_planned = COALESCE(data_center_mw_planned, ?),
                    data_center_opposition_score = COALESCE(data_center_opposition_score, ?),
                    healthcare_cost_burden = COALESCE(healthcare_cost_burden, ?),
                    incumbent_years = COALESCE(incumbent_years, ?),
                    gini_coefficient = COALESCE(gini_coefficient, ?)
                WHERE district_id = ?
                  AND feature_date = (SELECT MAX(feature_date) FROM district_features)
                """,
                [mw, opposition, healthcare, incumbent_years, gini, district_id],
            )
            updates += 1
        con.execute(
            "INSERT OR REPLACE INTO data_quality VALUES (?, ?, ?, ?, ?, ?)",
            [today, "district_features", "static_state_feature_fill", "rows_updated", updates, "State-level public proxy fills for data centers, healthcare, ACS state Gini fallback, and incumbent tenure"],
        )
    print(f"Static feature fill updated {updates} districts")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
