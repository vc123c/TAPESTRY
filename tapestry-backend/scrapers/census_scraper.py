from __future__ import annotations

import os
import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import polars as pl

from db.connection import ROOT, get_read_connection, write_connection
from scrapers.base import BaseScraper
from utils.geo import STATE_FIPS
from utils.logging import setup_logging

logger = setup_logging(__name__)

ACS_URL = "https://api.census.gov/data/2023/acs/acs5"
VARIABLES = [
    "B19013_001E",
    "B15003_022E",
    "B15003_001E",
    "B01002_001E",
    "B02001_001E",
    "B02001_002E",
    "B02001_003E",
    "B03001_003E",
    "B25064_001E",
    "B27010_033E",
    "B27010_001E",
    "C17002_001E",
    "C17002_002E",
    "B19083_001E",
]


def _float(row: dict, key: str) -> float | None:
    try:
        value = row.get(key)
        if value in (None, "", "-666666666", "-999999999"):
            return None
        return float(value)
    except Exception:
        return None


def _ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den in (None, 0):
        return None
    return num / den


def _district_id(state: str, district: str) -> str:
    if str(district).isdigit():
        number = int(district)
        return f"{state}-AL" if number == 0 else f"{state}-{number:02d}"
    return f"{state}-AL"


class CensusScraper(BaseScraper):
    source_name = "census"
    output_path = "data/raw/census_district_features_latest.parquet"

    def _cache_path(self, fips: str) -> Path:
        return ROOT / "data" / "raw" / f"census_{fips}.json"

    def _load_cached(self, fips: str) -> list | None:
        path = self._cache_path(fips)
        if not path.exists():
            return None
        cutoff = datetime.utcnow() - timedelta(days=30)
        if datetime.utcfromtimestamp(path.stat().st_mtime) < cutoff:
            return None
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload and isinstance(payload[0], list) and "B19083_001E" not in payload[0]:
            return None
        return payload

    def _write_cache(self, fips: str, payload: list) -> None:
        path = self._cache_path(fips)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload), encoding="utf-8")

    def fetch(self) -> pl.DataFrame:
        key = os.getenv("CENSUS_API_KEY")
        rows = []
        with get_read_connection() as con:
            roster = con.execute("SELECT district_id, state_abbr, incumbent_party, cook_pvi_numeric, last_margin, incumbent_first_elected FROM house_roster").fetchall()
        roster_by_state = {}
        for row in roster:
            roster_by_state.setdefault(row[1], []).append(row)

        processed = 0
        skipped = 0
        for state, fips in STATE_FIPS.items():
            params = {
                "get": ",".join(VARIABLES),
                "for": "congressional district:*",
                "in": f"state:{fips}",
            }
            if key:
                params["key"] = key
            try:
                payload = self._load_cached(fips)
                if payload is None:
                    response = httpx.get(ACS_URL, params=params, timeout=45)
                    response.raise_for_status()
                    payload = response.json()
                    self._write_cache(fips, payload)
                    time.sleep(0.5)
            except Exception as exc:
                logger.warning("Census ACS scrape failed url=%s state=%s error=%s timestamp=%s", ACS_URL, state, type(exc).__name__, datetime.utcnow().isoformat())
                skipped += 1
                continue
            headers = payload[0]
            for values in payload[1:]:
                raw = dict(zip(headers, values))
                district_id = _district_id(state, raw.get("congressional district", "0"))
                bachelor = _float(raw, "B15003_022E")
                pop25 = _float(raw, "B15003_001E")
                total_pop = _float(raw, "B02001_001E")
                white = _float(raw, "B02001_002E")
                black = _float(raw, "B02001_003E")
                hispanic = _float(raw, "B03001_003E")
                income = _float(raw, "B19013_001E")
                rent = _float(raw, "B25064_001E")
                uninsured = _float(raw, "B27010_033E")
                under65 = _float(raw, "B27010_001E")
                poverty_total = _float(raw, "C17002_001E")
                extreme_poverty = _float(raw, "C17002_002E")
                gini = _float(raw, "B19083_001E")
                roster_row = next((r for r in roster_by_state.get(state, []) if r[0] == district_id), None)
                pvi = float(roster_row[3]) if roster_row and roster_row[3] is not None else None
                last_margin = float(roster_row[4]) if roster_row and roster_row[4] is not None else None
                incumbent_years = 2026 - int(roster_row[5]) if roster_row and roster_row[5] else None
                poverty_proxy = _ratio(extreme_poverty, poverty_total)
                manufacturing_share = None
                rows.append({
                    "district_id": district_id,
                    "feature_date": date.today(),
                    "cook_pvi": pvi,
                    "margin_t0": last_margin,
                    "margin_t1": None,
                    "margin_t2": None,
                    "margin_trend": None,
                    "presidential_margin_2024": None,
                    "presidential_margin_2020": None,
                    "incumbent_party": roster_row[2] if roster_row else None,
                    "incumbent_running": True,
                    "incumbent_years": incumbent_years,
                    "open_seat": False,
                    "fundraising_ratio": None,
                    "cash_on_hand_ratio": None,
                    "outside_spending_ratio": None,
                    "college_educated_pct": _ratio(bachelor, pop25),
                    "median_age": _float(raw, "B01002_001E"),
                    "white_pct": _ratio(white, total_pop),
                    "hispanic_pct": _ratio(hispanic, total_pop),
                    "black_pct": _ratio(black, total_pop),
                    "population_density": None,
                    "urban_rural_class": None,
                    "median_income_real": income,
                    "income_growth_2yr": None,
                    "gini_coefficient": gini,
                    "unemployment_rate": None,
                    "unemployment_vs_national": None,
                    "medical_debt_per_capita": poverty_proxy,
                    "credit_card_debt_per_capita": poverty_proxy,
                    "healthcare_cost_burden": None,
                    "rent_burden_pct": (rent * 12 / income) if rent is not None and income not in (None, 0) else None,
                    "uninsured_rate": _ratio(uninsured, under65),
                    "ai_automation_exposure": (manufacturing_share or 0) * 0.7 if manufacturing_share is not None else None,
                    "manufacturing_share": manufacturing_share,
                    "tech_employment_share": None,
                    "recent_layoffs": None,
                    "net_hiring_trend": None,
                    "data_center_mw_planned": None,
                    "data_center_opposition_score": None,
                    "independent_media_penetration": None,
                    "local_news_intensity": None,
                    "abortion_measure": False,
                    "marijuana_measure": False,
                    "min_wage_measure": False,
                })
                processed += 1
        logger.info("Census scraper: processed %s district rows, skipped %s states", processed, skipped)
        return pl.DataFrame(rows)

    def run(self) -> bool:
        ok = super().run()
        if not ok:
            return False
        df = pl.read_parquet(self.output_path)
        if df.height == 0:
            return False
        with write_connection() as con:
            con.register("census_features_df", df)
            con.execute("INSERT OR REPLACE INTO district_features SELECT * FROM census_features_df")
        return True


if __name__ == "__main__":
    raise SystemExit(0 if CensusScraper().run() else 1)
