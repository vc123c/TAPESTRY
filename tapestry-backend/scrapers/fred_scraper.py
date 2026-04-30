from __future__ import annotations

import os
from datetime import date

import polars as pl

from scrapers.base import BaseScraper

FRED_SERIES = [
    "UMCSENT", "UNRATE", "CPIAUCSL", "MICH", "DSPIC96", "CES0500000003",
    "MORTGAGE30US", "FEDFUNDS", "PCEPI",
]


class FredScraper(BaseScraper):
    source_name = "fred"

    def __init__(self, series_id: str | None = None) -> None:
        self.series_id = series_id
        suffix = series_id or "all"
        super().__init__(f"data/raw/fred_{suffix}_{date.today().isoformat()}.parquet")

    def fetch(self) -> pl.DataFrame:
        try:
            from fredapi import Fred

            fred = Fred(api_key=os.getenv("FRED_API_KEY"))
            rows = []
            for series in ([self.series_id] if self.series_id else FRED_SERIES):
                values = fred.get_series(series).tail(240)
                rows.extend({"series_id": series, "date": idx.date(), "value": float(val)} for idx, val in values.items())
            return pl.DataFrame(rows)
        except Exception as exc:
            self.logger.warning("FRED scrape failed; returning empty results instead of synthetic values: %s", type(exc).__name__)
            return pl.DataFrame({"series_id": [], "date": [], "value": []})
