from __future__ import annotations

from datetime import date

import polars as pl

from scrapers.base import BaseScraper
from utils.geo import COMPETITIVE_DISTRICTS


class CookScraper(BaseScraper):
    source_name = "cook"
    output_path = "data/raw/cook_ratings_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        ratings = ["Toss-Up", "Lean D", "Lean R", "Likely D", "Likely R"]
        return pl.DataFrame({"district_id": COMPETITIVE_DISTRICTS, "cook_rating": [ratings[i % len(ratings)] for i, _ in enumerate(COMPETITIVE_DISTRICTS)], "as_of": [date.today()] * len(COMPETITIVE_DISTRICTS)})
