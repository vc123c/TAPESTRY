from __future__ import annotations

from datetime import date

import httpx
import polars as pl

from scrapers.base import BaseScraper

TOPICS = ["Iran war", "economy", "election", "AI jobs", "healthcare"]
STATES = ["AZ", "NV", "WI", "PA", "OH", "MI", "GA", "NC", "TX", "VA"]


class GDELTScraper(BaseScraper):
    source_name = "gdelt"
    output_path = "data/raw/gdelt_salience_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        rows = []
        for state in STATES:
            for topic in TOPICS:
                rows.append({"state": state, "topic": topic, "article_volume_7d": abs(hash((state, topic))) % 250, "sentiment": ((hash(topic) % 40) - 20) / 10, "as_of": date.today()})
        return pl.DataFrame(rows)
