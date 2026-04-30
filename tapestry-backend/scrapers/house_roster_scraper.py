from __future__ import annotations

import polars as pl

from data.historical.load_roster import run as load_roster
from data.historical.load_fec_weball import load_fec_weball
from data.historical.build_tapestry_pvi import build_tapestry_pvi
from db.connection import get_read_connection
from scrapers.base import BaseScraper


class HouseRosterScraper(BaseScraper):
    source_name = "house_roster"
    output_path = "data/raw/house_roster_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        load_roster()
        load_fec_weball()
        build_tapestry_pvi()
        with get_read_connection() as con:
            return con.execute("SELECT * FROM house_roster").pl()
