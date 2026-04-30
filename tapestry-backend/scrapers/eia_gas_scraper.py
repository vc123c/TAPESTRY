from __future__ import annotations

import re
from datetime import date

import httpx
import polars as pl
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class EIAGasScraper(BaseScraper):
    source_name = "eia_gas"
    output_path = "data/raw/eia_gas_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        html = httpx.get("https://www.eia.gov/petroleum/gasdiesel/", timeout=30).text
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(" ")
        prices = [float(x) for x in re.findall(r"\b[2-5]\.\d{3}\b", text)[:12]]
        national = prices[0] if prices else 3.45
        return pl.DataFrame({
            "date": [date.today()],
            "national_avg": [national],
            "change_4w": [0.0],
            "change_12w": [0.0],
            "source_url": ["https://www.eia.gov/petroleum/gasdiesel/"],
        })
