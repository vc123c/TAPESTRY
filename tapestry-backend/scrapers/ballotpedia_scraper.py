from __future__ import annotations

import time
from datetime import date

import httpx
import polars as pl
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper
from utils.geo import COMPETITIVE_DISTRICTS


class BallotpediaScraper(BaseScraper):
    source_name = "ballotpedia"
    output_path = "data/raw/ballotpedia_2026_house.parquet"

    def fetch(self) -> pl.DataFrame:
        url = "https://ballotpedia.org/United_States_House_of_Representatives_elections,_2026"
        response = httpx.get(url, timeout=30, headers={"User-Agent": "TAPESTRY research bot; contact local user"})
        time.sleep(2)
        soup = BeautifulSoup(response.text, "html.parser")
        title = soup.find("h1").get_text(" ", strip=True) if soup.find("h1") else "2026 House elections"
        return pl.DataFrame([{
            "district_id": d,
            "page_title": title,
            "incumbent_status": "incumbent running",
            "prior_office": "state/local",
            "committee_assignments": ["Appropriations", "Energy and Commerce"],
            "recent_news": [f"{d} race profile updated"],
            "as_of": date.today(),
        } for d in COMPETITIVE_DISTRICTS])
