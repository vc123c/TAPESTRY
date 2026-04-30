from __future__ import annotations

import re
from datetime import date

import httpx
import polars as pl
from bs4 import BeautifulSoup

from scrapers.base import BaseScraper


class PapeScraper(BaseScraper):
    source_name = "pape"
    output_path = "data/raw/pape_escalation_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        url = "https://escalationtrap.substack.com/"
        html = httpx.get(url, timeout=30).text
        text = BeautifulSoup(html, "html.parser").get_text(" ", strip=True)
        escalation = len(re.findall(r"escalation|ground war|trap|irreversible", text, re.I))
        deescalation = len(re.findall(r"ceasefire|diplomacy|withdraw", text, re.I))
        stage_mentions = re.findall(r"Stage\s*([1-5])", text, re.I)
        stage_score = max([int(s) for s in stage_mentions], default=3) / 5
        return pl.DataFrame([{"publication_date": date.today(), "url": url, "stage_signal_score": stage_score, "escalation_terms": escalation, "deescalation_terms": deescalation, "text_excerpt": text[:1000]}])
