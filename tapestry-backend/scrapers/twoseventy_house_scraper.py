from __future__ import annotations

import re
from datetime import datetime

import httpx
import polars as pl
from bs4 import BeautifulSoup

from db.connection import write_connection
from scrapers.base import BaseScraper
from utils.geo import normalize_district_id


RETIREMENTS_URL = "https://www.270towin.com/2026-house-election-find/2026-retirements"


def _pct(value: str | None) -> float | None:
    if not value:
        return None
    clean = value.replace(">", "").replace("+", "").replace("%", "").strip()
    try:
        return float(clean)
    except Exception:
        return None


def _year(value: str | None) -> int | None:
    try:
        number = int(str(value or "").strip())
        return number if 1900 <= number <= 2030 else None
    except Exception:
        return None


class TwoSeventyHouseScraper(BaseScraper):
    source_name = "twoseventy_house"
    output_path = "data/raw/twoseventy_house_context_latest.parquet"

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def fetch(self) -> pl.DataFrame:
        response = httpx.get(RETIREMENTS_URL, timeout=30, follow_redirects=True)
        response.raise_for_status()
        lines = [line.strip() for line in BeautifulSoup(response.text, "html.parser").get_text("\n", strip=True).splitlines() if line.strip()]
        rows = []
        context_group = "competitive_or_watch"
        for idx, line in enumerate(lines):
            if line.lower().startswith("the following races are rated"):
                context_group = "safe_open_seat"
                continue
            if not re.fullmatch(r"[A-Z]{2}-", line):
                continue
            district_num = lines[idx + 1] if idx + 1 < len(lines) else ""
            incumbent = lines[idx + 2] if idx + 2 < len(lines) else ""
            if not re.fullmatch(r"\d{1,2}|AL", district_num, re.I):
                continue
            if not re.search(r"[A-Za-z]{2,}\s+[A-Za-z]{2,}", incumbent):
                continue
            tail = lines[idx + 3: idx + 14]
            since = next((_year(item) for item in tail if _year(item)), None)
            term_label = next((item for item in tail if re.fullmatch(r"\d+(?:st|nd|rd|th)", item, re.I)), None)
            percents = [item for item in tail if re.fullmatch(r">?\d+(?:\.\d+)?%\+?", item)]
            house_margin = _pct(percents[0]) if len(percents) > 0 else None
            presidential_margin = _pct(percents[1]) if len(percents) > 1 else None
            kalshi_price = _pct(percents[2]) / 100 if len(percents) > 2 and _pct(percents[2]) is not None else None
            note = next((item for item in tail if re.search(r"retir|not running|re-election|senate|governor|attorney general", item, re.I)), None)
            rows.append({
                "district_id": normalize_district_id(f"{line[:2]}-{district_num}"),
                "incumbent_name": " ".join(incumbent.split()),
                "incumbent_party": None,
                "member_since": since,
                "term_label": term_label,
                "house_margin_2024": house_margin,
                "presidential_margin_2024": presidential_margin,
                "kalshi_house_price": kalshi_price,
                "race_note": note,
                "context_group": context_group,
                "source_url": RETIREMENTS_URL,
                "fetched_at": datetime.utcnow(),
            })
        deduped = {row["district_id"]: row for row in rows}
        columns = {
            "district_id": [], "incumbent_name": [], "incumbent_party": [], "member_since": [],
            "term_label": [], "house_margin_2024": [], "presidential_margin_2024": [],
            "kalshi_house_price": [], "race_note": [], "context_group": [], "source_url": [], "fetched_at": [],
        }
        return pl.DataFrame(list(deduped.values()), infer_schema_length=10000) if deduped else pl.DataFrame(columns)

    def run(self) -> bool:
        ok = super().run()
        try:
            df = pl.read_parquet(self.output_path)
            with write_connection() as con:
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS twoseventy_house_context (
                        district_id VARCHAR PRIMARY KEY,
                        incumbent_name VARCHAR,
                        incumbent_party VARCHAR,
                        member_since INTEGER,
                        term_label VARCHAR,
                        house_margin_2024 DOUBLE,
                        presidential_margin_2024 DOUBLE,
                        kalshi_house_price DOUBLE,
                        race_note TEXT,
                        context_group VARCHAR,
                        source_url VARCHAR,
                        fetched_at TIMESTAMP
                    )
                    """
                )
                if df.height:
                    con.register("twoseventy_df", df)
                    con.execute("INSERT OR REPLACE INTO twoseventy_house_context SELECT * FROM twoseventy_df")
                    con.execute(
                        """
                        UPDATE house_roster
                        SET retiring=true
                        WHERE district_id IN (
                            SELECT district_id FROM twoseventy_house_context
                            WHERE race_note IS NOT NULL
                        )
                        """
                    )
            self.logger.info("270toWin House context: loaded %s district rows", df.height)
            return ok
        except Exception as exc:
            self.logger.warning("Could not persist 270toWin House context rows: %s", exc)
            return False


if __name__ == "__main__":
    raise SystemExit(0 if TwoSeventyHouseScraper().run() else 1)
