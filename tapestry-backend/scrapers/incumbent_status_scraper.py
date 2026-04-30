from __future__ import annotations

import re
from datetime import datetime

import httpx
import polars as pl
from bs4 import BeautifulSoup

from db.connection import write_connection
from scrapers.base import BaseScraper
from utils.geo import normalize_district_id


SOURCES = [
    ("270toWin", "https://www.270towin.com/2026-house-election-find/2026-retirements"),
    ("NPR/KPBS", "https://www.kpbs.org/news/politics/2025/09/15/a-record-number-of-congressional-lawmakers-arent-running-for-reelection-in-2026-heres-the-list"),
]


class IncumbentStatusScraper(BaseScraper):
    source_name = "incumbent_status"
    output_path = "data/raw/incumbent_status_2026_latest.parquet"

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    @staticmethod
    def _status_from_text(text: str) -> tuple[str, str]:
        low = text.lower()
        if "senate" in low:
            return "not_running", "running_for_senate"
        if "governor" in low:
            return "not_running", "running_for_governor"
        if "attorney general" in low:
            return "not_running", "running_for_attorney_general"
        if "retir" in low or "not running" in low or "not seek" in low:
            return "not_running", "retiring_or_not_seeking_reelection"
        if "resign" in low or "vacan" in low or "died" in low or "dead" in low:
            return "vacant", "resignation_death_or_vacancy"
        return "unknown", "unclassified"

    def _fetch_270(self) -> list[dict]:
        source_name, url = SOURCES[0]
        response = httpx.get(url, timeout=30, follow_redirects=True)
        response.raise_for_status()
        text = BeautifulSoup(response.text, "html.parser").get_text("\n", strip=True)
        rows = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for idx, line in enumerate(lines):
            if not re.fullmatch(r"[A-Z]{2}-", line):
                continue
            number = lines[idx + 1] if idx + 1 < len(lines) else ""
            name = lines[idx + 2] if idx + 2 < len(lines) else ""
            if not re.fullmatch(r"\d{1,2}|AL", number, re.I):
                continue
            if not re.search(r"[A-Za-z]{2,}\s+[A-Za-z]{2,}", name):
                continue
            district_id = normalize_district_id(f"{line[:2]}-{number}")
            context = " ".join(lines[idx: idx + 14])
            status, reason = self._status_from_text(context)
            if status == "unknown":
                continue
            rows.append({
                "district_id": district_id,
                "incumbent_name": " ".join(name.split()),
                "party": None,
                "status": status,
                "reason": reason,
                "source_name": source_name,
                "source_url": url,
                "observed_at": datetime.utcnow(),
            })
        return rows

    def fetch(self) -> pl.DataFrame:
        rows = []
        for source_name, url in SOURCES:
            try:
                if source_name == "270toWin":
                    rows.extend(self._fetch_270())
                else:
                    # Keep source registered for audit; broad NPR text is harder to normalize safely.
                    httpx.get(url, timeout=20, follow_redirects=True).raise_for_status()
            except Exception as exc:
                self.logger.warning("Incumbent status source failed source=%s url=%s error=%s", source_name, url, type(exc).__name__)
        deduped = {}
        for row in rows:
            deduped[row["district_id"]] = row
        return pl.DataFrame(list(deduped.values()), infer_schema_length=10000) if deduped else pl.DataFrame({
            "district_id": [], "incumbent_name": [], "party": [], "status": [], "reason": [],
            "source_name": [], "source_url": [], "observed_at": [],
        })

    def run(self) -> bool:
        ok = super().run()
        try:
            df = pl.read_parquet(self.output_path)
            with write_connection() as con:
                con.execute(
                    """
                    CREATE TABLE IF NOT EXISTS incumbent_status_2026 (
                        district_id VARCHAR PRIMARY KEY,
                        incumbent_name VARCHAR,
                        party VARCHAR,
                        status VARCHAR,
                        reason VARCHAR,
                        source_name VARCHAR,
                        source_url VARCHAR,
                        observed_at TIMESTAMP
                    )
                    """
                )
                if df.height:
                    con.register("status_df", df)
                    con.execute("INSERT OR REPLACE INTO incumbent_status_2026 SELECT * FROM status_df")
                    con.execute(
                        """
                        UPDATE house_roster
                        SET retiring=true
                        WHERE district_id IN (
                            SELECT district_id FROM incumbent_status_2026
                            WHERE status IN ('not_running','vacant')
                        )
                        """
                    )
            self.logger.info("Incumbent status scraper: marked %s open/not-running seats", df.height)
            return ok
        except Exception as exc:
            self.logger.warning("Could not persist incumbent status rows: %s", exc)
            return False


if __name__ == "__main__":
    raise SystemExit(0 if IncumbentStatusScraper().run() else 1)
