from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from db.connection import ROOT
from scrapers.base import BaseScraper
from utils.geo import STATE_FIPS

PRIORITY_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "NJ", "VA", "WA", "AZ", "MA", "TN", "IN", "MO", "MD", "WI",
    "CO", "MN", "SC", "AL", "LA", "KY", "OR", "OK", "CT", "UT",
]

KEYWORD_GROUPS = {
    "economy": ["egg prices", "grocery prices", "can't afford rent", "laid off", "AI taking jobs"],
    "conflict": ["Iran war", "gas prices", "military draft", "Hormuz", "escalation"],
    "anti_establishment": ["corruption", "both parties same", "Epstein files", "drain the swamp"],
    "election": ["2026 midterms", "register to vote", "early voting 2026", "vote by mail"],
}


class GoogleTrendsScraper(BaseScraper):
    source_name = "google_trends"
    output_path = "data/raw/google_trends_latest.parquet"
    max_uncached_requests = 24
    request_delay_seconds = 2.0

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def _cache_path(self, state: str, group: str) -> Path:
        return ROOT / "data" / "raw" / f"trends_{state}_{group}.json"

    def _fresh_cache(self, path: Path) -> bool:
        return path.exists() and datetime.utcnow() - datetime.utcfromtimestamp(path.stat().st_mtime) < timedelta(days=7)

    def fetch(self) -> pl.DataFrame:
        try:
            from pytrends.request import TrendReq
        except Exception as exc:
            self.logger.warning("pytrends unavailable; returning empty real-data result: %s", type(exc).__name__)
            return pl.DataFrame({"geo": [], "group": [], "keyword": [], "score": [], "as_of": []})

        pytrends = TrendReq(hl="en-US", tz=360, timeout=(5, 15), retries=1, backoff_factor=0.2)
        rows = []
        uncached_requests = 0
        states = PRIORITY_STATES + [state for state in STATE_FIPS if state not in PRIORITY_STATES]
        for state in states:
            geo = f"US-{state}"
            for group, keywords in KEYWORD_GROUPS.items():
                cache = self._cache_path(state, group)
                payload = None
                if self._fresh_cache(cache):
                    payload = json.loads(cache.read_text(encoding="utf-8"))
                else:
                    if uncached_requests >= self.max_uncached_requests:
                        self.logger.info("Google Trends request budget reached; remaining uncached state/group pairs will refresh on a later run")
                        continue
                    try:
                        pytrends.build_payload(keywords, timeframe=f"2024-01-01 {date.today().isoformat()}", geo=geo)
                        frame = pytrends.interest_over_time()
                        payload = {
                            "geo": geo,
                            "group": group,
                            "keywords": keywords,
                            "rows": frame.reset_index().tail(12).to_dict("records"),
                        }
                        cache.parent.mkdir(parents=True, exist_ok=True)
                        cache.write_text(json.dumps(payload, default=str), encoding="utf-8")
                        uncached_requests += 1
                        time.sleep(self.request_delay_seconds)
                    except Exception as exc:
                        self.logger.warning("Google Trends fetch failed geo=%s group=%s error=%s", geo, group, type(exc).__name__)
                        continue
                latest = (payload.get("rows") or [{}])[-1]
                for keyword in keywords:
                    value = latest.get(keyword)
                    if value is None:
                        continue
                    rows.append({"geo": geo, "group": group, "keyword": keyword, "score": float(value), "as_of": date.today()})
        return pl.DataFrame(rows) if rows else pl.DataFrame({"geo": [], "group": [], "keyword": [], "score": [], "as_of": []})


if __name__ == "__main__":
    raise SystemExit(0 if GoogleTrendsScraper().run() else 1)
