from __future__ import annotations

import os
import re
import base64
from datetime import UTC, datetime
from pathlib import Path

import httpx
import polars as pl
try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
except Exception:  # pragma: no cover - lets bearer-token mode work without cryptography installed
    hashes = serialization = padding = None

from db.connection import ROOT, write_connection
from scrapers.base import BaseScraper


class KalshiScraper(BaseScraper):
    source_name = "kalshi"
    output_path = "data/raw/kalshi_markets_latest.parquet"
    base_url = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")

    states = {
        "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
        "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
        "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
        "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
        "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS",
        "Missouri": "MO", "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH",
        "New Jersey": "NJ", "New Mexico": "NM", "New York": "NY", "North Carolina": "NC",
        "North Dakota": "ND", "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA",
        "Rhode Island": "RI", "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN",
        "Texas": "TX", "Utah": "UT", "Vermont": "VT", "Virginia": "VA", "Washington": "WA",
        "West Virginia": "WV", "Wisconsin": "WI", "Wyoming": "WY",
    }
    ordinal_words = {
        "first": 1, "second": 2, "third": 3, "fourth": 4, "fifth": 5, "sixth": 6,
        "seventh": 7, "eighth": 8, "ninth": 9, "tenth": 10, "eleventh": 11, "twelfth": 12,
        "thirteenth": 13, "fourteenth": 14, "fifteenth": 15, "sixteenth": 16, "seventeenth": 17,
        "eighteenth": 18, "nineteenth": 19, "twentieth": 20, "twenty first": 21, "twenty-first": 21,
        "twenty second": 22, "twenty-second": 22, "twenty third": 23, "twenty-third": 23,
        "twenty fourth": 24, "twenty-fourth": 24, "twenty fifth": 25, "twenty-fifth": 25,
        "twenty sixth": 26, "twenty-sixth": 26, "twenty seventh": 27, "twenty-seventh": 27,
        "twenty eighth": 28, "twenty-eighth": 28, "twenty ninth": 29, "twenty-ninth": 29,
        "thirtieth": 30, "thirty first": 31, "thirty-first": 31, "thirty second": 32,
        "thirty-second": 32, "thirty third": 33, "thirty-third": 33, "thirty fourth": 34,
        "thirty-fourth": 34, "thirty fifth": 35, "thirty-fifth": 35, "thirty sixth": 36,
        "thirty-sixth": 36, "thirty seventh": 37, "thirty-seventh": 37, "thirty eighth": 38,
        "thirty-eighth": 38, "thirty ninth": 39, "thirty-ninth": 39, "fortieth": 40,
        "forty first": 41, "forty-first": 41, "forty second": 42, "forty-second": 42,
        "forty third": 43, "forty-third": 43, "forty fourth": 44, "forty-fourth": 44,
        "forty fifth": 45, "forty-fifth": 45, "forty sixth": 46, "forty-sixth": 46,
        "forty seventh": 47, "forty-seventh": 47, "forty eighth": 48, "forty-eighth": 48,
        "forty ninth": 49, "forty-ninth": 49, "fiftieth": 50, "fifty first": 51,
        "fifty-first": 51, "fifty second": 52, "fifty-second": 52,
    }

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def _district_from_title(self, title: str) -> str | None:
        upper = title.upper()
        match = re.search(r"\b([A-Z]{2})[-\s]?(\d{1,2})\b", upper)
        if match:
            return f"{match.group(1)}-{int(match.group(2)):02d}"
        state_abbr = None
        for state_name, abbr in self.states.items():
            if re.search(rf"\b{re.escape(state_name)}\b", title, re.I) or re.search(rf"\b{abbr}\b", upper):
                state_abbr = abbr
                break
        if not state_abbr:
            return None
        if re.search(r"\bsenate\b", title, re.I):
            return f"{state_abbr}-SEN"
        district_match = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)?\b", title, re.I)
        if district_match and re.search(r"district|congress|house", title, re.I):
            return f"{state_abbr}-{int(district_match.group(1)):02d}"
        lower = title.lower()
        for word, number in self.ordinal_words.items():
            if re.search(rf"\b{re.escape(word)}\b", lower) and re.search(r"district|congress|house", lower):
                return f"{state_abbr}-{number:02d}"
        return None

    def _signed_headers(self, method: str, path_without_query: str) -> dict[str, str] | None:
        key_id = (os.getenv("KALSHI_ACCESS_KEY_ID") or "").strip()
        key_path = (os.getenv("KALSHI_PRIVATE_KEY_PATH") or "").strip()
        if not key_id or not key_path:
            return None
        if serialization is None or hashes is None or padding is None:
            self.logger.warning("cryptography is not installed; cannot use Kalshi RSA auth")
            return None
        private_key_path = Path(key_path)
        if not private_key_path.is_absolute():
            private_key_path = ROOT / private_key_path
        if not private_key_path.exists():
            self.logger.warning("Kalshi private key file not found at %s", private_key_path)
            return None
        timestamp = str(int(datetime.now(UTC).timestamp() * 1000))
        private_key = serialization.load_pem_private_key(private_key_path.read_bytes(), password=None)
        message = f"{timestamp}{method.upper()}{path_without_query}".encode("utf-8")
        signature = private_key.sign(
            message,
            padding.PKCS1v15(),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": key_id,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(signature).decode("utf-8"),
        }

    def fetch(self) -> pl.DataFrame:
        api_key = (os.getenv("KALSHI_API_KEY") or "").strip()
        columns = {
            "market_id": [], "ticker": [], "district_id": [], "chamber": [], "market_title": [],
            "raw_title": [], "raw_ticker": [], "matched_district_id": [], "match_confidence": [],
            "yes_price": [], "no_price": [], "volume_24h": [], "open_interest": [], "last_price_change": [], "fetched_at": [],
        }
        path = "/trade-api/v2/markets"
        headers = self._signed_headers("GET", path)
        if headers is None:
            if not api_key:
                self.logger.warning("Kalshi credentials missing; set KALSHI_ACCESS_KEY_ID and KALSHI_PRIVATE_KEY_PATH")
                return pl.DataFrame(columns)
            if "BEGIN " in api_key or "\n" in api_key or "\r" in api_key:
                self.logger.warning("KALSHI_API_KEY appears to be a multiline private key; put it in a file and set KALSHI_PRIVATE_KEY_PATH instead")
                return pl.DataFrame(columns)
            headers = {"Authorization": f"Bearer {api_key}"}
        rows = []
        cursor = None
        fetched = 0
        try:
            param_sets = [
                {"limit": 200, "status": "open"},
                {"limit": 200, "status": "open", "series_ticker": "KXHOUSE"},
                {"limit": 200, "status": "open", "series_ticker": "KXSENATE"},
                {"limit": 200, "status": "open", "series_ticker": "KXBALANCEPOWERCOMBO"},
            ]
            seen_tickers = set()
            for base_params in param_sets:
                cursor = None
                for _ in range(10):
                    params = dict(base_params)
                    if cursor:
                        params["cursor"] = cursor
                    response = httpx.get(f"{self.base_url}/markets", params=params, headers=headers, timeout=30)
                    if response.status_code == 401:
                        self.logger.warning("Kalshi authentication failed with 401; returning empty results")
                        return pl.DataFrame(columns)
                    response.raise_for_status()
                    payload = response.json()
                    markets = payload.get("markets", [])
                    fetched += len(markets)
                    for market in markets:
                        ticker = market.get("ticker") or market.get("id")
                        if ticker in seen_tickers:
                            continue
                        seen_tickers.add(ticker)
                        title_text = (market.get("title") or "").strip()
                        raw_title = " ".join(str(part or "") for part in [market.get("title"), market.get("subtitle")]).strip()
                        district = self._district_from_title(" ".join([
                            str(market.get("title") or ""),
                            str(market.get("subtitle") or ""),
                            str(market.get("event_ticker") or ""),
                            str(market.get("series_ticker") or ""),
                            str(ticker or ""),
                        ]))
                        confidence = "exact" if district and re.search(r"\b[A-Z]{2}[-\s]?\d{1,2}\b", raw_title.upper()) else "fuzzy" if district else "none"
                        lower = raw_title.lower()
                        chamber = "senate" if district and district.endswith("-SEN") else "senate" if "senate" in lower else "house" if "house" in lower or "congress" in lower else None
                        if not district and not chamber and not any(term in lower for term in ["congress", "house", "senate", "election", "balance of power"]):
                            continue
                        rows.append({
                            "market_id": market.get("id") or ticker,
                            "ticker": ticker,
                            "district_id": district if district and not district.endswith("-SEN") else None,
                            "chamber": chamber,
                            "market_title": title_text or raw_title,
                            "raw_title": raw_title,
                            "raw_ticker": ticker,
                            "matched_district_id": district,
                            "match_confidence": confidence,
                            "yes_price": self._cents_to_probability(market.get("yes_bid") or market.get("last_price")),
                            "no_price": self._cents_to_probability(market.get("no_bid")),
                            "volume_24h": market.get("volume_24h") or market.get("volume"),
                            "open_interest": market.get("open_interest"),
                            "last_price_change": self._cents_to_probability(market.get("price_delta_24h")),
                            "fetched_at": datetime.now(UTC),
                        })
                    cursor = payload.get("cursor")
                    if not cursor:
                        break
        except Exception as exc:
            self.logger.warning("Kalshi fetch failed url=%s error=%s timestamp=%s", f"{self.base_url}/markets", type(exc).__name__, datetime.now(UTC).isoformat())
            return pl.DataFrame(columns)
        matched = sum(1 for row in rows if row.get("district_id"))
        chamber_matches = sum(1 for row in rows if row.get("chamber"))
        self.logger.info("Kalshi: %s total markets, %s matched to districts, %s to chamber control, %s unmatched political markets", fetched, matched, chamber_matches, max(0, len(rows) - matched - chamber_matches))
        return pl.DataFrame(rows) if rows else pl.DataFrame(columns)

    def _cents_to_probability(self, value: object) -> float | None:
        if value is None:
            return None
        try:
            number = float(value)
        except (TypeError, ValueError):
            return None
        return number / 100 if abs(number) > 1 else number

    def run(self) -> bool:
        ok = super().run()
        df = pl.read_parquet(self.output_path)
        with write_connection() as con:
            for col, ddl in {
                "raw_title": "VARCHAR",
                "raw_ticker": "VARCHAR",
                "matched_district_id": "VARCHAR",
                "match_confidence": "VARCHAR",
            }.items():
                con.execute(f"ALTER TABLE kalshi_market_mapping ADD COLUMN IF NOT EXISTS {col} {ddl}")
            con.execute("DELETE FROM kalshi_market_mapping")
            if df.height > 0:
                cols = [
                    "market_id", "ticker", "district_id", "chamber", "market_title", "raw_title", "raw_ticker",
                    "matched_district_id", "match_confidence", "yes_price", "no_price", "volume_24h",
                    "open_interest", "last_price_change", "fetched_at",
                ]
                for col in cols:
                    if col not in df.columns:
                        df = df.with_columns(pl.lit(None).alias(col))
                con.register("kalshi_df", df)
                con.execute(f"INSERT OR REPLACE INTO kalshi_market_mapping ({', '.join(cols)}) SELECT {', '.join(cols)} FROM kalshi_df")
        return ok


if __name__ == "__main__":
    raise SystemExit(0 if KalshiScraper().run() else 1)
