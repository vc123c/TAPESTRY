from __future__ import annotations

import hashlib
import json
import re
import os
import time
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote_plus

import feedparser
import httpx
import polars as pl
from bs4 import BeautifulSoup

from db.connection import ROOT, get_read_connection, write_connection
from scrapers.base import BaseScraper
from utils.geo import normalize_district_id

feedparser.USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


STATE_NAMES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi",
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania",
    "RI": "Rhode Island", "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee",
    "TX": "Texas", "UT": "Utah", "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}

POSITIVE = ["leads", "surge", "popular", "wins", "strong", "ahead", "endorsed", "gains"]
NEGATIVE = ["trails", "scandal", "criticized", "behind", "unpopular", "faces", "slipping", "indicted", "controversy", "questioned", "struggles"]
TOPICS = {
    "economy": ["jobs", "inflation", "prices", "economy", "unemployment", "wages"],
    "war": ["iran", "war", "military", "conflict", "troops", "escalation", "hormuz"],
    "healthcare": ["healthcare", "medicare", "insurance", "hospital", "drug"],
    "immigration": ["immigration", "border", "migrant", "deportation"],
    "scandal": ["scandal", "indicted", "accused", "investigation", "corruption"],
    "housing": ["housing", "rent", "mortgage", "affordable", "eviction"],
    "local": ["county", "city", "district", "local", "community", "school"],
}


class RaceWebScraper(BaseScraper):
    source_name = "race_web"
    output_path = "data/raw/race_web_latest.parquet"

    def __init__(self) -> None:
        super().__init__()
        self.newsapi_max_requests = int(os.getenv("NEWSAPI_MAX_REQUESTS", "75"))
        self.newsapi_requests_used = 0
        self.newsapi_disabled = os.getenv("RACE_WEB_SKIP_NEWSAPI", "0") == "1"

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    @staticmethod
    def _parse_date(value) -> datetime:
        if isinstance(value, datetime):
            return value.replace(tzinfo=None)
        if value:
            try:
                return parsedate_to_datetime(str(value)).replace(tzinfo=None)
            except Exception:
                try:
                    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception:
                    pass
        return datetime.utcnow()

    @staticmethod
    def _sentiment(text: str) -> str:
        low = text.lower()
        if any(word in low for word in NEGATIVE):
            return "NEGATIVE"
        if any(word in low for word in POSITIVE):
            return "POSITIVE"
        return "NEUTRAL"

    @staticmethod
    def _tags(text: str) -> list[str]:
        low = text.lower()
        return [topic for topic, words in TOPICS.items() if any(word in low for word in words)]

    @staticmethod
    def _article_id(district_id: str, url: str, headline: str) -> str:
        return hashlib.sha1(f"{district_id}:{url or headline}".encode("utf-8")).hexdigest()

    def _ensure_schema(self) -> None:
        with write_connection() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS race_web_articles (
                    article_id VARCHAR PRIMARY KEY,
                    district_id VARCHAR,
                    candidate_name VARCHAR,
                    query TEXT,
                    published_at TIMESTAMP,
                    headline TEXT,
                    url TEXT,
                    source_name VARCHAR,
                    source_type VARCHAR,
                    event_type VARCHAR,
                    incumbent_relevant BOOLEAN,
                    topic_tags VARCHAR[],
                    ideology_tags VARCHAR[],
                    salience_score DOUBLE,
                    sentiment VARCHAR,
                    race_specific BOOLEAN DEFAULT TRUE,
                    summary TEXT,
                    fetched_at TIMESTAMP
                )
                """
            )
            for col, ddl in {
                "incumbent_relevant": "BOOLEAN",
                "race_specific": "BOOLEAN DEFAULT TRUE",
                "event_type": "VARCHAR",
                "ideology_tags": "VARCHAR[]",
                "salience_score": "DOUBLE",
                "summary": "TEXT",
                "query": "TEXT",
            }.items():
                con.execute(f"ALTER TABLE race_web_articles ADD COLUMN IF NOT EXISTS {col} {ddl}")

    def _districts(self) -> list[dict]:
        with get_read_connection() as con:
            rows = con.execute(
                """
                SELECT
                    h.district_id,
                    h.state_abbr,
                    h.state_name,
                    h.district_number,
                    h.incumbent_name,
                    h.incumbent_party,
                    h.cook_pvi_numeric,
                    list(c.candidate_name) FILTER (WHERE c.candidate_name IS NOT NULL) AS candidates
                FROM house_roster h
                LEFT JOIN candidate_roster_2026 c ON c.district_id=h.district_id
                WHERE h.district_number IS NOT NULL
                GROUP BY h.district_id, h.state_abbr, h.state_name, h.district_number, h.incumbent_name, h.incumbent_party, h.cook_pvi_numeric
                ORDER BY ABS(COALESCE(h.cook_pvi_numeric, 99)), h.district_id
                """
            ).fetchall()
        districts = []
        for row in rows:
            pvi = row[6]
            threshold = float(__import__("os").getenv("RACE_WEB_PVI_THRESHOLD", "20"))
            if pvi is None or abs(float(pvi)) >= threshold:
                continue
            districts.append({
                "district_id": normalize_district_id(row[0]),
                "state_abbr": row[1],
                "state_name": row[2] or STATE_NAMES.get(row[1], row[1]),
                "district_number": row[3],
                "incumbent_name": row[4],
                "incumbent_party": row[5],
                "cook_pvi_numeric": pvi,
                "candidates": [name for name in (row[7] or []) if name],
            })
        return districts

    def _cache_path(self, district_id: str) -> Path:
        return ROOT / "data" / "raw" / f"race_news_{district_id}_{datetime.utcnow().date().isoformat()}.json"

    def _cache_fresh(self, district_id: str) -> Path | None:
        raw = ROOT / "data" / "raw"
        matches = sorted(raw.glob(f"race_news_{district_id}_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if matches and datetime.utcnow() - datetime.utcfromtimestamp(matches[0].stat().st_mtime) < timedelta(days=1):
            return matches[0]
        return None

    def _queries(self, district: dict) -> list[tuple[str, str | None]]:
        queries: list[tuple[str, str | None]] = []
        incumbent = district.get("incumbent_name")
        if incumbent:
            queries.append((f'"{incumbent}" {district["state_abbr"]} congress 2026', incumbent))
            queries.append((f'"{incumbent}" reelection 2026', incumbent))
        for candidate in district.get("candidates", [])[:4]:
            if candidate and candidate != incumbent:
                queries.append((f'"{candidate}" congress 2026', candidate))
        state = district.get("state_name") or STATE_NAMES.get(district["state_abbr"], district["state_abbr"])
        number = district.get("district_number")
        suffix = "th" if number not in [1, 2, 3] else {1: "st", 2: "nd", 3: "rd"}[number]
        queries.append((f'"{state}" {number}{suffix} congressional 2026', None))
        queries.append((f'"{state}" House race 2026 district {number}', None))
        queries.append((f'"{state}" "{number}" congressional race 2026', None))
        return queries

    def _row(self, district: dict, query: str, candidate_name: str | None, headline: str, url: str, source_name: str, source_type: str, published_at: datetime, summary: str = "") -> dict:
        text = f"{headline} {summary}"
        names = [district.get("incumbent_name") or "", *district.get("candidates", [])]
        return {
            "article_id": self._article_id(district["district_id"], url, headline),
            "district_id": district["district_id"],
            "candidate_name": candidate_name,
            "query": query,
            "published_at": published_at,
            "headline": headline,
            "url": url,
            "source_name": source_name,
            "source_type": source_type,
            "event_type": "race",
            "incumbent_relevant": any(name and name.lower() in text.lower() for name in names),
            "topic_tags": self._tags(text),
            "ideology_tags": [],
            "salience_score": 0.5,
            "sentiment": self._sentiment(text),
            "race_specific": True,
            "summary": summary,
            "fetched_at": datetime.utcnow(),
        }

    def _google_news(self, district: dict, query: str, candidate_name: str | None) -> list[dict]:
        url = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        try:
            response = httpx.get(url, timeout=10, follow_redirects=True, headers={"User-Agent": feedparser.USER_AGENT})
        except Exception as exc:
            self.logger.warning("Google News failed for %s query=%s error=%s", district["district_id"], query, type(exc).__name__)
            return []
        if response.status_code == 429:
            time.sleep(30)
            response = httpx.get(url, timeout=10, follow_redirects=True, headers={"User-Agent": feedparser.USER_AGENT})
            if response.status_code == 429:
                self.logger.warning("Google News rate-limited for query=%s", query)
                return []
        if response.status_code >= 400:
            self.logger.warning("Google News failed status=%s query=%s", response.status_code, query)
            return []
        parsed = feedparser.parse(response.text)
        rows = []
        for entry in parsed.entries[:8]:
            headline = getattr(entry, "title", "") or ""
            link = getattr(entry, "link", "") or ""
            source = getattr(getattr(entry, "source", None), "title", None) or (headline.split(" - ")[-1] if " - " in headline else "Google News")
            rows.append(self._row(district, query, candidate_name, headline, link, source, "google_news", self._parse_date(getattr(entry, "published", "")), getattr(entry, "summary", "")))
        time.sleep(1.0)
        return rows

    def _newsapi(self, district: dict, query: str, candidate_name: str | None) -> list[dict]:
        key = os.getenv("NEWSAPI_KEY")
        if not key or self.newsapi_disabled:
            return []
        if self.newsapi_requests_used >= self.newsapi_max_requests:
            return []
        try:
            search = f"{candidate_name} congress 2026" if candidate_name else query.replace('"', "")
            self.newsapi_requests_used += 1
            response = httpx.get(
                "https://newsapi.org/v2/everything",
                params={"q": search, "language": "en", "sortBy": "publishedAt", "pageSize": 5, "apiKey": key},
                timeout=10,
            )
            if response.status_code == 429:
                self.logger.warning("NewsAPI rate limit hit after %s requests; disabling for this run", self.newsapi_requests_used)
                self.newsapi_disabled = True
                time.sleep(30)
                if self.newsapi_requests_used >= self.newsapi_max_requests:
                    return []
                self.newsapi_requests_used += 1
                response = httpx.get(
                    "https://newsapi.org/v2/everything",
                    params={"q": search, "language": "en", "sortBy": "publishedAt", "pageSize": 5, "apiKey": key},
                    timeout=10,
                )
            response.raise_for_status()
            rows = []
            for article in response.json().get("articles", [])[:5]:
                rows.append(self._row(
                    district, query, candidate_name, article.get("title") or "", article.get("url") or "",
                    (article.get("source") or {}).get("name") or "NewsAPI", "newsapi", self._parse_date(article.get("publishedAt")), article.get("description") or "",
                ))
            return rows
        except Exception as exc:
            self.logger.warning("NewsAPI failed for %s query=%s error=%s", district["district_id"], query, type(exc).__name__)
            return []

    def _gdelt(self, district: dict, query: str, candidate_name: str | None) -> list[dict]:
        params = {"query": query, "mode": "ArtList", "maxrecords": 15, "timespan": "30d", "format": "json"}
        rows = []
        try:
            data = httpx.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=8).json()
            for article in data.get("articles", [])[:15]:
                rows.append(self._row(
                    district,
                    query,
                    candidate_name,
                    article.get("title") or "",
                    article.get("url") or "",
                    article.get("sourcename") or "GDELT",
                    "gdelt",
                    self._parse_date(article.get("seendate")),
                    "",
                ))
        except Exception as exc:
            self.logger.warning("Race GDELT failed for %s query=%s error=%s", district["district_id"], query, type(exc).__name__)
        time.sleep(1.0)
        return rows

    def _ballotpedia(self, district: dict) -> list[dict]:
        number = district.get("district_number")
        if not number:
            return []
        state = (district.get("state_name") or "").replace(" ", "_")
        suffix = "th" if number not in [1, 2, 3] else {1: "st", 2: "nd", 3: "rd"}[number]
        url = f"https://ballotpedia.org/{state}_{number}{suffix}_congressional_district,_2026"
        cache = ROOT / "data" / "raw" / f"ballotpedia_race_{district['district_id']}.html"
        html = ""
        try:
            if cache.exists() and datetime.utcnow() - datetime.utcfromtimestamp(cache.stat().st_mtime) < timedelta(days=7):
                html = cache.read_text(encoding="utf-8", errors="ignore")
            else:
                response = httpx.get(url, timeout=20, follow_redirects=True)
                response.raise_for_status()
                html = response.text
                cache.write_text(html, encoding="utf-8")
        except Exception as exc:
            self.logger.warning("Ballotpedia race page failed for %s url=%s error=%s", district["district_id"], url, type(exc).__name__)
            return []
        finally:
            time.sleep(2.0)
        soup = BeautifulSoup(html, "html.parser")
        rows = []
        for link in soup.find_all("a", href=True):
            text = " ".join(link.get_text(" ", strip=True).split())
            href = link["href"]
            if len(text) < 20 or not re.search(r"election|campaign|candidate|congress|primary|debate", text, re.I):
                continue
            rows.append(self._row(district, "Ballotpedia race page news", None, text, href if href.startswith("http") else f"https://ballotpedia.org{href}", "Ballotpedia", "ballotpedia", datetime.utcnow()))
            if len(rows) >= 5:
                break
        return rows

    def _persist_rows(self, rows: list[dict]) -> None:
        if not rows:
            return
        keep = [
            "article_id", "district_id", "candidate_name", "query", "published_at", "headline", "url",
            "source_name", "source_type", "event_type", "incumbent_relevant", "topic_tags", "ideology_tags",
            "salience_score", "sentiment", "race_specific", "summary", "fetched_at",
        ]
        df = pl.DataFrame(rows, infer_schema_length=10000)
        for col in keep:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))
        self._ensure_schema()
        with write_connection() as con:
            con.register("race_web_incremental_df", df.select(keep))
            con.execute(
                f"""
                INSERT OR REPLACE INTO race_web_articles ({", ".join(keep)})
                SELECT {", ".join(keep)} FROM race_web_incremental_df
                """
            )

    def fetch(self) -> pl.DataFrame:
        self._ensure_schema()
        rows: list[dict] = []
        districts = self._districts()
        max_districts = int(__import__("os").getenv("RACE_WEB_MAX_DISTRICTS", "120"))
        attempted = with_articles = 0
        force = __import__("os").getenv("RACE_WEB_FORCE", "").lower() in {"1", "true", "yes"}
        fast = os.getenv("RACE_WEB_FAST", "").lower() in {"1", "true", "yes"}
        for district in districts[:max_districts]:
            attempted += 1
            cached = None if force else self._cache_fresh(district["district_id"])
            if cached:
                try:
                    rows.extend(json.loads(cached.read_text(encoding="utf-8")))
                    continue
                except Exception:
                    pass
            district_rows = []
            for query, candidate_name in self._queries(district):
                district_rows.extend(self._newsapi(district, query, candidate_name))
                if os.getenv("RACE_WEB_SKIP_GDELT", "0") != "1":
                    district_rows.extend(self._gdelt(district, query, candidate_name))
                if not fast and os.getenv("RACE_WEB_SKIP_GOOGLE", "0") != "1":
                    district_rows.extend(self._google_news(district, query, candidate_name))
            if not fast and os.getenv("RACE_WEB_SKIP_BALLOTPEDIA", "0") != "1":
                district_rows.extend(self._ballotpedia(district))
            seen = set()
            deduped = []
            for row in district_rows:
                key = row.get("url") or row["article_id"]
                if not key or key in seen:
                    continue
                seen.add(key)
                deduped.append(row)
            self._cache_path(district["district_id"]).write_text(json.dumps(deduped, default=str, indent=2), encoding="utf-8")
            if deduped:
                with_articles += 1
                self._persist_rows(deduped)
            if attempted % 5 == 0:
                sources = {}
                for row in deduped:
                    sources[row.get("source_type") or "unknown"] = sources.get(row.get("source_type") or "unknown", 0) + 1
                source_text = ", ".join(f"{count} {source}" for source, count in sorted(sources.items()))
                self.logger.info("[%s/%s] %s: %s articles (%s)", attempted, min(len(districts), max_districts), district["district_id"], len(deduped), source_text)
            rows.extend(deduped)
        self.logger.info(
            "Race web summary: districts attempted=%s with_articles=%s total_articles=%s newsapi_requests=%s/%s",
            attempted,
            with_articles,
            len(rows),
            self.newsapi_requests_used,
            self.newsapi_max_requests,
        )
        if not rows:
            return pl.DataFrame({"article_id": [], "district_id": [], "headline": []})
        return pl.DataFrame(rows, infer_schema_length=10000)

    def run(self) -> bool:
        ok = super().run()
        try:
            df = pl.read_parquet(self.output_path)
            if df.height == 0:
                return ok
            keep = [
                "article_id", "district_id", "candidate_name", "query", "published_at", "headline", "url",
                "source_name", "source_type", "event_type", "incumbent_relevant", "topic_tags", "ideology_tags",
                "salience_score", "sentiment", "race_specific", "summary", "fetched_at",
            ]
            for col in keep:
                if col not in df.columns:
                    df = df.with_columns(pl.lit(None).alias(col))
            self._ensure_schema()
            with write_connection() as con:
                con.register("race_web_df", df.select(keep))
                con.execute(
                    f"""
                    INSERT OR REPLACE INTO race_web_articles ({", ".join(keep)})
                    SELECT {", ".join(keep)} FROM race_web_df
                    """
                )
        except Exception as exc:
            self.logger.warning("Could not persist race web rows: %s", exc)
            return False
        self.logger.info("Race web scraper: wrote %s article rows", df.height)
        return ok


if __name__ == "__main__":
    raise SystemExit(0 if RaceWebScraper().run() else 1)
