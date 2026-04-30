from __future__ import annotations

import hashlib
import os
import sys
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import quote_plus

import feedparser
import httpx
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
load_dotenv(Path(__file__).resolve().parents[1] / ".env")

from db.connection import get_read_connection, write_connection
from scrapers.race_web_scraper import STATE_NAMES, RaceWebScraper


USER_AGENT = "Mozilla/5.0 TAPESTRY/1.0 (empty district news fill)"
NEWSAPI_LIMIT = int(os.getenv("EMPTY_DISTRICT_NEWSAPI_LIMIT", "80"))


def parse_date(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    if value:
        for parser in (
            lambda v: datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=None),
            lambda v: parsedate_to_datetime(str(v)).replace(tzinfo=None),
        ):
            try:
                return parser(value)
            except Exception:
                pass
    return datetime.utcnow()


def article_id(district_id: str, url: str, headline: str) -> str:
    return hashlib.sha1(f"{district_id}:{url or headline}".encode("utf-8")).hexdigest()


def empty_districts() -> list[dict]:
    with get_read_connection() as con:
        rows = con.execute(
            """
            WITH c AS (
                SELECT district_id, COUNT(*) n
                FROM race_web_articles
                GROUP BY district_id
            )
            SELECT h.district_id, h.state_abbr, COALESCE(h.state_name, '') AS state_name
            FROM house_roster h
            LEFT JOIN c ON h.district_id = c.district_id
            WHERE COALESCE(c.n, 0) = 0
            ORDER BY h.state_abbr, h.district_id
            """
        ).fetchall()
    return [
        {"district_id": row[0], "state_abbr": row[1], "state_name": row[2] or STATE_NAMES.get(row[1], row[1])}
        for row in rows
    ]


def clean_article(article: dict, state_name: str) -> bool:
    text = f"{article.get('headline') or ''} {article.get('summary') or ''}".lower()
    state = state_name.lower()
    if state not in text:
        return False
    bad = ["latest polls", "who is ahead in", "governor election 2026"]
    if any(pattern in text for pattern in bad):
        return False
    political = ["congress", "house", "election", "midterm", "campaign", "lawmakers", "democrat", "republican", "politics"]
    return any(word in text for word in political)


def fetch_newsapi(query: str, used: int) -> tuple[list[dict], int]:
    key = os.getenv("NEWSAPI_KEY", "")
    if not key or used >= NEWSAPI_LIMIT:
        return [], used
    try:
        r = httpx.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": query.replace('"', ""),
                "language": "en",
                "sortBy": "publishedAt",
                "from": (datetime.utcnow() - timedelta(days=30)).date().isoformat(),
                "pageSize": 10,
                "apiKey": key,
            },
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        used += 1
        if r.status_code == 429:
            return [], NEWSAPI_LIMIT
        r.raise_for_status()
        return [
            {
                "headline": item.get("title") or "",
                "url": item.get("url") or "",
                "source_name": (item.get("source") or {}).get("name") or "NewsAPI",
                "published_at": item.get("publishedAt"),
                "summary": item.get("description") or "",
            }
            for item in r.json().get("articles", [])
        ], used
    except Exception:
        return [], used


def fetch_gdelt(query: str) -> list[dict]:
    try:
        r = httpx.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={"query": query, "mode": "ArtList", "maxrecords": 12, "timespan": "30d", "format": "json"},
            headers={"User-Agent": USER_AGENT},
            timeout=8,
        )
        r.raise_for_status()
        return [
            {
                "headline": item.get("title") or "",
                "url": item.get("url") or "",
                "source_name": item.get("sourcename") or "GDELT",
                "published_at": item.get("seendate"),
                "summary": "",
            }
            for item in r.json().get("articles", [])
        ]
    except Exception:
        return []


def fetch_google(query: str) -> list[dict]:
    try:
        r = httpx.get(
            f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en",
            headers={"User-Agent": feedparser.USER_AGENT},
            timeout=10,
            follow_redirects=True,
        )
        r.raise_for_status()
        parsed = feedparser.parse(r.text)
        rows = []
        for entry in parsed.entries[:8]:
            headline = getattr(entry, "title", "") or ""
            rows.append({
                "headline": headline,
                "url": getattr(entry, "link", "") or "",
                "source_name": getattr(getattr(entry, "source", None), "title", None) or (headline.split(" - ")[-1] if " - " in headline else "Google News"),
                "published_at": getattr(entry, "published", ""),
                "summary": getattr(entry, "summary", ""),
            })
        return rows
    except Exception:
        return []


def queries_for_state(state_name: str) -> list[str]:
    return [
        f'"{state_name}" congressional election 2026',
        f'"{state_name}" House race 2026',
        f'"{state_name}" politics 2026',
        f'"{state_name}" midterm election 2026',
        f'"{state_name}" lawmakers Congress',
    ]


def insert_rows(district: dict, articles: list[dict]) -> int:
    RaceWebScraper()._ensure_schema()
    inserted = 0
    keep = [
        "article_id", "district_id", "candidate_name", "query", "published_at", "headline", "url",
        "source_name", "source_type", "event_type", "incumbent_relevant", "topic_tags", "ideology_tags",
        "salience_score", "sentiment", "race_specific", "summary", "fetched_at",
    ]
    placeholders = ",".join(["?"] * len(keep))
    with write_connection() as con:
        for article in articles[:2]:
            headline = article.get("headline") or ""
            summary = article.get("summary") or ""
            text = f"{headline} {summary}"
            row = {
                "article_id": article_id(district["district_id"], article.get("url") or "", headline),
                "district_id": district["district_id"],
                "candidate_name": None,
                "query": f"{district['state_name']} state political coverage",
                "published_at": parse_date(article.get("published_at")),
                "headline": headline,
                "url": article.get("url") or "",
                "source_name": article.get("source_name") or "Current News",
                "source_type": "STATE COVERAGE",
                "event_type": "race",
                "incumbent_relevant": False,
                "topic_tags": RaceWebScraper._tags(text),
                "ideology_tags": [],
                "salience_score": 0.32,
                "sentiment": RaceWebScraper._sentiment(text),
                "race_specific": False,
                "summary": summary,
                "fetched_at": datetime.utcnow(),
            }
            con.execute(
                f"INSERT OR REPLACE INTO race_web_articles ({', '.join(keep)}) VALUES ({placeholders})",
                [row[col] for col in keep],
            )
            inserted += 1
        con.commit()
    return inserted


def main() -> None:
    districts = empty_districts()
    by_state: dict[str, list[dict]] = {}
    for district in districts:
        by_state.setdefault(district["state_abbr"], []).append(district)

    newsapi_used = 0
    total_inserted = 0
    state_articles: dict[str, list[dict]] = {}
    for state_abbr, items in by_state.items():
        state_name = items[0]["state_name"]
        raw: list[dict] = []
        for query in queries_for_state(state_name):
            rows, newsapi_used = fetch_newsapi(query, newsapi_used)
            raw.extend(rows)
            raw.extend(fetch_gdelt(query))
            if len(raw) < 2:
                raw.extend(fetch_google(query))
            if len(raw) >= 4:
                break
        seen: set[str] = set()
        clean = []
        for article in raw:
            key = article.get("url") or article.get("headline")
            if key in seen:
                continue
            seen.add(key)
            if clean_article(article, state_name):
                clean.append(article)
        state_articles[state_abbr] = clean
        print(f"{state_abbr}: {len(clean)} clean state articles")
        for district in items:
            n = insert_rows(district, clean)
            total_inserted += n
            print(f"  {district['district_id']}: +{n}")

    print(f"Inserted rows: {total_inserted}")
    print(f"NewsAPI requests used: {newsapi_used}/{NEWSAPI_LIMIT}")


if __name__ == "__main__":
    main()
