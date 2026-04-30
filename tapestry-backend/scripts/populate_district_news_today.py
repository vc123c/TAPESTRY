from __future__ import annotations

import hashlib
import os
import re
import sys
import time
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


USER_AGENT = "Mozilla/5.0 TAPESTRY/1.0 (submission news coverage)"
MIN_ARTICLES = int(os.getenv("DISTRICT_NEWS_MIN_ARTICLES", "2"))
MAX_DIRECT_DISTRICTS = int(os.getenv("DISTRICT_NEWS_MAX_DIRECT", "435"))
NEWSAPI_LIMIT = int(os.getenv("DISTRICT_NEWSAPI_LIMIT", "125"))
GENERIC_POLL_PATTERNS = [
    "latest polls - the new york times",
    "the race for congress: latest 2026 polls",
    "who is ahead in",
    "governor election 2026: latest polls",
    "u.s. senate election 2026: latest polls",
]


ORDINAL_WORDS = {
    1: "first",
    2: "second",
    3: "third",
    4: "fourth",
    5: "fifth",
    6: "sixth",
    7: "seventh",
    8: "eighth",
    9: "ninth",
    10: "tenth",
    11: "11th",
    12: "12th",
    13: "13th",
    14: "14th",
    15: "15th",
    16: "16th",
    17: "17th",
    18: "18th",
    19: "19th",
    20: "20th",
    21: "21st",
    22: "22nd",
    23: "23rd",
    24: "24th",
    25: "25th",
    26: "26th",
    27: "27th",
    28: "28th",
    29: "29th",
    30: "30th",
    31: "31st",
    32: "32nd",
    33: "33rd",
    34: "34th",
    35: "35th",
    36: "36th",
    37: "37th",
    38: "38th",
    39: "39th",
    40: "40th",
    41: "41st",
    42: "42nd",
    43: "43rd",
    44: "44th",
    45: "45th",
    46: "46th",
    47: "47th",
    48: "48th",
    49: "49th",
    50: "50th",
    51: "51st",
    52: "52nd",
}


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


def district_rows():
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT
                h.district_id,
                h.state_abbr,
                h.state_name,
                h.district_number,
                h.incumbent_name,
                list(c.candidate_name) FILTER (WHERE c.candidate_name IS NOT NULL) AS candidates
            FROM house_roster h
            LEFT JOIN candidate_roster_2026 c ON c.district_id = h.district_id
            GROUP BY h.district_id, h.state_abbr, h.state_name, h.district_number, h.incumbent_name
            ORDER BY h.state_abbr, h.district_number NULLS LAST, h.district_id
            """
        ).fetchall()
    return [
        {
            "district_id": row[0],
            "state_abbr": row[1],
            "state_name": row[2] or STATE_NAMES.get(row[1], row[1]),
            "district_number": row[3],
            "incumbent_name": row[4],
            "candidates": [name for name in (row[5] or []) if name],
        }
        for row in rows
    ]


def current_counts() -> dict[str, int]:
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT district_id, COUNT(*) AS n
            FROM race_web_articles
            GROUP BY district_id
            """
        ).fetchall()
    return {row[0]: int(row[1]) for row in rows}


def district_query(district: dict) -> str:
    incumbent = str(district.get("incumbent_name") or "").strip()
    if incumbent and not incumbent.lower().startswith("vacant"):
        return f'"{incumbent}" congress 2026'
    state = district["state_name"]
    number = district.get("district_number")
    if number:
        return f'"{state}" "{number}" congressional district 2026'
    return f'"{state}" congressional election 2026'


def state_query(state_name: str) -> str:
    return f'"{state_name}" congressional election 2026 OR "{state_name}" House race 2026'


def state_queries(state_name: str, capital: str | None = None) -> list[str]:
    queries = [
        f'"{state_name}" congressional election 2026',
        f'"{state_name}" House race 2026',
        f'"{state_name}" politics 2026',
        f'"{state_name}" midterm election 2026',
    ]
    if capital:
        queries.append(f'"{capital}" politics 2026')
    return queries


def relevant(headline: str, district: dict, allow_state_only: bool = False) -> bool:
    text = (headline or "").lower()
    state = str(district["state_name"]).lower()
    state_abbr = str(district["state_abbr"]).lower()
    district_id = district["district_id"].lower()
    number = district.get("district_number")
    incumbent = str(district.get("incumbent_name") or "").lower()
    if is_generic_or_wrong_state(text, state):
        return False
    candidate_names = []
    for name in [incumbent, *[str(c).lower() for c in district.get("candidates", [])]]:
        parts = [p for p in name.replace(".", " ").split() if len(p) > 2]
        if parts:
            candidate_names.append((parts[0], parts[-1]))
    if any(first in text and last in text for first, last in candidate_names):
        return True
    if district_id in text:
        return True
    if number and state in text and district_number_in_text(text, int(number)):
        return True
    if state in text and any(word in text for word in ["congress", "house", "election", "district", "primary", "campaign"]):
        return True
    if f"{state_abbr}-" in text and any(word in text for word in ["congress", "house", "election", "district", "primary", "campaign"]):
        return True
    return allow_state_only and state in text and not reassigns_other_state_poll(text, state)


def district_number_in_text(text: str, number: int) -> bool:
    terms = {str(number), f"{number:02d}", ordinal_suffix(number), ORDINAL_WORDS.get(number, "")}
    return any(term and re.search(rf"\b{re.escape(term)}\b", text) for term in terms)


def ordinal_suffix(number: int) -> str:
    if 10 <= number % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(number % 10, "th")
    return f"{number}{suffix}"


def is_generic_or_wrong_state(text: str, state_name: str) -> bool:
    low = text.lower()
    if any(pattern in low for pattern in GENERIC_POLL_PATTERNS):
        return True
    mentioned_states = [name.lower() for name in STATE_NAMES.values() if name.lower() in low]
    return bool(mentioned_states and state_name.lower() not in mentioned_states)


def reassigns_other_state_poll(text: str, state_name: str) -> bool:
    low = text.lower()
    if "latest polls" not in low:
        return False
    if state_name.lower() in low:
        return False
    return any(name.lower() in low for name in STATE_NAMES.values())


def make_row(district: dict, query: str, article: dict, source_type: str, state_fallback: bool = False) -> dict:
    headline = article.get("headline") or article.get("title") or ""
    url = article.get("url") or ""
    source_name = article.get("source_name") or article.get("source") or source_type
    summary = article.get("summary") or article.get("description") or ""
    published_at = parse_date(article.get("published_at") or article.get("publishedAt") or article.get("seendate"))
    text = f"{headline} {summary}"
    tags = RaceWebScraper._tags(text)
    return {
        "article_id": article_id(district["district_id"], url, headline),
        "district_id": district["district_id"],
        "candidate_name": None,
        "query": query,
        "published_at": published_at,
        "headline": headline,
        "url": url,
        "source_name": source_name,
        "source_type": "STATE COVERAGE" if state_fallback else source_type,
        "event_type": "race",
        "incumbent_relevant": relevant(headline, district),
        "topic_tags": tags,
        "ideology_tags": [],
        "salience_score": 0.35 if state_fallback else 0.55,
        "sentiment": RaceWebScraper._sentiment(text),
        "race_specific": not state_fallback,
        "summary": summary,
        "fetched_at": datetime.utcnow(),
    }


def fetch_gdelt(query: str, maxrecords: int = 5) -> list[dict]:
    try:
        r = httpx.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params={"query": query, "mode": "ArtList", "maxrecords": maxrecords, "timespan": "30d", "format": "json"},
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
            }
            for item in r.json().get("articles", [])
        ]
    except Exception:
        return []


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
                "pageSize": 5,
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


def fetch_google(query: str, maxrecords: int = 3) -> list[dict]:
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
        for entry in parsed.entries[:maxrecords]:
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


def persist(rows: list[dict]) -> None:
    if not rows:
        return
    RaceWebScraper()._ensure_schema()
    keep = [
        "article_id", "district_id", "candidate_name", "query", "published_at", "headline", "url",
        "source_name", "source_type", "event_type", "incumbent_relevant", "topic_tags", "ideology_tags",
        "salience_score", "sentiment", "race_specific", "summary", "fetched_at",
    ]
    placeholders = ",".join(["?"] * len(keep))
    with write_connection() as con:
        for row in rows:
            con.execute(
                f"INSERT OR REPLACE INTO race_web_articles ({', '.join(keep)}) VALUES ({placeholders})",
                [row.get(col) for col in keep],
            )


def main() -> None:
    districts = district_rows()
    counts = current_counts()
    state_cache: dict[str, list[dict]] = {}
    newsapi_used = 0
    inserted = 0
    direct_attempted = 0

    for district in districts:
        have = counts.get(district["district_id"], 0)
        if have >= MIN_ARTICLES:
            continue
        direct_rows = []
        if direct_attempted < MAX_DIRECT_DISTRICTS:
            query = district_query(district)
            raw = []
            newsapi_rows, newsapi_used = fetch_newsapi(query, newsapi_used)
            raw.extend(newsapi_rows)
            raw.extend(fetch_gdelt(query, maxrecords=5))
            if not raw:
                raw.extend(fetch_google(query, maxrecords=3))
            for item in raw:
                if item.get("headline") and item.get("url") and relevant(item["headline"], district):
                    direct_rows.append(make_row(district, query, item, item.get("source_type") or "CURRENT NEWS"))
            direct_attempted += 1
            time.sleep(0.35)

        rows = direct_rows[: max(0, MIN_ARTICLES - have)]
        if len(rows) + have < MIN_ARTICLES:
            state = district["state_abbr"]
            if state not in state_cache:
                raw = []
                for query in state_queries(district["state_name"]):
                    if newsapi_used < NEWSAPI_LIMIT:
                        newsapi_rows, newsapi_used = fetch_newsapi(query, newsapi_used)
                        raw.extend(newsapi_rows)
                    raw.extend(fetch_gdelt(query, maxrecords=8))
                    if len(raw) < 2:
                        raw.extend(fetch_google(query, maxrecords=5))
                    if len(raw) >= 6:
                        break
                state_cache[state] = raw
                time.sleep(0.5)
            for item in state_cache.get(state, []):
                if item.get("headline") and item.get("url") and relevant(item["headline"], district, allow_state_only=True):
                    rows.append(make_row(district, state_query(district["state_name"]), item, "STATE COVERAGE", state_fallback=True))
                if len(rows) + have >= MIN_ARTICLES:
                    break

        persist(rows)
        inserted += len(rows)
        if rows:
            counts[district["district_id"]] = have + len(rows)
        print(f"{district['district_id']}: {have} -> {counts.get(district['district_id'], have)} articles")

    final_counts = current_counts()
    covered = sum(1 for d in districts if final_counts.get(d["district_id"], 0) >= MIN_ARTICLES)
    any_covered = sum(1 for d in districts if final_counts.get(d["district_id"], 0) > 0)
    print("\nDistrict news population complete")
    print(f"Inserted rows: {inserted}")
    print(f"Districts with >= {MIN_ARTICLES} articles: {covered}/{len(districts)}")
    print(f"Districts with any articles: {any_covered}/{len(districts)}")
    print(f"NewsAPI requests used: {newsapi_used}/{NEWSAPI_LIMIT}")


if __name__ == "__main__":
    main()
