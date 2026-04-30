from __future__ import annotations

import hashlib
import os
import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import feedparser
import httpx
import polars as pl

from db.connection import get_read_connection, write_connection
from scrapers.base import BaseScraper
from utils.geo import COMPETITIVE_DISTRICTS, STATE_FIPS, state_from_district

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

RSS_FEEDS = {
    "AZ": [("Arizona Republic", "newspaper", "https://www.azcentral.com/rss/news/"), ("12News Phoenix", "tv", "https://www.12news.com/feeds/syndication/rss/news/local")],
    "NV": [("Las Vegas Review-Journal", "newspaper", "https://www.reviewjournal.com/feed/"), ("KTNV", "tv", "https://www.ktnv.com/news.rss")],
    "PA": [("Pittsburgh Post-Gazette", "newspaper", "https://www.post-gazette.com/rss/local"), ("PennLive", "newspaper", "https://www.pennlive.com/arc/outboundfeeds/rss/?outputType=xml")],
    "WI": [("Milwaukee Journal Sentinel", "newspaper", "https://www.jsonline.com/rss/news/"), ("WISC-TV", "tv", "https://www.channel3000.com/news/?outputType=xml")],
    "MI": [("Detroit Free Press", "newspaper", "https://www.freep.com/rss/news/"), ("MLive", "newspaper", "https://www.mlive.com/arc/outboundfeeds/rss/?outputType=xml")],
    "GA": [("Atlanta Journal-Constitution", "newspaper", "https://www.ajc.com/arc/outboundfeeds/rss/"), ("WSB-TV", "tv", "https://www.wsbtv.com/news/?outputType=xml")],
    "NC": [("Charlotte Observer", "newspaper", "https://www.charlotteobserver.com/news/?widgetName=rssfeed&widgetContentId=712015&getXmlFeed=true"), ("WRAL", "tv", "https://www.wral.com/news/local/?output=rss")],
    "OH": [("Cleveland Plain Dealer", "newspaper", "https://www.cleveland.com/arc/outboundfeeds/rss/?outputType=xml"), ("Columbus Dispatch", "newspaper", "https://www.dispatch.com/rss/news/")],
    "MT": [("Billings Gazette", "newspaper", "https://billingsgazette.com/search/?f=rss&t=article&c=news/local"), ("KTVQ", "tv", "https://www.ktvq.com/news.rss")],
    "NH": [("Union Leader", "newspaper", "https://www.unionleader.com/search/?f=rss&t=article&c=news"), ("WMUR", "tv", "https://www.wmur.com/topstories-rss")],
    "ME": [("Portland Press Herald", "newspaper", "https://www.pressherald.com/feed/"), ("WGME", "tv", "https://wgme.com/news/local/rss")],
    "CO": [("Denver Post", "newspaper", "https://www.denverpost.com/feed/"), ("9News", "tv", "https://www.9news.com/feeds/syndication/rss/news/local")],
    "VA": [("Richmond Times-Dispatch", "newspaper", "https://richmond.com/search/?f=rss&t=article&c=news"), ("WTVR", "tv", "https://www.wtvr.com/news.rss")],
    "TX": [("Houston Chronicle", "newspaper", "https://www.houstonchronicle.com/rss/feed/News-270.php"), ("Dallas Morning News", "newspaper", "https://www.dallasnews.com/arc/outboundfeeds/rss/?outputType=xml")],
    "FL": [("Tampa Bay Times", "newspaper", "https://www.tampabay.com/arc/outboundfeeds/rss/"), ("Miami Herald", "newspaper", "https://www.miamiherald.com/news/?widgetName=rssfeed&widgetContentId=712015&getXmlFeed=true")],
}

NATIONAL_FEEDS = [
    ("Drop Site News", "independent", "https://www.dropsitenews.com/feed"),
    ("Drop Site News Podcast", "independent", "https://api.substack.com/feed/podcast/2510348/s/153051.rss"),
    ("The Intercept", "independent", "https://theintercept.com/feed"),
    ("The Intercept Politics", "independent", "https://theintercept.com/politics/feed"),
    ("The Lever", "independent", "https://www.levernews.com/feed"),
    ("Axios Politics", "mainstream", "https://www.axios.com/politics-policy/feed"),
]

DEFAULT_FEEDS = {
    "AL": [("AL.com", "newspaper", "https://www.al.com/arc/outboundfeeds/rss/?outputType=xml")],
    "AK": [("Anchorage Daily News", "newspaper", "https://www.adn.com/arc/outboundfeeds/rss/")],
    "AR": [("Arkansas Democrat-Gazette", "newspaper", "https://www.arkansasonline.com/news/?f=rss")],
    "CA": [("Los Angeles Times", "newspaper", "https://www.latimes.com/california/rss2.0.xml")],
    "CT": [("CT Mirror", "newspaper", "https://ctmirror.org/feed/")],
    "DE": [("Delaware Online", "newspaper", "https://www.delawareonline.com/rss/news/")],
    "HI": [("Honolulu Civil Beat", "newspaper", "https://www.civilbeat.org/feed/")],
    "ID": [("Idaho Statesman", "newspaper", "https://www.idahostatesman.com/news/?widgetName=rssfeed&widgetContentId=712015&getXmlFeed=true")],
    "IL": [("Chicago Sun-Times", "newspaper", "https://chicago.suntimes.com/rss/index.xml")],
    "IN": [("IndyStar", "newspaper", "https://www.indystar.com/rss/news/")],
    "IA": [("Des Moines Register", "newspaper", "https://www.desmoinesregister.com/rss/news/")],
    "KS": [("Kansas Reflector", "newspaper", "https://kansasreflector.com/feed/")],
    "KY": [("Louisville Courier Journal", "newspaper", "https://www.courier-journal.com/rss/news/")],
    "LA": [("NOLA.com", "newspaper", "https://www.nola.com/search/?f=rss&t=article&c=news")],
    "MD": [("Maryland Matters", "newspaper", "https://www.marylandmatters.org/feed/")],
    "MA": [("Boston Globe", "newspaper", "https://www.bostonglobe.com/rss/metro")],
    "MN": [("MinnPost", "newspaper", "https://www.minnpost.com/feed/")],
    "MS": [("Mississippi Today", "newspaper", "https://mississippitoday.org/feed/")],
    "MO": [("St. Louis Post-Dispatch", "newspaper", "https://www.stltoday.com/search/?f=rss&t=article&c=news")],
    "NE": [("Nebraska Examiner", "newspaper", "https://nebraskaexaminer.com/feed/")],
    "NJ": [("NJ.com", "newspaper", "https://www.nj.com/arc/outboundfeeds/rss/?outputType=xml")],
    "NM": [("Santa Fe New Mexican", "newspaper", "https://www.santafenewmexican.com/search/?f=rss&t=article&c=news")],
    "NY": [("Gothamist", "newspaper", "https://gothamist.com/rss")],
    "ND": [("North Dakota Monitor", "newspaper", "https://northdakotamonitor.com/feed/")],
    "OK": [("Oklahoma Voice", "newspaper", "https://oklahomavoice.com/feed/")],
    "OR": [("Oregon Capital Chronicle", "newspaper", "https://oregoncapitalchronicle.com/feed/")],
    "RI": [("Rhode Island Current", "newspaper", "https://rhodeislandcurrent.com/feed/")],
    "SC": [("The State", "newspaper", "https://www.thestate.com/news/?widgetName=rssfeed&widgetContentId=712015&getXmlFeed=true")],
    "SD": [("South Dakota Searchlight", "newspaper", "https://southdakotasearchlight.com/feed/")],
    "TN": [("Tennessee Lookout", "newspaper", "https://tennesseelookout.com/feed/")],
    "UT": [("Salt Lake Tribune", "newspaper", "https://www.sltrib.com/arc/outboundfeeds/rss/")],
    "VT": [("VTDigger", "newspaper", "https://vtdigger.org/feed/")],
    "WA": [("Seattle Times", "newspaper", "https://www.seattletimes.com/feed/")],
    "WV": [("West Virginia Watch", "newspaper", "https://westvirginiawatch.com/feed/")],
    "WY": [("WyoFile", "newspaper", "https://wyofile.com/feed/")],
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

CHALLENGER_TERMS = ["announces", "launches", "primary", "challenger", "candidate", "endorsement", "fundraising", "campaign"]
GDELT_PRIORITY_STATES = [
    "CA", "TX", "FL", "NY", "PA", "IL", "OH", "GA", "NC", "MI",
    "AZ", "NV", "WI", "VA", "CO", "NH", "ME", "MT",
]


class LocalNewsScraper(BaseScraper):
    source_name = "local_news"
    output_path = "data/raw/local_news_latest.parquet"

    def _competitive_by_state(self) -> dict[str, list[str]]:
        grouped: dict[str, list[str]] = {}
        for district in COMPETITIVE_DISTRICTS:
            grouped.setdefault(state_from_district(district), []).append(district)
        return grouped

    def _candidate_names(self) -> dict[str, list[str]]:
        names = {district: [] for district in COMPETITIVE_DISTRICTS}
        try:
            with get_read_connection() as con:
                rows = con.execute("SELECT district_id, leading_candidate FROM district_forecasts").fetchall()
            for district_id, candidate in rows:
                if district_id in names and candidate:
                    names[district_id].append(candidate.lower())
        except Exception:
            pass
        return names

    @staticmethod
    def _parse_date(value) -> datetime:
        if isinstance(value, datetime):
            return value
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
    def _id(url: str, district_id: str) -> str:
        return hashlib.sha1(f"{district_id}:{url}".encode("utf-8")).hexdigest()

    def _article_rows(self, state: str, title: str, url: str, source: str, source_type: str, published_at: datetime, summary: str = "", tone: float | None = None) -> list[dict]:
        grouped = self._competitive_by_state()
        candidates = self._candidate_names()
        text = f"{title} {summary}"
        rows = []
        district_ids = COMPETITIVE_DISTRICTS if state == "US" else grouped.get(state, [])
        for district_id in district_ids:
            incumbent_relevant = any(name and name in text.lower() for name in candidates.get(district_id, []))
            rows.append({
                "article_id": self._id(url, district_id),
                "district_id": district_id,
                "state_fips": "00" if state == "US" else STATE_FIPS[state],
                "published_at": published_at,
                "headline": title,
                "url": url,
                "source_name": source,
                "source_type": source_type,
                "incumbent_relevant": incumbent_relevant,
                "sentiment": self._sentiment(text),
                "topic_tags": self._tags(text),
                "gdelt_tone": tone,
                "fetched_at": datetime.utcnow(),
            })
        return rows

    def _fetch_gdelt(self, states: list[str]) -> list[dict]:
        rows = []
        client = httpx.Client(timeout=6)
        ordered = [state for state in GDELT_PRIORITY_STATES if state in states]
        self.logger.info("GDELT local news: best-effort pass over %s priority states", len(ordered))
        for state in ordered:
            try:
                params = {
                    "query": f"{STATE_NAMES[state]} congressional election 2026",
                    "mode": "ArtList",
                    "maxrecords": 10,
                    "timespan": "7d",
                    "format": "json",
                }
                data = client.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params).json()
                for article in data.get("articles", []):
                    rows.extend(self._article_rows(
                        state,
                        article.get("title") or "Untitled local election article",
                        article.get("url") or "",
                        article.get("sourcename") or "GDELT",
                        "gdelt",
                        self._parse_date(article.get("seendate")),
                        "",
                        float(article.get("tone", 0) or 0),
                    ))
            except Exception as exc:
                self.logger.warning("[GDELT timeout] %s - skipping: %s", state, type(exc).__name__)
            time.sleep(0.2)
        client.close()
        return rows

    def _fetch_newsapi(self, states: list[str]) -> list[dict]:
        key = os.getenv("NEWSAPI_KEY", "")
        if not key:
            return []
        rows = []
        for state in states:
            queries = [
                f"{STATE_NAMES[state]} congressional election 2026",
                f"{STATE_NAMES[state]} House race 2026",
                f"{STATE_NAMES[state]} politics 2026",
                f"{STATE_NAMES[state]} midterm 2026",
            ]
            for query in queries:
                try:
                    response = httpx.get(
                        "https://newsapi.org/v2/everything",
                        params={
                            "q": query,
                            "language": "en",
                            "sortBy": "publishedAt",
                            "pageSize": 10,
                            "apiKey": key,
                        },
                        timeout=8,
                    )
                    if response.status_code != 200:
                        continue
                    for article in response.json().get("articles", []):
                        rows.extend(self._article_rows(
                            state,
                            article.get("title") or "Untitled state election article",
                            article.get("url") or "",
                            (article.get("source") or {}).get("name") or "NewsAPI",
                            "newsapi",
                            self._parse_date(article.get("publishedAt")),
                            article.get("description") or "",
                        ))
                except Exception as exc:
                    self.logger.warning("NewsAPI local news failed for %s query=%s error=%s", state, query, type(exc).__name__)
            time.sleep(0.15)
        return rows

    def _fetch_rss(self, states: list[str]) -> list[dict]:
        cutoff = datetime.utcnow() - timedelta(days=7)
        rows = []
        for state in states:
            for source, source_type, feed_url in RSS_FEEDS.get(state, DEFAULT_FEEDS.get(state, [])):
                parsed = feedparser.parse(feed_url)
                for entry in parsed.entries[:20]:
                    published_at = self._parse_date(entry.get("published") or entry.get("updated"))
                    if published_at < cutoff:
                        continue
                    rows.extend(self._article_rows(
                        state,
                        entry.get("title", "Untitled local article"),
                        entry.get("link", feed_url),
                        source,
                        source_type,
                        published_at,
                        entry.get("summary", ""),
                    ))
        return rows

    def _fetch_google_news(self, states: list[str]) -> list[dict]:
        rows = []
        for state in states:
            query = f"{STATE_NAMES[state]} congressional race 2026 OR House campaign"
            feed_url = f"https://news.google.com/rss/search?q={query.replace(' ', '+')}&hl=en-US&gl=US&ceid=US:en"
            try:
                parsed = feedparser.parse(feed_url)
                for entry in parsed.entries[:15]:
                    rows.extend(self._article_rows(
                        state,
                        re.sub(r"\s+-\s+[^-]+$", "", entry.get("title", "Untitled campaign article")),
                        entry.get("link", feed_url),
                        "Google News",
                        "news_search",
                        self._parse_date(entry.get("published") or entry.get("updated")),
                        entry.get("summary", ""),
                    ))
            except Exception as exc:
                self.logger.warning("Google News local feed failed for %s: %s", state, exc)
        return rows

    def _fetch_national_feeds(self) -> list[dict]:
        cutoff = datetime.utcnow() - timedelta(days=7)
        rows = []
        for source, source_type, feed_url in NATIONAL_FEEDS:
            try:
                parsed = feedparser.parse(feed_url)
                for entry in parsed.entries[:30]:
                    published_at = self._parse_date(entry.get("published") or entry.get("updated"))
                    if published_at < cutoff:
                        continue
                    title = entry.get("title", "Untitled national politics article")
                    summary = entry.get("summary", "")
                    tags = self._tags(f"{title} {summary}")
                    if not tags and not any(word in f"{title} {summary}".lower() for word in ["election", "congress", "senate", "house", "campaign", "trump", "democrat", "republican"]):
                        continue
                    rows.extend(self._article_rows(
                        "US",
                        title,
                        entry.get("link", feed_url),
                        source,
                        source_type,
                        published_at,
                        summary,
                    ))
            except Exception as exc:
                self.logger.warning("National feed failed for %s: %s", source, exc)
        return rows

    def fetch(self) -> pl.DataFrame:
        states = sorted(set(self._competitive_by_state()) | set(RSS_FEEDS) | set(DEFAULT_FEEDS))
        rows = self._fetch_newsapi(states) + self._fetch_rss(states) + self._fetch_google_news(states) + self._fetch_gdelt(states) + self._fetch_national_feeds()
        seen = set()
        deduped = []
        for row in rows:
            key = (row["district_id"], row["url"])
            if row["url"] and key not in seen:
                seen.add(key)
                deduped.append(row)
        return pl.DataFrame(deduped) if deduped else pl.DataFrame({
            "article_id": [], "district_id": [], "state_fips": [], "published_at": [], "headline": [], "url": [],
            "source_name": [], "source_type": [], "incumbent_relevant": [], "sentiment": [], "topic_tags": [],
            "gdelt_tone": [], "fetched_at": [],
        })

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def run(self) -> bool:
        ok = super().run()
        try:
            df = pl.read_parquet(self.output_path)
            if df.height > 0:
                with write_connection() as con:
                    con.register("local_news_df", df)
                    con.execute("INSERT OR REPLACE INTO local_news SELECT * FROM local_news_df")
        except Exception as exc:
            self.logger.exception("Could not insert local news rows: %s", exc)
            return False
        return ok

    def run_state(self, state: str, fast: bool = False) -> dict:
        state = state.upper()
        if state not in STATE_NAMES:
            raise ValueError(f"Unknown state abbreviation: {state}")
        rows = self._fetch_rss([state]) + self._fetch_google_news([state])
        if not fast:
            rows = self._fetch_gdelt([state]) + rows
        seen = set()
        deduped = []
        for row in rows:
            key = (row["district_id"], row["url"])
            if row["url"] and key not in seen:
                seen.add(key)
                deduped.append(row)
        df = pl.DataFrame(deduped) if deduped else pl.DataFrame({
            "article_id": [], "district_id": [], "state_fips": [], "published_at": [], "headline": [], "url": [],
            "source_name": [], "source_type": [], "incumbent_relevant": [], "sentiment": [], "topic_tags": [],
            "gdelt_tone": [], "fetched_at": [],
        })
        path = f"data/raw/local_news_{state.lower()}_latest.parquet"
        df.write_parquet(path)
        if df.height > 0 and len(df.columns) > 0:
            with write_connection() as con:
                con.register("local_news_state_df", df)
                con.execute("INSERT OR REPLACE INTO local_news SELECT * FROM local_news_state_df")
        return {"state": state, "articles": len(deduped), "path": path, "source_coverage": "pending" if not deduped else "queried"}
