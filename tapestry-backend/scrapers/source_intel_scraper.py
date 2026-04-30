from __future__ import annotations

import hashlib
import json
import math
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path

import feedparser
import httpx
import polars as pl

from db.connection import ROOT, get_read_connection, write_connection
from scrapers.base import BaseScraper
from utils.geo import STATE_FIPS, state_from_district


RAW_DIR = ROOT / "data" / "raw"
CACHE_DIR = RAW_DIR / "source_intel"

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

SOURCE_FEEDS = [
    # Independent and investigative
    ("Drop Site News", "independent", "investigative", "national", None, "https://www.dropsitenews.com/feed"),
    ("The Intercept", "independent", "investigative", "national", None, "https://theintercept.com/feed"),
    ("The Lever", "independent", "investigative", "national", None, "https://www.levernews.com/feed"),
    ("ProPublica", "nonprofit", "investigative", "national", None, "https://feeds.propublica.org/propublica/main"),
    ("Reveal", "nonprofit", "investigative", "national", None, "https://revealnews.org/feed/"),
    ("Sludge", "independent", "money_in_politics", "national", None, "https://readsludge.com/feed/"),
    ("Jacobin", "independent", "ideological", "national", None, "https://jacobin.com/feed"),
    ("The American Prospect", "magazine", "policy", "national", None, "https://prospect.org/api/rss/content.rss"),
    ("Reason", "magazine", "ideological", "national", None, "https://reason.com/feed/"),
    ("The Bulwark", "independent", "politics", "national", None, "https://www.thebulwark.com/feed/"),
    ("Punchbowl News", "politics", "capitol_hill", "national", None, "https://punchbowl.news/feed/"),
    ("Politico Congress", "mainstream", "capitol_hill", "national", None, "https://www.politico.com/rss/congress.xml"),
    ("Axios Politics", "mainstream", "politics", "national", None, "https://www.axios.com/politics-policy/feed"),
    ("NPR Politics", "public_media", "politics", "national", None, "https://feeds.npr.org/1014/rss.xml"),
    ("PBS NewsHour Politics", "public_media", "politics", "national", None, "https://www.pbs.org/newshour/feeds/rss/politics"),
    ("AP Politics", "wire", "mainstream", "national", None, "https://apnews.com/hub/politics?output=1"),
    ("Reuters Politics", "wire", "mainstream", "national", None, "https://www.reutersagency.com/feed/?best-topics=political-general&post_type=best"),
    ("Roll Call", "politics", "capitol_hill", "national", None, "https://rollcall.com/feed/"),
    ("OpenSecrets News", "nonprofit", "money_in_politics", "national", None, "https://www.opensecrets.org/news/feed/"),
    ("Robert Pape - Escalation Trap", "substack", "foreign_policy", "national", None, "https://escalationtrap.substack.com/feed"),
    ("Eschaton", "blog", "politics", "national", None, "https://www.eschatonblog.com/feeds/posts/default"),
    # National issue/event signals
    ("Defense One", "trade", "defense", "national", None, "https://www.defenseone.com/rss/all/"),
    ("War on the Rocks", "magazine", "foreign_policy", "national", None, "https://warontherocks.com/feed/"),
    ("OilPrice", "trade", "energy", "national", None, "https://oilprice.com/rss/main"),
    ("Inside Climate News", "nonprofit", "energy", "national", None, "https://insideclimatenews.org/feed/"),
    ("KFF Health News", "nonprofit", "healthcare", "national", None, "https://kffhealthnews.org/feed/"),
    ("Fierce Healthcare", "trade", "healthcare", "national", None, "https://www.fiercehealthcare.com/rss/xml"),
    ("Stat News", "trade", "healthcare", "national", None, "https://www.statnews.com/feed/"),
    ("EdSurge", "trade", "education", "national", None, "https://www.edsurge.com/articles_rss"),
    ("Route Fifty", "trade", "state_local", "national", None, "https://www.route-fifty.com/rss/all/"),
    ("Governing", "trade", "state_local", "national", None, "https://www.governing.com/rss.xml"),
    ("TechCrunch AI", "mainstream", "technology", "national", None, "https://techcrunch.com/category/artificial-intelligence/feed/"),
    ("Wired", "magazine", "technology", "national", None, "https://www.wired.com/feed/rss"),
    # Statehouse networks, useful for policy shocks and local election climate
    ("Arizona Mirror", "states_newsroom", "statehouse", "state", "AZ", "https://azmirror.com/feed/"),
    ("Nevada Current", "states_newsroom", "statehouse", "state", "NV", "https://nevadacurrent.com/feed/"),
    ("Pennsylvania Capital-Star", "states_newsroom", "statehouse", "state", "PA", "https://penncapital-star.com/feed/"),
    ("Wisconsin Examiner", "states_newsroom", "statehouse", "state", "WI", "https://wisconsinexaminer.com/feed/"),
    ("Michigan Advance", "states_newsroom", "statehouse", "state", "MI", "https://michiganadvance.com/feed/"),
    ("Georgia Recorder", "states_newsroom", "statehouse", "state", "GA", "https://georgiarecorder.com/feed/"),
    ("NC Newsline", "states_newsroom", "statehouse", "state", "NC", "https://ncnewsline.com/feed/"),
    ("Ohio Capital Journal", "states_newsroom", "statehouse", "state", "OH", "https://ohiocapitaljournal.com/feed/"),
    ("Montana Free Press", "nonprofit", "statehouse", "state", "MT", "https://montanafreepress.org/feed/"),
    ("New Hampshire Bulletin", "states_newsroom", "statehouse", "state", "NH", "https://newhampshirebulletin.com/feed/"),
    ("Maine Beacon", "independent", "statehouse", "state", "ME", "https://mainebeacon.com/feed/"),
    ("Colorado Newsline", "states_newsroom", "statehouse", "state", "CO", "https://coloradonewsline.com/feed/"),
    ("Virginia Mercury", "states_newsroom", "statehouse", "state", "VA", "https://virginiamercury.com/feed/"),
    ("Texas Tribune", "nonprofit", "statehouse", "state", "TX", "https://www.texastribune.org/feeds/main/"),
    ("Florida Phoenix", "states_newsroom", "statehouse", "state", "FL", "https://floridaphoenix.com/feed/"),
    ("CalMatters", "nonprofit", "statehouse", "state", "CA", "https://calmatters.org/feed/"),
    ("New York Focus", "nonprofit", "statehouse", "state", "NY", "https://nysfocus.com/feed"),
]

GDELT_QUERIES = {
    "election": '"congressional election" OR "House race" OR "Senate race"',
    "economy": '"grocery prices" OR inflation OR layoffs OR "medical debt" OR rent',
    "war": '"Iran war" OR Hormuz OR escalation OR "military draft"',
    "corruption": 'corruption OR scandal OR indicted OR investigation',
    "ai_jobs": '"AI taking jobs" OR automation OR layoffs',
    "healthcare": 'Medicare OR healthcare OR hospital OR "drug prices"',
    "immigration": 'border OR immigration OR migrant OR deportation',
    "energy": '"gas prices" OR oil OR refinery OR "data center"',
}

TOPIC_KEYWORDS = {
    "economy": ["inflation", "prices", "rent", "wages", "jobs", "layoffs", "medical debt", "credit card"],
    "war": ["iran", "war", "hormuz", "draft", "troops", "escalation", "ceasefire", "missile"],
    "corruption": ["corruption", "scandal", "indicted", "investigation", "ethics", "bribery", "fraud"],
    "ai_jobs": ["ai", "automation", "data center", "layoffs", "algorithm", "robot"],
    "healthcare": ["medicare", "medicaid", "hospital", "insurance", "drug prices", "healthcare"],
    "immigration": ["border", "immigration", "migrant", "deportation", "asylum"],
    "energy": ["gas prices", "oil", "refinery", "pipeline", "electricity", "utility"],
    "campaign": ["campaign", "candidate", "primary", "endorsement", "fundraising", "poll", "congress"],
    "social_event": ["protest", "march", "rally", "boycott", "civil rights", "community meeting", "public hearing"],
    "culture": ["school board", "book ban", "transgender", "lgbtq", "religion", "church", "speech", "campus"],
    "criminal_justice": ["police", "sheriff", "jail", "prison", "detention", "prosecutor", "assault", "charged"],
    "education": ["school", "teacher", "college", "university", "student", "tuition", "curriculum"],
    "institutional_scandal": ["misconduct", "abuse", "harassment", "cover-up", "lawsuit", "settlement", "resignation"],
    "labor": ["union", "strike", "worker", "working class", "labor", "wages", "organizing"],
    "capital_power": ["billionaire", "corporate", "monopoly", "private equity", "wall street", "donor", "pac"],
    "democracy": ["democracy", "voting rights", "ballot access", "election denial", "authoritarian"],
    "civil_liberties": ["surveillance", "free speech", "civil liberties", "privacy", "protest"],
    "housing": ["housing", "rent", "eviction", "mortgage", "landlord", "homeless"],
}

BAD_EVENT_NAMES = {
    "stonks", "stocks", "untitled", "news", "latest", "update", "market watch",
}


def _usable_event_name(value: str | None) -> bool:
    if not value:
        return False
    cleaned = re.sub(r"\s+", " ", value).strip()
    if len(cleaned) < 12:
        return False
    if cleaned.lower() in BAD_EVENT_NAMES:
        return False
    return bool(re.search(r"[a-zA-Z]{4,}", cleaned))


def _event_type_label(event_type: str | None) -> str:
    return (event_type or "news_signal").replace("_", " ").title()

IDEOLOGY_FRAMES = {
    "marx_labor_capital": ["working class", "capital", "capitalist", "labor", "exploitation", "class", "worker"],
    "anti_monopoly": ["monopoly", "trust", "oligopoly", "corporate power", "private equity", "market power"],
    "liberal_democracy": ["democracy", "rights", "republic", "representation", "voting", "constitution"],
    "civil_liberties": ["liberty", "free speech", "surveillance", "privacy", "due process", "protest"],
    "social_welfare": ["poverty", "welfare", "medicare", "healthcare", "old age", "relief", "unemployment"],
    "anti_war": ["war", "empire", "militarism", "draft", "intervention", "ceasefire"],
}

EVENT_RULES = [
    ("scandal", ["scandal", "indicted", "ethics", "investigation", "corruption", "fraud", "bribery"]),
    ("institutional_scandal", ["misconduct", "abuse", "harassment", "cover-up", "lawsuit", "settlement", "resignation", "whistleblower"]),
    ("campaign_finance", ["fundraising", "campaign dwarfs", "campaign cash", "donor", "pac", "super pac", "aipac", "dark money", "outside spending"]),
    ("criminal_justice", ["police", "sheriff", "jail", "prison", "detention", "prosecutor", "assault", "charged", "arrested"]),
    ("immigration", ["ice", "detention center", "deportation", "border", "immigration", "migrant", "asylum"]),
    ("social_event", ["protest", "march", "rally", "boycott", "civil rights", "public hearing", "community meeting"]),
    ("culture", ["school board", "book ban", "transgender", "lgbtq", "religion", "church", "free speech", "campus protest"]),
    ("education", ["school", "teacher", "college", "university", "student", "tuition", "curriculum"]),
    ("climate", ["climate", "el nino", "el niño", "heat wave", "hotter", "wildfire", "flood", "drought", "emissions"]),
    ("housing", ["housing", "rent", "eviction", "mortgage", "landlord", "homeless", "affordable housing"]),
    ("labor", ["union", "strike", "worker", "working class", "labor", "wages", "organizing"]),
    ("healthcare", ["medicare", "medicaid", "aca", "affordable care act", "planned parenthood", "healthcare", "health care", "hospital", "insurance", "drug prices"]),
    ("technology", ["ai", "automation", "data center", "algorithm", "robot"]),
    ("energy", ["gas prices", "oil", "refinery", "pipeline", "electricity", "utility"]),
    ("conflict", ["iran", "hormuz", "ground war", "troops", "escalation", "ceasefire", "missile", "military draft", "donbas", "ukraine", "russia", "invasion", "battlefield"]),
    ("economic_shock", ["inflation", "grocery prices", "egg prices", "layoffs", "medical debt", "credit card debt", "unemployment"]),
    ("policy_shock", ["abortion", "minimum wage", "ballot measure", "supreme court", "federal rule"]),
    ("campaign_event", ["campaign", "candidate", "primary", "endorsement", "poll", "governor race", "congressional race", "race"]),
]

KEYWORD_SCORES = {
    "economic_shock": {
        "recession": 0.9, "inflation": 0.8, "layoffs": 0.8, "unemployment": 0.8,
        "gas prices": 0.7, "egg prices": 0.6, "grocery": 0.6, "mortgage": 0.7,
        "tariff": 0.7, "debt": 0.5,
    },
    "conflict_escalation": {
        "iran": 0.9, "war": 0.9, "escalation": 0.9, "troops": 0.8,
        "military": 0.7, "hormuz": 0.9, "strike": 0.7, "ceasefire": 0.6,
        "negotiation": 0.5,
    },
    "scandal_corruption": {
        "corruption": 0.9, "bribery": 0.95, "indicted": 0.9, "fraud": 0.9,
        "ethics": 0.7, "investigation": 0.7, "cover-up": 0.85,
        "embezzlement": 0.95, "kickback": 0.95,
    },
    "scandal_personal": {
        "affair": 0.85, "harassment": 0.85, "assault": 0.8,
        "addiction": 0.7, "dui": 0.8, "lies": 0.6,
    },
    "anti_establishment": {
        "both parties": 0.8, "establishment": 0.8, "deep state": 0.7,
        "epstein": 0.9, "elite": 0.6, "outsider": 0.7, "corruption": 0.5,
    },
    "policy_reversal": {
        "reversed": 0.8, "flip-flopped": 0.9, "backtracked": 0.8,
        "broke promise": 0.9, "voted against": 0.6, "changed position": 0.7,
    },
    "social_event": {
        "protest": 0.8, "rally": 0.6, "civil rights": 0.8, "boycott": 0.7,
        "detention center": 0.7, "public hearing": 0.5,
    },
    "immigration_policy": {
        "immigration": 0.8, "asylum": 0.8, "border": 0.7, "migrant": 0.7,
        "deportation": 0.8, "ice": 0.7, "detention center": 0.6,
    },
    "climate_environment": {
        "climate": 0.8, "el nino": 0.7, "el niño": 0.7, "wildfire": 0.7,
        "flood": 0.6, "drought": 0.6, "heat wave": 0.7,
    },
}

SOURCE_CREDIBILITY = {
    "reuters": 1.0, "ap": 1.0, "apnews": 1.0, "propublica": 0.95,
    "npr": 0.9, "local_paper": 0.8, "local_tv": 0.75,
    "political_blog": 0.4, "campaign": 0.2, "unknown": 0.35,
}

HALF_LIFE_DAYS = {
    "economic_shock": 60, "conflict_escalation": 30, "scandal_corruption": 45,
    "scandal_personal": 21, "anti_establishment": 90, "policy_reversal": 30,
    "social_event": 21,
    "immigration_policy": 45,
    "climate_environment": 60,
}


def apply_exclusion_rules(scores: dict[str, float], text: str) -> dict[str, float]:
    tl = (text or "").lower()
    immigration_terms = ["ice ", "asylum", "border patrol", "deportation", "migrant", "immigration"]
    military_terms = ["troops", "war", "escalation", "hormuz", "airstrike", "military force"]
    if any(term in tl for term in immigration_terms) and not any(term in tl for term in military_terms):
        scores["conflict_escalation"] = min(scores.get("conflict_escalation", 0.0), 0.2)

    legal_generic = ["court ruling", "supreme court", "appeals court", "lawsuit", "filed suit"]
    corruption_specific = ["bribery", "kickback", "fraud", "indicted", "embezzlement"]
    if any(term in tl for term in legal_generic) and not any(term in tl for term in corruption_specific):
        scores["scandal_corruption"] = min(scores.get("scandal_corruption", 0.0), 0.3)
    return scores


def _keyword_hit(text: str, keyword: str) -> bool:
    pattern = r"(?<![a-z0-9])" + re.escape(keyword.lower()) + r"(?![a-z0-9])"
    return re.search(pattern, text) is not None


def score_event(text: str, source_name: str = "unknown", source_count: int = 1) -> dict:
    tl = (text or "").lower()
    scores = {cat: min(1.0, sum(weight for kw, weight in kws.items() if _keyword_hit(tl, kw))) for cat, kws in KEYWORD_SCORES.items()}
    scores = apply_exclusion_rules(scores, tl)
    primary = max(scores, key=scores.get)
    src = (source_name or "unknown").lower().split(".")[0]
    credibility = SOURCE_CREDIBILITY.get(src, 0.8 if source_count >= 2 else 0.35)
    if any(token in (source_name or "").lower() for token in ["campaign", "for congress"]):
        for key in ["scandal_corruption", "scandal_personal"]:
            scores[key] *= 0.4
        primary = max(scores, key=scores.get)
        credibility = min(credibility, 0.4)
    primary_score = scores[primary]
    return {
        "type_scores": scores,
        "primary_type": primary if primary_score > 0 else "news_signal",
        "primary_score": primary_score,
        "credibility": credibility,
        "credibility_weighted_salience": primary_score * credibility,
    }


def apply_decay(salience: float, days_since: int, primary_type: str) -> float:
    half_life = HALF_LIFE_DAYS.get(primary_type, 45)
    return float(salience or 0.0) * math.exp(-max(days_since, 0) / half_life)

NEGATIVE = ["trails", "scandal", "criticized", "behind", "unpopular", "faces", "slipping", "indicted", "controversy", "questioned", "struggles", "protest"]
POSITIVE = ["leads", "surge", "popular", "wins", "strong", "ahead", "endorsed", "gains", "passes", "secures"]
DISHONESTY_TERMS = {
    "lie": 1.0, "lied": 1.0, "lying": 1.0, "false": 0.9, "misleading": 0.9,
    "misled": 0.9, "contradict": 0.7, "contradicted": 0.7, "hypocrisy": 0.7,
    "hypocrite": 0.7, "cover-up": 1.0, "coverup": 1.0, "undisclosed": 0.8,
    "ethics": 0.7, "disclosure": 0.6, "corruption": 1.0, "fraud": 1.0,
    "bribery": 1.0, "fabricated": 1.0, "deceptive": 0.9, "questioned": 0.5,
    "investigation": 0.7, "indicted": 1.0, "settlement": 0.5,
}


def source_credibility_score(source_name: str | None) -> float:
    source = (source_name or "").lower()
    if any(token in source for token in ["reuters", "associated press", "ap news", "apnews"]):
        return 1.0
    if any(token in source for token in ["propublica", "publica"]):
        return 0.95
    if any(token in source for token in ["npr", "pbs", "kff health", "calmatters", "texas tribune"]):
        return 0.9
    if any(token in source for token in ["local", "journal", "gazette", "times", "post", "dispatch", "tribune", "ledger", "observer"]):
        return 0.8
    if any(token in source for token in ["tv", "wral", "wsb", "wmur", "ktnv", "wisc", "wgme", "9news", "12news"]):
        return 0.75
    if any(token in source for token in ["campaign", "for congress", "for senate"]):
        return 0.2
    if any(token in source for token in ["blog", "substack", "jacobin", "reason", "bulwark", "eschaton"]):
        return 0.4
    return 0.35
OPEN_SEAT_TERMS = {
    "not running": "not_seeking_reelection_article",
    "not seeking reelection": "not_seeking_reelection_article",
    "not seek reelection": "not_seeking_reelection_article",
    "retiring": "retirement_article",
    "retire": "retirement_article",
    "resigning": "resignation_article",
    "resign": "resignation_article",
    "vacant": "vacancy_article",
    "running for senate": "running_for_senate_article",
    "run for senate": "running_for_senate_article",
    "running for governor": "running_for_governor_article",
    "run for governor": "running_for_governor_article",
}


def _parse_dt(value: object) -> datetime:
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


def _clean(text: str) -> str:
    return re.sub(r"\s+", " ", re.sub("<[^>]+>", " ", text or "")).strip()


def _article_id(url: str, title: str, district_id: str | None) -> str:
    return hashlib.sha1(f"{url}:{title}:{district_id or 'national'}".encode("utf-8")).hexdigest()


class SourceIntelScraper(BaseScraper):
    source_name = "source_intel"
    output_path = "data/raw/source_intel_latest.parquet"
    retries = 1

    def __init__(self, output_path: str | None = None) -> None:
        super().__init__(output_path)
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        self._districts_by_state: dict[str, list[str]] | None = None
        self._candidate_lookup: dict[str, list[str]] | None = None

    def _districts(self) -> dict[str, list[str]]:
        if self._districts_by_state is None:
            grouped: dict[str, list[str]] = defaultdict(list)
            try:
                with get_read_connection() as con:
                    rows = con.execute("SELECT district_id FROM house_roster ORDER BY district_id").fetchall()
                for (district_id,) in rows:
                    grouped[state_from_district(district_id)].append(district_id)
            except Exception:
                pass
            self._districts_by_state = dict(grouped)
        return self._districts_by_state

    def _candidate_names(self) -> dict[str, list[str]]:
        if self._candidate_lookup is None:
            lookup: dict[str, list[str]] = defaultdict(list)
            try:
                with get_read_connection() as con:
                    rows = con.execute(
                        """
                        SELECT district_id, incumbent_name FROM house_roster
                        UNION ALL
                        SELECT district_id, candidate_name FROM candidate_roster_2026
                        """
                    ).fetchall()
                for district_id, name in rows:
                    if district_id and name:
                        lookup[district_id].append(str(name).lower())
            except Exception:
                pass
            self._candidate_lookup = dict(lookup)
        return self._candidate_lookup

    @staticmethod
    def _topics(text: str) -> list[str]:
        low = text.lower()
        return [topic for topic, words in TOPIC_KEYWORDS.items() if any(word in low for word in words)]

    @staticmethod
    def _ideology_tags(text: str) -> list[str]:
        low = text.lower()
        return [tag for tag, words in IDEOLOGY_FRAMES.items() if any(word in low for word in words)]

    @staticmethod
    def _event_type(text: str) -> str:
        low = text.lower()
        scores = []
        for event_type, words in EVENT_RULES:
            score = sum(1 for word in words if word in low)
            if score:
                scores.append((score, event_type))
        if not scores:
            return "news_signal"
        scores.sort(key=lambda item: (-item[0], item[1]))
        return scores[0][1]

    @staticmethod
    def _target_party(text: str) -> str | None:
        low = text.lower()
        d_hit = any(word in low for word in ["democrat", "democratic", "biden", "harris"])
        r_hit = any(word in low for word in ["republican", "gop", "trump"])
        if d_hit and r_hit:
            return "both"
        if d_hit:
            return "D"
        if r_hit:
            return "R"
        return None

    @staticmethod
    def _sentiment(text: str) -> str:
        low = text.lower()
        if any(word in low for word in NEGATIVE):
            return "NEGATIVE"
        if any(word in low for word in POSITIVE):
            return "POSITIVE"
        return "NEUTRAL"

    @staticmethod
    def _salience(text: str, source_type: str, outlet_tier: str) -> float:
        low = text.lower()
        score = 0.25
        score += 0.10 * sum(1 for words in TOPIC_KEYWORDS.values() if any(word in low for word in words))
        score += 0.15 if outlet_tier in {"investigative", "capitol_hill", "money_in_politics"} else 0.0
        score += 0.10 if source_type in {"wire", "mainstream", "nonprofit"} else 0.0
        score += 0.20 if any(word in low for word in ["breaking", "indicted", "war", "layoffs", "poll", "scandal", "strike", "ceasefire"]) else 0.0
        return min(score, 1.0)

    def _affected_districts(self, state: str | None, text: str) -> tuple[list[str], bool]:
        names = self._candidate_names()
        low = text.lower()
        hits = [district_id for district_id, people in names.items() if any(name and name in low for name in people)]
        if hits:
            return sorted(set(hits)), True
        if state and state in self._districts():
            return self._districts()[state], False
        return [], False

    def _rows_for_article(self, source: str, source_type: str, outlet_tier: str, scope: str, state: str | None, title: str, url: str, published_at: datetime, summary: str) -> list[dict]:
        title = _clean(title)
        summary = _clean(summary)
        if not title or not url:
            return []
        text = f"{title} {summary}"
        topics = self._topics(text)
        if not topics and not any(term in text.lower() for term in ["congress", "election", "campaign", "senate", "house", "trump", "democrat", "republican"]):
            return []
        event_type = self._event_type(text)
        affected, incumbent_relevant = self._affected_districts(state, text)
        targets = affected or [None]
        rows = []
        for district_id in targets:
            rows.append({
                "article_id": _article_id(url, title, district_id),
                "published_at": published_at,
                "headline": title,
                "url": url,
                "source_name": source,
                "source_type": source_type,
                "outlet_tier": outlet_tier,
                "scope": scope,
                "state_abbr": state,
                "district_id": district_id,
                "event_type": event_type,
                "topic_tags": topics,
                "ideology_tags": self._ideology_tags(text),
                "target_party": self._target_party(text),
                "incumbent_relevant": incumbent_relevant,
                "salience_score": self._salience(text, source_type, outlet_tier),
                "sentiment": self._sentiment(text),
                "summary": summary[:1200],
                "fetched_at": datetime.utcnow(),
            })
        return rows

    def _feed_cache_path(self, source: str) -> Path:
        safe = re.sub(r"[^a-z0-9]+", "_", source.lower()).strip("_")
        return CACHE_DIR / f"rss_{safe}.json"

    def _fetch_feeds(self) -> list[dict]:
        cutoff = datetime.utcnow() - timedelta(days=10)
        rows = []
        for source, source_type, outlet_tier, scope, state, feed_url in SOURCE_FEEDS:
            cache_path = self._feed_cache_path(source)
            try:
                parsed = feedparser.parse(feed_url)
                entries = []
                for entry in parsed.entries[:35]:
                    published_at = _parse_dt(entry.get("published") or entry.get("updated"))
                    if published_at < cutoff:
                        continue
                    entries.append({
                        "title": entry.get("title", ""),
                        "link": entry.get("link", feed_url),
                        "published": published_at.isoformat(),
                        "summary": entry.get("summary", ""),
                    })
                    rows.extend(self._rows_for_article(source, source_type, outlet_tier, scope, state, entry.get("title", ""), entry.get("link", feed_url), published_at, entry.get("summary", "")))
                cache_path.write_text(json.dumps({"source": source, "feed_url": feed_url, "entries": entries}, indent=2), encoding="utf-8")
            except Exception as exc:
                self.logger.warning("Source intelligence RSS failed for %s (%s): %s", source, feed_url, exc)
            time.sleep(0.15)
        return rows

    def _fetch_gdelt(self) -> list[dict]:
        rows = []
        client = httpx.Client(timeout=25)
        for event_type, query in GDELT_QUERIES.items():
            try:
                params = {"query": query, "mode": "ArtList", "maxrecords": 35, "timespan": "7d", "format": "json"}
                data = client.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params).json()
                (CACHE_DIR / f"gdelt_{event_type}.json").write_text(json.dumps(data, indent=2), encoding="utf-8")
                for article in data.get("articles", []):
                    title = article.get("title") or ""
                    url = article.get("url") or ""
                    state = None
                    text = f"{title} {article.get('sourcecountry', '')}"
                    for abbr, name in STATE_NAMES.items():
                        if name.lower() in text.lower():
                            state = abbr
                            break
                    rows.extend(self._rows_for_article(
                        article.get("sourcename") or "GDELT",
                        "gdelt",
                        "news_volume",
                        "state" if state else "national",
                        state,
                        title,
                        url,
                        _parse_dt(article.get("seendate")),
                        "",
                    ))
            except Exception as exc:
                self.logger.warning("Source intelligence GDELT failed for %s: %s", event_type, exc)
            time.sleep(1)
        client.close()
        return rows

    def fetch(self) -> pl.DataFrame:
        rows = self._fetch_feeds() + self._fetch_gdelt()
        deduped = {}
        for row in rows:
            if row["url"]:
                current = deduped.get(row["article_id"])
                if current is None or row["salience_score"] > current["salience_score"]:
                    deduped[row["article_id"]] = row
        columns = [
            "article_id", "published_at", "headline", "url", "source_name", "source_type", "outlet_tier",
            "scope", "state_abbr", "district_id", "event_type", "topic_tags", "ideology_tags", "target_party",
            "incumbent_relevant", "salience_score", "sentiment", "summary", "fetched_at",
        ]
        if not deduped:
            return pl.DataFrame({col: [] for col in columns})
        return pl.DataFrame(
            list(deduped.values()),
            infer_schema_length=None,
            schema_overrides={
                "article_id": pl.Utf8,
                "headline": pl.Utf8,
                "url": pl.Utf8,
                "source_name": pl.Utf8,
                "source_type": pl.Utf8,
                "outlet_tier": pl.Utf8,
                "scope": pl.Utf8,
                "state_abbr": pl.Utf8,
                "district_id": pl.Utf8,
                "event_type": pl.Utf8,
                "target_party": pl.Utf8,
                "sentiment": pl.Utf8,
                "summary": pl.Utf8,
                "salience_score": pl.Float64,
                "incumbent_relevant": pl.Boolean,
            },
        ).select(columns)

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def fallback(self) -> pl.DataFrame:
        path = Path(self.output_path)
        if path.exists():
            self.logger.warning("Using last known good source intelligence at %s", path)
            return pl.read_parquet(path)
        return self.fetch().head(0)

    def _write_signal_summary(self, df: pl.DataFrame) -> None:
        if df.height == 0:
            return
        rows = []
        for (event_type, topic), group in df.with_columns(
            pl.col("topic_tags").list.join("|").alias("topic_key")
        ).group_by(["event_type", "topic_key"]):
            sorted_group = group.sort("salience_score", descending=True)
            top = sorted_group.row(0, named=True)
            district_ids = sorted([item for item in group["district_id"].drop_nulls().unique().to_list()])
            source_count = group["source_name"].n_unique()
            article_count = group.height
            key_text = f"{event_type}:{topic or 'general'}"
            rows.append({
                "signal_date": date.today(),
                "signal_key": hashlib.sha1(key_text.encode("utf-8")).hexdigest()[:16],
                "event_type": event_type,
                "topic_tags": topic.split("|") if topic else [],
                "source_count": int(source_count),
                "article_count": int(article_count),
                "max_salience": float(group["salience_score"].max()),
                "avg_salience": float(group["salience_score"].mean()),
                "affected_districts": district_ids,
                "representative_headline": top["headline"],
                "representative_url": top["url"],
            })
        if rows:
            summary = pl.DataFrame(rows)
            summary.write_parquet(RAW_DIR / "media_signal_summary_latest.parquet")
            with write_connection() as con:
                con.register("media_signal_summary_df", summary)
                con.execute("INSERT OR REPLACE INTO media_signal_summary SELECT * FROM media_signal_summary_df")

    def _write_event_tokens(self) -> None:
        with write_connection() as con:
            con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS type_scores JSON")
            con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0")
            con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS credibility_weighted_salience DOUBLE")
            rows = con.execute(
                """
                SELECT signal_key, event_type, topic_tags, affected_districts, representative_headline,
                       article_count, avg_salience
                FROM media_signal_summary
                WHERE signal_date = ?
                """,
                [date.today()],
            ).fetchall()
            above_threshold = 0
            for signal_key, event_type, topic_tags, affected, headline, article_count, salience in rows:
                if int(article_count or 0) < 2 or not _usable_event_name(headline):
                    self.logger.info("Source intelligence skipped low-confidence token headline=%r article_count=%s", headline, article_count)
                    continue
                scored = score_event(headline, "media_signal_summary", int(article_count or 0))
                primary_type = scored["primary_type"] if scored["primary_score"] > 0.4 else event_type
                event_id = f"media_{date.today().isoformat()}_{signal_key}"
                anti_loading = min(float(salience or 0) * 0.8, 1.0)
                partisan_loading = 0.0
                half_life = HALF_LIFE_DAYS.get(primary_type, 14 if event_type in {"campaign_event", "news_signal"} else 30)
                con.execute(
                    """
                    INSERT OR REPLACE INTO event_tokens
                    (event_id, event_name, event_date, event_type, scandal_subtype, primary_target_party,
                     anti_establishment_loading, partisan_loading, embedding, half_life_days,
                     affected_districts, similar_event_ids, outcome_seat_swing, outcome_magnitude,
                     resolved, notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        event_id, headline, date.today(), primary_type, None, None,
                        anti_loading, partisan_loading, None, half_life,
                        affected or [], [], None, None, False,
                        f"Auto-tokenized from {article_count} media articles during overnight source intelligence scrape.",
                    ],
                )
                con.execute(
                    """
                    UPDATE event_tokens
                    SET type_scores=?, source_count=?, credibility_weighted_salience=?
                    WHERE event_id=?
                    """,
                    [
                        json.dumps(scored),
                        int(article_count or 0),
                        float(scored["credibility_weighted_salience"]),
                        event_id,
                    ],
                )
                con.execute(
                    "INSERT OR REPLACE INTO event_salience VALUES (?, ?, ?, ?, ?, ?)",
                    [event_id, date.today(), 0.0, int(article_count or 0), 0.0, float(salience or 0)],
                )
                if scored["credibility_weighted_salience"] > 0.25 and int(article_count or 0) >= 2 and scored["primary_score"] > 0.4:
                    above_threshold += 1
            self.logger.info("Event taxonomy reprocessed: %s tokens; above threshold: %s", len(rows), above_threshold)

    def _write_integrity_signals(self, df: pl.DataFrame) -> None:
        if df.height == 0:
            return
        try:
            with get_read_connection() as read_con:
                people = read_con.execute(
                    """
                    SELECT district_id, incumbent_name FROM house_roster WHERE incumbent_name IS NOT NULL
                    UNION ALL
                    SELECT district_id, candidate_name FROM candidate_roster_2026 WHERE candidate_name IS NOT NULL
                    """
                ).fetchall()
        except Exception:
            people = []
        rows = []
        articles = df.select(["district_id", "headline", "url", "source_name", "summary"]).to_dicts()
        for district_id, candidate_name in people:
            if not district_id or not candidate_name:
                continue
            candidate_low = str(candidate_name).lower()
            hits = []
            weighted = 0.0
            for article in articles:
                if article.get("district_id") not in {district_id, None}:
                    continue
                credibility = source_credibility_score(article.get("source_name"))
                if credibility < 0.60:
                    continue
                text = f"{article.get('headline') or ''} {article.get('summary') or ''}".lower()
                if candidate_low not in text:
                    continue
                term_score = sum(weight for term, weight in DISHONESTY_TERMS.items() if term in text)
                if term_score <= 0:
                    continue
                if any(token in str(article.get("source_name") or "").lower() for token in ["campaign", "for congress"]):
                    term_score *= 0.4
                weighted += min(term_score, 3.0)
                hits.append({
                    "headline": article.get("headline"),
                    "url": article.get("url"),
                    "source": article.get("source_name"),
                    "credibility": round(credibility, 2),
                    "score": round(min(term_score, 3.0), 2),
                })
            if hits:
                max_credibility = max(float(hit.get("credibility") or 0.0) for hit in hits)
                rows.append({
                    "district_id": district_id,
                    "candidate_name": candidate_name,
                    "signal_date": date.today(),
                    "perceived_dishonesty_score": min(100.0, (weighted / max(len(hits), 1)) * 22.0),
                    "article_count": len(hits),
                    "source_count": len({hit.get("source") for hit in hits if hit.get("source")}),
                    "max_source_credibility": max_credibility,
                    "evidence": json.dumps(hits[:8]),
                    "source_table": "media_event_articles",
                })
        if not rows:
            return
        out = pl.DataFrame(rows)
        with write_connection() as con:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS politician_integrity_signals (
                    district_id VARCHAR,
                    candidate_name VARCHAR,
                    signal_date DATE,
                    perceived_dishonesty_score DOUBLE,
                    article_count INTEGER,
                    source_count INTEGER,
                    max_source_credibility DOUBLE,
                    evidence JSON,
                    source_table VARCHAR,
                    PRIMARY KEY (district_id, candidate_name, signal_date, source_table)
                )
                """
            )
            con.execute("ALTER TABLE politician_integrity_signals ADD COLUMN IF NOT EXISTS source_count INTEGER")
            con.execute("ALTER TABLE politician_integrity_signals ADD COLUMN IF NOT EXISTS max_source_credibility DOUBLE")
            con.register("integrity_df", out)
            con.execute(
                """
                INSERT OR REPLACE INTO politician_integrity_signals
                (district_id, candidate_name, signal_date, perceived_dishonesty_score,
                 article_count, source_count, max_source_credibility, evidence, source_table)
                SELECT district_id, candidate_name, signal_date, perceived_dishonesty_score,
                       article_count, source_count, max_source_credibility, evidence, source_table
                FROM integrity_df
                """
            )

    def _reprocess_existing_event_tokens(self) -> None:
        with write_connection() as con:
            con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS type_scores JSON")
            con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0")
            con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS credibility_weighted_salience DOUBLE")
            rows = con.execute(
                """
                SELECT e.event_id, e.event_name, e.event_type,
                       COALESCE(e.source_count, m.source_count, s.news_volume, 1) AS source_count
                FROM event_tokens e
                LEFT JOIN media_signal_summary m
                  ON m.signal_key = regexp_extract(e.event_id, '([a-f0-9]{16})$', 1)
                LEFT JOIN event_salience s
                  ON s.event_id=e.event_id
                 AND s.salience_date=(SELECT MAX(salience_date) FROM event_salience WHERE event_id=e.event_id)
                """
            ).fetchall()
            above = []
            for event_id, event_name, event_type, source_count in rows:
                scored = score_event(str(event_name or ""), "event_tokens", int(source_count or 1))
                primary_type = scored["primary_type"] if scored["primary_score"] > 0.4 else (event_type or scored["primary_type"])
                con.execute(
                    """
                    UPDATE event_tokens
                    SET event_type=?, type_scores=?, source_count=?, credibility_weighted_salience=?
                    WHERE event_id=?
                    """,
                    [
                        primary_type,
                        json.dumps(scored),
                        int(source_count or 1),
                        float(scored["credibility_weighted_salience"]),
                        event_id,
                    ],
                )
                if scored["credibility_weighted_salience"] > 0.25 and int(source_count or 1) >= 2 and scored["primary_score"] > 0.4:
                    above.append((event_name, primary_type, scored["credibility_weighted_salience"]))
            top = sorted(above, key=lambda row: row[2], reverse=True)[:5]
            self.logger.info("Event taxonomy reprocessed: %s tokens; above threshold: %s; top=%s", len(rows), len(above), top)

    def _write_article_status_signals(self, df: pl.DataFrame) -> None:
        if df.height == 0:
            return
        try:
            with get_read_connection() as read_con:
                people = read_con.execute(
                    "SELECT district_id, incumbent_name, incumbent_party FROM house_roster WHERE incumbent_name IS NOT NULL"
                ).fetchall()
        except Exception:
            people = []
        articles = df.select(["headline", "url", "source_name", "summary"]).to_dicts()
        rows = []
        for district_id, incumbent_name, party in people:
            name_low = str(incumbent_name or "").lower()
            if not name_low:
                continue
            for article in articles:
                text = f"{article.get('headline') or ''} {article.get('summary') or ''}".lower()
                if name_low not in text:
                    continue
                reason = next((value for term, value in OPEN_SEAT_TERMS.items() if term in text), None)
                if not reason:
                    continue
                status = "vacant" if "resign" in reason or "vacancy" in reason else "not_running"
                rows.append({
                    "district_id": district_id,
                    "incumbent_name": incumbent_name,
                    "party": party,
                    "status": status,
                    "reason": reason,
                    "source_name": article.get("source_name") or "article_scrape",
                    "source_url": article.get("url"),
                    "observed_at": datetime.utcnow(),
                })
                break
        if not rows:
            return
        out = pl.DataFrame(rows)
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
            con.register("article_status_df", out)
            con.execute("INSERT OR REPLACE INTO incumbent_status_2026 SELECT * FROM article_status_df")
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

    def _update_feature_intensity(self, df: pl.DataFrame) -> None:
        if df.height == 0:
            return
        district_scores = (
            df.filter(pl.col("district_id").is_not_null())
            .group_by("district_id")
            .agg([
                pl.len().alias("article_count"),
                pl.col("salience_score").mean().alias("avg_salience"),
                pl.col("source_name").n_unique().alias("source_count"),
            ])
            .with_columns([
                (pl.min_horizontal((pl.col("article_count") / 40.0) + (pl.col("avg_salience") * 0.6), pl.lit(1.0)) * 100).alias("local_news_intensity"),
                (pl.min_horizontal(pl.col("source_count") / 12.0, pl.lit(1.0)) * 100).alias("independent_media_penetration"),
            ])
        )
        if district_scores.height == 0:
            return
        with write_connection() as con:
            for row in district_scores.to_dicts():
                con.execute(
                    """
                    UPDATE district_features
                    SET local_news_intensity = ?, independent_media_penetration = ?
                    WHERE district_id = ?
                      AND feature_date = (SELECT MAX(feature_date) FROM district_features WHERE district_id = ?)
                    """,
                    [
                        row["local_news_intensity"],
                        row["independent_media_penetration"],
                        row["district_id"],
                        row["district_id"],
                    ],
                )

    def run(self) -> bool:
        ok = super().run()
        try:
            df = pl.read_parquet(self.output_path)
            if df.height > 0:
                with write_connection() as con:
                    con.execute("ALTER TABLE media_event_articles ADD COLUMN IF NOT EXISTS ideology_tags VARCHAR[]")
                    con.register("media_event_articles_df", df)
                    con.execute(
                        """
                        INSERT OR REPLACE INTO media_event_articles (
                            article_id, published_at, headline, url, source_name, source_type,
                            outlet_tier, scope, state_abbr, district_id, event_type, topic_tags,
                            ideology_tags, target_party, incumbent_relevant, salience_score,
                            sentiment, summary, fetched_at
                        )
                        SELECT
                            article_id, published_at, headline, url, source_name, source_type,
                            outlet_tier, scope, state_abbr, district_id, event_type, topic_tags,
                            ideology_tags, target_party, incumbent_relevant, salience_score,
                            sentiment, summary, fetched_at
                        FROM media_event_articles_df
                        """
                    )
            self._write_signal_summary(df)
            self._write_event_tokens()
            self._reprocess_existing_event_tokens()
            self._write_integrity_signals(df)
            self._write_article_status_signals(df)
            self._update_feature_intensity(df)
        except Exception as exc:
            self.logger.exception("Could not persist source intelligence rows: %s", exc)
            return False
        return ok


if __name__ == "__main__":
    raise SystemExit(0 if SourceIntelScraper().run() else 1)
