from __future__ import annotations

import hashlib
import re
from datetime import date, datetime

import httpx
from bs4 import BeautifulSoup

from db.connection import init_db, write_connection

USER_AGENT = "Mozilla/5.0 TAPESTRY/1.0 (research)"

PRESIDENTS = [
    ("Barack Obama", "D", "https://www.presidency.ucsb.edu/statistics/data/barack-obama-public-approval"),
    ("Donald Trump", "R", "https://www.presidency.ucsb.edu/statistics/data/donald-j-trump-public-approval"),
    ("Joseph Biden", "D", "https://www.presidency.ucsb.edu/statistics/data/joseph-biden-public-approval"),
    ("Donald Trump 2nd Term", "R", "https://www.presidency.ucsb.edu/statistics/data/donald-j-trump-2nd-term-public-approval"),
]


def _parse_date(value: str) -> date | None:
    text = value.strip()
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass
    return None


def _number(value: str) -> float | None:
    cleaned = re.sub(r"[^0-9.]", "", value or "")
    if not cleaned:
        return None
    try:
        number = float(cleaned)
    except ValueError:
        return None
    return number / 100.0 if number > 1 else number


def _midterm_year(end_date: date) -> int | None:
    for year in [2008, 2010, 2012, 2014, 2016, 2018, 2020, 2022, 2024, 2026]:
        if end_date.year <= year:
            return year
    return None


def _poll_id(president: str, start: date | None, end: date | None, approve: float | None) -> str:
    return hashlib.sha1(f"{president}:{start}:{end}:{approve}".encode("utf-8")).hexdigest()


def fetch_president(president: str, party: str, url: str) -> list[list]:
    rows = []
    try:
        r = httpx.get(url, headers={"User-Agent": USER_AGENT}, timeout=20, follow_redirects=True)
        if r.status_code != 200:
            print(f"{president}: HTTP {r.status_code}")
            return rows
        soup = BeautifulSoup(r.text, "html.parser")
        for table in soup.find_all("table"):
            for tr in table.find_all("tr")[1:]:
                cols = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                if len(cols) < 4:
                    continue
                start = _parse_date(cols[0])
                end = _parse_date(cols[1])
                approve = _number(cols[2])
                disapprove = _number(cols[3])
                no_opinion = _number(cols[4]) if len(cols) > 4 else None
                if not end or approve is None or disapprove is None or not (0.15 <= approve <= 0.75):
                    continue
                rows.append([
                    _poll_id(president, start, end, approve),
                    president,
                    party,
                    start,
                    end,
                    approve,
                    disapprove,
                    no_opinion,
                    None,
                    _midterm_year(end),
                    "UCSB/Gallup",
                ])
        print(f"{president}: parsed {len(rows)} rows")
    except Exception as exc:
        print(f"{president}: {type(exc).__name__}: {exc}")
    return rows


def run() -> int:
    init_db()
    all_rows = []
    for president, party, url in PRESIDENTS:
        all_rows.extend(fetch_president(president, party, url))
    with write_connection() as con:
        for row in all_rows:
            con.execute(
                """
                INSERT OR REPLACE INTO historical_approval_gallup VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                row,
            )
        count = con.execute("SELECT COUNT(*) FROM historical_approval_gallup").fetchone()[0]
    print(f"historical_approval_gallup rows: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
