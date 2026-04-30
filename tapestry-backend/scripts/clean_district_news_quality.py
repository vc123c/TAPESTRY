from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.connection import get_read_connection, write_connection
from scrapers.race_web_scraper import STATE_NAMES


GENERIC_PATTERNS = [
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
}


def load_district_meta() -> dict[str, tuple[str, int | None]]:
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT district_id, state_abbr, COALESCE(state_name, '') AS state_name, district_number
            FROM house_roster
            """
        ).fetchall()
    return {row[0]: ((row[2] or STATE_NAMES.get(row[1], row[1])).lower(), row[3]) for row in rows}


def mentioned_states(text: str) -> set[str]:
    low = text.lower()
    return {name.lower() for name in STATE_NAMES.values() if re.search(rf"\b{re.escape(name.lower())}\b", low)}


def congressional_numbers(text: str) -> set[int]:
    numbers: set[int] = set()
    for match in re.finditer(r"\b(\d{1,2})(?:st|nd|rd|th)?\s+congressional district\b", text):
        numbers.add(int(match.group(1)))
    for number, word in ORDINAL_WORDS.items():
        if re.search(rf"\b{word}\s+congressional district\b", text):
            numbers.add(number)
    return numbers


def should_delete(district_id: str, headline: str, summary: str, state_name: str, district_number: int | None, source_type: str) -> str | None:
    text = f"{headline or ''} {summary or ''}".lower()
    if not text.strip():
        return "empty headline"
    if any(pattern in text for pattern in GENERIC_PATTERNS):
        return "generic polling page"

    mentioned_districts = congressional_numbers(text)
    if district_number and mentioned_districts and int(district_number) not in mentioned_districts:
        return "mentions another congressional district"

    states = mentioned_states(text)
    if states and state_name not in states:
        return "mentions another state only"

    if source_type in {"STATE COVERAGE", "CURRENT NEWS"}:
        if states and state_name not in states:
            return "fallback wrong state"
        if not states and district_id.lower() not in text:
            return "fallback lacks state or district marker"

    return None


def main() -> None:
    meta = load_district_meta()
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT article_id, district_id, headline, COALESCE(summary, ''), COALESCE(source_type, '')
            FROM race_web_articles
            """
        ).fetchall()

    deletes: list[tuple[str, str, str, str]] = []
    for article_id, district_id, headline, summary, source_type in rows:
        state_name, district_number = meta.get(district_id, ("", None))
        reason = should_delete(district_id, headline, summary, state_name, district_number, source_type)
        if reason:
            deletes.append((article_id, district_id, headline or "", reason))

    with write_connection() as con:
        for article_id, *_ in deletes:
            con.execute("DELETE FROM race_web_articles WHERE article_id = ?", [article_id])
        con.commit()

    print(f"Deleted low-quality district news rows: {len(deletes)}")
    for _, district_id, headline, reason in deletes[:50]:
        print(f"  {district_id}: {reason}: {headline[:110]}")

    with get_read_connection() as con:
        total = con.execute("SELECT COUNT(*) FROM race_web_articles").fetchone()[0]
        any_districts = con.execute("SELECT COUNT(DISTINCT district_id) FROM race_web_articles").fetchone()[0]
        two_plus = con.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT district_id, COUNT(*) n
                FROM race_web_articles
                GROUP BY district_id
                HAVING n >= 2
            )
            """
        ).fetchone()[0]
    print(f"Remaining race_web_articles: {total}")
    print(f"Districts with any race news: {any_districts}/435")
    print(f"Districts with >=2 race news rows: {two_plus}/435")


if __name__ == "__main__":
    main()
