from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.connection import write_connection
from utils.geo import STATE_NAME_TO_ABBR


CITY_TO_STATE = {
    "romulus": "MI",
    "detroit": "MI",
    "lansing": "MI",
    "flint": "MI",
    "durango": "CO",
    "phoenix": "AZ",
    "tucson": "AZ",
    "atlanta": "GA",
    "houston": "TX",
    "dallas": "TX",
    "austin": "TX",
    "los angeles": "CA",
    "san francisco": "CA",
    "sacramento": "CA",
    "fresno": "CA",
    "philadelphia": "PA",
    "pittsburgh": "PA",
    "milwaukee": "WI",
    "madison": "WI",
    "las vegas": "NV",
    "reno": "NV",
    "albuquerque": "NM",
    "santa fe": "NM",
}

NATIONAL_TERMS = [
    "congress",
    "senate",
    "house",
    "supreme court",
    "white house",
    "federal",
    "national",
    "iran",
    "hormuz",
    "war",
    "inflation",
    "grocery",
    "tariff",
    "medicare",
    "social security",
    "epstein",
    "ai",
    "data center",
]


def _as_text(*values: object) -> str:
    return " ".join(str(v or "") for v in values)


def _extract_states(text: str) -> list[str]:
    lowered = text.lower()
    states: set[str] = set()
    for name, abbr in STATE_NAME_TO_ABBR.items():
        if re.search(rf"\b{re.escape(name)}\b", lowered):
            states.add(abbr)
    for city, abbr in CITY_TO_STATE.items():
        if re.search(rf"\b{re.escape(city)}\b", lowered):
            states.add(abbr)
    for match in re.finditer(r"\b([A-Z]{2})-\d{1,2}\b", text):
        states.add(match.group(1).upper())
    return sorted(states)


def _is_national(text: str, states: list[str], type_scores: object) -> bool:
    lowered = text.lower()
    if not states and any(term in lowered for term in NATIONAL_TERMS):
        return True
    try:
        scores = json.loads(type_scores) if isinstance(type_scores, str) else (type_scores or {})
        primary = str(scores.get("primary_type") or "")
    except Exception:
        primary = ""
    return not states and primary in {"conflict_escalation", "economic_shock", "anti_establishment"}


def main() -> None:
    with write_connection() as con:
        con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS affected_states VARCHAR[]")
        con.execute("ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS is_national_signal BOOLEAN DEFAULT FALSE")
        rows = con.execute(
            """
            SELECT event_id, event_name, event_type, notes, affected_districts, type_scores
            FROM event_tokens
            """
        ).fetchall()
        updated = 0
        national = 0
        with_states = 0
        for event_id, name, event_type, notes, affected_districts, type_scores in rows:
            text = _as_text(name, event_type, notes)
            states = _extract_states(text)
            district_states = sorted({str(d).split("-", 1)[0].upper() for d in (affected_districts or []) if "-" in str(d)})
            if not states and 0 < len(district_states) <= 2:
                states = district_states
            is_national = _is_national(text, states, type_scores)
            if is_national:
                national += 1
            if states:
                with_states += 1
            con.execute(
                """
                UPDATE event_tokens
                SET affected_states = ?, is_national_signal = ?
                WHERE event_id = ?
                """,
                [states, is_national, event_id],
            )
            updated += 1
    print(f"{updated} tokens updated")
    print(f"events with explicit states: {with_states}")
    print(f"national signals: {national}")


if __name__ == "__main__":
    main()
