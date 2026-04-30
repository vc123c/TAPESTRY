from __future__ import annotations

import json
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[1]))

from db.connection import get_read_connection


def main() -> int:
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT event_id, event_name, event_date,
                   event_type, credibility_weighted_salience,
                   COALESCE(source_count, 1) AS source_count,
                   type_scores
            FROM event_tokens
            WHERE credibility_weighted_salience > 0.1
            ORDER BY credibility_weighted_salience DESC
            LIMIT 50
            """
        ).fetchall()
    for _, name, event_date, event_type, salience, source_count, scores in rows:
        try:
            parsed = json.loads(scores) if isinstance(scores, str) else scores
            primary = parsed.get("primary_type") or event_type if isinstance(parsed, dict) else event_type
        except Exception:
            primary = event_type
        print(f"{str(event_date)[:10]} | {str(primary or event_type):20s} | sal={float(salience or 0):.2f} | src={int(source_count or 1)} | {str(name or '')[:60]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
