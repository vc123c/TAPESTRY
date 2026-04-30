from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path

from db.connection import ROOT, write_connection


SUMMARY_PATH = ROOT / "data" / "models" / "embedding_backfill_latest.json"
BATCH_SIZE = 50


def _log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", flush=True)


def _ensure_columns() -> None:
    with write_connection() as con:
        con.execute("ALTER TABLE race_web_articles ADD COLUMN IF NOT EXISTS embedding BLOB")
        con.execute("ALTER TABLE media_event_articles ADD COLUMN IF NOT EXISTS embedding BLOB")


def _backfill_table(table: str, id_col: str, text_expr: str, text_col_alias: str = "text") -> int:
    from utils.embeddings import embed_text

    total = 0
    while True:
        with write_connection() as con:
            rows = con.execute(
                f"""
                SELECT {id_col}, {text_expr} AS {text_col_alias}
                FROM {table}
                WHERE embedding IS NULL
                LIMIT {BATCH_SIZE}
                """
            ).fetchall()
        if not rows:
            break

        updates = []
        for row_id, text in rows:
            try:
                updates.append((embed_text(text), row_id))
            except Exception as exc:
                _log(f"Embedding skipped for {table}:{row_id}: {type(exc).__name__}")

        if updates:
            with write_connection() as con:
                con.executemany(f"UPDATE {table} SET embedding = ? WHERE {id_col} = ?", updates)
            total += len(updates)
            if total % 100 == 0 or total == len(updates):
                _log(f"{table}: embedded {total} rows")

    return total


def main() -> int:
    started = datetime.now(UTC)
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    _ensure_columns()

    try:
        # Import lazily so an unavailable local model does not break the rest of overnight.py.
        from utils.embeddings import get_model

        get_model()
    except Exception as exc:
        summary = {
            "status": "skipped",
            "reason": f"sentence-transformers model unavailable: {type(exc).__name__}: {exc}",
            "completed_at": datetime.now(UTC).isoformat(),
            "rows_embedded": {},
        }
        SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        _log(summary["reason"])
        return 0

    embedded = {
        "race_web_articles": _backfill_table("race_web_articles", "article_id", "headline"),
        "media_event_articles": _backfill_table("media_event_articles", "article_id", "headline"),
        "ideology_corpus_chunks": _backfill_table("ideology_corpus_chunks", "chunk_id", "chunk_text"),
    }
    completed = datetime.now(UTC)
    summary = {
        "status": "complete",
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "duration_seconds": round((completed - started).total_seconds(), 2),
        "rows_embedded": embedded,
    }
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    _log(f"Embedding backfill complete: {embedded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
