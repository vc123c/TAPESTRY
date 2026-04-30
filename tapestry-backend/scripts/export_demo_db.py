from __future__ import annotations

import os
import shutil
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "data" / "tapestry.duckdb"
DEST = ROOT / "data" / "tapestry_demo.duckdb"
TMP_DIR = ROOT / "data" / "tmp_export"

STRIP_BLOBS = {
    "race_web_articles": ["embedding"],
    "media_event_articles": ["embedding"],
    "ideology_corpus_chunks": ["embedding"],
}


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def main() -> None:
    if not SOURCE.exists():
        raise FileNotFoundError(f"Source database not found: {SOURCE}")

    if DEST.exists():
        DEST.unlink()

    shutil.rmtree(TMP_DIR, ignore_errors=True)
    TMP_DIR.mkdir(parents=True, exist_ok=True)

    src = duckdb.connect(str(SOURCE), read_only=True)
    dst = duckdb.connect(str(DEST))

    try:
        tables = src.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            ORDER BY table_name
            """
        ).fetchall()

        print(f"Exporting {len(tables)} tables...")

        for (table,) in tables:
            try:
                cols = src.execute(f"DESCRIBE {quote_ident(table)}").fetchall()
                col_names = [c[0] for c in cols]

                strip = STRIP_BLOBS.get(table, [])
                keep_cols = [c for c in col_names if c not in strip]
                cols_str = ", ".join(quote_ident(c) for c in keep_cols)
                parquet_path = TMP_DIR / f"{table}.parquet"
                parquet_sql = str(parquet_path).replace("\\", "/").replace("'", "''")

                src.execute(
                    f"""
                    COPY (SELECT {cols_str} FROM {quote_ident(table)})
                    TO '{parquet_sql}' (FORMAT PARQUET)
                    """
                )

                dst.execute(
                    f"""
                    CREATE TABLE {quote_ident(table)} AS
                    SELECT * FROM read_parquet('{parquet_sql}')
                    """
                )

                n = dst.execute(f"SELECT COUNT(*) FROM {quote_ident(table)}").fetchone()[0]
                blob_info = f" (stripped: {', '.join(strip)})" if strip else ""
                print(f"  OK: {table} ({n} rows){blob_info}")
            except Exception as exc:
                print(f"  SKIP: {table}: {exc}")
    finally:
        src.close()
        dst.close()
        shutil.rmtree(TMP_DIR, ignore_errors=True)

    orig = os.path.getsize(SOURCE) / (1024 * 1024)
    demo = os.path.getsize(DEST) / (1024 * 1024)
    saved = orig - demo
    pct = (1 - demo / orig) * 100 if orig else 0
    print(f"\nOriginal DB: {orig:.1f} MB")
    print(f"Demo DB:     {demo:.1f} MB")
    print(f"Saved:       {saved:.1f} MB ({pct:.0f}% reduction)")


if __name__ == "__main__":
    main()
