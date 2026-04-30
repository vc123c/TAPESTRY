from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path

import duckdb

from utils.env import load_local_env
from utils.logging import setup_logging

load_local_env()
logger = setup_logging(__name__)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "tapestry.duckdb"
SCHEMA_PATH = ROOT / "db" / "schema.sql"
WRITE_LOCK = threading.Lock()


def get_db_path() -> Path:
    return Path(os.getenv("DATABASE_PATH", str(DEFAULT_DB)))


def get_read_connection() -> duckdb.DuckDBPyConnection:
    database_path = get_db_path()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(database_path), read_only=True)


@contextmanager
def write_connection():
    database_path = get_db_path()
    database_path.parent.mkdir(parents=True, exist_ok=True)
    with WRITE_LOCK:
        con = duckdb.connect(str(database_path), read_only=False)
        try:
            yield con
        finally:
            con.close()


def init_db() -> None:
    with write_connection() as con:
        con.execute(SCHEMA_PATH.read_text(encoding="utf-8"))
    logger.info("DuckDB initialized at %s", get_db_path())


def table_count(table: str) -> int:
    with get_read_connection() as con:
        try:
            return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        except Exception:
            return 0


if __name__ == "__main__":
    init_db()
