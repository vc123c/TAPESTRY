from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import polars as pl

from db.connection import write_connection
from utils.logging import setup_logging


class BaseScraper(ABC):
    """
    All scrapers inherit from this. Handles retry logic, logging,
    validation, scraper_runs bookkeeping, and parquet output.
    """

    source_name: str = "base"
    output_path: str = "data/raw/base.parquet"
    retries: int = 3

    def __init__(self, output_path: str | None = None) -> None:
        self.logger = setup_logging(f"scrapers.{self.source_name}")
        if output_path:
            self.output_path = output_path

    @abstractmethod
    def fetch(self) -> pl.DataFrame:
        raise NotImplementedError

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame) and df.height > 0

    def fallback(self) -> pl.DataFrame:
        path = Path(self.output_path)
        if path.exists():
            self.logger.warning("Using last known good data at %s", path)
            return pl.read_parquet(path)
        self.logger.warning("No last known good data; writing empty fallback for %s", self.source_name)
        return pl.DataFrame({"source": [self.source_name], "as_of": [datetime.utcnow()]})

    def _record_run(self, status: str, rows: int, error: str | None = None) -> None:
        try:
            with write_connection() as con:
                con.execute(
                    "INSERT INTO scraper_runs VALUES (?, ?, ?, ?, ?, ?)",
                    [self.source_name, datetime.utcnow(), status, rows, self.output_path, error],
                )
        except Exception as exc:
            self.logger.warning("Could not record scraper run: %s", exc)

    def run(self) -> bool:
        last_error: Exception | None = None
        for attempt in range(1, self.retries + 1):
            try:
                df = self.fetch()
                if not self.validate(df):
                    raise ValueError(f"{self.source_name} validation failed")
                path = Path(self.output_path)
                path.parent.mkdir(parents=True, exist_ok=True)
                df.write_parquet(path)
                self._record_run("success", df.height)
                self.logger.info("%s wrote %s rows to %s", self.source_name, df.height, path)
                return True
            except Exception as exc:
                last_error = exc
                self.logger.exception("%s attempt %s failed", self.source_name, attempt)
                time.sleep(min(2**attempt, 8))

        df = self.fallback()
        Path(self.output_path).parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(self.output_path)
        self._record_run("fallback", df.height, str(last_error))
        return False
