from __future__ import annotations

from datetime import date

import polars as pl

from db.connection import write_connection
from scrapers.base import BaseScraper


DONOR_COLUMNS = [
    "district_id",
    "incumbent_name",
    "as_of",
    "source_name",
    "top_donor_sector",
    "top_donor_amount",
    "pro_israel_pac_amount",
    "aipac_related_amount",
    "defense_sector_amount",
    "healthcare_sector_amount",
    "finance_sector_amount",
    "small_dollar_share",
    "medicare_posture",
    "israel_posture",
    "defense_industry_posture",
    "labor_posture",
    "notes",
]


class OpenSecretsScraper(BaseScraper):
    source_name = "opensecrets"
    output_path = "data/raw/opensecrets_latest.parquet"

    def fetch(self) -> pl.DataFrame:
        self.logger.warning(
            "OpenSecrets scraper has no free authenticated source configured; returning no donor_transparency rows"
        )
        return pl.DataFrame({column: [] for column in DONOR_COLUMNS})

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def run(self) -> bool:
        ok = super().run()
        df = pl.read_parquet(self.output_path)
        if df.height == 0:
            return ok
        with write_connection() as con:
            con.register("donor_transparency_df", df)
            con.execute("INSERT OR REPLACE INTO donor_transparency SELECT * FROM donor_transparency_df")
        return ok


if __name__ == "__main__":
    raise SystemExit(0 if OpenSecretsScraper().run() else 1)
