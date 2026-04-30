from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
import polars as pl

from db.connection import ROOT, get_read_connection, write_connection
from scrapers.base import BaseScraper


class FECScraper(BaseScraper):
    source_name = "fec"
    output_path = "data/raw/fec_competitive_latest.parquet"
    base_url = "https://api.open.fec.gov/v1"

    def validate(self, df: pl.DataFrame) -> bool:
        return isinstance(df, pl.DataFrame)

    def _cache_path(self, candidate_id: str) -> Path:
        return ROOT / "data" / "raw" / f"fec_cache_{candidate_id}.json"

    def _fetch_totals(self, candidate_id: str) -> dict | None:
        cache = self._cache_path(candidate_id)
        if cache.exists() and datetime.utcnow() - datetime.utcfromtimestamp(cache.stat().st_mtime) < timedelta(days=7):
            return json.loads(cache.read_text(encoding="utf-8"))
        params = {"cycle": 2026, "api_key": os.getenv("FEC_API_KEY", "DEMO_KEY")}
        try:
            response = httpx.get(f"{self.base_url}/candidates/{candidate_id}/totals/", params=params, timeout=30)
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            self.logger.warning("FEC fetch failed url=%s candidate=%s error=%s timestamp=%s", f"{self.base_url}/candidates/{candidate_id}/totals/", candidate_id, type(exc).__name__, datetime.utcnow().isoformat())
            return None
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(payload), encoding="utf-8")
        return payload

    def fetch(self) -> pl.DataFrame:
        with get_read_connection() as con:
            local_count = con.execute(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='fec_candidate_finance'"
            ).fetchone()[0]
            if local_count:
                finance = con.execute("SELECT * FROM fec_candidate_finance").pl()
                if finance.height:
                    self.logger.info("Using local FEC weball finance table with %s rows", finance.height)
                    return finance
        with get_read_connection() as con:
            candidates = con.execute(
                "SELECT candidate_id, district_id, candidate_name, party, fec_candidate_id FROM candidate_roster_2026 WHERE fec_candidate_id IS NOT NULL"
            ).fetchall()
        rows = []
        for candidate_id, district_id, name, party, fec_candidate_id in candidates:
            payload = self._fetch_totals(fec_candidate_id)
            if not payload:
                continue
            result = (payload.get("results") or [{}])[0]
            receipts = result.get("receipts") or result.get("total_receipts")
            small = result.get("individual_unitemized_contributions") or result.get("small_individual_contributions")
            rows.append({
                "candidate_id": candidate_id,
                "district_id": district_id,
                "candidate_name": name,
                "party": party,
                "fec_candidate_id": fec_candidate_id,
                "receipts": receipts,
                "disbursements": result.get("disbursements") or result.get("total_disbursements"),
                "cash_on_hand": result.get("cash_on_hand_end_period"),
                "small_dollar_share": (small / receipts) if small is not None and receipts not in (None, 0) else None,
                "as_of": date.today(),
            })
        return pl.DataFrame(rows) if rows else pl.DataFrame({
            "candidate_id": [], "district_id": [], "candidate_name": [], "party": [], "fec_candidate_id": [],
            "receipts": [], "disbursements": [], "cash_on_hand": [], "small_dollar_share": [], "as_of": [],
        })

    def run(self) -> bool:
        ok = super().run()
        df = pl.read_parquet(self.output_path)
        if df.height == 0:
            self.logger.warning("No FEC rows loaded; candidates may be missing fec_candidate_id")
            return ok
        if "fec_candidate_id" in df.columns and "total_receipts" in df.columns:
            return ok
        by_district = {}
        for row in df.to_dicts():
            by_district.setdefault(row["district_id"], {})[row["party"]] = row
        with write_connection() as con:
            for row in df.to_dicts():
                peers = by_district.get(row["district_id"], {})
                d_receipts = (peers.get("D") or {}).get("receipts")
                r_receipts = (peers.get("R") or {}).get("receipts")
                fundraising_ratio = (d_receipts / r_receipts) if d_receipts not in (None, 0) and r_receipts not in (None, 0) else None
                con.execute(
                    "INSERT OR REPLACE INTO candidate_quality VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        row["candidate_id"], row["district_id"], row["as_of"], row["party"], None,
                        fundraising_ratio, row["small_dollar_share"], None, None, None, None, False, None, None, None,
                    ],
                )
        return ok


if __name__ == "__main__":
    raise SystemExit(0 if FECScraper().run() else 1)
