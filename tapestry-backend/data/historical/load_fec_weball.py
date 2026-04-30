from __future__ import annotations

import csv
import hashlib
import sys
import zipfile
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.connection import init_db, write_connection
from utils.geo import normalize_district_id

DEFAULT_ZIP = Path.home() / "Downloads" / "weball26.zip"
LOCAL_ZIP = ROOT / "data" / "historical" / "weball26.zip"
SOURCE_NAME = "FEC weball26"


def _money(value: str | None) -> float | None:
    if value in (None, "", " "):
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None


def _party(value: str | None) -> str | None:
    value = (value or "").upper()
    if value == "DEM":
        return "D"
    if value == "REP":
        return "R"
    if value in {"IND", "GRE", "LIB"}:
        return value
    return value or None


def _name(value: str) -> str:
    parts = [part.strip(" .") for part in value.split(",") if part.strip(" .")]
    if len(parts) >= 2:
        return f"{parts[1].title()} {parts[0].title()}"
    return value.title()


def _district_id(state: str, district: str) -> str:
    number = int(district or "0")
    return normalize_district_id(f"{state}-AL" if number == 0 else f"{state}-{number}")


def _find_zip(path: str | Path | None = None) -> Path:
    candidates = [Path(path)] if path else [LOCAL_ZIP, DEFAULT_ZIP]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"FEC weball26.zip not found. Put it at {LOCAL_ZIP} or {DEFAULT_ZIP}.")


def _rows_from_zip(path: Path) -> list[dict]:
    with zipfile.ZipFile(path) as archive:
        name = next((n for n in archive.namelist() if n.lower().endswith(".txt")), None)
        if not name:
            raise FileNotFoundError("No .txt file found inside FEC weball zip")
        with archive.open(name) as raw:
            text = (line.decode("latin-1") for line in raw)
            reader = csv.reader(text, delimiter="|")
            rows = []
            for cols in reader:
                if len(cols) < 30 or not cols[0].startswith("H"):
                    continue
                state = cols[18]
                if not state:
                    continue
                district_id = _district_id(state, cols[19])
                rows.append({
                    "fec_candidate_id": cols[0],
                    "district_id": district_id,
                    "candidate_name": _name(cols[1]),
                    "party": _party(cols[4]),
                    "incumbent_status": cols[2] or None,
                    "total_receipts": _money(cols[5]),
                    "total_disbursements": _money(cols[7]),
                    "cash_on_hand": _money(cols[10]),
                    "individual_contributions": _money(cols[17]),
                    "pac_contributions": _money(cols[25]),
                    "party_contributions": _money(cols[26]),
                    "coverage_end_date": _date(cols[27]),
                    "source_file": str(path),
                    "last_updated": date.today(),
                })
    return rows


def load_fec_weball(path: str | Path | None = None) -> None:
    init_db()
    source = _find_zip(path)
    rows = _rows_from_zip(source)
    with write_connection() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS fec_candidate_finance (
                fec_candidate_id VARCHAR PRIMARY KEY,
                district_id VARCHAR,
                candidate_name VARCHAR,
                party VARCHAR,
                incumbent_status VARCHAR,
                total_receipts DOUBLE,
                total_disbursements DOUBLE,
                cash_on_hand DOUBLE,
                individual_contributions DOUBLE,
                pac_contributions DOUBLE,
                party_contributions DOUBLE,
                coverage_end_date DATE,
                source_file VARCHAR,
                last_updated DATE
            )
        """)
        con.execute("DELETE FROM fec_candidate_finance")
        for row in rows:
            con.execute(
                "INSERT OR REPLACE INTO fec_candidate_finance VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(row.values()),
            )
            candidate_id = hashlib.sha1(f"{row['district_id']}:{row['fec_candidate_id']}".encode("utf-8")).hexdigest()[:16]
            con.execute(
                "INSERT OR REPLACE INTO candidate_roster_2026 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    candidate_id,
                    row["district_id"],
                    row["candidate_name"],
                    row["party"],
                    row["incumbent_status"] == "I",
                    None,
                    row["fec_candidate_id"],
                    None,
                    f"https://www.fec.gov/data/candidate/{row['fec_candidate_id']}/",
                    "incumbent" if row["incumbent_status"] == "I" else "declared",
                    SOURCE_NAME,
                    date.today(),
                ],
            )
            if row["incumbent_status"] == "I":
                con.execute(
                    "UPDATE house_roster SET fec_candidate_id=? WHERE district_id=?",
                    [row["fec_candidate_id"], row["district_id"]],
                )
        con.execute("""
            INSERT OR REPLACE INTO candidate_quality
            SELECT
                fec_candidate_id,
                district_id,
                last_updated,
                party,
                NULL,
                NULL,
                CASE WHEN total_receipts IS NOT NULL AND total_receipts != 0
                     THEN individual_contributions / total_receipts
                     ELSE NULL END,
                NULL, NULL, NULL, NULL, FALSE, NULL, NULL, NULL
            FROM fec_candidate_finance
        """)
    print(f"Loaded {len(rows)} FEC House candidate finance rows from {source}")


if __name__ == "__main__":
    load_fec_weball()
