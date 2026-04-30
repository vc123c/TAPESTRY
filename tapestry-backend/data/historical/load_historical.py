from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date
from pathlib import Path

from db.connection import init_db, write_connection
from utils.geo import normalize_district_id

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_HOUSE_RESULTS = [
    ROOT / "data" / "historical" / "1976-2024-house.tab",
    Path.home() / "Downloads" / "1976-2024-house.tab",
]


def _redistricting_era(year: int) -> str:
    if year >= 2022:
        return "2022"
    if year >= 2012:
        return "2012"
    if year >= 2002:
        return "2002"
    if year >= 1992:
        return "1992"
    if year >= 1982:
        return "1982"
    return "1972"


def _district_id(state_po: str, district: str) -> str:
    number = int(float(district))
    return normalize_district_id(f"{state_po}-AL" if number == 0 else f"{state_po}-{number}")


def _bool(value: str) -> bool:
    return str(value).strip().upper() == "TRUE"


def _find_house_results(path: str | Path | None = None) -> Path:
    candidates = [Path(path)] if path else DEFAULT_HOUSE_RESULTS
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not find 1976-2024-house.tab. Put it in data/historical or Downloads.")


def _candidate_name(candidates: list[tuple[str, int]]) -> str | None:
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[1])[0]


def load_historical(path: str | Path | None = None) -> None:
    """Load MIT Election Lab / Dataverse U.S. House returns into DuckDB."""
    init_db()
    source = _find_house_results(path)
    grouped = defaultdict(lambda: {
        "d_votes": 0,
        "r_votes": 0,
        "totalvotes": 0,
        "special": False,
        "d_candidates": [],
        "r_candidates": [],
    })

    with source.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("office") != "US HOUSE" or row.get("stage") != "GEN":
                continue
            if _bool(row.get("writein", "FALSE")):
                continue
            party = (row.get("party") or "").upper()
            if party not in {"DEMOCRAT", "REPUBLICAN"}:
                continue
            year = int(row["year"])
            district_id = _district_id(row["state_po"], row["district"])
            key = (district_id, year)
            votes = int(row.get("candidatevotes") or 0)
            grouped[key]["totalvotes"] = max(grouped[key]["totalvotes"], int(row.get("totalvotes") or 0))
            grouped[key]["special"] = grouped[key]["special"] or _bool(row.get("special", "FALSE"))
            if party == "DEMOCRAT":
                grouped[key]["d_votes"] += votes
                grouped[key]["d_candidates"].append((row.get("candidate") or "Democratic nominee", votes))
            else:
                grouped[key]["r_votes"] += votes
                grouped[key]["r_candidates"].append((row.get("candidate") or "Republican nominee", votes))

    with write_connection() as con:
        for (district_id, year), data in grouped.items():
            total = data["totalvotes"] or (data["d_votes"] + data["r_votes"])
            if total <= 0:
                continue
            d_pct = 100 * data["d_votes"] / total
            r_pct = 100 * data["r_votes"] / total
            margin = d_pct - r_pct
            uncontested = data["d_votes"] == 0 or data["r_votes"] == 0
            con.execute(
                "INSERT OR REPLACE INTO election_results VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    district_id,
                    year,
                    "midterm" if year % 4 == 2 else "presidential",
                    _redistricting_era(year),
                    d_pct,
                    r_pct,
                    margin,
                    "D" if margin > 0 else "R",
                    uncontested,
                    data["special"],
                    "D" if margin > 0 else "R",
                    True,
                    _candidate_name(data["d_candidates"]),
                    _candidate_name(data["r_candidates"]),
                    1.0,
                ],
            )
    print(f"Historical load complete for {len(grouped)} district-years from {source} at {date.today()}")


if __name__ == "__main__":
    load_historical()
