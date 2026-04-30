from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from db.connection import ROOT
from db.connection import init_db, write_connection
from utils.geo import normalize_district_id


def _pvi_label(value: float | None) -> str | None:
    if value is None:
        return None
    if abs(value) < 0.05:
        return "EVEN"
    return f"{'D' if value > 0 else 'R'}+{abs(value):.1f}"

def _load_presidential_margins(path: Path) -> dict[tuple[str, int], float]:
    if not path.exists():
        print(
            "Presidential-by-CD data not found at data/historical/presidential_by_cd.csv.\n"
            "Download from: https://dataverse.harvard.edu/dataset.xhtml?persistentId=doi:10.7910/DVN/DGDRDT\n"
            "Place the CSV at that path and rerun. Keeping existing House-only TAPESTRY PVI."
        )
        return {}
    df = pl.read_csv(path, infer_schema_length=10000, ignore_errors=True)
    cols = {c.lower(): c for c in df.columns}

    def col(*names: str) -> str | None:
        for name in names:
            if name.lower() in cols:
                return cols[name.lower()]
        return None

    district_col = col("district_id", "district", "cd_id")
    state_col = col("state_po", "state_abbr", "state")
    cd_col = col("district", "cd", "congressional_district")
    year_col = col("year")
    party_col = col("party", "party_simplified")
    candidatevotes_col = col("candidatevotes", "votes", "candidate_votes")
    totalvotes_col = col("totalvotes", "total_votes")
    d_pct_col = col("d_vote_pct", "democratic_pct", "dem_pct")
    r_pct_col = col("r_vote_pct", "republican_pct", "rep_pct")

    margins: dict[tuple[str, int], float] = {}
    if d_pct_col and r_pct_col and year_col:
        for row in df.select([c for c in [district_col, state_col, cd_col, year_col, d_pct_col, r_pct_col] if c]).iter_rows(named=True):
            district_id = row.get(district_col) if district_col else None
            if not district_id and state_col and cd_col:
                cd = row.get(cd_col)
                district_id = f"{row.get(state_col)}-{int(cd):02d}" if str(cd).isdigit() and int(cd) > 0 else f"{row.get(state_col)}-AL"
            if district_id:
                margins[(normalize_district_id(str(district_id)), int(row[year_col]))] = float(row[d_pct_col]) - float(row[r_pct_col])
        return margins

    if not all([state_col, cd_col, year_col, party_col, candidatevotes_col]):
        print("Presidential-by-CD file format not recognized. Keeping House-only TAPESTRY PVI.")
        return {}

    grouped = {}
    for row in df.iter_rows(named=True):
        party = str(row.get(party_col, "")).upper()
        if party not in {"DEMOCRAT", "DEMOCRATIC", "REPUBLICAN"}:
            continue
        cd = row.get(cd_col)
        district_id = normalize_district_id(f"{row.get(state_col)}-{int(cd)}" if str(cd).isdigit() and int(cd) > 0 else f"{row.get(state_col)}-AL")
        key = (district_id.upper(), int(row[year_col]))
        grouped.setdefault(key, {"D": 0.0, "R": 0.0})
        side = "D" if party in {"DEMOCRAT", "DEMOCRATIC"} else "R"
        grouped[key][side] += float(row.get(candidatevotes_col) or 0)
    for key, votes in grouped.items():
        total = votes["D"] + votes["R"]
        if total:
            margins[(normalize_district_id(key[0]), key[1])] = (votes["D"] - votes["R"]) / total * 100
    return margins


def build_tapestry_pvi() -> None:
    """
    Free Cook-PVI substitute.

    Uses MIT House results already loaded into election_results:
    60% 2024 House margin + 25% 2022 House margin + 15% 2020 House margin.
    This is not Cook PVI, and it is labeled as TAPESTRY PVI in the data.
    """
    init_db()
    presidential = _load_presidential_margins(ROOT / "data" / "historical" / "presidential_by_cd.csv")
    composite_available = bool(presidential)
    with write_connection() as con:
        rows = con.execute("""
            WITH margins AS (
                SELECT
                    district_id,
                    MAX(CASE WHEN year = 2024 THEN margin END) AS m2024,
                    MAX(CASE WHEN year = 2022 THEN margin END) AS m2022,
                    MAX(CASE WHEN year = 2020 THEN margin END) AS m2020
                FROM election_results
                GROUP BY district_id
            )
            SELECT district_id, m2024, m2022, m2020
            FROM margins
        """).fetchall()
        updated = 0
        for district_id, m2024, m2022, m2020 in rows:
            if composite_available:
                parts = [
                    (presidential.get((district_id, 2024)), 0.40),
                    (m2024, 0.25),
                    (m2022, 0.20),
                    (presidential.get((district_id, 2020)), 0.15),
                ]
                source = "TAPESTRY_PVI_COMPOSITE"
            else:
                parts = [(m2024, 0.60), (m2022, 0.25), (m2020, 0.15)]
                source = "TAPESTRY_PVI_MIT_HOUSE"
            available = [(float(v), w) for v, w in parts if v is not None]
            if not available:
                continue
            total_w = sum(w for _, w in available)
            value = sum(v * w for v, w in available) / total_w
            con.execute(
                """
                UPDATE house_roster
                SET cook_pvi = ?,
                    cook_pvi_numeric = ?,
                    data_source = CASE
                        WHEN data_source LIKE '%' || ? || '%' THEN data_source
                        ELSE data_source || '; ' || ?
                    END
                WHERE district_id = ?
                """,
                [_pvi_label(value), value, source, source, district_id],
            )
            updated += 1
    label = "composite presidential + House returns" if composite_available else "MIT House returns"
    print(f"Built TAPESTRY PVI for {updated} districts from {label} at {date.today()}")


if __name__ == "__main__":
    build_tapestry_pvi()
