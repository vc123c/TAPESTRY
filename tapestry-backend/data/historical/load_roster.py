from __future__ import annotations

import csv
import hashlib
import re
import time
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote
from xml.etree import ElementTree as ET

import httpx
from bs4 import BeautifulSoup

from db.connection import init_db, write_connection
from utils.geo import STATE_DISTRICT_COUNT, normalize_district_id
from utils.logging import setup_logging

logger = setup_logging(__name__)

ROOT = Path(__file__).resolve().parents[2]
HOUSE_XML_URL = "https://clerk.house.gov/xml/lists/MemberData.xml"
COOK_PVI_URL = "https://www.cookpolitical.com/cook-pvi/2025-partisan-voting-index/house-map"
BALLOTPEDIA_2026_URL = "https://ballotpedia.org/United_States_House_of_Representatives_elections,_2026"
MIT_RESULTS_CSV = ROOT / "data" / "historical" / "mit_house_results.csv"
MIT_RESULTS_TAB = ROOT / "data" / "historical" / "1976-2024-house.tab"
DOWNLOAD_TAB = Path.home() / "Downloads" / "1976-2024-house.tab"
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS",
    "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY",
    "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY",
}


def _valid_district(state: str, district_number: int) -> bool:
    count = STATE_DISTRICT_COUNT.get(state)
    if not count:
        logger.warning("Roster data error: invalid state %s", state)
        return False
    if count == 1 and district_number == 0:
        return True
    if 1 <= district_number <= count:
        return True
    logger.warning("Roster data error: %s district %s out of expected range 1-%s", state, district_number, count)
    return False


def ensure_roster_tables(con) -> None:
    """Roster tables are fully re-ingestable, so recreate them to migrate old scaffold schemas."""
    con.execute("DROP TABLE IF EXISTS house_roster")
    con.execute("""
        CREATE TABLE house_roster (
            district_id VARCHAR PRIMARY KEY,
            state_name VARCHAR,
            state_abbr VARCHAR,
            district_number INTEGER,
            incumbent_name VARCHAR,
            incumbent_party VARCHAR,
            incumbent_first_elected INTEGER,
            incumbent_bioguide_id VARCHAR,
            incumbent_url VARCHAR,
            fec_candidate_id VARCHAR,
            cook_pvi VARCHAR,
            cook_pvi_numeric DOUBLE,
            last_margin DOUBLE,
            retiring BOOLEAN DEFAULT FALSE,
            data_source VARCHAR,
            last_updated DATE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS candidate_roster_2026 (
            candidate_id VARCHAR PRIMARY KEY,
            district_id VARCHAR,
            candidate_name VARCHAR,
            party VARCHAR,
            is_incumbent BOOLEAN,
            declared_date DATE,
            fec_candidate_id VARCHAR,
            ballotpedia_url VARCHAR,
            campaign_website VARCHAR,
            primary_status VARCHAR,
            data_source VARCHAR,
            last_updated DATE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS member_committees (
            bioguide_id VARCHAR,
            committee_name VARCHAR,
            role VARCHAR,
            PRIMARY KEY (bioguide_id, committee_name)
        )
    """)


def _text(node: ET.Element | None, path: str, default: str = "") -> str:
    found = node.find(path) if node is not None else None
    return (found.text or "").strip() if found is not None else default


def _district_id(state: str, statedistrict: str, district_text: str) -> tuple[str, int]:
    if statedistrict.endswith("00") or "large" in district_text.lower():
        return normalize_district_id(f"{state}-AL"), 0
    number = int(statedistrict[-2:])
    return normalize_district_id(f"{state}-{number}"), number


def _first_elected(info: ET.Element | None) -> int | None:
    elected = info.find("elected-date") if info is not None else None
    raw = elected.attrib.get("date") if elected is not None else ""
    return int(raw[:4]) if raw[:4].isdigit() else None


def _pvi_numeric(value: str | None) -> float | None:
    if not value:
        return None
    clean = value.strip().upper().replace(" ", "")
    if clean in {"EVEN", "TIE"}:
        return 0.0
    match = re.search(r"([DR])\+?(\d+(?:\.\d+)?)", clean)
    if not match:
        return None
    number = float(match.group(2))
    return number if match.group(1) == "D" else -number


def _house_url(bioguide_id: str | None) -> str | None:
    return f"https://clerk.house.gov/members/{bioguide_id}" if bioguide_id else None


def load_house_clerk() -> tuple[list[dict], dict[str, list[tuple[str, str]]], str]:
    response = httpx.get(HOUSE_XML_URL, timeout=45, follow_redirects=True)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    publish_date = root.attrib.get("publish-date", "")
    committee_names = {
        committee.attrib.get("comcode", ""): _text(committee, "committee-fullname").replace("Committee on ", "").strip()
        for committee in root.findall(".//committees/committee")
    }
    roster = []
    committees: dict[str, list[tuple[str, str]]] = {}
    for member in root.findall(".//members/member"):
        info = member.find("member-info")
        state_node = info.find("state") if info is not None else None
        state = state_node.attrib.get("postal-code", "") if state_node is not None else ""
        if state not in US_STATES:
            continue
        district_text = _text(info, "district")
        if "delegate" in district_text.lower() or "resident" in district_text.lower():
            continue
        district_id, district_number = _district_id(state, _text(member, "statedistrict"), district_text)
        if not _valid_district(state, district_number):
            continue
        bioguide_id = _text(info, "bioguideID")
        official_name = _text(info, "official-name")
        party = _text(info, "party")
        caucus = _text(info, "caucus")
        if party not in {"D", "R"} and caucus in {"D", "R"}:
            party = caucus
        first_elected = _first_elected(info)
        retiring = False
        if not official_name:
            predecessor = member.find("predecessor-info")
            pred_name = _text(predecessor, "pred-official-name")
            if pred_name:
                official_name = f"Vacant (formerly {pred_name})"
                party = _text(predecessor, "pred-party")
                bioguide_id = _text(predecessor, "pred-memindex")
                retiring = True
        member_committees = []
        for assignment in member.findall("./committee-assignments/committee"):
            name = committee_names.get(assignment.attrib.get("comcode", ""))
            if name:
                role = assignment.attrib.get("leadership") or "member"
                member_committees.append((name, role))
        committees[bioguide_id] = member_committees
        roster.append({
            "district_id": district_id,
            "state_name": _text(state_node, "state-fullname"),
            "state_abbr": state,
            "district_number": district_number,
            "incumbent_name": official_name,
            "incumbent_party": party,
            "incumbent_first_elected": first_elected,
            "incumbent_bioguide_id": bioguide_id,
            "incumbent_url": _house_url(bioguide_id),
            "fec_candidate_id": None,
            "cook_pvi": None,
            "cook_pvi_numeric": None,
            "last_margin": None,
            "retiring": retiring,
            "data_source": HOUSE_XML_URL,
            "last_updated": date.today(),
        })
    return roster, committees, publish_date


def load_cook_pvi() -> dict[str, tuple[str, float | None]]:
    try:
        response = httpx.get(COOK_PVI_URL, timeout=45, follow_redirects=True)
        response.raise_for_status()
    except Exception as exc:
        logger.warning("Cook PVI scrape failed url=%s error=%s timestamp=%s", COOK_PVI_URL, type(exc).__name__, datetime.utcnow().isoformat())
        return {}
    soup = BeautifulSoup(response.text, "html.parser")
    mapping: dict[str, tuple[str, float | None]] = {}
    pattern = re.compile(r"\b([A-Z]{2})[-\s]?(\d{1,2}|AL|At Large)\b.*?\b((?:D|R)\+?\d+|EVEN)\b", re.I)
    for text in soup.stripped_strings:
        match = pattern.search(text)
        if not match:
            continue
        state = match.group(1).upper()
        district = match.group(2).upper()
        district_id = normalize_district_id(f"{state}-AL" if district in {"AL", "AT LARGE"} else f"{state}-{int(district)}")
        pvi = match.group(3).upper().replace(" ", "")
        mapping[district_id] = (pvi, _pvi_numeric(pvi))
    return mapping


def load_2024_margins() -> dict[str, float]:
    path = MIT_RESULTS_CSV if MIT_RESULTS_CSV.exists() else MIT_RESULTS_TAB if MIT_RESULTS_TAB.exists() else DOWNLOAD_TAB
    if not path.exists():
        logger.warning("MIT Election Lab data not found at %s; skipping 2024 margins", MIT_RESULTS_CSV)
        return {}
    grouped: dict[str, dict[str, int]] = {}
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        first_line = handle.readline()
        handle.seek(0)
        delimiter = "\t" if "\t" in first_line else ","
        reader = csv.DictReader(handle, delimiter=delimiter)
        for row in reader:
            if str(row.get("year")) != "2024" or row.get("office") != "US HOUSE" or row.get("stage") != "GEN":
                continue
            party = (row.get("party") or "").upper()
            if party not in {"DEMOCRAT", "REPUBLICAN"}:
                continue
            state = row.get("state_po")
            district_num = int(float(row.get("district") or 0))
            district_id = normalize_district_id(f"{state}-AL" if district_num == 0 else f"{state}-{district_num}")
            grouped.setdefault(district_id, {"D": 0, "R": 0, "total": 0})
            votes = int(row.get("candidatevotes") or 0)
            grouped[district_id]["D" if party == "DEMOCRAT" else "R"] += votes
            grouped[district_id]["total"] = max(grouped[district_id]["total"], int(row.get("totalvotes") or 0))
    return {
        district_id: 100 * (values["D"] - values["R"]) / values["total"]
        for district_id, values in grouped.items()
        if values["total"] > 0
    }


def load_ballotpedia_candidates() -> list[dict]:
    try:
        response = httpx.get(BALLOTPEDIA_2026_URL, timeout=45, follow_redirects=True)
        response.raise_for_status()
        time.sleep(1.5)
    except Exception as exc:
        logger.warning("Ballotpedia scrape failed url=%s error=%s timestamp=%s", BALLOTPEDIA_2026_URL, type(exc).__name__, datetime.utcnow().isoformat())
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    candidates = []
    district_pattern = re.compile(r"\b([A-Z][a-z]+|[A-Z]{2})['’]?\s*(?:-| )?(\d{1,2})(?:st|nd|rd|th)? Congressional District", re.I)
    abbr_by_name = {row["state_name"]: row["state_abbr"] for row in load_house_clerk()[0]}
    for link in soup.find_all("a", href=True):
        name = link.get_text(" ", strip=True)
        href = link["href"]
        context = link.find_parent().get_text(" ", strip=True) if link.find_parent() else ""
        match = district_pattern.search(context)
        if not match or len(name.split()) < 2 or len(name) > 80:
            continue
        state_raw = match.group(1)
        state = state_raw.upper() if len(state_raw) == 2 else abbr_by_name.get(state_raw)
        if not state:
            continue
        party = "D" if re.search(r"\bDemocrat", context, re.I) else "R" if re.search(r"\bRepublican", context, re.I) else None
        district_id = normalize_district_id(f"{state}-{int(match.group(2))}")
        candidate_id = hashlib.sha1(f"{district_id}:{name}:{party}".encode("utf-8")).hexdigest()[:16]
        candidates.append({
            "candidate_id": candidate_id,
            "district_id": district_id,
            "candidate_name": name,
            "party": party,
            "is_incumbent": "incumbent" in context.lower(),
            "declared_date": None,
            "fec_candidate_id": None,
            "ballotpedia_url": href if href.startswith("http") else f"https://ballotpedia.org{href}",
            "campaign_website": None,
            "primary_status": "declared",
            "data_source": BALLOTPEDIA_2026_URL,
            "last_updated": date.today(),
        })
    unique = {}
    for candidate in candidates:
        unique[candidate["candidate_id"]] = candidate
    return list(unique.values())


def incumbent_candidate_rows(roster: list[dict]) -> list[dict]:
    rows = []
    for member in roster:
        if str(member["incumbent_name"] or "").startswith("Vacant"):
            continue
        candidate_id = hashlib.sha1(f"{member['district_id']}:{member['incumbent_bioguide_id']}:incumbent".encode("utf-8")).hexdigest()[:16]
        rows.append({
            "candidate_id": candidate_id,
            "district_id": member["district_id"],
            "candidate_name": member["incumbent_name"],
            "party": member["incumbent_party"],
            "is_incumbent": True,
            "declared_date": None,
            "fec_candidate_id": member["fec_candidate_id"],
            "ballotpedia_url": None,
            "campaign_website": member["incumbent_url"],
            "primary_status": "incumbent",
            "data_source": HOUSE_XML_URL,
            "last_updated": date.today(),
        })
    return rows


def run() -> None:
    init_db()
    roster, committees, publish_date = load_house_clerk()
    pvi = load_cook_pvi()
    margins = load_2024_margins()
    candidates = incumbent_candidate_rows(roster) + load_ballotpedia_candidates()
    for row in roster:
        if row["district_id"] in pvi:
            row["cook_pvi"], row["cook_pvi_numeric"] = pvi[row["district_id"]]
        row["last_margin"] = margins.get(row["district_id"])
    with write_connection() as con:
        ensure_roster_tables(con)
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS incumbent_status_2026 (
                district_id VARCHAR PRIMARY KEY,
                incumbent_name VARCHAR,
                party VARCHAR,
                status VARCHAR,
                reason VARCHAR,
                source_name VARCHAR,
                source_url VARCHAR,
                observed_at TIMESTAMP
            )
            """
        )
        con.execute("DELETE FROM member_committees")
        con.execute("DELETE FROM candidate_roster_2026 WHERE data_source = ?", [HOUSE_XML_URL])
        con.execute("DELETE FROM candidate_roster_2026 WHERE data_source = ?", [BALLOTPEDIA_2026_URL])
        for row in roster:
            con.execute(
                "INSERT OR REPLACE INTO house_roster VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    row["district_id"], row["state_name"], row["state_abbr"], row["district_number"],
                    row["incumbent_name"], row["incumbent_party"], row["incumbent_first_elected"],
                    row["incumbent_bioguide_id"], row["incumbent_url"], row["fec_candidate_id"],
                    row["cook_pvi"], row["cook_pvi_numeric"], row["last_margin"], row["retiring"],
                    row["data_source"], row["last_updated"],
                ],
            )
            if row["retiring"] and str(row["incumbent_name"] or "").startswith("Vacant"):
                former = row["incumbent_name"].replace("Vacant (formerly ", "").rstrip(")")
                con.execute(
                    "INSERT OR REPLACE INTO incumbent_status_2026 VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        row["district_id"], former, row["incumbent_party"], "vacant",
                        "house_clerk_vacancy", HOUSE_XML_URL, row["incumbent_url"], datetime.utcnow(),
                    ],
                )
        for bioguide_id, rows in committees.items():
            for committee_name, role in rows:
                con.execute("INSERT OR REPLACE INTO member_committees VALUES (?, ?, ?)", [bioguide_id, committee_name, role])
        for candidate in candidates:
            con.execute(
                "INSERT OR REPLACE INTO candidate_roster_2026 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                list(candidate.values()),
            )
    print(
        f"Loaded {len(roster)} house members, {len(candidates)} 2026 candidates, "
        f"{sum(1 for row in roster if row['cook_pvi'])} districts with Cook PVI, "
        f"{sum(1 for row in roster if row['last_margin'] is not None)} districts with 2024 margin"
    )
    if not pvi:
        print("Cook PVI scrape returned no rows; rerun when the Cook table is reachable or add a local CSV import.")
    print(f"House Clerk publish date: {publish_date}")


if __name__ == "__main__":
    run()
