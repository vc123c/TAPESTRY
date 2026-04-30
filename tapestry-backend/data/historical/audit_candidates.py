from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from db.connection import get_read_connection, write_connection
from utils.geo import normalize_district_id


REPORT_JSON = ROOT / "data" / "candidate_sanity_report.json"
REPORT_TXT = ROOT / "data" / "candidate_sanity_report.txt"


def _tokens(name: str | None) -> list[str]:
    drop = {"jr", "sr", "ii", "iii", "iv", "j", "g", "c", "a", "lynn", "allison"}
    return [t for t in re.findall(r"[a-z]+", (name or "").lower()) if t not in drop]


def person_key(name: str | None, party: str | None = None) -> str:
    tokens = _tokens(name)
    compact = f"{tokens[0]}:{tokens[-1]}" if len(tokens) >= 2 else "".join(tokens)
    return f"{compact}|{party or ''}"


def same_person(left: str | None, right: str | None) -> bool:
    a = _tokens(left)
    b = _tokens(right)
    if not a or not b:
        return False
    if a[0] == b[0] and a[-1] == b[-1]:
        return True
    ca = "".join(a)
    cb = "".join(b)
    return bool(ca and cb and (ca in cb or cb in ca))


def _candidate_score(row: dict) -> tuple[int, int, float]:
    return (
        1 if row.get("fec_candidate_id") else 0,
        1 if row.get("is_incumbent") else 0,
        float(row.get("receipts") or 0),
    )


def load_rows() -> tuple[list[dict], list[dict], list[dict], list[dict]]:
    with get_read_connection() as con:
        roster_cols = [d[0] for d in con.execute("SELECT * FROM house_roster LIMIT 0").description]
        roster = [dict(zip(roster_cols, row)) for row in con.execute("SELECT * FROM house_roster").fetchall()]
        cand_cols = [d[0] for d in con.execute("SELECT * FROM candidate_roster_2026 LIMIT 0").description]
        candidates = [dict(zip(cand_cols, row)) for row in con.execute("SELECT * FROM candidate_roster_2026").fetchall()]
        fec_cols = [d[0] for d in con.execute("SELECT * FROM fec_candidate_finance LIMIT 0").description]
        fec = [dict(zip(fec_cols, row)) for row in con.execute("SELECT * FROM fec_candidate_finance").fetchall()]
        status_cols = [d[0] for d in con.execute("SELECT * FROM incumbent_status_2026 LIMIT 0").description]
        statuses = [dict(zip(status_cols, row)) for row in con.execute("SELECT * FROM incumbent_status_2026").fetchall()]
    return roster, candidates, fec, statuses


def audit(state_filter: str | None = None) -> tuple[dict, list[str]]:
    roster, candidates, fec, statuses = load_rows()
    roster_by_district = {normalize_district_id(r["district_id"]): r for r in roster}
    fec_by_id = {f.get("fec_candidate_id"): f for f in fec if f.get("fec_candidate_id")}
    candidates_by_district: dict[str, list[dict]] = defaultdict(list)
    for candidate in candidates:
        did = normalize_district_id(candidate.get("district_id"))
        candidate["district_id"] = did
        if candidate.get("fec_candidate_id") in fec_by_id:
            candidate["receipts"] = fec_by_id[candidate["fec_candidate_id"]].get("total_receipts")
            candidate["fec_incumbent_status"] = fec_by_id[candidate["fec_candidate_id"]].get("incumbent_status")
        candidates_by_district[did].append(candidate)

    issues: list[dict] = []
    fixes: list[str] = []
    duplicate_candidate_ids: list[str] = []
    wrong_incumbent_candidate_ids: list[str] = []
    incumbent_seen: dict[str, list[str]] = defaultdict(list)

    wanted_prefix = f"{state_filter.upper()}-" if state_filter else None
    for did, member in sorted(roster_by_district.items()):
        if wanted_prefix and not did.startswith(wanted_prefix):
            continue
        incumbent = member.get("incumbent_name")
        if incumbent and not str(incumbent).startswith("Vacant"):
            incumbent_seen[person_key(incumbent, member.get("incumbent_party"))].append(did)

        grouped: dict[str, list[dict]] = defaultdict(list)
        for candidate in candidates_by_district.get(did, []):
            grouped[person_key(candidate.get("candidate_name"), candidate.get("party"))].append(candidate)

        for key, rows in grouped.items():
            if len(rows) <= 1:
                continue
            best = sorted(rows, key=_candidate_score, reverse=True)[0]
            losers = [r for r in rows if r.get("candidate_id") != best.get("candidate_id")]
            duplicate_candidate_ids.extend([r["candidate_id"] for r in losers if r.get("candidate_id")])
            issues.append({
                "type": "duplicate_candidate_rows",
                "district_id": did,
                "candidate": best.get("candidate_name"),
                "kept_candidate_id": best.get("candidate_id"),
                "dropped_candidate_ids": [r.get("candidate_id") for r in losers],
            })

        if incumbent and not str(incumbent).startswith("Vacant"):
            matches = [
                c for c in candidates_by_district.get(did, [])
                if c.get("is_incumbent") or c.get("fec_incumbent_status") == "I" or same_person(c.get("candidate_name"), incumbent)
            ]
            if not matches and not member.get("retiring"):
                issues.append({
                    "type": "incumbent_missing_from_2026_candidates",
                    "district_id": did,
                    "incumbent_name": incumbent,
                    "party": member.get("incumbent_party"),
                })
            mismarked = [
                c for c in candidates_by_district.get(did, [])
                if c.get("is_incumbent") and not same_person(c.get("candidate_name"), incumbent)
            ]
            for row in mismarked:
                if row.get("candidate_id"):
                    wrong_incumbent_candidate_ids.append(row["candidate_id"])
                issues.append({
                    "type": "wrong_incumbent_flag",
                    "district_id": did,
                    "incumbent_name": incumbent,
                    "candidate_name": row.get("candidate_name"),
                    "candidate_id": row.get("candidate_id"),
                })

    repeated_incumbents = {
        key: districts
        for key, districts in incumbent_seen.items()
        if key.split("|", 1)[0] and len(districts) > 1
    }
    for key, districts in repeated_incumbents.items():
        issues.append({"type": "same_incumbent_in_multiple_districts", "person_key": key, "districts": districts})

    status_by_district = {normalize_district_id(s.get("district_id")): s for s in statuses}
    for did, status in status_by_district.items():
        member = roster_by_district.get(did)
        if not member:
            issues.append({"type": "status_without_roster_row", "district_id": did, "status": status.get("status")})
            continue
        if status.get("status") in {"not_running", "retiring", "vacant"} and not member.get("retiring"):
            issues.append({
                "type": "retirement_signal_not_reflected_on_roster",
                "district_id": did,
                "incumbent_name": member.get("incumbent_name"),
                "status": status.get("status"),
                "source": status.get("source_name"),
            })

    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "state_filter": state_filter,
        "house_roster_rows": len(roster),
        "candidate_rows": len(candidates),
        "fec_finance_rows": len(fec),
        "districts_with_candidates": len(candidates_by_district),
        "duplicate_candidate_rows": len(duplicate_candidate_ids),
        "issue_count": len(issues),
        "issues": issues,
        "duplicate_candidate_ids": duplicate_candidate_ids,
        "wrong_incumbent_candidate_ids": wrong_incumbent_candidate_ids,
    }
    lines = [
        "TAPESTRY CANDIDATE SANITY REPORT",
        f"Generated: {report['generated_at']}",
        f"House roster rows: {len(roster)}",
        f"Candidate rows: {len(candidates)}",
        f"FEC finance rows: {len(fec)}",
        f"Districts with candidates: {len(candidates_by_district)}",
        f"Duplicate candidate rows: {len(duplicate_candidate_ids)}",
        f"Issues found: {len(issues)}",
        "",
    ]
    for issue in issues[:300]:
        lines.append(json.dumps(issue, default=str))
    REPORT_JSON.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    REPORT_TXT.write_text("\n".join(lines), encoding="utf-8")
    return report, fixes


def apply_fixes(report: dict) -> int:
    ids = [cid for cid in report.get("duplicate_candidate_ids", []) if cid]
    wrong_ids = [cid for cid in report.get("wrong_incumbent_candidate_ids", []) if cid]
    fixed = 0
    with write_connection() as con:
        for candidate_id in ids:
            con.execute("DELETE FROM candidate_roster_2026 WHERE candidate_id=?", [candidate_id])
            fixed += 1
        for candidate_id in wrong_ids:
            con.execute("UPDATE candidate_roster_2026 SET is_incumbent=FALSE WHERE candidate_id=?", [candidate_id])
            fixed += 1
    return fixed


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit 2026 candidate/roster consistency.")
    parser.add_argument("--fix", action="store_true", help="Remove duplicate same-person candidate rows, preserving rows with FEC ids.")
    parser.add_argument("--state", help="Print a focused state audit, e.g. NM.")
    args = parser.parse_args()
    state_filter = args.state.upper() if args.state else None
    if state_filter:
        roster, candidates, fec, _statuses = load_rows()
        fec_by_id = {f.get("fec_candidate_id"): f for f in fec if f.get("fec_candidate_id")}
        print(f"{state_filter} candidate detail:")
        for member in sorted([r for r in roster if normalize_district_id(r.get("district_id", "")).startswith(f"{state_filter}-")], key=lambda r: r.get("district_id")):
            did = normalize_district_id(member.get("district_id"))
            print(f"\n{did}: incumbent={member.get('incumbent_name')} ({member.get('incumbent_party')})")
            for c in sorted([c for c in candidates if normalize_district_id(c.get("district_id", "")) == did], key=lambda c: (not c.get("is_incumbent"), c.get("party") or "", c.get("candidate_name") or "")):
                match = same_person(c.get("candidate_name"), member.get("incumbent_name"))
                fec_row = fec_by_id.get(c.get("fec_candidate_id"))
                print(f"  {c.get('candidate_name')} | {c.get('party')} | incumbent_flag={bool(c.get('is_incumbent'))} | FEC={c.get('fec_candidate_id')} | matches_house={match} | fec_status={(fec_row or {}).get('incumbent_status')}")
    report, _fixes = audit(state_filter)
    fixed = apply_fixes(report) if args.fix else 0
    if fixed:
        report, _fixes = audit(state_filter)
    print(f"House roster rows: {report['house_roster_rows']}")
    print(f"Candidate rows: {report['candidate_rows']}")
    print(f"FEC finance rows: {report['fec_finance_rows']}")
    print(f"Duplicate same-person candidate rows: {report['duplicate_candidate_rows']}")
    print(f"Issues found: {report['issue_count']}")
    if fixed:
        print(f"Removed duplicate candidate rows: {fixed}")
    print(f"Wrote {REPORT_TXT}")
    print(f"Wrote {REPORT_JSON}")


if __name__ == "__main__":
    main()
