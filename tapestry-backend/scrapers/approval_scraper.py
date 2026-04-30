from __future__ import annotations

import hashlib
import math
import re
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
from bs4 import BeautifulSoup

from db.connection import init_db, write_connection
from utils.logging import setup_logging

logger = setup_logging("scrapers.approval")

USER_AGENT = "Mozilla/5.0 TAPESTRY/1.0 (research)"
VOTEHUB_BASE = "https://votehub.com/polls/api"


POLLSTER_QUALITY = {
    "AP-NORC": 1.00,
    "Gallup": 0.98,
    "Marquette": 0.97,
    "Quinnipiac University": 0.95,
    "Reuters/Ipsos": 0.94,
    "Ipsos": 0.93,
    "CNN/SSRS": 0.93,
    "Marist": 0.92,
    "Fox News": 0.90,
    "ABC News/Washington Post": 0.96,
    "Emerson College": 0.85,
    "YouGov": 0.84,
    "Morning Consult": 0.80,
    "SurveyMonkey": 0.78,
    "Rasmussen Reports": 0.65,
}


def _poll_id(*parts: object) -> str:
    return hashlib.sha1(":".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def _parse_date(value: Any) -> date | None:
    if isinstance(value, date):
        return value
    if not value:
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(str(value).strip()[:20], fmt).date()
        except ValueError:
            pass
    try:
        return datetime.fromisoformat(str(value)[:10]).date()
    except Exception:
        return None


def _stored_approval() -> float:
    with write_connection() as con:
        row = con.execute(
            "SELECT presidential_approval FROM national_factors ORDER BY factor_date DESC LIMIT 1"
        ).fetchone()
    return float(row[0]) if row and row[0] is not None else 0.37


def fetch_votehub_approval() -> list[dict]:
    try:
        r = httpx.get(
            f"{VOTEHUB_BASE}/polls",
            params={"poll_type": "approval", "subject": "donald-trump"},
            timeout=15,
            headers={"User-Agent": USER_AGENT},
        )
        if r.status_code == 200:
            payload = r.json()
            polls = payload.get("polls") or payload.get("data") or payload.get("results") or []
            print(f"  VoteHub: {len(polls)} polls fetched")
            return polls
        print(f"  VoteHub: HTTP {r.status_code}")
    except Exception as exc:
        print(f"  VoteHub: {exc}")
    return []


def parse_votehub_polls(polls: list[dict]) -> list[dict]:
    today = date.today()
    cutoff = today - timedelta(days=60)
    parsed = []
    for poll in polls:
        if poll.get("internal") or poll.get("partisan"):
            continue
        end_date = _parse_date(poll.get("end_date") or poll.get("endDate") or poll.get("date"))
        if not end_date or end_date < cutoff:
            continue
        approve = disapprove = None
        for answer in poll.get("answers", []) or []:
            choice = str(answer.get("choice") or answer.get("answer") or "").lower()
            pct = answer.get("pct") or answer.get("percentage")
            try:
                pct = float(pct) / 100.0 if float(pct) > 1 else float(pct)
            except Exception:
                continue
            if "approve" in choice and "dis" not in choice:
                approve = pct
            elif "disapprove" in choice:
                disapprove = pct
        if approve is None or disapprove is None:
            continue
        days_ago = max(0, (today - end_date).days)
        time_weight = 0.5 ** (days_ago / 14)
        pollster = str(poll.get("pollster") or "unknown")
        quality = POLLSTER_QUALITY.get(pollster, 0.70)
        population = str(poll.get("population") or "a")
        pop_weight = {"a": 1.0, "rv": 0.90, "lv": 0.85}.get(population, 0.80)
        weight = time_weight * quality * pop_weight
        parsed.append({
            "poll_id": f"vh_{poll.get('id') or _poll_id(pollster, end_date, approve)}",
            "pollster": pollster,
            "poll_date": end_date,
            "approve_pct": approve,
            "disapprove_pct": disapprove,
            "net_approval": approve - disapprove,
            "population": population,
            "source_name": "VoteHub",
            "source_url": "https://votehub.com/polls/api",
            "weight": weight,
        })
    return parsed


def fetch_votehub_generic_ballot() -> float | None:
    try:
        r = httpx.get(
            f"{VOTEHUB_BASE}/polls",
            params={"poll_type": "generic-ballot"},
            timeout=15,
            headers={"User-Agent": USER_AGENT},
        )
        if r.status_code != 200:
            print(f"  VoteHub generic ballot: HTTP {r.status_code}")
            return None
        polls = r.json().get("polls") or []
        cutoff = date.today() - timedelta(days=30)
        margins = []
        for poll in polls:
            end = _parse_date(poll.get("end_date") or poll.get("endDate") or poll.get("date"))
            if not end or end < cutoff:
                continue
            d_pct = r_pct = None
            for answer in poll.get("answers", []) or []:
                choice = str(answer.get("choice") or answer.get("answer") or "").lower()
                try:
                    pct = float(answer.get("pct") or answer.get("percentage") or 0)
                except Exception:
                    continue
                if "dem" in choice:
                    d_pct = pct
                elif "rep" in choice:
                    r_pct = pct
            if d_pct is not None and r_pct is not None:
                margins.append(d_pct - r_pct)
        if margins:
            avg = sum(margins) / len(margins)
            print(f"  VoteHub generic ballot: D{avg:+.1f} ({len(margins)} polls)")
            return avg
    except Exception as exc:
        print(f"  VoteHub generic ballot: {exc}")
    return None


def fetch_gelliottmorris_approval() -> dict:
    results: dict[str, float] = {}
    headers = {"User-Agent": USER_AGENT}
    try:
        r = httpx.get("https://fiftyplusone.news/polls/approval/president", timeout=15, headers=headers)
        if r.status_code == 200:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            pcts = re.findall(r"(\d{2}(?:\.\d)?)\s*%", text)
            if pcts:
                approve = float(pcts[0]) / 100.0
                if 0.25 < approve < 0.60:
                    results["overall"] = approve
                    print(f"  FiftyPlusOne: approval={approve:.3f}")
        else:
            print(f"  FiftyPlusOne: HTTP {r.status_code}")
    except Exception as exc:
        print(f"  FiftyPlusOne: {exc}")

    try:
        r = httpx.get("https://www.gelliottmorris.com/p/data", timeout=15, headers=headers)
        if r.status_code == 200:
            text = BeautifulSoup(r.text, "html.parser").get_text(" ", strip=True)
            patterns = {
                "prices_approval": [r"prices?\s*[:\-]\s*(\d+)\s*%", r"(\d+)\s*%\s*approve.*prices", r"inflation.*?(\d+)\s*%"],
                "immigration_approval": [r"immigration\s*[:\-]\s*(\d+)\s*%", r"(\d+)\s*%\s*approve.*immigration"],
                "border_security_approval": [r"border\s*[:\-]\s*(\d+)\s*%", r"(\d+)\s*%\s*approve.*border"],
            }
            for issue, issue_patterns in patterns.items():
                for pattern in issue_patterns:
                    match = re.search(pattern, text, re.I)
                    if match:
                        pct = float(match.group(1)) / 100.0
                        if 0.10 < pct < 0.80:
                            results[issue] = pct
                            break
            if results:
                print(f"  GElliottMorris: {results}")
        else:
            print(f"  GElliottMorris data: HTTP {r.status_code}")
    except Exception as exc:
        print(f"  GElliottMorris data: {exc}")
    return results


def fetch_ucsb_approval() -> list[dict]:
    url = "https://www.presidency.ucsb.edu/statistics/data/donald-j-trump-2nd-term-public-approval"
    try:
        r = httpx.get(url, timeout=15, headers={"User-Agent": USER_AGENT}, follow_redirects=True)
        if r.status_code != 200:
            print(f"  UCSB: HTTP {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        polls = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr")[1:]:
                cols = [td.get_text(" ", strip=True) for td in row.find_all("td")]
                if len(cols) < 4:
                    continue
                start = _parse_date(cols[0])
                end = _parse_date(cols[1])
                try:
                    approve = float(re.sub(r"[^0-9.]", "", cols[2])) / 100.0
                    disapprove = float(re.sub(r"[^0-9.]", "", cols[3])) / 100.0
                except Exception:
                    continue
                if not end or not (0.20 < approve < 0.70):
                    continue
                polls.append({
                    "poll_id": f"ucsb_gallup_{end.isoformat()}",
                    "pollster": "Gallup",
                    "poll_date": end,
                    "approve_pct": approve,
                    "disapprove_pct": disapprove,
                    "net_approval": approve - disapprove,
                    "population": "a",
                    "source_name": "UCSB/Gallup",
                    "source_url": url,
                    "weight": 1.0,
                    "start_date": start,
                })
        print(f"  UCSB: {len(polls)} Gallup polls parsed")
        return polls
    except Exception as exc:
        print(f"  UCSB: {exc}")
        return []


def seed_verified_april_2026() -> None:
    with write_connection() as con:
        con.execute(
            """
            UPDATE national_factors
            SET presidential_approval = 0.35,
                prices_approval = 0.26,
                gas_prices_approval = COALESCE(gas_prices_approval, 0.24),
                inflation_approval = 0.26,
                immigration_approval = 0.44,
                border_security_approval = 0.50,
                approval_source = 'SIN_Verasight_Apr2026',
                approval_n_polls = 1,
                economy_approval_gap = COALESCE(economy_approval, 0.31) - 0.35,
                immigration_approval_gap = 0.44 - 0.35
            WHERE factor_date = (SELECT MAX(factor_date) FROM national_factors)
            """
        )
        con.execute(
            """
            INSERT OR REPLACE INTO approval_polls (
                poll_id, pollster, subject, poll_type, start_date, end_date,
                approve_pct, disapprove_pct, sample_size, population,
                quality_weight, time_weight, combined_weight, source_url,
                fetched_at, net_approval, source_name
            ) VALUES (
                'sin_verasight_apr2026',
                'Strength In Numbers/Verasight',
                'Donald Trump',
                'approval',
                '2026-04-10',
                '2026-04-14',
                0.35, 0.61, 1514, 'a',
                1.0, 1.0, 1.0,
                'https://www.gelliottmorris.com/p/2026-04-21-april-poll',
                CURRENT_TIMESTAMP,
                -0.26,
                'gelliottmorris.com'
            )
            """
        )


def run() -> float:
    init_db()
    print("=" * 50)
    print("APPROVAL SCRAPER -- multi-source")
    print("=" * 50)
    stored = _stored_approval()
    print(f"  Stored approval: {stored:.3f}")

    all_polls = []
    print("Trying VoteHub API...")
    all_polls.extend(parse_votehub_polls(fetch_votehub_approval()))

    print("Trying G. Elliott Morris / FiftyPlusOne...")
    gem_results = fetch_gelliottmorris_approval()

    print("Trying UCSB American Presidency Project...")
    all_polls.extend(fetch_ucsb_approval())

    generic_ballot = fetch_votehub_generic_ballot()
    total_weight = sum(float(poll.get("weight") or 1.0) for poll in all_polls)
    final_approve = None
    if total_weight > 0:
        final_approve = sum(poll["approve_pct"] * float(poll.get("weight") or 1.0) for poll in all_polls) / total_weight
    elif "overall" in gem_results:
        final_approve = gem_results["overall"]
        print(f"  Using FiftyPlusOne average: {final_approve:.3f}")

    if final_approve is not None:
        if not (0.25 < final_approve < 0.58):
            print(f"  VALIDATION FAILED: {final_approve:.3f} out of bounds")
            final_approve = None
        elif abs(final_approve - stored) > 0.12:
            print(f"  VALIDATION FAILED: jump too large ({stored:.3f} -> {final_approve:.3f})")
            final_approve = None

    if final_approve is None:
        print("  All live sources failed or validation failed; keeping stored/seeded value.")
        final_approve = stored
    else:
        print(f"  Final weighted approval: {final_approve:.3f}")
        print(f"  Source polls: {len(all_polls)}")

    with write_connection() as con:
        for poll in all_polls:
            con.execute(
                """
                INSERT OR REPLACE INTO approval_polls (
                    poll_id, pollster, subject, poll_type, start_date, end_date,
                    approve_pct, disapprove_pct, sample_size, population,
                    quality_weight, time_weight, combined_weight, source_url,
                    fetched_at, net_approval, source_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    poll["poll_id"],
                    poll["pollster"],
                    "Donald Trump",
                    "approval",
                    poll.get("start_date") or poll["poll_date"],
                    poll["poll_date"],
                    poll["approve_pct"],
                    poll["disapprove_pct"],
                    None,
                    poll["population"],
                    poll.get("weight"),
                    1.0,
                    poll.get("weight"),
                    poll.get("source_url"),
                    datetime.now(UTC),
                    poll["net_approval"],
                    poll["source_name"],
                ],
            )
        issue_updates = {
            key: value for key, value in gem_results.items()
            if key in {"prices_approval", "immigration_approval", "border_security_approval"}
        }
        set_parts = ["presidential_approval = ?", "approval_source = ?", "approval_n_polls = ?"]
        params: list[object] = [final_approve, "multi_source" if all_polls or gem_results else "stored_fallback", len(all_polls)]
        for col, value in issue_updates.items():
            set_parts.append(f"{col} = ?")
            params.append(value)
        if generic_ballot is not None:
            set_parts.append("generic_ballot_d_margin = ?")
            params.append(generic_ballot)
        con.execute(
            f"""
            UPDATE national_factors
            SET {', '.join(set_parts)}
            WHERE factor_date = (SELECT MAX(factor_date) FROM national_factors)
            """,
            params,
        )
    seed_verified_april_2026()
    with write_connection() as con:
        con.execute(
            """
            UPDATE national_factors
            SET generic_ballot_d_margin = 7.0
            WHERE factor_date = (SELECT MAX(factor_date) FROM national_factors)
            """
        )
    print("  Seeded April 2026 SIN/Verasight approval data")
    print("  Generic ballot updated to conservative D+7.0")
    print(f"  Wrote {len(all_polls)} live polls plus SIN/Verasight seed to approval_polls")
    return final_approve


if __name__ == "__main__":
    raise SystemExit(0 if run() is not None else 1)
