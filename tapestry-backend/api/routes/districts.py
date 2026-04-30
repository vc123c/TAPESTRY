from __future__ import annotations

import json
import re
from datetime import datetime
from datetime import timezone
from html import escape
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from api.schemas import DistrictForecast, HouseMember, NewsArticle
from db.connection import get_read_connection
from utils.geo import normalize_district_id

router = APIRouter(prefix="/api/districts", tags=["districts"])
CACHE_30_DAYS = "public, max-age=2592000, stale-while-revalidate=86400"


def _time_ago(published_at: datetime) -> str:
    now = datetime.utcnow()
    delta = now - published_at
    if delta.days <= 0:
        hours = max(1, int(delta.total_seconds() // 3600))
        return f"{hours} hour{'s' if hours != 1 else ''} ago"
    if delta.days == 1:
        return "Yesterday"
    return f"{delta.days} days ago"


def _wants_html(request: Request) -> bool:
    accept = request.headers.get("accept", "")
    return "text/html" in accept and "application/json" not in accept


def _money(value) -> str:
    if value is None:
        return "Unavailable"
    return f"${float(value):,.0f}"


def _percent(value) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value) * 100:.0f}%"


def _transparency_html(data: dict) -> str:
    if not data.get("seeded"):
        rows = "<p class='muted'>Transparency data has not been ingested for this district yet.</p>"
    else:
        fields = [
            ("Incumbent", data.get("incumbent_name")),
            ("Source", data.get("source_name")),
            ("As of", data.get("as_of")),
            ("Top donor sector", data.get("top_donor_sector")),
            ("Top donor sector amount", _money(data.get("top_donor_amount"))),
            ("AIPAC-related amount", _money(data.get("aipac_related_amount"))),
            ("Pro-Israel PAC amount", _money(data.get("pro_israel_pac_amount"))),
            ("Defense sector amount", _money(data.get("defense_sector_amount"))),
            ("Healthcare sector amount", _money(data.get("healthcare_sector_amount"))),
            ("Finance sector amount", _money(data.get("finance_sector_amount"))),
            ("Small-dollar share", _percent(data.get("small_dollar_share"))),
            ("Medicare posture", data.get("medicare_posture")),
            ("Israel posture", data.get("israel_posture")),
            ("Defense industry posture", data.get("defense_industry_posture")),
            ("Labor posture", data.get("labor_posture")),
            ("Notes", data.get("notes")),
        ]
        rows = "\n".join(
            f"<div class='row'><span>{escape(label)}</span><strong>{escape(str(value or 'Unavailable'))}</strong></div>"
            for label, value in fields
        )
    district_id = escape(str(data.get("district_id", "")))
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>TAPESTRY · {district_id} Transparency</title>
  <style>
    :root {{
      color-scheme: dark;
      --bg: #080b10;
      --panel: #0f1117;
      --border: #1e2130;
      --text: #e2e8f0;
      --muted: #64748b;
      --accent: #7c3aed;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      background:
        linear-gradient(90deg, rgba(124,58,237,.10) 1px, transparent 1px),
        linear-gradient(0deg, rgba(124,58,237,.08) 1px, transparent 1px),
        var(--bg);
      background-size: 28px 28px;
      color: var(--text);
      font-family: Inter, system-ui, -apple-system, Segoe UI, sans-serif;
      padding: 32px;
    }}
    main {{
      width: min(780px, calc(100vw - 64px));
      background: rgba(15, 17, 23, .96);
      border: 1px solid var(--border);
      box-shadow: 0 0 32px rgba(124, 58, 237, .16);
    }}
    header {{
      padding: 24px 28px 18px;
      border-bottom: 1px solid var(--border);
    }}
    h1 {{
      margin: 0;
      font-size: 20px;
      letter-spacing: .22em;
      text-shadow: 0 0 14px rgba(124, 58, 237, .65);
    }}
    .tagline {{
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      font-style: italic;
    }}
    section {{ padding: 10px 28px 28px; }}
    .kicker {{
      color: var(--muted);
      font-size: 12px;
      letter-spacing: .08em;
      text-transform: uppercase;
      margin: 12px 0 18px;
    }}
    .row {{
      display: grid;
      grid-template-columns: minmax(170px, 1fr) minmax(220px, 2fr);
      gap: 18px;
      padding: 13px 0;
      border-bottom: 1px solid var(--border);
    }}
    .row span {{ color: #93a4bd; }}
    .row strong {{ text-align: right; line-height: 1.35; }}
    .muted {{ color: var(--muted); }}
    a {{ color: #c4b5fd; }}
  </style>
</head>
<body>
  <main>
    <header>
      <h1>TAPESTRY</h1>
      <div class="tagline">the country, woven together</div>
    </header>
    <section>
      <div class="kicker">{district_id} · transparency cache</div>
      {rows}
    </section>
  </main>
</body>
</html>"""


def _row_to_house_member(row) -> HouseMember:
    def item(index, default=None):
        return row[index] if index < len(row) else default

    return HouseMember(
        district_id=item(0),
        state_name=item(1),
        state_abbr=item(2),
        district_number=item(3),
        incumbent_name=item(4),
        incumbent_party=item(5),
        incumbent_first_elected=item(6),
        incumbent_bioguide_id=item(7),
        incumbent_url=item(8),
        fec_candidate_id=item(9),
        cook_pvi=item(10),
        cook_pvi_numeric=item(11),
        last_margin=item(12),
        retiring=bool(item(13, False)),
        data_source=item(14, "house_roster"),
        last_updated=item(15).isoformat() if hasattr(item(15), "isoformat") else str(item(15) or ""),
    )


def _forecast_roster_fields(roster: dict | None) -> dict:
    if not roster:
        return {}
    return {
        "incumbent_name": roster.get("incumbent_name"),
        "incumbent_party": roster.get("incumbent_party"),
        "incumbent_bioguide_id": roster.get("incumbent_bioguide_id"),
        "incumbent_hometown": None,
        "incumbent_office": None,
        "incumbent_phone": None,
        "incumbent_committees": roster.get("committees") or [],
        "roster_source": roster.get("data_source"),
        "roster_publish_date": roster.get("last_updated"),
        "candidates_2026": roster.get("candidates_2026") or [],
        "major_challengers_2026": roster.get("major_challengers_2026") or [],
        "fundraising": roster.get("fundraising") or [],
        "district_features": roster.get("district_features") or {},
        "integrity_signals": roster.get("integrity_signals") or [],
        "incumbent_status_2026": roster.get("incumbent_status_2026"),
        "race_intelligence": roster.get("race_intelligence") or {},
        "twoseventy_context": roster.get("twoseventy_context"),
    }

def _same_person(left: str | None, right: str | None) -> bool:
    if not left or not right:
        return False
    drop = {"jr", "sr", "ii", "iii", "iv", "j", "g", "c"}
    def tokens(value: str) -> list[str]:
        return [t for t in re.findall(r"[a-z]+", value.lower()) if t not in drop]
    a = tokens(left)
    b = tokens(right)
    if not a or not b:
        return False
    if a[0] == b[0] and a[-1] == b[-1]:
        return True
    compact_a = "".join(a)
    compact_b = "".join(b)
    return bool(compact_a and compact_b and (compact_a in compact_b or compact_b in compact_a))


def _person_key(name: str | None, party: str | None = None) -> tuple[str, str | None]:
    tokens = [t for t in re.findall(r"[a-z]+", (name or "").lower()) if t not in {"jr", "sr", "ii", "iii", "iv"}]
    if len(tokens) >= 2:
        compact = f"{tokens[0]}:{tokens[-1]}"
    else:
        compact = "".join(tokens)
    return compact, party


def _best_candidate_rows(rows) -> list[dict]:
    by_key: dict[tuple[str, str | None], dict] = {}
    for row in rows:
        candidate_name = row[0]
        party = row[1]
        key = _person_key(candidate_name, party)
        candidate = {
            "candidate_name": candidate_name,
            "party": party,
            "is_incumbent": bool(row[2]),
            "ballotpedia_url": row[3],
            "fec_candidate_id": row[4],
            "primary_status": row[5] if len(row) > 5 else None,
            "data_source": row[6] if len(row) > 6 else None,
        }
        current = by_key.get(key)
        if not current:
            by_key[key] = candidate
            continue
        # Prefer rows with a real FEC id, then rows explicitly marked incumbent.
        if (candidate.get("fec_candidate_id") and not current.get("fec_candidate_id")) or (
            candidate.get("is_incumbent") and not current.get("is_incumbent")
        ):
            by_key[key] = {**current, **candidate}
    return sorted(
        by_key.values(),
        key=lambda item: (not item.get("is_incumbent"), item.get("party") or "", item.get("candidate_name") or ""),
    )


def _annotate_current_candidates(member: dict) -> None:
    incumbent_name = member.get("incumbent_name")
    incumbent_party = member.get("incumbent_party")
    finance_by_id = {row.get("fec_candidate_id"): row for row in member.get("fundraising", []) if row.get("fec_candidate_id")}
    finance_by_name = {(_person_key(row.get("candidate_name"), row.get("party"))): row for row in member.get("fundraising", [])}
    for candidate in member.get("candidates_2026", []):
        finance = finance_by_id.get(candidate.get("fec_candidate_id")) or finance_by_name.get(_person_key(candidate.get("candidate_name"), candidate.get("party"))) or {}
        candidate["finance"] = finance
        is_house_incumbent = (
            bool(incumbent_name)
            and not str(incumbent_name).startswith("Vacant")
            and _same_person(candidate.get("candidate_name"), incumbent_name)
        )
        candidate["is_incumbent"] = is_house_incumbent
        receipts = float(finance.get("total_receipts") or 0)
        cash = float(finance.get("cash_on_hand") or 0)
        fec_status = (finance.get("incumbent_status") or "").upper()
        fec_id = candidate.get("fec_candidate_id") or ""
        primary_status = (candidate.get("primary_status") or "").lower()
        stale_reasons = []
        if not is_house_incumbent and (primary_status == "incumbent" or fec_status == "I"):
            stale_reasons.append("incumbent_filing_for_someone_else")
        if not is_house_incumbent and fec_status == "O" and candidate.get("party") == incumbent_party:
            stale_reasons.append("prior_open_seat_same_party_filing")
        if not is_house_incumbent and re.match(r"H[024]", fec_id) and receipts < 10000:
            stale_reasons.append("old_committee_low_current_activity")
        candidate["active_2026"] = is_house_incumbent or not stale_reasons
        candidate["stale_reasons"] = stale_reasons
        candidate["is_major_challenger"] = (
            not is_house_incumbent
            and candidate["active_2026"]
            and (
                (candidate.get("party") and candidate.get("party") != incumbent_party)
                or receipts >= 25000
                or cash >= 25000
                or "ballotpedia" in str(candidate.get("data_source") or "").lower()
            )
        )
    member["major_challengers_2026"] = [
        candidate for candidate in member.get("candidates_2026", [])
        if candidate.get("is_major_challenger")
    ]


def _row_to_forecast(row, roster: dict | None = None) -> DistrictForecast:
    factor = row[7]
    if isinstance(factor, str):
        factor = json.loads(factor)
    margin = float(row[4])
    candidate = row[2]
    statement = None
    if candidate:
        statement = f"{candidate} is trending to win {row[0]} by {abs(margin):.1f} points with an uncertainty of +/-{float(row[5]):.1f}."
    return DistrictForecast(
        district_id=row[0], statement=statement, leading_candidate=row[2], leading_party=row[3],
        projected_margin=margin, uncertainty=float(row[5]), win_probability_d=float(row[6]),
        factor_attribution=factor or {}, narrative=row[8], kalshi_price=row[9],
        model_implied_price=float(row[10]), kalshi_gap=row[11], kalshi_gap_flag=bool(row[12]),
        gap_explanation=row[13], suspect_flag=bool(row[14]), last_updated=datetime.combine(row[1], datetime.min.time()),
        **_forecast_roster_fields(roster),
    )


def _roster_map(con, district_ids: list[str]) -> dict[str, dict]:
    if not district_ids:
        return {}
    placeholders = ", ".join(["?"] * len(district_ids))
    rows = con.execute(f"SELECT * FROM house_roster WHERE district_id IN ({placeholders})", district_ids).fetchall()
    members = [_row_to_house_member(row).model_dump() for row in rows]
    for member in members:
        if member.get("incumbent_bioguide_id"):
            committee_rows = con.execute(
                "SELECT committee_name, role FROM member_committees WHERE bioguide_id=? ORDER BY committee_name",
                [member["incumbent_bioguide_id"]],
            ).fetchall()
            member["committees"] = [{"committee_name": row[0], "role": row[1]} for row in committee_rows]
        fundraising_rows = con.execute(
            """
            SELECT
                f.fec_candidate_id,
                f.candidate_name,
                f.party,
                f.incumbent_status,
                f.total_receipts,
                f.total_disbursements,
                f.cash_on_hand,
                f.individual_contributions,
                f.pac_contributions,
                f.party_contributions,
                f.coverage_end_date
            FROM fec_candidate_finance f
            WHERE f.district_id=?
            ORDER BY COALESCE(f.total_receipts, 0) DESC
            """,
            [member["district_id"]],
        ).fetchall()
        member["fundraising"] = [
            {
                "fec_candidate_id": row[0],
                "candidate_name": row[1],
                "party": row[2],
                "incumbent_status": row[3],
                "total_receipts": row[4],
                "total_disbursements": row[5],
                "cash_on_hand": row[6],
                "individual_contributions": row[7],
                "pac_contributions": row[8],
                "party_contributions": row[9],
                "coverage_end_date": row[10].isoformat() if hasattr(row[10], "isoformat") else row[10],
            }
            for row in fundraising_rows
        ]
        candidate_rows = con.execute(
            """
            SELECT candidate_name, party, is_incumbent, ballotpedia_url, fec_candidate_id, primary_status, data_source
            FROM candidate_roster_2026
            WHERE district_id=?
            ORDER BY is_incumbent DESC, candidate_name
            """,
            [member["district_id"]],
        ).fetchall()
        member["candidates_2026"] = _best_candidate_rows(candidate_rows)
        _annotate_current_candidates(member)
        status_row = None
        try:
            status_row = con.execute(
                """
                SELECT district_id, incumbent_name, party, status, reason, source_name, source_url, observed_at
                FROM incumbent_status_2026
                WHERE district_id=?
                """,
                [member["district_id"]],
            ).fetchone()
        except Exception:
            status_row = None
        if status_row:
            member["incumbent_status_2026"] = {
                "district_id": status_row[0],
                "incumbent_name": status_row[1],
                "party": status_row[2],
                "status": status_row[3],
                "reason": status_row[4],
                "source_name": status_row[5],
                "source_url": status_row[6],
                "observed_at": status_row[7].isoformat() if hasattr(status_row[7], "isoformat") else status_row[7],
            }
        context_row = None
        try:
            context_row = con.execute(
                """
                SELECT district_id, incumbent_name, incumbent_party, member_since, term_label,
                       house_margin_2024, presidential_margin_2024, kalshi_house_price,
                       race_note, context_group, source_url, fetched_at
                FROM twoseventy_house_context
                WHERE district_id=?
                """,
                [member["district_id"]],
            ).fetchone()
        except Exception:
            context_row = None
        if context_row:
            member["twoseventy_context"] = {
                "district_id": context_row[0],
                "incumbent_name": context_row[1],
                "incumbent_party": context_row[2],
                "member_since": context_row[3],
                "term_label": context_row[4],
                "house_margin_2024": context_row[5],
                "presidential_margin_2024": context_row[6],
                "kalshi_house_price": context_row[7],
                "race_note": context_row[8],
                "context_group": context_row[9],
                "source_url": context_row[10],
                "fetched_at": context_row[11].isoformat() if hasattr(context_row[11], "isoformat") else context_row[11],
            }
        feature_row = con.execute(
            """
            SELECT
                college_educated_pct,
                median_age,
                white_pct,
                hispanic_pct,
                black_pct,
                median_income_real,
                rent_burden_pct,
                uninsured_rate,
                local_news_intensity,
                independent_media_penetration,
                feature_date
            FROM district_features
            WHERE district_id=?
            ORDER BY feature_date DESC
            LIMIT 1
            """,
            [member["district_id"]],
        ).fetchone()
        if feature_row:
            member["district_features"] = {
                "college_educated_pct": feature_row[0],
                "median_age": feature_row[1],
                "white_pct": feature_row[2],
                "hispanic_pct": feature_row[3],
                "black_pct": feature_row[4],
                "median_income_real": feature_row[5],
                "rent_burden_pct": feature_row[6],
                "uninsured_rate": feature_row[7],
                "local_news_intensity": feature_row[8],
                "independent_media_penetration": feature_row[9],
                "feature_date": feature_row[10].isoformat() if hasattr(feature_row[10], "isoformat") else feature_row[10],
            }
        try:
            integrity_cols = {row[1] for row in con.execute("PRAGMA table_info('politician_integrity_signals')").fetchall()}
        except Exception:
            integrity_cols = set()
        source_count_expr = "COALESCE(source_count, article_count, 0)" if "source_count" in integrity_cols else "COALESCE(article_count, 0)"
        credibility_expr = "COALESCE(max_source_credibility, 0)" if "max_source_credibility" in integrity_cols else "0"
        integrity_rows = con.execute(
            f"""
            SELECT candidate_name, perceived_dishonesty_score, article_count, evidence, signal_date,
                   source_table, {source_count_expr} AS source_count, {credibility_expr} AS max_source_credibility
            FROM politician_integrity_signals
            WHERE district_id=?
              AND signal_date >= CURRENT_DATE - INTERVAL 365 DAY
              AND {source_count_expr} >= 2
              AND {credibility_expr} >= 0.80
            ORDER BY signal_date DESC, perceived_dishonesty_score DESC
            """,
            [member["district_id"]],
        ).fetchall()
        signals = []
        for row in integrity_rows:
            try:
                evidence = json.loads(row[3]) if row[3] else []
            except Exception:
                evidence = []
            score = row[1]
            try:
                signal_date = row[4]
                days_old = (datetime.now(timezone.utc).date() - signal_date).days if hasattr(signal_date, "day") else 0
                if days_old > 180:
                    score = float(score or 0) * 0.5
            except Exception:
                pass
            signals.append({
                "candidate_name": row[0],
                "integrity_pressure_score": score,
                "article_count": row[2],
                "source_count": row[6],
                "max_source_credibility": row[7],
                "evidence": evidence,
                "signal_date": row[4].isoformat() if hasattr(row[4], "isoformat") else row[4],
                "source_table": row[5],
            })
        member["integrity_signals"] = signals
        member["race_intelligence"] = _race_intelligence(con, member)
    return {member["district_id"]: member for member in members}


def _race_intelligence(con, member: dict) -> dict:
    district_id = member["district_id"]
    rows = []
    try:
        rows.extend(con.execute(
            """
            SELECT headline, url, source_name, source_type, published_at, sentiment, topic_tags
            FROM race_web_articles
            WHERE district_id=?
            ORDER BY published_at DESC
            LIMIT 30
            """,
            [district_id],
        ).fetchall())
    except Exception:
        pass
    try:
        rows.extend(con.execute(
            """
            SELECT headline, url, source_name, source_type, published_at, sentiment, topic_tags
            FROM local_news
            WHERE district_id=?
            ORDER BY published_at DESC
            LIMIT 30
            """,
            [district_id],
        ).fetchall())
    except Exception:
        pass
    rows = sorted(rows, key=lambda row: row[4] or datetime.min, reverse=True)[:40]
    candidates = [member.get("incumbent_name")] + [c.get("candidate_name") for c in member.get("candidates_2026", [])]
    candidates = [c for c in candidates if c]
    lower_articles = [(f"{row[0] or ''}".lower(), row) for row in rows]
    challenger_mentions = {}
    for candidate in candidates:
        if _same_person(candidate, member.get("incumbent_name")):
            continue
        tokens = [t for t in re.findall(r"[a-z]+", candidate.lower()) if len(t) > 2]
        if not tokens:
            continue
        count = sum(1 for text, _row in lower_articles if all(token in text for token in tokens[-1:]))
        if count:
            challenger_mentions[candidate] = count
    open_terms = ["not running", "not seeking", "retiring", "resign", "vacant", "running for senate", "running for governor"]
    open_headlines = [row for text, row in lower_articles if any(term in text for term in open_terms)]
    issue_counts = {}
    for _text, row in lower_articles:
        for tag in row[6] or []:
            issue_counts[tag] = issue_counts.get(tag, 0) + 1
    top_issues = sorted(issue_counts.items(), key=lambda item: item[1], reverse=True)[:5]
    status = member.get("incumbent_status_2026")
    twoseventy = member.get("twoseventy_context")
    incumbent_name = member.get("incumbent_name")
    incumbent_running_sources = []
    if incumbent_name and not str(incumbent_name).startswith("Vacant") and not member.get("retiring"):
        for candidate in member.get("candidates_2026", []):
            if candidate.get("is_incumbent") or _same_person(candidate.get("candidate_name"), incumbent_name):
                source = "FEC filing" if candidate.get("fec_candidate_id") else "candidate roster"
                if source not in incumbent_running_sources:
                    incumbent_running_sources.append(source)
        for row in member.get("fundraising", []):
            if row.get("incumbent_status") == "I" or _same_person(row.get("candidate_name"), incumbent_name):
                if "FEC finance" not in incumbent_running_sources:
                    incumbent_running_sources.append("FEC finance")
    if status:
        status_text = f"{status.get('status', 'status').replace('_', ' ').title()}: {status.get('reason', '').replace('_', ' ')}"
    elif twoseventy and twoseventy.get("race_note"):
        status_text = f"270toWin: {twoseventy.get('race_note')}"
    elif open_headlines:
        status_text = "Article signal: possible open-seat or retirement story"
    elif incumbent_running_sources:
        status_text = "Incumbent filed/running for reelection"
    elif incumbent_name and not str(incumbent_name).startswith("Vacant") and not member.get("retiring"):
        status_text = "Incumbent not flagged as retiring; filing confirmation pending"
    else:
        status_text = "Open-seat status pending"
    return {
        "article_count": len(rows),
        "status_text": status_text,
        "incumbent_running_confirmed": bool(incumbent_running_sources),
        "incumbent_running_sources": incumbent_running_sources,
        "open_seat_articles": [
            {"headline": row[0], "url": row[1], "source_name": row[2], "published_at": row[4].isoformat() if hasattr(row[4], "isoformat") else row[4]}
            for row in open_headlines[:3]
        ],
        "challenger_mentions": challenger_mentions,
        "top_issues": [{"topic": topic, "count": count} for topic, count in top_issues],
        "recent_sources": sorted({row[2] for row in rows if row[2]})[:8],
        "twoseventy": twoseventy,
    }


@router.get("", response_model=list[DistrictForecast])
def list_districts(
    state: Optional[str] = None,
    competitive_only: bool = False,
    party_lean: Optional[str] = None,
    min_margin: Optional[float] = Query(None),
    max_margin: Optional[float] = Query(None),
):
    where = ["forecast_date = (SELECT MAX(forecast_date) FROM district_forecasts)"]
    params = []
    if state:
        where.append("district_id LIKE ?")
        params.append(f"{state.upper()}-%")
    if competitive_only:
        where.append("ABS(projected_margin) <= 10")
    if party_lean:
        where.append("leading_party = ?")
        params.append(party_lean.upper())
    if min_margin is not None:
        where.append("projected_margin >= ?")
        params.append(min_margin)
    if max_margin is not None:
        where.append("projected_margin <= ?")
        params.append(max_margin)
    with get_read_connection() as con:
        rows = con.execute(f"SELECT * FROM district_forecasts WHERE {' AND '.join(where)} ORDER BY ABS(projected_margin)", params).fetchall()
        roster = _roster_map(con, [row[0] for row in rows])
    return [_row_to_forecast(row, roster.get(row[0])) for row in rows]


@router.get("/summary")
def list_district_summaries(state: Optional[str] = None, competitive_only: bool = False):
    where = ["df.forecast_date = (SELECT MAX(forecast_date) FROM district_forecasts)"]
    params = []
    if state:
        where.append("df.district_id LIKE ?")
        params.append(f"{state.upper()}-%")
    if competitive_only:
        where.append("ABS(df.projected_margin) <= 10")
    with get_read_connection() as con:
        rows = con.execute(
            f"""
            SELECT
                df.district_id,
                df.leading_candidate,
                df.leading_party,
                df.projected_margin,
                df.uncertainty,
                df.win_probability_d,
                df.model_implied_price,
                df.kalshi_price,
                df.kalshi_gap,
                df.kalshi_gap_flag,
                df.forecast_date,
                hr.incumbent_name,
                hr.incumbent_party,
                hr.cook_pvi,
                hr.cook_pvi_numeric,
                hr.retiring
            FROM district_forecasts df
            LEFT JOIN house_roster hr ON df.district_id = hr.district_id
            WHERE {' AND '.join(where)}
            ORDER BY df.district_id
            """,
            params,
        ).fetchall()
    return [
        {
            "district_id": row[0],
            "leading_candidate": row[1],
            "leading_party": row[2],
            "projected_margin": row[3],
            "uncertainty": row[4],
            "win_probability_d": row[5],
            "model_implied_price": row[6],
            "kalshi_price": row[7],
            "kalshi_gap": row[8],
            "kalshi_gap_flag": bool(row[9]),
            "last_updated": row[10].isoformat() if hasattr(row[10], "isoformat") else row[10],
            "incumbent_name": row[11],
            "incumbent_party": row[12],
            "cook_pvi": row[13],
            "cook_pvi_numeric": row[14],
            "retiring": bool(row[15]),
        }
        for row in rows
    ]


@router.get("/roster", response_model=list[HouseMember])
def list_house_roster(state: Optional[str] = None):
    where = []
    params = []
    if state:
        where.append("state_abbr = ?")
        params.append(state.upper())
    query = "SELECT * FROM house_roster"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY state_abbr, district_number NULLS LAST, district_id"
    with get_read_connection() as con:
        rows = con.execute(query, params).fetchall()
    return [_row_to_house_member(row) for row in rows]


@router.get("/{district_id}/roster", response_model=HouseMember)
def get_house_roster_member(district_id: str):
    district_id = normalize_district_id(district_id)
    with get_read_connection() as con:
        row = con.execute("SELECT * FROM house_roster WHERE district_id=?", [district_id]).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="House roster member not found")
    return _row_to_house_member(row)


@router.get("/{district_id}", response_model=DistrictForecast)
def get_district(district_id: str):
    district_id = normalize_district_id(district_id)
    with get_read_connection() as con:
        row = con.execute(
            "SELECT * FROM district_forecasts WHERE district_id=? ORDER BY forecast_date DESC LIMIT 1",
            [district_id],
        ).fetchone()
        roster = _roster_map(con, [district_id])
    if not row:
        raise HTTPException(status_code=404, detail="District forecast not found")
    return _row_to_forecast(row, roster.get(row[0]))


@router.get("/{district_id}/news")
def get_district_news(
    district_id: str,
    limit: int = Query(5, ge=1, le=20),
    topic: Optional[str] = None,
    incumbent_only: bool = False,
):
    district = normalize_district_id(district_id)
    where = ["district_id = ?"]
    params = [district]
    if topic:
        where.append("? = ANY(topic_tags)")
        params.append(topic.lower())
    if incumbent_only:
        where.append("incumbent_relevant = true")
    with get_read_connection() as con:
        total = 0
        rows = []
        try:
            total += con.execute("SELECT COUNT(*) FROM local_news WHERE district_id=?", [district]).fetchone()[0]
            rows.extend(con.execute(
                f"""
                SELECT headline,url,source_name,'LOCAL' AS source_type,published_at,sentiment,incumbent_relevant,topic_tags
                FROM local_news
                WHERE {' AND '.join(where)}
                """,
                params,
            ).fetchall())
        except Exception:
            pass
        try:
            total += con.execute("SELECT COUNT(*) FROM race_web_articles WHERE district_id=?", [district]).fetchone()[0]
            rows.extend(con.execute(
                f"""
                SELECT headline,url,source_name,'RACE COVERAGE' AS source_type,published_at,sentiment,incumbent_relevant,topic_tags
                FROM race_web_articles
                WHERE {' AND '.join(where)}
                """,
                params,
            ).fetchall())
        except Exception:
            pass
    rows = sorted(rows, key=lambda r: r[4] or datetime.min, reverse=True)[:limit]
    articles = [
        NewsArticle(
            headline=r[0],
            url=r[1],
            source_name=r[2],
            source_type=r[3],
            published_at=r[4].isoformat() if hasattr(r[4], "isoformat") else str(r[4]),
            time_ago=_time_ago(r[4] if isinstance(r[4], datetime) else datetime.utcnow()),
            sentiment=r[5],
            incumbent_relevant=bool(r[6]),
            topic_tags=list(r[7] or []),
        ).model_dump()
        for r in rows
    ]
    coverage_rows = []
    if articles:
        seen_sources = {}
        for article in articles:
            key = f"{article['source_name']} ({article['source_type']})"
            seen_sources[key] = seen_sources.get(key, 0) + 1
        coverage_rows = [{"source": source, "articles": count} for source, count in seen_sources.items()]
    return {
        "seeded": total > 0,
        "articles": articles,
        "source_coverage": coverage_rows if coverage_rows else "pending",
    }


@router.get("/{district_id}/transparency")
def get_district_transparency(district_id: str, request: Request):
    district_id = normalize_district_id(district_id)
    with get_read_connection() as con:
        try:
            row = con.execute(
                "SELECT * FROM donor_transparency WHERE district_id=?",
                [district_id],
            ).fetchone()
        except Exception:
            row = None
    if not row:
        data = {"seeded": False, "district_id": district_id.upper()}
        if _wants_html(request):
            return HTMLResponse(_transparency_html(data), headers={"Cache-Control": CACHE_30_DAYS})
        return data
    keys = [
        "district_id", "incumbent_name", "as_of", "source_name", "top_donor_sector", "top_donor_amount",
        "pro_israel_pac_amount", "aipac_related_amount", "defense_sector_amount", "healthcare_sector_amount",
        "finance_sector_amount", "small_dollar_share", "medicare_posture", "israel_posture",
        "defense_industry_posture", "labor_posture", "notes",
    ]
    data = dict(zip(keys, row))
    data["seeded"] = True
    data["as_of"] = data["as_of"].isoformat() if hasattr(data["as_of"], "isoformat") else str(data["as_of"])
    if _wants_html(request):
        return HTMLResponse(_transparency_html(data), headers={"Cache-Control": CACHE_30_DAYS})
    return data
