from __future__ import annotations

import json
from html import escape
from pathlib import Path

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse

from api.schemas import MorningBrief, NationalSummary
from db.connection import ROOT, get_read_connection

router = APIRouter(tags=["national"])
BAD_EVENT_NAMES = {"stonks", "stocks", "untitled", "news", "latest", "update", "market watch"}


def _usable_event_name(name: object) -> bool:
    value = str(name or "").strip()
    return len(value) >= 12 and value.lower() not in BAD_EVENT_NAMES and any(ch.isalpha() for ch in value)


def _pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.0f}%"
    except Exception:
        return "--"


def _seats(value: object) -> str:
    try:
        return f"{float(value):.1f} expected seats"
    except Exception:
        return "expected seats pending"


def _morning_brief_html(data: dict) -> str:
    generated = escape(str(data.get("generated_at", ""))[:10])
    senate = data.get("senate", {}) or {}
    house = data.get("house", {}) or {}
    national = data.get("national", {}) or {}
    top_moves = data.get("top_moves", []) or []
    events = [event for event in (data.get("active_events", []) or []) if _usable_event_name(event.get("event_name"))]
    narrative = escape(str(data.get("narrative") or "Morning brief generated."))
    moves_html = "".join(
        f"<li><strong>{escape(str(move.get('district_id', '')))}</strong><span>{escape(str(move.get('text') or move.get('cause') or 'Updated forecast'))}</span></li>"
        for move in top_moves[:6]
    ) or "<li><span>No district moves available yet.</span></li>"
    events_html = "".join(
        f"<li><strong>{escape(str(event.get('event_type', 'event')).replace('_', ' ').title())}</strong><span>{escape(str(event.get('event_name', 'Untitled signal')))}</span><em>salience {float(event.get('salience') or 0):.2f}</em></li>"
        for event in events[:8]
    ) or "<li><span>No active source-intelligence events yet.</span></li>"
    return f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>TAPESTRY Morning Brief</title>
  <style>
    :root {{ color-scheme: dark; }}
    body {{ margin:0; min-height:100vh; background:#080b10; color:#e2e8f0; font-family:Inter,system-ui,Segoe UI,Arial,sans-serif;
      background-image: radial-gradient(circle at 1px 1px, rgba(124,58,237,.22) 1px, transparent 0), linear-gradient(135deg, rgba(124,58,237,.06), transparent 42%);
      background-size: 22px 22px, 100% 100%; }}
    main {{ max-width:920px; margin:0 auto; padding:42px 24px 60px; }}
    .brand {{ letter-spacing:.22em; font-weight:900; text-shadow:0 0 18px rgba(124,58,237,.65); }}
    .tag {{ color:#64748b; font-style:italic; margin-left:10px; font-size:13px; letter-spacing:.03em; }}
    h1 {{ margin:34px 0 8px; font-size:24px; letter-spacing:.08em; }}
    .date {{ color:#64748b; font-size:13px; text-transform:uppercase; letter-spacing:.1em; }}
    .grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:1px; background:#1e2130; border:1px solid #1e2130; margin:24px 0; }}
    .metric {{ background:#0f1117; padding:18px; }}
    .metric b {{ display:block; color:#93c5fd; font-size:24px; margin-top:6px; }}
    .metric small {{ display:block; color:#64748b; margin-top:4px; font-size:12px; }}
    .label {{ color:#64748b; font-size:11px; text-transform:uppercase; letter-spacing:.12em; }}
    section {{ border-top:1px solid #1e2130; padding-top:18px; margin-top:24px; }}
    p {{ color:#cbd5e1; line-height:1.55; }}
    ul {{ list-style:none; padding:0; margin:0; }}
    li {{ display:grid; grid-template-columns:130px 1fr auto; gap:14px; border-bottom:1px solid #1e2130; padding:12px 0; align-items:start; }}
    li strong {{ color:#fff; }}
    li span {{ color:#cbd5e1; }}
    li em {{ color:#64748b; font-style:normal; font-size:12px; }}
    a {{ color:#93c5fd; }}
    @media (max-width:720px) {{ .grid {{ grid-template-columns:1fr; }} li {{ grid-template-columns:1fr; }} .tag {{ display:block; margin:6px 0 0; }} }}
  </style>
</head>
<body>
  <main>
    <div><span class="brand">TAPESTRY</span><span class="tag">the country, woven together</span></div>
    <h1>MORNING BRIEF</h1>
    <div class="date">{generated}</div>
    <div class="grid">
      <div class="metric"><span class="label">Senate D Control · Model</span><b>{_pct(senate.get('d_control_probability'))}</b><small>{_seats(senate.get('d_expected_seats'))}</small></div>
      <div class="metric"><span class="label">House D Control · Model</span><b>{_pct(house.get('d_control_probability'))}</b><small>{_seats(house.get('d_expected_seats'))}</small></div>
      <div class="metric"><span class="label">Days To Election</span><b>{escape(str(national.get('days_to_election', '--')))}</b></div>
    </div>
    <section><span class="label">Narrative</span><p>{narrative}</p></section>
    <section><span class="label">Top Moves</span><ul>{moves_html}</ul></section>
    <section><span class="label">Active Source-Intelligence Events</span><ul>{events_html}</ul></section>
    <section><span class="label">Model Caveat</span><p>This is TAPESTRY's local model estimate, not a published consensus forecast. House control is low here because the simulated Democratic seat count is below the 218-seat control threshold in most runs.</p></section>
    <section><span class="label">Raw API</span><p>React still receives JSON from this endpoint. For raw JSON, request with <code>Accept: application/json</code>.</p></section>
  </main>
</body>
</html>"""


@router.get("/api/morning-brief")
def morning_brief(request: Request):
    path = ROOT / "data" / "morning_brief.json"
    if not path.exists():
        raise HTTPException(status_code=404, detail="Morning brief has not been generated")
    data = json.loads(path.read_text(encoding="utf-8"))
    data["active_events"] = [event for event in (data.get("active_events", []) or []) if _usable_event_name(event.get("event_name"))]
    accept = request.headers.get("accept", "")
    if "text/html" in accept and "application/json" not in accept:
        return HTMLResponse(_morning_brief_html(data))
    return JSONResponse(data)


@router.get("/api/national", response_model=NationalSummary)
def national_summary():
    with get_read_connection() as con:
        n = con.execute("SELECT * FROM national_factors ORDER BY factor_date DESC LIMIT 1").fetchone()
        n_cols = [desc[0] for desc in con.description] if n else []
        c = con.execute("SELECT current_stage, escalation_trap_prob FROM conflict_states ORDER BY assessment_date DESC LIMIT 1").fetchone()
    if not n:
        raise HTTPException(status_code=404, detail="National factors not found")
    data = dict(zip(n_cols, n))
    overall = data.get("presidential_approval")
    issue_map = {}
    for key, col in {
        "economy": "economy_approval",
        "inflation": "inflation_approval",
        "immigration": "immigration_approval",
        "iran_war": "iran_war_approval",
        "gas_prices": "gas_prices_approval",
        "healthcare": "healthcare_approval",
        "crime": "crime_approval",
        "tariffs": "tariffs_approval",
    }.items():
        value = data.get(col)
        if value is not None and overall is not None:
            issue_map[key] = {"approve": value, "gap_vs_overall": value - overall}
    if issue_map:
        worst = min(issue_map, key=lambda key: issue_map[key]["approve"])
        strength = max(issue_map, key=lambda key: issue_map[key]["gap_vs_overall"])
        issue_map["worst_issue"] = worst
        issue_map["relative_strength"] = strength
        issue_map["overall"] = overall
    return {
        "presidential_approval": data.get("presidential_approval"),
        "generic_ballot_margin": data.get("generic_ballot_d_margin"),
        "kitchen_table_index": data.get("kitchen_table_index"),
        "gas_price_national": data.get("gas_price_national"),
        "gas_price_3m_change": data.get("gas_price_3m_change"),
        "gas_prices_approval": data.get("gas_prices_approval"),
        "anti_establishment_index": data.get("anti_establishment_index"),
        "college_realignment_index": data.get("college_realignment_index"),
        "conflict_stage_iran": c[0] if c else 3.0, "escalation_trap_probability": c[1] if c else 0.3,
        "days_to_election": max(0, (__import__("datetime").date(2026, 11, 3) - __import__("datetime").date.today()).days),
        "issue_approval": issue_map or None,
    }
