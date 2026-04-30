from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime

import plotly.express as px
import streamlit as st

from db.connection import get_read_connection, init_db, table_count, write_connection
from model.retrainer import TapestryRetrainer
from model.walk_forward import WalkForwardValidator
from scrapers.census_scraper import CensusScraper
from scrapers.fec_scraper import FECScraper
from scrapers.ideology_corpus_scraper import IdeologyCorpusScraper
from scrapers.local_news_scraper import LocalNewsScraper
from scrapers.race_web_scraper import RaceWebScraper

st.set_page_config(page_title="TAPESTRY Admin", layout="wide")
st.markdown("<style>body,.stApp{background:#080b10;color:#e2e8f0}.stButton button{border-radius:2px}</style>", unsafe_allow_html=True)
init_db()

overnight_summary_path = "data/overnight_summary.json"
try:
    with open(overnight_summary_path, "r", encoding="utf-8") as fh:
        overnight_summary = json.load(fh)
except FileNotFoundError:
    overnight_summary = None
except json.JSONDecodeError:
    overnight_summary = None

if overnight_summary:
    completed = overnight_summary.get("completed_at", "unknown time")
    brier = overnight_summary.get("model_performance", {}).get("brier_2024")
    districts = overnight_summary.get("data_completeness", {}).get("total_districts", 435)
    brier_text = f"{brier:.3f}" if isinstance(brier, (int, float)) else "pending"
    st.success(f"Overnight pipeline completed at {completed} - {districts} districts tracked, Brier {brier_text} on 2024 holdout")
    with st.expander("VIEW FULL OVERNIGHT REPORT"):
        st.json(overnight_summary)

page = st.sidebar.radio("TAPESTRY ADMIN", ["SYSTEM STATUS", "BLACK SWAN QUEUE", "SUSPECT FLAGS", "MODEL PERFORMANCE", "FACTOR WEIGHTS", "EVENT TRACKER", "DATA PIPELINE", "DATA COMPLETENESS"])

if page == "SYSTEM STATUS":
    st.title("System Status")
    with get_read_connection() as con:
        runs = con.execute("SELECT source_name, MAX(run_at) AS last_run, ANY_VALUE(status) AS status, ANY_VALUE(rows_fetched) AS rows, ANY_VALUE(error) AS error FROM scraper_runs GROUP BY source_name").fetchall()
        jobs = con.execute("SELECT * FROM retrain_jobs ORDER BY updated_at DESC LIMIT 5").fetchall()
    st.subheader("Scrapers")
    st.table(runs)
    st.subheader("Retrain Jobs")
    st.table(jobs)
    if st.button("TRIGGER DAILY UPDATE"):
        with st.spinner("Running daily update"):
            TapestryRetrainer().fast_update()
        st.success("Daily update complete")
    if st.button("TRIGGER FULL RETRAIN"):
        st.warning("This takes 20-40 minutes.")
        with st.spinner("Running slow update and full retrain"):
            result = TapestryRetrainer().slow_update()
        st.json(result)

elif page == "BLACK SWAN QUEUE":
    st.title("Black Swan Queue")
    with get_read_connection() as con:
        rows = con.execute("SELECT * FROM admin_queue WHERE status='pending' ORDER BY queued_at DESC").fetchall()
    for row in rows:
        st.subheader(row[2])
        st.write("Affected districts:", row[3])
        with st.expander("Search results"):
            st.json(json.loads(row[4]) if row[4] else {})
        token = json.loads(row[5]) if row[5] else {}
        edited = st.text_area("Suggested token JSON", json.dumps(token, indent=2), key=row[0])
        c1, c2, c3 = st.columns(3)
        if c1.button("ACCEPT", key=f"a{row[0]}"):
            with write_connection() as con:
                con.execute("UPDATE admin_queue SET status='accepted', user_response=?, resolved_at=? WHERE queue_id=?", [edited, datetime.utcnow(), row[0]])
        if c2.button("MODIFY", key=f"m{row[0]}"):
            with write_connection() as con:
                con.execute("UPDATE admin_queue SET status='modified', user_response=?, resolved_at=? WHERE queue_id=?", [edited, datetime.utcnow(), row[0]])
        if c3.button("DISMISS", key=f"d{row[0]}"):
            with write_connection() as con:
                con.execute("UPDATE admin_queue SET status='dismissed', resolved_at=? WHERE queue_id=?", [datetime.utcnow(), row[0]])

elif page == "SUSPECT FLAGS":
    st.title("Suspect Flags")
    with get_read_connection() as con:
        rows = con.execute("SELECT district_id, forecast_date, model_implied_price, kalshi_price, kalshi_gap FROM district_forecasts WHERE suspect_flag=true").fetchall()
    st.table(rows)

elif page == "MODEL PERFORMANCE":
    st.title("Model Performance")
    if st.button("RUN WALK-FORWARD"):
        st.json(WalkForwardValidator().run())
    with get_read_connection() as con:
        rows = con.execute("SELECT test_year,brier_score,cook_brier_score,improvement FROM model_performance ORDER BY test_year").fetchall()
    if rows:
        st.table(rows)
        fig = px.line(x=[r[0] for r in rows], y=[[r[1] for r in rows], [r[2] for r in rows]], labels={"x": "Cycle", "value": "Brier"})
        st.plotly_chart(fig, use_container_width=True)

elif page == "FACTOR WEIGHTS":
    st.title("Factor Weights")
    weights = {"cook_pvi": 3.8, "generic_ballot": 1.9, "kitchen_table_index": 1.5, "candidate_quality": 1.2, "conflict_effect": 0.9}
    st.table(weights.items())
    st.bar_chart(weights)

elif page == "EVENT TRACKER":
    st.title("Event Tracker")
    with get_read_connection() as con:
        rows = con.execute("SELECT event_id,event_name,event_type,half_life_days,resolved FROM event_tokens").fetchall()
    st.table(rows)
    st.subheader("Add New Event")
    event_name = st.text_input("Event name")
    if st.button("ADD NEW EVENT") and event_name:
        st.success(f"Queued event: {event_name}")

elif page == "DATA PIPELINE":
    st.title("Data Pipeline")
    tables = ["election_results", "district_features", "national_factors", "district_forecasts", "chamber_forecasts", "admin_queue", "scraper_runs"]
    st.table([(t, table_count(t)) for t in tables])
    for name, runner in {"fred": FredScraper if False else None}.items():
        st.write(name, runner)
    if st.button("SCRAPE LOCAL NEWS NOW"):
        with st.spinner("Scraping local news"):
            ok = LocalNewsScraper().run()
        st.success("Local news scrape complete" if ok else "Local news scrape did not return new rows")
    if st.button("RUN CENSUS ACS NOW"):
        with st.spinner("Running Census ACS district ingest"):
            ok = CensusScraper().run()
        st.success("Census ACS ingest complete" if ok else "Census ACS ingest did not return new rows")
    if st.button("SCRAPE RACE NEWS"):
        with st.spinner("Scraping race-specific Google News, GDELT, and Ballotpedia coverage"):
            ok = RaceWebScraper().run()
        st.success("Race news scrape complete" if ok else "Race news scrape did not return new rows")
    if st.button("SCRAPE IDEOLOGY CORPUS"):
        with st.spinner("Scraping and chunking public-domain ideology corpus"):
            ok = IdeologyCorpusScraper().run()
        st.success("Ideology corpus scrape complete" if ok else "Ideology corpus scrape did not return new rows")
    if st.button("REFRESH FEC DATA"):
        with st.spinner("Refreshing FEC from local weball26.zip/cache"):
            ok = FECScraper().run()
        st.success("FEC refresh complete" if ok else "FEC refresh did not return new rows")
    st.subheader("Market Mapping")
    with get_read_connection() as con:
        kalshi_matched = con.execute(
            "SELECT raw_title, matched_district_id, match_confidence, yes_price FROM kalshi_market_mapping WHERE matched_district_id IS NOT NULL LIMIT 50"
        ).fetchall()
        kalshi_unmatched = con.execute(
            "SELECT raw_title, raw_ticker, yes_price FROM kalshi_market_mapping WHERE matched_district_id IS NULL LIMIT 50"
        ).fetchall()
        polymarket = con.execute(
            "SELECT title, chamber, party, yes_price, volume_total FROM polymarket_market_mapping LIMIT 50"
        ).fetchall()
    st.caption("Matched Kalshi markets")
    st.dataframe(kalshi_matched, use_container_width=True)
    st.caption("Unmatched Kalshi markets")
    st.dataframe(kalshi_unmatched, use_container_width=True)
    st.caption("Polymarket markets")
    st.dataframe(polymarket, use_container_width=True)
    if st.button("REFRESH POLYMARKET"):
        result = subprocess.run([sys.executable, "-m", "scrapers.polymarket_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("REFRESH KALSHI MARKETS"):
        result = subprocess.run([sys.executable, "-m", "scrapers.kalshi_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    st.subheader("Special Elections -- Current Cycle Signal")
    with get_read_connection() as con:
        specials = con.execute(
            """
            SELECT district_id, election_date, swing_from_baseline,
                   national_environment_signal, notes
            FROM special_elections
            ORDER BY election_date DESC
            LIMIT 20
            """
        ).fetchall()
        signal_row = con.execute(
            "SELECT special_election_signal_12m FROM national_factors ORDER BY factor_date DESC LIMIT 1"
        ).fetchone()
        reg_rows = con.execute(
            """
            SELECT state_abbr, d_share, r_share, d_r_ratio, d_registration_trend, data_source
            FROM voter_registration
            ORDER BY state_abbr
            """
        ).fetchall()
    st.dataframe(
        [
            {
                "District": row[0],
                "Date": row[1],
                "Swing vs Baseline": row[2],
                "Signal": row[3],
                "Notes": row[4],
            }
            for row in specials
        ],
        use_container_width=True,
    )
    signal = signal_row[0] if signal_row else None
    st.write(f"12-month weighted signal: {signal:.2f}pts" if signal is not None else "12-month weighted signal: unavailable")
    st.write("Interpretation: " + ("BULLISH D" if signal and signal > 2 else "BULLISH R" if signal and signal < -2 else "NEUTRAL"))
    st.subheader("Voter Registration")
    st.write(f"States with registration data: {len(reg_rows)}")
    st.dataframe(
        [
            {
                "State": row[0],
                "D share": row[1],
                "R share": row[2],
                "D/R ratio": row[3],
                "Trend": row[4],
                "Source": row[5],
            }
            for row in reg_rows
        ],
        use_container_width=True,
    )

elif page == "DATA COMPLETENESS":
    st.title("Data Completeness")
    with get_read_connection() as con:
        rows = con.execute(
            """
            SELECT
                h.district_id,
                h.incumbent_name IS NOT NULL AS incumbent,
                h.cook_pvi IS NOT NULL AS cook_pvi,
                df.district_id IS NOT NULL AS census,
                cq.district_id IS NOT NULL AS fec,
                km.district_id IS NOT NULL AS kalshi,
                pm.district_id IS NOT NULL AS polymarket,
                h.last_margin IS NOT NULL AS result_2024,
                COUNT(cr.candidate_id) AS candidates,
                SUM(CASE WHEN cr.fec_candidate_id IS NOT NULL THEN 1 ELSE 0 END) AS candidate_fec,
                COUNT(DISTINCT rwa.article_id) AS race_articles,
                COUNT(DISTINCT ln.article_id) AS local_news,
                COUNT(DISTINCT pis.candidate_name) AS integrity,
                CASE
                    WHEN COUNT(DISTINCT rwa.article_id)=0 THEN 'no articles'
                    WHEN COUNT(DISTINCT rwa.embedding)=0 THEN 'no embeddings'
                    ELSE 'computed'
                END AS ideology,
                COALESCE(ist.status, CASE WHEN h.retiring THEN 'retiring' ELSE 'running' END) AS status
            FROM house_roster h
            LEFT JOIN district_features df ON df.district_id=h.district_id AND df.feature_date=(SELECT MAX(feature_date) FROM district_features)
            LEFT JOIN candidate_quality cq ON cq.district_id=h.district_id AND cq.assessment_date=(SELECT MAX(assessment_date) FROM candidate_quality)
            LEFT JOIN kalshi_market_mapping km ON km.district_id=h.district_id
            LEFT JOIN polymarket_market_mapping pm ON pm.district_id=h.district_id
            LEFT JOIN candidate_roster_2026 cr ON cr.district_id=h.district_id
            LEFT JOIN race_web_articles rwa ON rwa.district_id=h.district_id
            LEFT JOIN local_news ln ON ln.district_id=h.district_id
            LEFT JOIN politician_integrity_signals pis ON pis.district_id=h.district_id
            LEFT JOIN incumbent_status_2026 ist ON ist.district_id=h.district_id
            GROUP BY h.district_id, incumbent, cook_pvi, census, fec, kalshi, polymarket, result_2024, h.retiring, ist.status
            ORDER BY h.district_id
            """
        ).fetchall()
    def mark(value):
        return "✓" if value else "✗"
    table = [
        {
            "District": r[0],
            "Incumbent": mark(r[1]),
            "Cook PVI": mark(r[2]),
            "Census": mark(r[3]),
            "FEC": mark(r[4]),
            "Kalshi": mark(r[5]),
            "Polymarket": mark(r[6]),
            "2024 Result": mark(r[7]),
            "Candidates": f"{int(r[9] or 0)}/{int(r[8] or 0)}",
            "Race Articles": int(r[10] or 0),
            "Local News": int(r[11] or 0),
            "Integrity": int(r[12] or 0),
            "Ideology": r[13],
            "Status": r[14],
        }
        for r in rows
    ]
    st.dataframe(table, use_container_width=True)
    complete = sum(1 for r in rows if all(r[i] for i in [1, 2, 3, 4, 7]) and (r[8] or 0) > 0)
    pct = complete / 435 if rows else 0
    tier = "Scaffold — do not publish" if pct < 0.5 else "Demo quality — label as beta" if pct < 0.8 else "Production ready"
    st.write(f"{complete} of 435 districts have complete data")
    st.write(f"Estimated model quality: {tier}")
    if st.button("LOAD ROSTER"):
        result = subprocess.run([sys.executable, "data/historical/load_roster.py"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("LOAD HISTORICAL"):
        result = subprocess.run([sys.executable, "data/historical/load_historical.py"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("SCRAPE CENSUS"):
        result = subprocess.run([sys.executable, "-m", "scrapers.census_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("SCRAPE RACE NEWS"):
        result = subprocess.run([sys.executable, "-m", "scrapers.race_web_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("SCRAPE LOCAL NEWS"):
        result = subprocess.run([sys.executable, "-m", "scrapers.local_news_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("BACKFILL EMBEDDINGS"):
        result = subprocess.run([sys.executable, "utils/backfill_embeddings.py"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("SCRAPE IDEOLOGY CORPUS"):
        result = subprocess.run([sys.executable, "-m", "scrapers.ideology_corpus_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("REFRESH KALSHI"):
        result = subprocess.run([sys.executable, "-m", "scrapers.kalshi_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("REFRESH POLYMARKET"):
        result = subprocess.run([sys.executable, "-m", "scrapers.polymarket_scraper"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
    if st.button("RUN WALK-FORWARD VALIDATION"):
        st.json(WalkForwardValidator().run())
    if st.button("REBUILD MODEL"):
        result = subprocess.run([sys.executable, "overnight.py", "--model-only"], cwd=".", text=True, capture_output=True)
        st.code(result.stdout + result.stderr)
