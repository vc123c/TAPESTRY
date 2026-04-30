from __future__ import annotations

from datetime import date

import polars as pl
import json
import math

from db.connection import get_read_connection, write_connection
from utils.logging import setup_logging

logger = setup_logging(__name__)

NUMERIC_DEFAULTS = {
    "cook_pvi": 0.0, "margin_t0": 0.0, "margin_t1": 0.0, "margin_t2": 0.0, "margin_trend": 0.0,
    "presidential_margin_2024": 0.0, "presidential_margin_2020": 0.0, "incumbent_years": 0,
    "fundraising_ratio": 1.0, "cash_on_hand_ratio": 1.0, "outside_spending_ratio": 1.0,
    "college_educated_pct": 0.35, "median_age": 39.0, "white_pct": 0.60, "hispanic_pct": 0.19,
    "black_pct": 0.13, "population_density": 0.0, "median_income_real": 75000.0, "income_growth_2yr": 0.0,
    "gini_coefficient": 0.41, "unemployment_rate": 4.1, "unemployment_vs_national": 0.0,
    "medical_debt_per_capita": 0.0, "credit_card_debt_per_capita": 0.0, "healthcare_cost_burden": 0.0,
    "rent_burden_pct": 0.30, "uninsured_rate": 0.08, "ai_automation_exposure": 0.0,
    "manufacturing_share": 0.0, "tech_employment_share": 0.0, "recent_layoffs": 0,
    "net_hiring_trend": 0.0, "data_center_mw_planned": 0.0, "data_center_opposition_score": 0.0,
    "independent_media_penetration": 0.0, "local_news_intensity": 0.0,
    "reg_d_advantage": 0.0, "reg_d_r_ratio": 1.0, "reg_momentum_d": 0.5, "reg_net_momentum": 0.0,
    "reg_d_trend_90d": 0.0, "total_reg_growth_90d": 0.0, "weighted_issue_approval": 0.0,
    "immigration_approval_relevance": 0.10, "economy_approval_relevance": 0.25,
    "iran_war_approval_relevance": 0.10,
}

_IMPUTATION_COUNTS: dict[str, int] | None = None


def _decode_embedding(blob) -> list[float] | None:
    if blob is None:
        return None
    try:
        if isinstance(blob, memoryview):
            blob = blob.tobytes()
        if isinstance(blob, bytes):
            return [float(x) for x in json.loads(blob.decode("utf-8"))]
        if isinstance(blob, str):
            return [float(x) for x in json.loads(blob)]
    except Exception:
        return None
    return None


def _cosine(left: list[float], right: list[float]) -> float:
    dot = sum(a * b for a, b in zip(left, right))
    left_norm = math.sqrt(sum(a * a for a in left)) or 1.0
    right_norm = math.sqrt(sum(b * b for b in right)) or 1.0
    return dot / (left_norm * right_norm)


def compute_ideology_alignment_score(district_id: str, ideology_frame: str, db=None) -> float | None:
    """
    Score how closely recent race-specific text in a district aligns with a public-domain ideology frame.
    Returns None until both race article and ideology corpus embeddings/text are present.
    """
    close_db = False
    con = db
    if con is None:
        con = get_read_connection()
        close_db = True
    try:
        articles = con.execute(
            """
            SELECT headline, summary
            FROM race_web_articles
            WHERE district_id=?
            ORDER BY published_at DESC
            LIMIT 25
            """,
            [district_id.upper()],
        ).fetchall()
        chunks = con.execute(
            """
            SELECT embedding, chunk_text, text
            FROM ideology_corpus_chunks
            WHERE ideology_frame=? OR ?=ANY(ideology_tags)
            LIMIT 80
            """,
            [ideology_frame, ideology_frame],
        ).fetchall()
        if not articles or not chunks:
            return None
        chunk_texts = [f"{row[1] or row[2] or ''}".lower() for row in chunks]
        article_words = set(" ".join(f"{h or ''} {s or ''}" for h, s in articles).lower().split())
        keyword_overlap = 0.0
        if chunk_texts and article_words:
            overlaps = []
            for text in chunk_texts[:20]:
                words = set(text.split())
                overlaps.append(len(article_words & words) / max(len(article_words), 1))
            keyword_overlap = sum(overlaps) / len(overlaps)
        embeddings = [_decode_embedding(row[0]) for row in chunks]
        embeddings = [e for e in embeddings if e]
        if embeddings:
            # Article embeddings are not persisted yet; use the corpus-text overlap as a stable fallback score.
            return max(0.0, min(1.0, keyword_overlap * 8.0))
        return max(0.0, min(1.0, keyword_overlap * 8.0))
    except Exception as exc:
        logger.debug("Ideology alignment unavailable for %s/%s: %s", district_id, ideology_frame, exc)
        return None
    finally:
        if close_db:
            con.close()


def _val(row: dict, key: str, district_id: str):
    global _IMPUTATION_COUNTS
    value = row.get(key)
    if value is None:
        fallback = NUMERIC_DEFAULTS[key]
        if _IMPUTATION_COUNTS is not None:
            _IMPUTATION_COUNTS[key] = _IMPUTATION_COUNTS.get(key, 0) + 1
        return fallback
    return value


def build_district_features(feature_date: date | None = None) -> pl.DataFrame:
    global _IMPUTATION_COUNTS
    feature_date = feature_date or date.today()
    _IMPUTATION_COUNTS = {}
    with get_read_connection() as con:
        rows = con.execute(
            """
            WITH latest_candidate_quality AS (
                SELECT
                    district_id,
                    AVG(fundraising_vs_expected) AS fundraising_ratio,
                    AVG(small_dollar_share) AS small_dollar_share
                FROM candidate_quality
                WHERE assessment_date = (SELECT MAX(assessment_date) FROM candidate_quality)
                GROUP BY district_id
            ),
            media_by_district AS (
                SELECT
                    district_id,
                    AVG(CASE WHEN event_type = 'scandal' THEN salience_score ELSE NULL END) AS scandal_salience,
                    AVG(CASE WHEN event_type = 'conflict' THEN salience_score ELSE NULL END) AS conflict_salience,
                    AVG(salience_score) AS media_salience,
                    COUNT(*) AS media_article_count,
                    COUNT(DISTINCT source_name) AS media_source_count
                FROM media_event_articles
                WHERE district_id IS NOT NULL
                GROUP BY district_id
            ),
            latest_voter_registration AS (
                SELECT *
                FROM voter_registration vr
                WHERE report_date = (
                    SELECT MAX(report_date)
                    FROM voter_registration
                    WHERE state_abbr = vr.state_abbr
                )
            ),
            issue_avgs AS (
                SELECT
                    MAX(CASE WHEN issue_key = 'economy' THEN net_approval END) AS economy_net,
                    MAX(CASE WHEN issue_key = 'inflation' THEN net_approval END) AS inflation_net,
                    MAX(CASE WHEN issue_key = 'immigration' THEN net_approval END) AS immigration_net,
                    MAX(CASE WHEN issue_key = 'iran_war' THEN net_approval END) AS iran_war_net,
                    MAX(CASE WHEN issue_key = 'healthcare' THEN net_approval END) AS healthcare_net,
                    MAX(CASE WHEN issue_key = 'gas_prices' THEN net_approval END) AS gas_prices_net,
                    MAX(CASE WHEN issue_key = 'crime' THEN net_approval END) AS crime_net,
                    MAX(CASE WHEN issue_key = 'tariffs' THEN net_approval END) AS tariffs_net
                FROM issue_approval_averages
            )
            SELECT
                h.district_id,
                h.cook_pvi_numeric AS cook_pvi,
                h.last_margin AS margin_t0,
                h.incumbent_party,
                NOT h.retiring AS incumbent_running,
                h.retiring AS open_seat,
                COALESCE(f.incumbent_years, 2026 - h.incumbent_first_elected) AS incumbent_years,
                f.college_educated_pct, f.median_age, f.white_pct, f.hispanic_pct, f.black_pct,
                f.population_density, f.urban_rural_class, f.median_income_real, f.income_growth_2yr,
                f.gini_coefficient, f.unemployment_rate, f.medical_debt_per_capita,
                f.credit_card_debt_per_capita, f.healthcare_cost_burden, f.rent_burden_pct,
                f.uninsured_rate, f.ai_automation_exposure, f.manufacturing_share,
                f.tech_employment_share, f.recent_layoffs, f.net_hiring_trend,
                f.data_center_mw_planned, f.data_center_opposition_score,
                f.independent_media_penetration, f.local_news_intensity,
                f.reg_d_advantage, f.reg_d_r_ratio, f.reg_momentum_d, f.reg_net_momentum,
                f.reg_d_trend_90d, f.total_reg_growth_90d, f.weighted_issue_approval,
                f.immigration_approval_relevance, f.economy_approval_relevance, f.iran_war_approval_relevance,
                f.abortion_measure, f.marijuana_measure, f.min_wage_measure,
                vr.d_share, vr.r_share, vr.d_r_ratio AS vr_d_r_ratio, vr.d_registration_trend, vr.total_registered,
                ia.economy_net, ia.inflation_net, ia.immigration_net, ia.iran_war_net,
                ia.healthcare_net, ia.gas_prices_net, ia.crime_net, ia.tariffs_net,
                cq.fundraising_ratio AS fundraising_ratio,
                cq.small_dollar_share,
                m.scandal_salience,
                m.conflict_salience,
                m.media_salience,
                m.media_article_count,
                m.media_source_count
            FROM house_roster h
            LEFT JOIN district_features f
              ON f.district_id = h.district_id
             AND f.feature_date = (SELECT MAX(feature_date) FROM district_features)
            LEFT JOIN latest_candidate_quality cq
              ON cq.district_id = h.district_id
            LEFT JOIN media_by_district m
              ON m.district_id = h.district_id
            LEFT JOIN latest_voter_registration vr
              ON vr.state_abbr = h.state_abbr
            CROSS JOIN issue_avgs ia
            ORDER BY h.district_id
            """
        ).fetchall()
        cols = [desc[0] for desc in con.description]
        national = con.execute(
            "SELECT unemployment_rate FROM national_factors ORDER BY factor_date DESC LIMIT 1"
        ).fetchone()
    national_unemployment = float(national[0]) if national and national[0] is not None else 4.1
    out = []
    try:
        for values in rows:
            raw = dict(zip(cols, values))
            district_id = raw["district_id"]
            unemployment = raw.get("unemployment_rate")
            manufacturing = raw.get("manufacturing_share")
            incumbent_party = raw.get("incumbent_party")
            scandal_salience = raw.get("scandal_salience") or 0.0
            conflict_salience = raw.get("conflict_salience") or 0.0
            media_salience = raw.get("media_salience") or 0.0
            media_count = raw.get("media_article_count") or 0
            media_sources = raw.get("media_source_count") or 0
            scandal_direction = -1.0 if incumbent_party == "D" else 1.0 if incumbent_party == "R" else 0.0
            reg_d_advantage = raw.get("reg_d_advantage")
            if reg_d_advantage is None and raw.get("d_share") is not None and raw.get("r_share") is not None:
                reg_d_advantage = float(raw["d_share"]) - float(raw["r_share"])
            reg_d_r_ratio = raw.get("reg_d_r_ratio") if raw.get("reg_d_r_ratio") is not None else raw.get("vr_d_r_ratio")
            issue_weights = {
                "economy": 0.25,
                "inflation": 0.25,
                "immigration": 0.10,
                "iran_war": 0.10,
                "healthcare": 0.10,
                "gas_prices": 0.10,
                "crime": 0.05,
                "tariffs": 0.05,
            }
            if (manufacturing or 0.0) > 0.15:
                issue_weights["tariffs"] += 0.08
                issue_weights["economy"] += 0.05
            if (raw.get("hispanic_pct") or 0.0) > 0.25:
                issue_weights["immigration"] += 0.08
            if (raw.get("rent_burden_pct") or 0.0) > 0.35:
                issue_weights["inflation"] += 0.06
                issue_weights["gas_prices"] += 0.04
            if (raw.get("uninsured_rate") or 0.0) > 0.12:
                issue_weights["healthcare"] += 0.06
            total_issue_weight = sum(issue_weights.values()) or 1.0
            issue_weights = {key: value / total_issue_weight for key, value in issue_weights.items()}
            issue_values = {
                "economy": raw.get("economy_net"),
                "inflation": raw.get("inflation_net"),
                "immigration": raw.get("immigration_net"),
                "iran_war": raw.get("iran_war_net"),
                "healthcare": raw.get("healthcare_net"),
                "gas_prices": raw.get("gas_prices_net"),
                "crime": raw.get("crime_net"),
                "tariffs": raw.get("tariffs_net"),
            }
            weighted_issue = raw.get("weighted_issue_approval")
            if weighted_issue is None and any(value is not None for value in issue_values.values()):
                weighted_issue = sum(issue_weights[key] * float(issue_values.get(key) or 0.0) for key in issue_weights)
            out.append({
                "district_id": district_id,
                "feature_date": feature_date,
                "cook_pvi": _val(raw, "cook_pvi", district_id),
                "margin_t0": _val(raw, "margin_t0", district_id),
                "margin_t1": _val(raw, "margin_t0", district_id),
                "margin_t2": _val(raw, "margin_t0", district_id),
                "margin_trend": 0.0,
                "presidential_margin_2024": _val(raw, "cook_pvi", district_id),
                "presidential_margin_2020": _val(raw, "cook_pvi", district_id),
                "incumbent_party": raw.get("incumbent_party"),
                "incumbent_running": bool(raw.get("incumbent_running")),
                "incumbent_years": _val(raw, "incumbent_years", district_id),
                "open_seat": bool(raw.get("open_seat")),
                "fundraising_ratio": raw.get("fundraising_ratio") if raw.get("fundraising_ratio") is not None else 1.0,
                "cash_on_hand_ratio": 1.0,
                "outside_spending_ratio": 1.0,
                "college_educated_pct": _val(raw, "college_educated_pct", district_id),
                "median_age": _val(raw, "median_age", district_id),
                "white_pct": _val(raw, "white_pct", district_id),
                "hispanic_pct": _val(raw, "hispanic_pct", district_id),
                "black_pct": _val(raw, "black_pct", district_id),
                "population_density": _val(raw, "population_density", district_id),
                "urban_rural_class": raw.get("urban_rural_class") or "not_available",
                "median_income_real": _val(raw, "median_income_real", district_id),
                "income_growth_2yr": _val(raw, "income_growth_2yr", district_id),
                "gini_coefficient": _val(raw, "gini_coefficient", district_id),
                "unemployment_rate": unemployment if unemployment is not None else national_unemployment,
                "unemployment_vs_national": (unemployment - national_unemployment) if unemployment is not None else 0.0,
                "medical_debt_per_capita": _val(raw, "medical_debt_per_capita", district_id),
                "credit_card_debt_per_capita": _val(raw, "credit_card_debt_per_capita", district_id),
                "healthcare_cost_burden": _val(raw, "healthcare_cost_burden", district_id),
                "rent_burden_pct": _val(raw, "rent_burden_pct", district_id),
                "uninsured_rate": _val(raw, "uninsured_rate", district_id),
                "ai_automation_exposure": raw.get("ai_automation_exposure") if raw.get("ai_automation_exposure") is not None else ((manufacturing or 0.0) * 0.7),
                "manufacturing_share": manufacturing or 0.0,
                "tech_employment_share": _val(raw, "tech_employment_share", district_id),
                "recent_layoffs": _val(raw, "recent_layoffs", district_id),
                "net_hiring_trend": _val(raw, "net_hiring_trend", district_id),
                "data_center_mw_planned": _val(raw, "data_center_mw_planned", district_id),
                "data_center_opposition_score": _val(raw, "data_center_opposition_score", district_id),
                "reg_d_advantage": reg_d_advantage if reg_d_advantage is not None else _val(raw, "reg_d_advantage", district_id),
                "reg_d_r_ratio": reg_d_r_ratio if reg_d_r_ratio is not None else _val(raw, "reg_d_r_ratio", district_id),
                "reg_momentum_d": raw.get("reg_momentum_d") if raw.get("reg_momentum_d") is not None else _val(raw, "reg_momentum_d", district_id),
                "reg_net_momentum": raw.get("reg_net_momentum") if raw.get("reg_net_momentum") is not None else _val(raw, "reg_net_momentum", district_id),
                "reg_d_trend_90d": raw.get("reg_d_trend_90d") if raw.get("reg_d_trend_90d") is not None else _val(raw, "reg_d_trend_90d", district_id),
                "total_reg_growth_90d": raw.get("total_reg_growth_90d") if raw.get("total_reg_growth_90d") is not None else _val(raw, "total_reg_growth_90d", district_id),
                "weighted_issue_approval": weighted_issue if weighted_issue is not None else _val(raw, "weighted_issue_approval", district_id),
                "immigration_approval_relevance": issue_weights["immigration"],
                "economy_approval_relevance": issue_weights["economy"],
                "iran_war_approval_relevance": issue_weights["iran_war"],
                "independent_media_penetration": raw.get("independent_media_penetration") if raw.get("independent_media_penetration") is not None else min(float(media_sources) / 12.0, 1.0) * 100.0,
                "local_news_intensity": raw.get("local_news_intensity") if raw.get("local_news_intensity") is not None else min((float(media_count) / 40.0) + (float(media_salience) * 0.6), 1.0) * 100.0,
                "scandal_effect": float(scandal_salience) * 5.0 * scandal_direction,
                "conflict_loading": float(conflict_salience) * 2.0,
                "abortion_measure": bool(raw.get("abortion_measure")),
                "marijuana_measure": bool(raw.get("marijuana_measure")),
                "min_wage_measure": bool(raw.get("min_wage_measure")),
            })
        if _IMPUTATION_COUNTS:
            summary = ", ".join(f"{key}: {count} districts used national/default avg" for key, count in sorted(_IMPUTATION_COUNTS.items()))
            logger.info("Feature imputation summary: %s (run census/FEC scrapers to fill these gaps)", summary)
            try:
                with write_connection() as con:
                    for key, count in _IMPUTATION_COUNTS.items():
                        con.execute(
                            "INSERT OR REPLACE INTO data_quality VALUES (?, ?, ?, ?, ?, ?)",
                            [feature_date, "district_features", key, "imputed_count", count, "National/default average used during feature build"],
                        )
            except Exception as exc:
                logger.warning("Could not write feature imputation summary: %s", exc)
        return pl.DataFrame(out)
    finally:
        _IMPUTATION_COUNTS = None


def build_national_factors(factor_date: date | None = None) -> dict:
    factor_date = factor_date or date.today()
    with get_read_connection() as con:
        row = con.execute("SELECT * FROM national_factors ORDER BY factor_date DESC LIMIT 1").fetchone()
        columns = [desc[0] for desc in con.description] if row else []
    if row:
        data = dict(zip(columns, row))
        data["factor_date"] = factor_date
        return data
    logger.warning("No national_factors rows found; using neutral defaults and marking values as initialization defaults")
    return {
        "factor_date": factor_date, "presidential_approval": 44.0, "generic_ballot_d_margin": 0.0,
        "real_income_growth_yoy": 0.0, "unemployment_rate": 4.1, "unemployment_3m_change": 0.0,
        "consumer_sentiment": 0.0, "inflation_yoy": 0.0, "real_wage_growth": 0.0,
        "gas_price_national": 0.0, "gas_price_3m_change": 0.0, "grocery_price_index": 0.0,
        "egg_price_avg": 0.0, "egg_price_3m_change": 0.0, "rent_burden_national": 0.0,
        "mortgage_rate_30y": 0.0, "kitchen_table_index": 0.0, "dw_nominate_spread": 0.0,
        "college_realignment_index": 0.0, "anti_establishment_index": 0.0,
        "independent_media_index": 0.0, "local_news_employment_index": 0.0,
        "internet_penetration": 0.0, "social_media_penetration": 0.0,
        "era_pre_social": False, "era_social_media": False, "era_post_trust": True,
        "economy_approval": None, "inflation_approval": None, "immigration_approval": None,
    }
