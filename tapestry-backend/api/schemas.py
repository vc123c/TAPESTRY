from __future__ import annotations

from datetime import date, datetime
from typing import Any, Optional

from pydantic import BaseModel, Field


class DistrictForecast(BaseModel):
    district_id: str
    statement: Optional[str] = None
    leading_candidate: Optional[str] = None
    leading_party: str
    projected_margin: float
    uncertainty: float
    win_probability_d: float
    factor_attribution: dict[str, Any]
    narrative: str
    kalshi_price: Optional[float] = None
    model_implied_price: float
    kalshi_gap: Optional[float] = None
    kalshi_gap_flag: bool = False
    gap_explanation: Optional[str] = None
    suspect_flag: bool = False
    last_updated: datetime
    incumbent_name: Optional[str] = None
    incumbent_party: Optional[str] = None
    incumbent_bioguide_id: Optional[str] = None
    incumbent_hometown: Optional[str] = None
    incumbent_office: Optional[str] = None
    incumbent_phone: Optional[str] = None
    incumbent_committees: list[dict[str, Any]] = Field(default_factory=list)
    roster_source: Optional[str] = None
    roster_publish_date: Optional[str] = None
    candidates_2026: list[dict[str, Any]] = Field(default_factory=list)
    major_challengers_2026: list[dict[str, Any]] = Field(default_factory=list)
    fundraising: list[dict[str, Any]] = Field(default_factory=list)
    district_features: dict[str, Any] = Field(default_factory=dict)
    integrity_signals: list[dict[str, Any]] = Field(default_factory=list)
    incumbent_status_2026: Optional[dict[str, Any]] = None
    race_intelligence: dict[str, Any] = Field(default_factory=dict)
    twoseventy_context: Optional[dict[str, Any]] = None


class HouseMember(BaseModel):
    district_id: str
    state_name: str
    state_abbr: str
    district_number: Optional[int] = None
    incumbent_name: str
    incumbent_party: str
    incumbent_first_elected: Optional[int] = None
    incumbent_bioguide_id: Optional[str] = None
    incumbent_url: Optional[str] = None
    fec_candidate_id: Optional[str] = None
    cook_pvi: Optional[str] = None
    cook_pvi_numeric: Optional[float] = None
    last_margin: Optional[float] = None
    retiring: bool = False
    data_source: str
    last_updated: str


class ChamberForecast(BaseModel):
    chamber: str
    d_control_probability: float
    d_expected_seats: float
    d_seats_low: float
    d_seats_high: float
    kalshi_price: Optional[float] = None
    polymarket_price: Optional[float] = None
    model_implied_price: float
    kalshi_gap: Optional[float] = None
    model_vs_polymarket_gap: Optional[float] = None
    model_vs_kalshi_gap: Optional[float] = None
    narrative: str


class MarketGap(BaseModel):
    chamber: Optional[str] = None
    district_id: Optional[str] = None
    tapestry_probability: float
    polymarket_price: Optional[float] = None
    kalshi_price: Optional[float] = None
    largest_gap: float
    gap_direction: str
    explanation: str


class MarketGapsResponse(BaseModel):
    chamber_gaps: list[MarketGap]
    district_gaps: list[MarketGap]
    generated_at: str


class NationalSummary(BaseModel):
    presidential_approval: float
    generic_ballot_margin: float
    kitchen_table_index: float
    gas_price_national: Optional[float] = None
    gas_price_3m_change: Optional[float] = None
    gas_prices_approval: Optional[float] = None
    anti_establishment_index: float
    college_realignment_index: float
    conflict_stage_iran: float
    escalation_trap_probability: float
    days_to_election: int
    issue_approval: Optional[dict] = None


class DistrictMove(BaseModel):
    district_id: str
    old_probability: Optional[float] = None
    new_probability: float
    cause: Optional[str] = None
    text: Optional[str] = None


class EventSummary(BaseModel):
    event_id: str
    event_name: str
    event_type: str
    salience: float
    affected_districts: list[str] = Field(default_factory=list)
    source_count: Optional[int] = None
    article_count: Optional[int] = None
    source_url: Optional[str] = None


class KalshiGap(BaseModel):
    market_id: str
    district_id: Optional[str] = None
    chamber: Optional[str] = None
    kalshi_price: float
    model_implied_price: float
    gap: float
    explanation: str


class MorningBrief(BaseModel):
    generated_at: datetime
    senate: ChamberForecast
    house: ChamberForecast
    national: NationalSummary
    top_moves: list[DistrictMove]
    active_events: list[EventSummary]
    kalshi_disagreements: list[KalshiGap]
    narrative: str
    anomalies_pending: int


class ConflictState(BaseModel):
    conflict_id: str
    conflict_name: str
    assessment_date: date
    current_stage: float
    stage_probabilities: dict[int, float]
    escalation_trap_probability: float
    domestic_political_loadings: dict[str, float]
    latest_signal: str
    days_in_conflict: int


class EventToken(BaseModel):
    event_id: str
    event_name: str
    event_date: date
    event_type: str
    target_party: str
    salience: float = 0.0
    notes: Optional[str] = None


class RetrainJobStatus(BaseModel):
    job_id: str
    status: str
    progress: float
    message: str


class ResolveQueueRequest(BaseModel):
    accepted_token: Optional[dict[str, Any]] = None
    dismissed: bool = False


class NewsArticle(BaseModel):
    headline: str
    url: str
    source_name: str
    source_type: str
    published_at: str
    time_ago: str
    sentiment: str
    incumbent_relevant: bool
    topic_tags: list[str] = Field(default_factory=list)
