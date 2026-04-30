CREATE TABLE IF NOT EXISTS election_results (
    district_id VARCHAR,
    year INTEGER,
    cycle_type VARCHAR,
    redistricting_era VARCHAR,
    d_vote_pct DOUBLE,
    r_vote_pct DOUBLE,
    margin DOUBLE,
    winner_party VARCHAR,
    uncontested BOOLEAN,
    special_election BOOLEAN,
    incumbent_party VARCHAR,
    incumbent_running BOOLEAN,
    d_candidate VARCHAR,
    r_candidate VARCHAR,
    crosswalk_weight DOUBLE,
    PRIMARY KEY (district_id, year)
);

CREATE TABLE IF NOT EXISTS district_features (
    district_id VARCHAR,
    feature_date DATE,
    cook_pvi DOUBLE,
    margin_t0 DOUBLE,
    margin_t1 DOUBLE,
    margin_t2 DOUBLE,
    margin_trend DOUBLE,
    presidential_margin_2024 DOUBLE,
    presidential_margin_2020 DOUBLE,
    incumbent_party VARCHAR,
    incumbent_running BOOLEAN,
    incumbent_years INTEGER,
    open_seat BOOLEAN,
    fundraising_ratio DOUBLE,
    cash_on_hand_ratio DOUBLE,
    outside_spending_ratio DOUBLE,
    college_educated_pct DOUBLE,
    median_age DOUBLE,
    white_pct DOUBLE,
    hispanic_pct DOUBLE,
    black_pct DOUBLE,
    population_density DOUBLE,
    urban_rural_class VARCHAR,
    median_income_real DOUBLE,
    income_growth_2yr DOUBLE,
    gini_coefficient DOUBLE,
    unemployment_rate DOUBLE,
    unemployment_vs_national DOUBLE,
    medical_debt_per_capita DOUBLE,
    credit_card_debt_per_capita DOUBLE,
    healthcare_cost_burden DOUBLE,
    rent_burden_pct DOUBLE,
    uninsured_rate DOUBLE,
    ai_automation_exposure DOUBLE,
    manufacturing_share DOUBLE,
    tech_employment_share DOUBLE,
    recent_layoffs INTEGER,
    net_hiring_trend DOUBLE,
    data_center_mw_planned DOUBLE,
    data_center_opposition_score DOUBLE,
    independent_media_penetration DOUBLE,
    local_news_intensity DOUBLE,
    abortion_measure BOOLEAN,
    marijuana_measure BOOLEAN,
    min_wage_measure BOOLEAN,
    PRIMARY KEY (district_id, feature_date)
);

CREATE TABLE IF NOT EXISTS national_factors (
    factor_date DATE,
    presidential_approval DOUBLE,
    generic_ballot_d_margin DOUBLE,
    real_income_growth_yoy DOUBLE,
    unemployment_rate DOUBLE,
    unemployment_3m_change DOUBLE,
    consumer_sentiment DOUBLE,
    inflation_yoy DOUBLE,
    real_wage_growth DOUBLE,
    gas_price_national DOUBLE,
    gas_price_3m_change DOUBLE,
    grocery_price_index DOUBLE,
    egg_price_avg DOUBLE,
    egg_price_3m_change DOUBLE,
    rent_burden_national DOUBLE,
    mortgage_rate_30y DOUBLE,
    kitchen_table_index DOUBLE,
    dw_nominate_spread DOUBLE,
    college_realignment_index DOUBLE,
    anti_establishment_index DOUBLE,
    independent_media_index DOUBLE,
    local_news_employment_index DOUBLE,
    internet_penetration DOUBLE,
    social_media_penetration DOUBLE,
    era_pre_social BOOLEAN,
    era_social_media BOOLEAN,
    era_post_trust BOOLEAN,
    PRIMARY KEY (factor_date)
);

ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS economy_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS inflation_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS immigration_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS iran_war_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS healthcare_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS crime_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS tariffs_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS gas_prices_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS prices_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS border_security_approval DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS approval_source VARCHAR;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS approval_n_polls INTEGER;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS economy_approval_gap DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS immigration_approval_gap DOUBLE;
ALTER TABLE national_factors ADD COLUMN IF NOT EXISTS special_election_signal_12m DOUBLE;

CREATE TABLE IF NOT EXISTS approval_polls (
    poll_id VARCHAR PRIMARY KEY,
    pollster VARCHAR,
    subject VARCHAR,
    poll_type VARCHAR,
    start_date DATE,
    end_date DATE,
    approve_pct DOUBLE,
    disapprove_pct DOUBLE,
    sample_size DOUBLE,
    population VARCHAR,
    quality_weight DOUBLE,
    time_weight DOUBLE,
    combined_weight DOUBLE,
    source_url VARCHAR,
    fetched_at TIMESTAMP
);

ALTER TABLE approval_polls ADD COLUMN IF NOT EXISTS net_approval DOUBLE;
ALTER TABLE approval_polls ADD COLUMN IF NOT EXISTS source_name VARCHAR;

CREATE TABLE IF NOT EXISTS historical_approval_gallup (
    poll_id VARCHAR PRIMARY KEY,
    president VARCHAR,
    president_party VARCHAR,
    start_date DATE,
    end_date DATE,
    approve_pct DOUBLE,
    disapprove_pct DOUBLE,
    no_opinion_pct DOUBLE,
    days_into_term INTEGER,
    midterm_year INTEGER,
    source VARCHAR DEFAULT 'UCSB/Gallup'
);

CREATE TABLE IF NOT EXISTS special_elections (
    election_id VARCHAR PRIMARY KEY,
    district_id VARCHAR,
    election_date DATE,
    reason VARCHAR,
    d_vote_pct DOUBLE,
    r_vote_pct DOUBLE,
    margin DOUBLE,
    winner_party VARCHAR,
    turnout_estimate INTEGER,
    prior_general_margin DOUBLE,
    swing_from_baseline DOUBLE,
    national_environment_signal DOUBLE,
    days_before_next_general INTEGER,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS voter_registration (
    state_abbr VARCHAR,
    report_date DATE,
    total_registered INTEGER,
    d_registered INTEGER,
    r_registered INTEGER,
    independent_registered INTEGER,
    other_registered INTEGER,
    d_share DOUBLE,
    r_share DOUBLE,
    ind_share DOUBLE,
    d_r_ratio DOUBLE,
    net_new_d_30d INTEGER,
    net_new_r_30d INTEGER,
    net_new_total_30d INTEGER,
    d_registration_trend DOUBLE,
    r_registration_trend DOUBLE,
    data_source VARCHAR,
    PRIMARY KEY (state_abbr, report_date)
);

CREATE TABLE IF NOT EXISTS issue_approval (
    record_id VARCHAR PRIMARY KEY,
    issue_key VARCHAR,
    pollster VARCHAR,
    poll_date DATE,
    approve_pct DOUBLE,
    disapprove_pct DOUBLE,
    net_approval DOUBLE,
    population VARCHAR,
    source_url VARCHAR,
    scraped_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS issue_approval_averages (
    issue_key VARCHAR PRIMARY KEY,
    approve_pct DOUBLE,
    disapprove_pct DOUBLE,
    net_approval DOUBLE,
    source_count INTEGER,
    updated_at TIMESTAMP
);

ALTER TABLE district_features ADD COLUMN IF NOT EXISTS reg_d_advantage DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS reg_d_r_ratio DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS reg_momentum_d DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS reg_net_momentum DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS reg_d_trend_90d DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS total_reg_growth_90d DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS weighted_issue_approval DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS immigration_approval_relevance DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS economy_approval_relevance DOUBLE;
ALTER TABLE district_features ADD COLUMN IF NOT EXISTS iran_war_approval_relevance DOUBLE;

CREATE TABLE IF NOT EXISTS conflict_states (
    conflict_id VARCHAR,
    assessment_date DATE,
    conflict_name VARCHAR,
    start_date DATE,
    current_stage DOUBLE,
    stage_1_prob DOUBLE,
    stage_2_prob DOUBLE,
    stage_3_prob DOUBLE,
    stage_4_prob DOUBLE,
    stage_5_prob DOUBLE,
    escalation_trap_prob DOUBLE,
    days_in_conflict INTEGER,
    oil_price_level DOUBLE,
    oil_price_change_7d DOUBLE,
    defense_equity_change_7d DOUBLE,
    hormuz_disruption_index DOUBLE,
    pape_stage_signal DOUBLE,
    latest_signal_text TEXT,
    PRIMARY KEY (conflict_id, assessment_date)
);

CREATE TABLE IF NOT EXISTS event_tokens (
    event_id VARCHAR PRIMARY KEY,
    event_name VARCHAR,
    event_date DATE,
    event_type VARCHAR,
    scandal_subtype VARCHAR,
    primary_target_party VARCHAR,
    anti_establishment_loading DOUBLE,
    partisan_loading DOUBLE,
    embedding BLOB,
    half_life_days INTEGER,
    affected_districts VARCHAR[],
    similar_event_ids VARCHAR[],
    outcome_seat_swing INTEGER,
    outcome_magnitude DOUBLE,
    resolved BOOLEAN DEFAULT FALSE,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS event_salience (
    event_id VARCHAR,
    salience_date DATE,
    google_trends_score DOUBLE,
    news_volume DOUBLE,
    social_mentions DOUBLE,
    composite_salience DOUBLE,
    PRIMARY KEY (event_id, salience_date)
);

CREATE TABLE IF NOT EXISTS district_forecasts (
    district_id VARCHAR,
    forecast_date DATE,
    leading_candidate VARCHAR,
    leading_party VARCHAR,
    projected_margin DOUBLE,
    uncertainty DOUBLE,
    win_probability_d DOUBLE,
    factor_attribution JSON,
    narrative TEXT,
    kalshi_price DOUBLE,
    model_implied_price DOUBLE,
    kalshi_gap DOUBLE,
    kalshi_gap_flag BOOLEAN,
    gap_explanation TEXT,
    suspect_flag BOOLEAN DEFAULT FALSE,
    brier_score_historical DOUBLE,
    PRIMARY KEY (district_id, forecast_date)
);

CREATE TABLE IF NOT EXISTS chamber_forecasts (
    forecast_date DATE,
    chamber VARCHAR,
    d_control_probability DOUBLE,
    d_expected_seats DOUBLE,
    d_seats_10th_pct DOUBLE,
    d_seats_90th_pct DOUBLE,
    kalshi_price DOUBLE,
    model_implied_price DOUBLE,
    kalshi_gap DOUBLE,
    narrative TEXT,
    PRIMARY KEY (forecast_date, chamber)
);

CREATE TABLE IF NOT EXISTS candidate_quality (
    candidate_id VARCHAR,
    district_id VARCHAR,
    assessment_date DATE,
    party VARCHAR,
    quality_score DOUBLE,
    fundraising_vs_expected DOUBLE,
    small_dollar_share DOUBLE,
    prior_office_held BOOLEAN,
    prior_office_level VARCHAR,
    endorsement_score DOUBLE,
    positive_news_ratio DOUBLE,
    active_scandal BOOLEAN,
    scandal_type VARCHAR,
    scandal_severity INTEGER,
    scandal_days_active INTEGER,
    PRIMARY KEY (candidate_id, assessment_date)
);

CREATE TABLE IF NOT EXISTS fec_candidate_finance (
    fec_candidate_id VARCHAR PRIMARY KEY,
    district_id VARCHAR,
    candidate_name VARCHAR,
    party VARCHAR,
    incumbent_status VARCHAR,
    total_receipts DOUBLE,
    total_disbursements DOUBLE,
    cash_on_hand DOUBLE,
    individual_contributions DOUBLE,
    pac_contributions DOUBLE,
    party_contributions DOUBLE,
    coverage_end_date DATE,
    source_file VARCHAR,
    last_updated DATE
);

CREATE TABLE IF NOT EXISTS model_performance (
    evaluation_date DATE,
    train_years VARCHAR,
    test_year INTEGER,
    brier_score DOUBLE,
    cook_brier_score DOUBLE,
    improvement DOUBLE,
    n_races INTEGER,
    calibration_data JSON,
    PRIMARY KEY (evaluation_date, test_year)
);

CREATE TABLE IF NOT EXISTS admin_queue (
    queue_id VARCHAR PRIMARY KEY,
    queued_at TIMESTAMP,
    event_description TEXT,
    affected_districts VARCHAR[],
    search_results JSON,
    suggested_token JSON,
    status VARCHAR DEFAULT 'pending',
    user_response JSON,
    resolved_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS local_news (
    article_id VARCHAR PRIMARY KEY,
    district_id VARCHAR,
    state_fips VARCHAR,
    published_at TIMESTAMP,
    headline TEXT,
    url TEXT,
    source_name VARCHAR,
    source_type VARCHAR,
    incumbent_relevant BOOLEAN,
    sentiment VARCHAR,
    topic_tags VARCHAR[],
    gdelt_tone DOUBLE,
    fetched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_event_articles (
    article_id VARCHAR PRIMARY KEY,
    published_at TIMESTAMP,
    headline TEXT,
    url TEXT,
    source_name VARCHAR,
    source_type VARCHAR,
    outlet_tier VARCHAR,
    scope VARCHAR,
    state_abbr VARCHAR,
    district_id VARCHAR,
    event_type VARCHAR,
    topic_tags VARCHAR[],
    ideology_tags VARCHAR[],
    target_party VARCHAR,
    incumbent_relevant BOOLEAN,
    salience_score DOUBLE,
    sentiment VARCHAR,
    summary TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS media_signal_summary (
    signal_date DATE,
    signal_key VARCHAR,
    event_type VARCHAR,
    topic_tags VARCHAR[],
    source_count INTEGER,
    article_count INTEGER,
    max_salience DOUBLE,
    avg_salience DOUBLE,
    affected_districts VARCHAR[],
    representative_headline TEXT,
    representative_url TEXT,
    PRIMARY KEY (signal_date, signal_key)
);

CREATE TABLE IF NOT EXISTS ideology_corpus_chunks (
    chunk_id VARCHAR PRIMARY KEY,
    source_title VARCHAR,
    source_text VARCHAR,
    author VARCHAR,
    ideology_frame VARCHAR,
    publication_year INTEGER,
    source_url VARCHAR,
    chunk_index INTEGER,
    text TEXT,
    chunk_text TEXT,
    ideology_tags VARCHAR[],
    key_themes VARCHAR[],
    embedding BLOB,
    word_count INTEGER,
    created_at TIMESTAMP,
    fetched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS race_web_articles (
    article_id VARCHAR PRIMARY KEY,
    district_id VARCHAR,
    candidate_name VARCHAR,
    query TEXT,
    published_at TIMESTAMP,
    headline TEXT,
    url TEXT,
    source_name VARCHAR,
    source_type VARCHAR,
    event_type VARCHAR,
    incumbent_relevant BOOLEAN,
    topic_tags VARCHAR[],
    ideology_tags VARCHAR[],
    salience_score DOUBLE,
    sentiment VARCHAR,
    race_specific BOOLEAN DEFAULT TRUE,
    summary TEXT,
    fetched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS donor_transparency (
    district_id VARCHAR PRIMARY KEY,
    incumbent_name VARCHAR,
    as_of DATE,
    source_name VARCHAR,
    top_donor_sector VARCHAR,
    top_donor_amount DOUBLE,
    pro_israel_pac_amount DOUBLE,
    aipac_related_amount DOUBLE,
    defense_sector_amount DOUBLE,
    healthcare_sector_amount DOUBLE,
    finance_sector_amount DOUBLE,
    small_dollar_share DOUBLE,
    medicare_posture VARCHAR,
    israel_posture VARCHAR,
    defense_industry_posture VARCHAR,
    labor_posture VARCHAR,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS house_roster (
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
);

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
);

CREATE TABLE IF NOT EXISTS member_committees (
    bioguide_id VARCHAR,
    committee_name VARCHAR,
    role VARCHAR,
    PRIMARY KEY (bioguide_id, committee_name)
);

CREATE TABLE IF NOT EXISTS kalshi_market_mapping (
    market_id VARCHAR PRIMARY KEY,
    ticker VARCHAR,
    district_id VARCHAR,
    chamber VARCHAR,
    market_title VARCHAR,
    raw_title VARCHAR,
    raw_ticker VARCHAR,
    matched_district_id VARCHAR,
    match_confidence VARCHAR,
    yes_price DOUBLE,
    no_price DOUBLE,
    volume_24h DOUBLE,
    open_interest DOUBLE,
    last_price_change DOUBLE,
    fetched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS polymarket_market_mapping (
    condition_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    outcome VARCHAR,
    district_id VARCHAR,
    chamber VARCHAR,
    party VARCHAR,
    yes_price DOUBLE,
    volume_total DOUBLE,
    volume_24h DOUBLE,
    last_updated TIMESTAMP,
    match_confidence VARCHAR
);

CREATE TABLE IF NOT EXISTS incumbent_status_2026 (
    district_id VARCHAR PRIMARY KEY,
    incumbent_name VARCHAR,
    party VARCHAR,
    status VARCHAR,
    reason VARCHAR,
    source_name VARCHAR,
    source_url VARCHAR,
    observed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS twoseventy_house_context (
    district_id VARCHAR PRIMARY KEY,
    incumbent_name VARCHAR,
    incumbent_party VARCHAR,
    member_since INTEGER,
    term_label VARCHAR,
    house_margin_2024 DOUBLE,
    presidential_margin_2024 DOUBLE,
    kalshi_house_price DOUBLE,
    race_note TEXT,
    context_group VARCHAR,
    source_url VARCHAR,
    fetched_at TIMESTAMP
);

ALTER TABLE chamber_forecasts ADD COLUMN IF NOT EXISTS polymarket_price DOUBLE;
ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS type_scores JSON;
ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS source_count INTEGER DEFAULT 0;
ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS credibility_weighted_salience DOUBLE;
ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS affected_states VARCHAR[];
ALTER TABLE event_tokens ADD COLUMN IF NOT EXISTS is_national_signal BOOLEAN DEFAULT FALSE;
ALTER TABLE race_web_articles ADD COLUMN IF NOT EXISTS embedding BLOB;
ALTER TABLE media_event_articles ADD COLUMN IF NOT EXISTS embedding BLOB;

CREATE TABLE IF NOT EXISTS scraper_runs (
    source_name VARCHAR,
    run_at TIMESTAMP,
    status VARCHAR,
    rows_fetched INTEGER,
    output_path VARCHAR,
    error TEXT
);

CREATE TABLE IF NOT EXISTS retrain_jobs (
    job_id VARCHAR PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    status VARCHAR,
    progress DOUBLE,
    message TEXT
);

CREATE TABLE IF NOT EXISTS data_quality (
    quality_date DATE,
    item_type VARCHAR,
    item_key VARCHAR,
    metric VARCHAR,
    value INTEGER,
    notes TEXT,
    PRIMARY KEY (quality_date, item_type, item_key, metric)
);

CREATE TABLE IF NOT EXISTS politician_integrity_signals (
    district_id VARCHAR,
    candidate_name VARCHAR,
    signal_date DATE,
    perceived_dishonesty_score DOUBLE,
    article_count INTEGER,
    evidence JSON,
    source_table VARCHAR,
    PRIMARY KEY (district_id, candidate_name, signal_date, source_table)
);
