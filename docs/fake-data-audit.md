# TAPESTRY Fake Data Audit

This file tracks every known place where TAPESTRY still uses generated, placeholder, seeded, or model-shell data instead of verified live data.

## Frontend

- `App.jsx` `fallbackDistrictDetail`
  - Creates a district forecast when the API does not return one.
  - Status: still present as a graceful UI fallback, but now labels missing candidate data as not ingested instead of inventing a defending incumbent.

- `App.jsx` `competitiveOverrides`
  - Hardcoded race ratings, probabilities, candidate fields, and district rows for the competitive-state demo layer.
  - Status: partially source-shaped but not authoritative. This should be replaced by backend district forecasts plus an official House roster table.

- `App.jsx` `stateData` base generation
  - Generates three sample districts per state from state lean.
  - Status: still used to keep the map clickable, but officeholder text now says roster pending instead of fake incumbents like “California incumbent.”

- `App.jsx` `representativeProfiles`
  - Hand-seeded profile rows for a small set of competitive House districts.
  - Status: still temporary for political positioning text, but official current incumbent names/parties now come from the backend `house_roster` ingest when a live forecast row is available.

- `App.jsx` `enrichDistrict`
  - Generates bios, committees, local conditions, fundraising, voting-record rows, floor statements, and challenger placeholders.
  - Status: still generated. It now marks unverified challenger/member fields as pending ingest instead of presenting invented names.

- `App.jsx` `localNews` fallback rows
  - Generates local-looking fallback headlines if the news API is empty.
  - Status: still present to avoid a blank sidebar, but should be removed once scraper coverage is reliable.

- `App.jsx` `countyIntel`
  - Generates county-level notes from county name and state FIPS.
  - Status: fake/model-shell. User has asked to remove the county tier if it duplicates districts.

- `App.jsx` `RaceRow`, `DistrictPanel`, `RightPanel`, `PoliticianOverlay`
  - Previously displayed placeholder strings as if they were incumbents/challengers.
  - Status: patched to display “Roster not ingested” when a real `Name (D/R)` is not present.

## Backend

- `tapestry-backend/model/features.py`
  - Builds synthetic district features for a limited district set.
  - Status: model scaffold only. Needs Census/ACS, FEC, election-results, Cook PVI, and district demographic ingest.

- `tapestry-backend/model/retrainer.py`
  - Creates forecasts for the synthetic feature rows and names candidates as `{district_id} D/R nominee`.
  - Status: model scaffold only. Needs candidate roster and challenger mapping before frontend should treat `leading_candidate` as real.

- `tapestry-backend/data/historical/load_historical.py`
  - Idempotent historical loader placeholder.
  - Status: not a real MIT Election Lab/FiveThirtyEight/Cook historical archive import yet.

- `tapestry-backend/scrapers/opensecrets_scraper.py`
  - Upserts donor transparency rows using seeded amounts and an `INCUMBENTS` dict for a small district set.
  - Status: placeholder until real OpenSecrets/FEC candidate IDs and API pulls are wired.

- `tapestry-backend/scrapers/local_news_scraper.py`
  - Real RSS/GDELT/Google News pathways exist, but fallback seed rows still exist for failure cases.
  - Status: mixed. Needs visible source provenance in API/UI and should avoid example.com seed rows in production mode.

- `tapestry-backend/scrapers/ballotpedia_scraper.py`
  - Current extraction is minimal and not a full candidate/challenger roster.
  - Status: scaffold.

- `tapestry-backend/scrapers/fec_scraper.py`
  - Competitive-district fundraising rows are seeded/scaffolded.
  - Status: scaffold until FEC candidate IDs and committee mappings are loaded.

- `tapestry-backend/scrapers/cook_scraper.py`
  - Cook ratings are seeded/scaffolded.
  - Status: scaffold unless wired to an owned licensed source/export.

- `tapestry-backend/scrapers/kalshi_scraper.py`
  - Uses mock control prices when no authenticated market response is available.
  - Status: mixed. Needs authenticated Kalshi client and market mapping.

## Required Next Data Fixes

1. Add a `candidate_roster_2026` table for declared challengers from Ballotpedia/FEC.
2. Join `district_forecasts` to `candidate_roster_2026` before returning API responses.
3. Expand forecasts from the current competitive subset to all 435 House districts.
4. Remove county mode or clearly separate it from congressional district mode with real county-level election returns.
5. Remove local news fallback cards once state-by-state scraping has adequate coverage.

## Fixed Since This Audit Started

- Added `house_roster` table from official House Clerk XML.
- Added `HouseRosterScraper`.
- Ingested 435 current voting House members from Clerk publish date April 22, 2026.
- Joined roster fields into `DistrictForecast` API responses.
- Added `/api/districts/roster` and `/api/districts/{district_id}/roster`.
- Loaded user-provided MIT Election Lab/Dataverse `1976-2024-house.tab` into `election_results`.
