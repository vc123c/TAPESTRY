import React, { useEffect, useMemo, useRef, useState } from "https://esm.sh/react@18.3.1";
import { createRoot } from "https://esm.sh/react-dom@18.3.1/client";
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup
} from "https://esm.sh/react-simple-maps@3.0.0";
import { scaleOrdinal } from "https://esm.sh/d3-scale@4.0.2";
import { geoArea } from "https://esm.sh/d3-geo@3.1.1";
import {
  getChambers,
  getConflicts,
  getDistrict,
  getDistrictNews,
  getDistrictSummaries,
  getDistrictTransparency,
  getEvents,
  getHouseRoster,
  getKalshiGaps,
  getMarketGaps,
  getMorningBrief,
  getNational,
  getNewsReadiness,
  getStates
} from "./src/api.js";

const h = React.createElement;
const STATES_URL = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json";
const DISTRICTS_URL = "https://cdn.jsdelivr.net/gh/civic-interconnect/civic-data-boundaries-us-cd118@main/data-out/national/cd118_us.geojson";
const ELECTION_DAY = new Date("2026-11-03T00:00:00");
const ACCENT = "#7c3aed";
const INFO_READY_THRESHOLD = 3;
const WARMUP_FACTS = [
  "The House has 435 voting seats, so 218 controls the chamber.",
  "Nebraska and Maine split presidential electors, but House races there are still single-district fights.",
  "A one-point national swing does not move every district equally. That is why the chamber forecast uses simulation.",
  "DuckDB is doing the heavy lifting for TAPESTRY's local analytics and forecast tables.",
  "Polymarket is shown as a market check, not as the source of the model probability.",
  "Some safe seats barely move in a wave year. Competitive seats move much more with the national environment."
];

const ratingColors = {
  "Solid D": "#1d4ed8",
  "Likely D": "#3b82f6",
  "Lean D": "#93c5fd",
  "Toss-Up": "#a855f7",
  "Lean R": "#fca5a5",
  "Likely R": "#f87171",
  "Solid R": "#dc2626"
};
const colorForRating = scaleOrdinal(Object.keys(ratingColors), Object.values(ratingColors));

function ratingFromProb(prob) {
  if (prob === null || prob === undefined || Number.isNaN(Number(prob))) return "Insufficient data";
  if (prob >= 0.85) return "Solid D";
  if (prob >= 0.70) return "Likely D";
  if (prob >= 0.55) return "Lean D";
  if (prob >= 0.45) return "Toss-Up";
  if (prob >= 0.30) return "Lean R";
  if (prob >= 0.15) return "Likely R";
  return "Solid R";
}

function colorFromProb(prob) {
  const rating = ratingFromProb(prob);
  return rating === "Insufficient data" ? "#2a2d3a" : colorForRating(rating);
}

function stateDistricts(districts, abbr) {
  return (districts || []).filter((d) => d.district_id?.startsWith(`${abbr}-`));
}

function geoidToDistrictId(props = {}) {
  const stateFips = String(props.STATEFP20 || props.STATEFP || props.STATEFP10 || props.GEOID20?.slice(0, 2) || props.GEOID?.slice(0, 2) || "").padStart(2, "0");
  const districtCode = String(props.CD118FP || props.CD116FP || props.CD115FP || props.GEOID20?.slice(2) || props.GEOID?.slice(2) || "").padStart(2, "0");
  if (!/^\d+$/.test(districtCode)) return null;
  const abbr = Object.values(stateData).find((s) => s.fipsCode === stateFips)?.abbreviation;
  if (!abbr) return null;
  const num = Number(districtCode);
  return num === 0 ? `${abbr}-AL` : `${abbr}-${String(num).padStart(2, "0")}`;
}

function stateProbability(districts, abbr) {
  const rows = stateDistricts(districts, abbr).filter((d) => d.win_probability_d !== null && d.win_probability_d !== undefined);
  if (!rows.length) return null;
  return rows.reduce((best, row) => Math.abs(row.win_probability_d - 0.5) > Math.abs(best.win_probability_d - 0.5) ? row : best, rows[0]).win_probability_d;
}

function chamberByName(chambers, chamber) {
  return (chambers || []).find((c) => c.chamber === chamber);
}

function fmtPct(value) {
  return value === null || value === undefined ? "-" : `${Math.round(Number(value) * 100)}%`;
}

function fmtMargin(value) {
  if (value === null || value === undefined) return "-";
  const n = Number(value);
  if (n === 0) return "EVEN";
  return `${n > 0 ? "D" : "R"}+${Math.abs(n).toFixed(1)}`;
}

function money(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "pending";
  const n = Number(value);
  if (n >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  return `$${Math.round(n / 1000)}K`;
}

function eventTypeLabel(type) {
  const key = String(type || "news_signal").toLowerCase();
  const labels = {
    economic_shock: "Economy",
    conflict_escalation: "Conflict",
    conflict: "Conflict",
    scandal_corruption: "Corruption",
    scandal_personal: "Personal Conduct",
    social_event: "Social",
    social_unrest: "Social",
    anti_establishment: "Anti-Establishment",
    policy_reversal: "Policy Reversal",
    campaign_event: "Campaign",
    healthcare: "Healthcare",
    immigration: "Immigration",
    immigrations: "Immigration",
    technology: "Technology",
    news_signal: "News",
  };
  return labels[key] || key.replaceAll("_", " ").replace(/\b\w/g, (c) => c.toUpperCase());
}

function eventMeta(event) {
  const pieces = [];
  if (event?.source_count) pieces.push(`${event.source_count} sources`);
  if (event?.article_count) pieces.push(`${event.article_count} articles`);
  if (event?.event_date) pieces.push(String(event.event_date).slice(0, 10));
  return pieces.join(" · ");
}

function asUpperList(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.map((item) => String(item).toUpperCase());
  return [String(value).toUpperCase()];
}

function eventMatchesScope(event, { districtId, stateAbbr, includeNational = false } = {}) {
  const districts = asUpperList(event?.affected_districts);
  const states = asUpperList(event?.affected_states);
  const district = districtId ? String(districtId).toUpperCase() : null;
  const state = stateAbbr ? String(stateAbbr).toUpperCase() : district?.split("-")[0];
  const isNational = Boolean(event?.is_national_signal) || (!districts.length && !states.length);
  if (district && districts.includes(district)) return true;
  if (state && states.includes(state)) return true;
  return includeNational && isNational;
}

function scopedEventList(events, scope, limit = 7, { fallbackNational = false } = {}) {
  const rows = events || [];
  const local = rows.filter((event) => eventMatchesScope(event, scope));
  if (local.length) return local.slice(0, limit);
  if (!fallbackNational) return [];
  return rows.filter((event) => eventMatchesScope(event, { ...scope, includeNational: true })).slice(0, limit);
}

function isVerifiedOfficeholder(name = "") {
  return /\([DR]\)/.test(name) && !/generic|proxy|pending|incumbent|challenger seat|open seat/i.test(name);
}

function displayOfficeholder(district) {
  if (!district) return "Roster not yet available";
  if (district.liveDistrict?.incumbent_name) {
    return `${district.liveDistrict.incumbent_name} (${district.liveDistrict.incumbent_party || "?"})`;
  }
  return isVerifiedOfficeholder(district.incumbent) ? district.incumbent : "Roster not yet available";
}

function displayOfficeholderName(district) {
  return displayOfficeholder(district).replace(/\s\([DR]\)/, "");
}

function probBarRace(record) {
  const d = Math.round((record?.win_probability_d ?? 0.5) * 100);
  return [d, 100 - d];
}

function districtFromApiRow(row, state) {
  const [dPct, rPct] = probBarRace(row);
  return {
    districtLabel: row?.district_id || "District pending",
    rating: ratingFromProb(row?.win_probability_d),
    dPct,
    rPct,
    incumbent: row?.incumbent_name ? `${row.incumbent_name} (${row.incumbent_party || "?"})` : "Roster not yet available",
    party: row?.incumbent_party || null,
    homeBase: state ? `${state.stateName}` : "",
    liveDistrict: row || null,
  };
}

function ratingMoveText(move) {
  if (!move) return "";
  if (typeof move === "string") return move;
  if (move.new_probability !== undefined && move.uncertainty !== undefined) {
    return `${move.district_id || move.district || "Race"}: D ${Math.round(Number(move.new_probability) * 100)}% +/-${Number(move.uncertainty).toFixed(1)}pts`;
  }
  if (move.old_probability !== undefined && move.new_probability !== undefined) {
    const delta = (Number(move.new_probability) - Number(move.old_probability)) * 100;
    const dir = delta >= 0 ? "toward D" : "toward R";
    return `${move.district_id || move.district || "Race"} moved ${Math.abs(delta).toFixed(1)} pts ${dir}`;
  }
  return move.text || move.description || `${move.district || move.district_id || "Race"} moved ${move.magnitude ?? ""} pts ${move.direction ?? ""}`.trim();
}

const fipsMeta = [
  ["AL", "Alabama", "01", [-86.8, 32.8], "Solid R"], ["AK", "Alaska", "02", [-150, 64], "Likely R"],
  ["AZ", "Arizona", "04", [-111.7, 34.2], "Lean R"], ["AR", "Arkansas", "05", [-92.3, 34.9], "Solid R"],
  ["CA", "California", "06", [-119.5, 37.2], "Solid D"], ["CO", "Colorado", "08", [-105.6, 39.0], "Likely D"],
  ["CT", "Connecticut", "09", [-72.7, 41.6], "Solid D"], ["DE", "Delaware", "10", [-75.5, 39.0], "Solid D"],
  ["FL", "Florida", "12", [-82.6, 28.4], "Likely R"], ["GA", "Georgia", "13", [-83.5, 32.7], "Toss-Up"],
  ["HI", "Hawaii", "15", [-157.5, 20.8], "Solid D"], ["ID", "Idaho", "16", [-114.6, 44.2], "Solid R"],
  ["IL", "Illinois", "17", [-89.3, 40.1], "Solid D"], ["IN", "Indiana", "18", [-86.2, 40.0], "Likely R"],
  ["IA", "Iowa", "19", [-93.5, 42.1], "Likely R"], ["KS", "Kansas", "20", [-98.2, 38.5], "Solid R"],
  ["KY", "Kentucky", "21", [-84.9, 37.8], "Solid R"], ["LA", "Louisiana", "22", [-91.9, 30.9], "Solid R"],
  ["ME", "Maine", "23", [-69.2, 45.3], "Lean D"], ["MD", "Maryland", "24", [-76.7, 39.0], "Solid D"],
  ["MA", "Massachusetts", "25", [-71.8, 42.2], "Solid D"], ["MI", "Michigan", "26", [-84.6, 44.3], "Lean D"],
  ["MN", "Minnesota", "27", [-94.6, 46.3], "Likely D"], ["MS", "Mississippi", "28", [-89.7, 32.7], "Solid R"],
  ["MO", "Missouri", "29", [-92.5, 38.4], "Likely R"], ["MT", "Montana", "30", [-110.5, 46.9], "Lean R"],
  ["NE", "Nebraska", "31", [-99.8, 41.5], "Solid R"], ["NV", "Nevada", "32", [-117.0, 39.0], "Toss-Up"],
  ["NH", "New Hampshire", "33", [-71.6, 43.7], "Lean D"], ["NJ", "New Jersey", "34", [-74.5, 40.1], "Solid D"],
  ["NM", "New Mexico", "35", [-106.1, 34.4], "Likely D"], ["NY", "New York", "36", [-75.5, 42.9], "Solid D"],
  ["NC", "North Carolina", "37", [-79.3, 35.5], "Toss-Up"], ["ND", "North Dakota", "38", [-100.5, 47.5], "Solid R"],
  ["OH", "Ohio", "39", [-82.8, 40.4], "Lean R"], ["OK", "Oklahoma", "40", [-97.5, 35.6], "Solid R"],
  ["OR", "Oregon", "41", [-120.5, 44.0], "Likely D"], ["PA", "Pennsylvania", "42", [-77.8, 41.0], "Toss-Up"],
  ["RI", "Rhode Island", "44", [-71.5, 41.7], "Solid D"], ["SC", "South Carolina", "45", [-80.9, 33.9], "Likely R"],
  ["SD", "South Dakota", "46", [-100.2, 44.4], "Solid R"], ["TN", "Tennessee", "47", [-86.3, 35.8], "Solid R"],
  ["TX", "Texas", "48", [-99.3, 31.3], "Likely R"], ["UT", "Utah", "49", [-111.7, 39.3], "Solid R"],
  ["VT", "Vermont", "50", [-72.7, 44.0], "Solid D"], ["VA", "Virginia", "51", [-78.6, 37.7], "Lean D"],
  ["WA", "Washington", "53", [-120.6, 47.4], "Solid D"], ["WV", "West Virginia", "54", [-80.6, 38.6], "Solid R"],
  ["WI", "Wisconsin", "55", [-89.9, 44.7], "Toss-Up"], ["WY", "Wyoming", "56", [-107.6, 43.0], "Solid R"]
];

const stateData = Object.fromEntries(fipsMeta.map(([abbr, stateName, fipsCode, center]) => [
  abbr,
  { stateName, fipsCode, abbreviation: abbr, center }
]));

const STATE_DISTRICT_COUNT = {
  AL: 7, AK: 1, AZ: 9, AR: 4, CA: 52, CO: 8, CT: 5, DE: 1, FL: 28, GA: 14,
  HI: 2, ID: 2, IL: 17, IN: 9, IA: 4, KS: 4, KY: 6, LA: 6, ME: 2, MD: 8,
  MA: 9, MI: 13, MN: 8, MS: 4, MO: 8, MT: 2, NE: 3, NV: 4, NH: 2, NJ: 12,
  NM: 3, NY: 26, NC: 14, ND: 1, OH: 15, OK: 5, OR: 6, PA: 17, RI: 2, SC: 7,
  SD: 1, TN: 9, TX: 38, UT: 4, VT: 1, VA: 11, WA: 10, WV: 2, WI: 8, WY: 1,
};

function daysToElection() {
  return Math.max(0, Math.ceil((ELECTION_DAY - new Date()) / 86400000));
}

function clsRating(rating) {
  return rating.replace(/\s|\+/g, "-").toLowerCase();
}

function ProbabilityBar({ dPct, rPct, large = false }) {
  return h("div", { className: `prob-wrap ${large ? "large" : ""}` },
    h("span", null, `D ${dPct}%`),
    h("div", { className: "prob-bar" },
      h("div", { className: "prob-d", style: { width: `${dPct}%` } }),
      h("div", { className: "prob-r", style: { width: `${rPct}%` } })
    ),
    h("span", null, `R ${rPct}%`)
  );
}

function Section({ title, children }) {
  return h("section", { className: "panel-section" }, h("h3", null, title), children);
}

function RaceRow({ race }) {
  const isDistrictRace = /^[A-Z]{2}-\d{1,2}$/.test(race[0]);
  const name = isDistrictRace && !isVerifiedOfficeholder(race[4]) ? "Roster not yet available" : (race[4] || race[0]);
  const meta = race[4] ? race[0] : "";
  return h("div", { className: "race-card" },
    h("div", { className: "race-top" }, h("strong", null, name), h("em", { className: clsRating(race[1]) }, race[1])),
    meta && h("small", { className: "race-meta" }, meta),
    h(ProbabilityBar, { dPct: race[2], rPct: race[3] })
  );
}

function SkeletonPanel() {
  return h("div", null,
    h("h2", null, "LOADING LIVE DATA"),
    [1, 2, 3, 4].map((i) => h("div", { className: "skeleton-line", key: i }))
  );
}

function WarmupPanel({ fact }) {
  return h("div", null,
    h("h2", null, "WAKING THE BACKEND"),
    h("p", { className: "muted tight" }, "Render is spinning the API up. Live forecasts and news should appear shortly."),
    fact && h("div", { className: "warmup-fact-card" },
      h("strong", null, "While you wait"),
      h("span", null, fact)
    ),
    [1, 2, 3, 4].map((i) => h("div", { className: "skeleton-line", key: i }))
  );
}

function FactorBars({ factors }) {
  const rows = Object.entries(factors || {})
    .map(([k, v]) => [k, Number(v)])
    .filter(([, v]) => !Number.isNaN(v))
    .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
    .slice(0, 6);
  if (!rows.length) return h("p", { className: "muted" }, "Factor attribution unavailable");
  const max = Math.max(...rows.map(([, v]) => Math.abs(v)), 1);
  return h("div", { className: "factor-bars" }, rows.map(([k, v]) =>
    h("div", { className: "factor-row", key: k },
      h("span", null, k.replaceAll("_", " ")),
      h("div", { className: "factor-track" }, h("i", { className: v >= 0 ? "d" : "r", style: { width: `${Math.max(8, Math.abs(v) / max * 100)}%` } })),
      h("b", null, `${v >= 0 ? "+" : ""}${v.toFixed(1)}`)
    )
  ));
}

function NewsCards({ newsState }) {
  if (!newsState) return h("div", null, [1, 2, 3].map((i) => h("div", { className: "news-card skeleton-card", key: i })));
  const articles = newsState.articles || [];
  if (!articles.length) return h("p", { className: "muted" }, "Race coverage loading");
  return h("div", null, articles.map((n) => h("div", { className: "news-card live-news", key: n.url + n.headline, title: n.headline },
    h("div", null, h("b", null, n.source_name), h("small", { className: "source-badge" }, n.source_type)),
    h("a", { href: n.url, target: "_blank", rel: "noreferrer", title: n.headline }, n.headline),
    h("div", { className: "news-meta" }, h("span", { className: `sentiment-dot ${n.sentiment?.toLowerCase()}` }), h("small", null, n.time_ago), h("em", null, n.sentiment)),
    h("div", { className: "tag-list" }, (n.topic_tags || []).map((tag) => h("span", { key: tag }, tag)))
  )));
}

function chamberMarketGap(marketGaps, chamber) {
  return (marketGaps?.chamber_gaps || []).find((gap) => gap.chamber === chamber);
}

function ProvenanceRow({ items = [] }) {
  if (!items.length) return null;
  return h("div", { className: "provenance-row" },
    items.map((item) => h("span", { className: "provenance-chip", key: item.label }, `${item.label}: ${item.value}`))
  );
}

function NationalPanel({ chambers, districts, loading, marketGaps, national }) {
  if (loading) return h(SkeletonPanel);
  const senate = chamberByName(chambers, "senate");
  const house = chamberByName(chambers, "house");
  if (!senate && !house) {
    return h("div", null,
      h("h2", null, "NATIONAL OVERVIEW"),
      h(Section, { title: "CHAMBER CONTROL" }, h("p", { className: "muted tight" }, "Forecast data is still warming up.")),
      h(Section, { title: "MARKET PRICES" }, h("p", { className: "muted tight" }, "Market and chamber data will populate as soon as the backend responds.")),
      h(Section, { title: "TOP COMPETITIVE RACES" }, h("p", { className: "muted tight" }, "District forecasts are pending."))
    );
  }
  const senateD = Math.round((senate?.d_control_probability ?? 0) * 100);
  const houseD = Math.round((house?.d_control_probability ?? 0) * 100);
  const housePolyGap = house?.polymarket_price == null ? null : Math.round(Math.abs((house.d_control_probability - house.polymarket_price) * 100));
  const senatePolyGap = senate?.polymarket_price == null ? null : Math.round(Math.abs((senate.d_control_probability - senate.polymarket_price) * 100));
  const houseGap = chamberMarketGap(marketGaps, "house");
  const senateGap = chamberMarketGap(marketGaps, "senate");
  const gapClass = (gap) => Number(gap?.largest_gap || 0) > 0.15 ? "gap-red" : Number(gap?.largest_gap || 0) > 0.05 ? "gap-amber" : "";
  const close = [...(districts || [])]
    .filter((d) => d.win_probability_d >= 0.35 && d.win_probability_d <= 0.65)
    .sort((a, b) => Math.abs(a.win_probability_d - 0.5) - Math.abs(b.win_probability_d - 0.5))
    .slice(0, 5);
  return h("div", null,
    h("h2", null, "NATIONAL OVERVIEW"),
    h(Section, { title: "CHAMBER CONTROL" },
      h(ProvenanceRow, { items: [
        { label: "MODEL", value: "TAPESTRY simulation" },
        { label: "INPUTS", value: "approval, ballot, district factors" }
      ] }),
      h("div", { className: "control-row" }, h("b", null, "SENATE"), h(ProbabilityBar, { dPct: senateD, rPct: 100 - senateD })),
      h("div", { className: "control-row" }, h("b", null, "HOUSE"), h(ProbabilityBar, { dPct: houseD, rPct: 100 - houseD }))
    ),
    h(Section, { title: "MARKET PRICES" },
      h(ProvenanceRow, { items: [
        { label: "MARKET", value: "Polymarket and Kalshi" },
        { label: "LAST LIVE", value: national?.factor_date || "pending" }
      ] }),
      h("div", { className: "market-label" }, "Prediction markets - live where available"),
      h("div", { className: "market-row" }, h("span", null, "Senate Control - D"), h("b", null, senate?.kalshi_price == null ? "-" : `$${Number(senate.kalshi_price).toFixed(2)}`)),
      h("div", { className: "market-row" }, h("span", null, "House Control - D"), h("b", null, house?.kalshi_price == null ? "-" : `$${Number(house.kalshi_price).toFixed(2)}`)),
      h("div", { className: "market-row" }, h("span", null, "Polymarket Senate - D"), h("b", null, senate?.polymarket_price == null ? "-" : fmtPct(senate.polymarket_price))),
      h("div", { className: "market-row" }, h("span", null, "Polymarket House - D"), h("b", { className: housePolyGap > 20 ? "gap-red" : housePolyGap > 10 ? "gap-amber" : "" }, house?.polymarket_price == null ? "-" : `${fmtPct(house.polymarket_price)}${housePolyGap ? ` (${housePolyGap}pt gap)` : ""}`)),
      senateGap && h("p", { className: `muted tight ${gapClass(senateGap)}` }, `${Math.round(Number(senateGap.largest_gap || 0) * 100)}pt Senate gap vs model`),
      houseGap && h("p", { className: `muted tight ${gapClass(houseGap)}` }, `${Math.round(Number(houseGap.largest_gap || 0) * 100)}pt House gap vs model`),
      senatePolyGap > 10 && h("p", { className: senatePolyGap > 20 ? "gap-alert" : "muted tight" }, `Senate market gap: ${senatePolyGap} pts`),
      housePolyGap > 10 && h("p", { className: housePolyGap > 20 ? "gap-alert" : "muted tight" }, `House market gap: TAPESTRY ${fmtPct(house?.d_control_probability)} vs Polymarket ${fmtPct(house?.polymarket_price)}`),
      h("small", null, `Updated: ${new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`)
    ),
    h(Section, { title: "TOP COMPETITIVE RACES" },
      h(ProvenanceRow, { items: [
        { label: "MODEL", value: "closest district forecasts" },
        { label: "UNCERTAINTY", value: "shown for tight races" }
      ] }),
      close.length ? close.map((d) => {
      const [dp, rp] = probBarRace(d);
      const showUncertainty = Math.abs(Number(d.projected_margin ?? 999)) < 8 && d.uncertainty !== null && d.uncertainty !== undefined;
      return h("div", { className: "race-card", key: d.district_id },
        h("div", { className: "race-top" }, h("strong", null, d.district_id), showUncertainty
          ? h("em", { style: { fontSize: "10px", color: "#f59e0b", fontWeight: 600, padding: "1px 5px", border: "1px solid #f59e0b", borderRadius: "2px" } }, `+/-${Number(d.uncertainty).toFixed(1)}pts`)
          : h("em", { className: clsRating(ratingFromProb(d.win_probability_d)) }, ratingFromProb(d.win_probability_d))),
        h("p", { className: "muted tight" }, d.statement),
        h(ProbabilityBar, { dPct: dp, rPct: rp }),
        h("small", null, `+/-${Number(d.uncertainty ?? 0).toFixed(1)} pts`)
      );
    }) : h("p", { className: "muted" }, "Model initializing - run overnight.py to generate forecasts."))
  );
}

function StatePanel({ state, districts }) {
  const races = stateDistricts(districts, state.abbreviation)
    .sort((a, b) => Math.abs(a.win_probability_d - 0.5) - Math.abs(b.win_probability_d - 0.5))
    .slice(0, 8)
    .map((row) => {
      const [dPct, rPct] = probBarRace(row);
      return [row.district_id, ratingFromProb(row.win_probability_d), dPct, rPct, row.incumbent_name ? `${row.incumbent_name} (${row.incumbent_party || "?"})` : "Roster not yet available"];
    });
  return h("div", null,
    h("h2", null, `${state.stateName} - ${state.abbreviation}`),
    h(Section, { title: "STATE RACES" }, races.length ? races.map((r) => h(RaceRow, { key: r[0], race: r })) : h("p", { className: "muted" }, "Model initializing")),
    h(Section, { title: "CURRENT POLITICAL ROSTER" },
      h("div", { className: "detail-row" }, h("span", null, "District forecasts"), h("b", null, races.length ? `${races.length} loaded` : "-")),
      h("div", { className: "detail-row" }, h("span", null, "Most watched district"), h("b", null, races[0]?.[0] || "-"))
    )
  );
}

function DistrictPanel({ state, district, detail, newsState, events, onMoreInfo }) {
  const live = detail || district.liveDistrict || {};
  const districtId = live.district_id || district.districtLabel;
  const prob = live.win_probability_d ?? district.dPct / 100;
  const dLivePct = Math.round(prob * 100);
  const rLivePct = 100 - dLivePct;
  const liveRating = ratingFromProb(prob);
  const verifiedOfficeholder = Boolean(district.liveDistrict?.incumbent_name) || isVerifiedOfficeholder(district.incumbent);
  const incumbentName = displayOfficeholderName(district);
  const candidates = live.candidates_2026 || [];
  const challengers = live.major_challengers_2026 || candidates.filter((c) => !c.is_incumbent && c.is_major_challenger !== false && c.active_2026 !== false);
  const committees = live.incumbent_committees || [];
  const visibleCandidateNames = new Set([live.incumbent_name, incumbentName, ...challengers.map((c) => c.candidate_name)].filter(Boolean).map((name) => name.toLowerCase()));
  const fundraising = (live.fundraising || []).filter((f) => visibleCandidateNames.has(String(f.candidate_name || "").toLowerCase()));
  const features = live.district_features || {};
  const integritySignals = live.integrity_signals || [];
  const incumbentStatus = live.incumbent_status_2026;
  const raceIntel = live.race_intelligence || {};
  const twoSeventy = live.twoseventy_context || raceIntel.twoseventy;
  const activeEvents = scopedEventList(events, { districtId, stateAbbr: state?.abbreviation }, 3);
  const dRaised = fundraising.filter((f) => f.party === "D").reduce((sum, f) => sum + Number(f.total_receipts || 0), 0);
  const rRaised = fundraising.filter((f) => f.party === "R").reduce((sum, f) => sum + Number(f.total_receipts || 0), 0);
  const dName = fundraising.find((f) => f.party === "D")?.candidate_name || "D candidate";
  const rName = fundraising.find((f) => f.party === "R")?.candidate_name || "R candidate";
  const maxRaised = Math.max(dRaised, rRaised, 1);
  const districtStatement = live.statement || (
    live.leading_candidate && live.projected_margin != null
      ? `${live.leading_candidate} leads by ${Math.abs(Number(live.projected_margin)).toFixed(1)} pts (+/-${Number(live.uncertainty || 0).toFixed(1)})`
      : null
  );
  return h("div", null,
    h("h2", null, `${districtId} - ${incumbentName}`),
    h("p", { className: "subhead" }, verifiedOfficeholder ? `${live.incumbent_party || district.party || "Party pending"} - ${state?.stateName || district.homeBase || districtId}${incumbentStatus ? " - open-seat watch" : ""}` : "Roster not yet available"),
    h(Section, { title: "RACE STATUS" },
      h(ProvenanceRow, { items: [
        { label: "MODEL", value: "district forecast" },
        { label: "MARKET", value: live.kalshi_price == null ? "none" : "Kalshi cross-check" }
      ] }),
      districtStatement && h("p", { className: "race-statement" }, districtStatement),
      h("div", { className: `badge ${clsRating(liveRating)}` }, liveRating),
      h(ProbabilityBar, { dPct: dLivePct, rPct: rLivePct, large: true }),
      live.uncertainty !== null && live.uncertainty !== undefined && h("div", { style: { fontSize: "11px", color: "#64748b", marginTop: "3px", letterSpacing: "0.02em" } }, `+/-${Number(live.uncertainty).toFixed(1)} point margin of uncertainty`),
      h("div", { className: "detail-row" }, h("span", null, "Projected margin"), h("b", null, fmtMargin(live.projected_margin))),
      h("div", { className: "detail-row" }, h("span", null, "Uncertainty"), h("b", null, `+/-${Number(live.uncertainty ?? 0).toFixed(1)} pts`)),
      twoSeventy?.house_margin_2024 != null && h("div", { className: "detail-row" }, h("span", null, "270toWin House margin"), h("b", null, `${Number(twoSeventy.house_margin_2024).toFixed(1)}%`)),
      twoSeventy?.presidential_margin_2024 != null && h("div", { className: "detail-row" }, h("span", null, "270toWin President"), h("b", null, `${Number(twoSeventy.presidential_margin_2024).toFixed(1)}%`)),
      twoSeventy?.kalshi_house_price != null && h("div", { className: "detail-row" }, h("span", null, "270toWin/Kalshi"), h("b", null, fmtPct(twoSeventy.kalshi_house_price))),
      h("div", { className: "detail-row" }, h("span", null, "Kalshi price"), h("b", null, live.kalshi_price == null ? "unavailable" : fmtPct(live.kalshi_price))),
      h("div", { className: "detail-row" }, h("span", null, "Model price"), h("b", null, live.model_implied_price == null ? fmtPct(prob) : fmtPct(live.model_implied_price))),
      raceIntel.status_text && h("div", { className: "detail-row" }, h("span", null, "2026 status"), h("b", null, raceIntel.status_text)),
      live.kalshi_gap_flag && h("p", { className: "gap-alert" }, `Kalshi disagrees by ${Number(live.kalshi_gap || 0).toFixed(1)} pts`),
      live.gap_explanation && h("p", { className: "muted tight" }, live.gap_explanation)
    ),
    h(Section, { title: "FACTOR ATTRIBUTION" }, h(FactorBars, { factors: live.factor_attribution })),
    live.narrative && h(Section, { title: "NARRATIVE" }, h("p", { className: "narrative" }, live.narrative)),
    h(Section, { title: "THE CANDIDATES" },
      h("div", { className: "detail-row" }, h("span", null, "Incumbent"), h("b", null, verifiedOfficeholder ? `${live.incumbent_name || incumbentName} (${live.incumbent_party || district.party || "?"})` : "Roster not yet available")),
      incumbentStatus && h("div", { className: "detail-row" }, h("span", null, "Incumbent 2026"), h("b", null, `${incumbentStatus.status?.replaceAll("_", " ") || "status pending"} - ${incumbentStatus.reason?.replaceAll("_", " ") || incumbentStatus.source_name || "source pending"}`)),
      twoSeventy?.race_note && h("div", { className: "detail-row" }, h("span", null, "270toWin note"), h("b", null, twoSeventy.race_note)),
      twoSeventy?.term_label && h("div", { className: "detail-row" }, h("span", null, "Tenure"), h("b", null, `${twoSeventy.term_label}${twoSeventy.member_since ? ` since ${twoSeventy.member_since}` : ""}`)),
      committees.length > 0 && h("div", { className: "detail-row" }, h("span", null, "Committees"), h("b", null, committees.map((c) => c.committee_name || c).slice(0, 2).join(", "))),
      h("div", { className: "challenger-row" }, h("span", null, "Major/current challengers"), h("b", null, challengers.length ? challengers.map((c) => `${c.candidate_name}${c.party ? ` (${c.party})` : ""}`).slice(0, 4).join(", ") : "No major challenger declared"))
    ),
    (raceIntel.article_count || raceIntel.open_seat_articles?.length || raceIntel.top_issues?.length) && h(Section, { title: "ARTICLE-DERIVED INTEL" },
      h(ProvenanceRow, { items: [
        { label: "ARTICLES", value: `${raceIntel.article_count || 0} scanned` },
        { label: "TYPE", value: "news-derived signal" }
      ] }),
      h("div", { className: "detail-row" }, h("span", null, "Articles read"), h("b", null, raceIntel.article_count || 0)),
      raceIntel.top_issues?.length > 0 && h("div", { className: "detail-row" }, h("span", null, "Top issues"), h("b", null, raceIntel.top_issues.map((i) => i.topic).join(", "))),
      Object.keys(raceIntel.challenger_mentions || {}).length > 0 && h("div", { className: "detail-row" }, h("span", null, "Challenger mentions"), h("b", null, Object.entries(raceIntel.challenger_mentions).map(([name, count]) => `${name}: ${count}`).join(", "))),
      (raceIntel.open_seat_articles || []).map((article) => h("a", { className: "fallback-link", href: article.url, target: "_blank", rel: "noreferrer", title: article.headline, key: article.url }, `${article.source_name}: ${article.headline}`))
    ),
    fundraising.length > 0 && h(Section, { title: "FEC FUNDRAISING" }, fundraising.slice(0, 5).map((f) =>
      h("div", { className: "detail-row", key: f.fec_candidate_id || f.candidate_name }, h("span", null, `${f.candidate_name} (${f.party || "?"})`), h("b", null, `${money(f.total_receipts)} raised`))
    ).concat(h("div", { className: "funding-bars", key: "funding-compare" },
      h("small", null, `${dName}: ${money(dRaised)} vs ${rName}: ${money(rRaised)}`),
      h("div", { className: "funding-track" },
        h("i", { className: "d", style: { width: `${(dRaised / maxRaised) * 100}%` } }),
        h("i", { className: "r", style: { width: `${(rRaised / maxRaised) * 100}%` } })
      )
    ))),
    h(Section, { title: "DISTRICT SIGNALS" },
      h("div", { className: "detail-row" }, h("span", null, "College educated"), h("b", null, features.college_educated_pct == null ? "-" : `${Math.round(Number(features.college_educated_pct) * 100)}%`)),
      h("div", { className: "detail-row" }, h("span", null, "Median income"), h("b", null, features.median_income_real == null ? "-" : money(features.median_income_real))),
      h("div", { className: "detail-row" }, h("span", null, "Uninsured"), h("b", null, features.uninsured_rate == null ? "-" : `${Math.round(Number(features.uninsured_rate) * 100)}%`)),
      h("div", { className: "detail-row" }, h("span", null, "Rent burden"), h("b", null, features.rent_burden_pct == null ? "-" : `${Math.round(Number(features.rent_burden_pct) * 100)}% of income`)),
      h("small", { className: "muted" }, features.feature_date ? `Census ACS 2023 - feature cache ${features.feature_date}` : "Census ACS 2023")
    ),
    activeEvents.length > 0 && h(Section, { title: "ACTIVE INTELLIGENCE" }, activeEvents.map((event) =>
      h("div", { className: "event-card", key: event.event_id },
        event.source_url
          ? h("a", { className: "event-title", href: event.source_url, target: "_blank", rel: "noreferrer", title: event.event_name }, event.event_name)
          : h("b", { className: "event-title" }, event.event_name),
        h("div", { className: "event-meta-row" },
          h("span", { className: "source-badge" }, eventTypeLabel(event.event_type)),
          eventMeta(event) && h("small", null, eventMeta(event))
        ),
        h("div", { className: "salience-track" }, h("i", { style: { width: `${Math.min(100, Math.round(Number(event.salience || 0) * 100))}%` } })),
        h("small", { className: "event-salience" }, `Salience ${Math.round(Number(event.salience || 0) * 100)}%`)
      )
    )),
    integritySignals.length > 0 && h(Section, { title: "INTEGRITY PRESSURE" }, integritySignals.slice(0, 3).map((signal) =>
      h("div", { className: "integrity-card", key: signal.candidate_name },
        h("div", { className: "detail-row" }, h("span", null, signal.candidate_name), h("b", null, `${Math.round(Number(signal.integrity_pressure_score || signal.perceived_dishonesty_score || 0))}/100`)),
        h("small", { className: "muted" }, `${signal.article_count} source${signal.article_count === 1 ? "" : "s"} flagged for contradiction, disclosure, ethics, or falsehood language`),
        (signal.evidence || []).slice(0, 2).map((item) => h("a", { className: "fallback-link", href: item.url, target: "_blank", rel: "noreferrer", title: item.headline, key: item.url || item.headline }, item.headline))
      )
    )),
    h(Section, { title: "RACE NEWS" }, h(NewsCards, { newsState })),
    h("button", { className: "more-info", onClick: onMoreInfo }, "MORE INFO")
  );
}

function LeftPanel({ activeState, activeDistrict, onMoreInfo, chambers, districts, loading, districtDetail, districtNews, events, marketGaps, national }) {
  const state = activeState ? stateData[activeState] : null;
  return h("aside", { className: "left panel" },
    activeDistrict ? h(DistrictPanel, { state, district: activeDistrict, detail: districtDetail, newsState: districtNews, events, onMoreInfo }) : state ? h(StatePanel, { state, districts }) : h(NationalPanel, { chambers, districts, loading, marketGaps, national })
  );
}

function StateRaceList({ activeState, districts, roster, focusedDistrictId, onRaceHover, onRaceLeave, onSelectDistrict }) {
  const rosterById = Object.fromEntries((roster || []).filter((row) => row?.district_id?.startsWith(`${activeState}-`)).map((row) => [row.district_id, row]));
  const apiRows = (districts || [])
    .filter((row) => row?.district_id?.startsWith(`${activeState}-`))
    .map((row) => ({ ...(rosterById[row.district_id] || {}), ...row }))
    .sort((a, b) => {
      const aNum = Number(String(a.district_id || "").split("-")[1]) || 0;
      const bNum = Number(String(b.district_id || "").split("-")[1]) || 0;
      return aNum - bNum;
    });
  const rosterRows = Object.values(rosterById).sort((a, b) => Number(a.district_number || 0) - Number(b.district_number || 0));
  const rows = apiRows.length ? apiRows : rosterRows.length ? rosterRows : Array.from({ length: STATE_DISTRICT_COUNT[activeState] || 0 }, (_, idx) => ({
    district_id: STATE_DISTRICT_COUNT[activeState] === 1 ? `${activeState}-AL` : `${activeState}-${String(idx + 1).padStart(2, "0")}`,
    win_probability_d: null,
    incumbent_name: null,
    incumbent_party: null,
  }));
  if (!activeState) return null;
  if (!rows.length) return h("p", { className: "muted tight" }, "Race list loading from API.");
  return h("div", { className: "state-race-list" },
    rows.map((row) => {
      const prob = row.win_probability_d;
      const isActive = focusedDistrictId === row.district_id;
      const incumbent = row.incumbent_name ? `${row.incumbent_name}${row.incumbent_party ? ` (${row.incumbent_party})` : ""}` : "Roster not yet available";
      return h("button", {
        key: row.district_id,
        className: `state-race-row ${isActive ? "active" : ""}`,
        onMouseEnter: () => onRaceHover?.(row.district_id),
        onMouseLeave: () => onRaceLeave?.(),
        onFocus: () => onRaceHover?.(row.district_id),
        onBlur: () => onRaceLeave?.(),
        onClick: () => onSelectDistrict?.(row),
      },
        h("div", null,
          h("strong", null, row.district_id),
          h("em", { className: clsRating(ratingFromProb(prob)) }, ratingFromProb(prob))
        ),
        h("small", null, incumbent),
        h("span", { className: "state-race-prob" }, prob == null ? "D -- / R --" : `D ${Math.round(Number(prob) * 100)}% / R ${Math.round((1 - Number(prob)) * 100)}%`)
      );
    })
  );
}

function RightPanel({ morningBrief, activeState, activeDistrict, districtDetail, districtNews, districts, roster, focusedDistrictId, onRaceHover, onRaceLeave, onSelectDistrict, events, onEventPulse, loading, warmupFact }) {
  const [infoTab, setInfoTab] = useState("dashboard");
  if (loading) {
    return h("aside", { className: "right panel" },
      h("div", null, h(WarmupPanel, { fact: warmupFact })),
      h("div", null,
        h(Section, { title: "RATINGS SCALE" },
          Object.entries(ratingColors).map(([label, color]) =>
            h("div", { className: "legend-row", key: label }, h("i", { style: { background: color } }), h("span", null, label))
          )
        )
      )
    );
  }
  const moves = (morningBrief?.top_moves || []).slice(0, 5);
  const newsCount = districtNews?.articles?.length || 0;
  const districtId = activeDistrict?.liveDistrict?.district_id || activeDistrict?.districtLabel || null;
  const districtFeedArticles = (districtNews?.articles || []).slice(0, 7);
  const isDistrictMode = Boolean(activeDistrict || districtDetail?.district_id || districtFeedArticles.length);
  const scopedFeed = isDistrictMode
    ? scopedEventList(events, { districtId, stateAbbr: activeState }, 7)
    : activeState
      ? scopedEventList(events, { stateAbbr: activeState }, 7)
      : (events || []).slice(0, 7);
  const emptyFeedText = isDistrictMode
    ? "No district-specific intelligence signals yet."
    : activeState
      ? "No state-specific intelligence signals yet."
      : "Not yet available";
  const verifiedOfficeholder = activeDistrict && (Boolean(activeDistrict.liveDistrict?.incumbent_name) || isVerifiedOfficeholder(activeDistrict.incumbent));
  const candidates = districtDetail?.candidates_2026 || activeDistrict?.liveDistrict?.candidates_2026 || [];
  const majorChallengers = districtDetail?.major_challengers_2026 || activeDistrict?.liveDistrict?.major_challengers_2026 || candidates.filter((c) => !c.is_incumbent && c.is_major_challenger !== false && c.active_2026 !== false);
  const challengerNames = majorChallengers.map((c) => `${c.candidate_name}${c.party ? ` (${c.party})` : ""}`).slice(0, 3);
  const raceIntel = districtDetail?.race_intelligence || activeDistrict?.liveDistrict?.race_intelligence || {};
  const twoSeventy = districtDetail?.twoseventy_context || raceIntel.twoseventy;
  return h("aside", { className: "right panel" },
    h("div", null,
      h("div", { className: "explainer-tabs", role: "tablist", "aria-label": "Right panel mode" },
        h("button", { className: infoTab === "dashboard" ? "active" : "", role: "tab", "aria-selected": infoTab === "dashboard", onClick: () => setInfoTab("dashboard") }, "Dashboard"),
        h("button", { className: infoTab === "explainer" ? "active" : "", role: "tab", "aria-selected": infoTab === "explainer", onClick: () => setInfoTab("explainer") }, "Explainer")
      ),
      infoTab === "explainer" ? h(Section, { title: "COMPETITION EXPLAINER" },
        h("div", { className: "explainer-card" },
          h("h2", null, "What Is TAPESTRY?"),
          h("p", null, "TAPESTRY is a live dashboard for the 2026 U.S. midterms. It estimates each House race, rolls those races up into chamber control odds, and compares the model with prediction market prices."),
          h("div", { className: "explainer-grid" },
            h("div", null, h("b", null, "Model"), h("span", null, "The forecast starts with fundamentals, then uses a residual model to catch patterns the first pass misses.")),
            h("div", null, h("b", null, "Validation"), h("span", null, "The model is tested by training on past cycles and holding out later elections. The 2024 competitive-race Brier score is about 0.135.")),
            h("div", null, h("b", null, "Sources"), h("span", null, "Inputs include MIT Election Lab, FEC, Census ACS, House Clerk, polling, FRED, Polymarket, and race news.")),
            h("div", null, h("b", null, "Simulation"), h("span", null, "The House forecast comes from 50,000 simulated elections with shared national swings."))
          ),
          h("p", { className: "muted tight" }, "How to use it: click a state, pick a district, then compare the race profile, candidates, money, local news, and uncertainty."),
          h("p", { className: "muted tight" }, "Data current as of Apr 29, 2026. Backend may take a moment to wake on the free Render tier.")
        )
      ) : h(React.Fragment, null,
      h(Section, { title: activeDistrict ? "RACE INTELLIGENCE" : "NATIONAL INTELLIGENCE" },
        activeDistrict ? h("div", { className: "intel-card" },
          h(ProvenanceRow, { items: [
            { label: "MODEL", value: "forecast and uncertainty" },
            { label: "NEWS", value: "article-derived signal" }
          ] }),
          h("h2", null, activeDistrict.districtLabel),
          h("div", { className: "detail-row" }, h("span", null, "Incumbent"), h("b", null, verifiedOfficeholder ? displayOfficeholder(activeDistrict) : "Roster not yet available")),
          h("div", { className: "detail-row" }, h("span", null, "2026 status"), h("b", null, raceIntel.status_text || districtDetail?.statement || "Data pending")),
          h("div", { className: "detail-row" }, h("span", null, "Model rating"), h("b", null, ratingFromProb(districtDetail?.win_probability_d ?? activeDistrict.liveDistrict?.win_probability_d))),
          twoSeventy?.kalshi_house_price != null && h("div", { className: "detail-row" }, h("span", null, "270toWin/Kalshi"), h("b", null, fmtPct(twoSeventy.kalshi_house_price))),
          h("p", { className: "muted tight" }, districtDetail?.narrative || "Not yet available"),
          h("div", { className: "detail-row" }, h("span", null, "Challenger watch"), h("b", null, challengerNames.length ? challengerNames.join(", ") : "Not yet available")),
          raceIntel.top_issues?.length > 0 && h("div", { className: "detail-row" }, h("span", null, "Article issues"), h("b", null, raceIntel.top_issues.map((i) => i.topic).slice(0, 3).join(", "))),
          h("div", { className: "detail-row" }, h("span", null, "News loaded"), h("b", null, `${newsCount} items`))
        ) : h("div", { className: "intel-card" },
          h(ProvenanceRow, { items: [
            { label: "MAP", value: "district forecast layer" },
            { label: "FEED", value: "scoped events and news" }
          ] }),
          h("h2", null, activeState ? `${stateData[activeState]?.stateName || activeState} RACES` : "2026 ELECTIONS"),
          h("p", { className: "muted tight" }, activeState ? "Hover a race to highlight it on the map. Click a row to open the district profile." : "Click a state to list its races, then use the race list and map together."),
          activeState
            ? h(StateRaceList, { activeState, districts, roster, focusedDistrictId, onRaceHover, onRaceLeave, onSelectDistrict })
            : h("p", { className: "muted tight" }, "Select a state to inspect every district race.")
        )
      ),
      !activeState && !isDistrictMode && h(Section, { title: "INTELLIGENCE FEED" },
        isDistrictMode ? (
          districtFeedArticles.length ? districtFeedArticles.map((article, idx) =>
            h("a", { className: "event-feed-row", key: article.url || `${article.headline}-${idx}`, href: article.url, target: "_blank", rel: "noreferrer", title: article.headline },
              h("span", { className: "event-feed-type" }, article.source_type || "Local"),
              h("b", null, article.headline),
              h("small", null, [article.source_name, article.time_ago, (article.topic_tags || []).slice(0, 2).join(", ")].filter(Boolean).join(" - "))
            )
          ) : [h("p", { className: "muted", key: "none" }, "No district-specific news loaded yet.")]
        ) : scopedFeed.map((event) => {
          const rowContent = [
            h("span", { className: "event-feed-type", key: "type" }, eventTypeLabel(event.event_type)),
            h("b", { key: "title" }, event.event_name),
            h("small", { key: "meta" }, [eventMeta(event), `Salience ${Math.round(Number(event.salience || 0) * 100)}%`].filter(Boolean).join(" - "))
          ];
          return event.source_url
            ? h("a", { className: "event-feed-row", key: event.event_id, href: event.source_url, target: "_blank", rel: "noreferrer", title: event.event_name, onMouseEnter: () => onEventPulse?.(event) }, rowContent)
            : h("button", { className: "event-feed-row", key: event.event_id, onClick: () => onEventPulse?.(event), title: event.event_name }, rowContent);
        }).concat(scopedFeed.length ? [] : [h("p", { className: "muted", key: "none" }, emptyFeedText)])
      )
      )
    ),
    h("div", null,
      h(Section, { title: "RATINGS SCALE" },
        Object.entries(ratingColors).map(([label, color]) =>
          h("div", { className: "legend-row", key: label }, h("i", { style: { background: color } }), h("span", null, label))
        )
      ),
      moves.length > 0 && h(Section, { title: "RECENT MOVES" }, moves.map((m, idx) => h("div", { className: "move-row", key: idx }, ratingMoveText(m))))
    )
  );
}

function TopBar({ national, loading }) {
  const ballot = national?.generic_ballot_margin;
  const ballotText = loading || ballot === undefined || ballot === null ? "-" : `${Number(ballot) >= 0 ? "D" : "R"}+${Math.abs(Number(ballot)).toFixed(1)}`;
  const approvalValue = Number(national?.presidential_approval);
  const approvalText = loading || national?.presidential_approval === undefined || national?.presidential_approval === null ? "-" : `${Math.round(approvalValue <= 1 ? approvalValue * 100 : approvalValue)}%`;
  const gasValue = Number(national?.gas_price_national);
  const gasChange = Number(national?.gas_price_3m_change);
  const gasText = loading || national?.gas_price_national === undefined || national?.gas_price_national === null
    ? "-"
    : `$${gasValue.toFixed(2)}${Number.isFinite(gasChange) ? ` (${gasChange >= 0 ? "+" : ""}${gasChange.toFixed(2)} 3mo)` : ""}`;
  return h("header", { className: "topbar" },
    h("div", { className: "brand-wrap" }, h("span", { className: "brand" }, "TAPESTRY"), h("span", { className: "tagline" }, "the country, woven together")),
    h("div", { className: "date-tag" }, "2026 MIDTERMS - Nov 3, 2026"),
    h("div", { className: "pills" },
      h("span", { className: `pill ${Number(ballot || 0) >= 0 ? "blue" : "red"}` }, `Generic Ballot: ${ballotText}`),
      h("span", { className: "pill amber" }, `Presidential Approval: ${approvalText}`),
      h("span", { className: "pill amber" }, `Gas: ${gasText}`),
      h("span", { className: "pill amber" }, `Days to Election: ${daysToElection()}`)
    )
  );
}

function MorningBriefModal({ brief, marketGaps, onDismiss }) {
  useEffect(() => {
    const id = setTimeout(onDismiss, 30000);
    return () => clearTimeout(id);
  }, [onDismiss]);
  const senate = brief?.senate;
  const house = brief?.house;
  const moves = (brief?.top_moves || []).slice(0, 4);
  const gaps = (brief?.kalshi_disagreements || []).slice(0, 2);
  const marketWatch = (marketGaps?.chamber_gaps || []).filter((gap) => Number(gap.largest_gap || 0) > 0.05);
  return h("div", { className: "brief-modal" },
    h("div", { className: "brief-title" }, "TAPESTRY - MORNING BRIEF - ", new Date().toLocaleDateString([], { month: "short", day: "numeric" })),
    h("div", { className: "brief-grid" },
      h("span", null, `Senate: ${fmtPct(senate?.d_control_probability)} D control`),
      h("span", null, `House: ${fmtPct(house?.d_control_probability)} D control`)
    ),
    h("p", null, brief?.narrative || "Morning brief loaded from the live backend."),
    moves.length > 0 && h("h4", null, "TOP MOVES TODAY"),
    moves.map((m, idx) => h("div", { className: "move-row", key: `m-${idx}` }, ratingMoveText(m))),
    marketWatch.length > 0 && h("h4", null, "MARKET WATCH"),
    marketWatch.map((gap) => h("div", { className: "move-row", key: `market-${gap.chamber}` },
      `${String(gap.chamber || "Market").toUpperCase()}: TAPESTRY ${fmtPct(gap.tapestry_probability)} - Polymarket ${gap.polymarket_price == null ? "-" : fmtPct(gap.polymarket_price)} - ${Math.round(Number(gap.largest_gap || 0) * 100)}pt gap`
    )),
    gaps.length > 0 && h("h4", null, "KALSHI DISAGREEMENTS"),
    gaps.map((g, idx) => h("div", { className: "move-row", key: `g-${idx}` }, `${g.district || g.district_id || "Market"}: Tapestry ${fmtPct(g.model_implied_price)} vs Kalshi ${fmtPct(g.kalshi_price)}`)),
    h("button", { onClick: onDismiss }, "DISMISS")
  );
}

function EnterSplash({ entered, onEnter }) {
  const [leaving, setLeaving] = useState(false);
  if (entered) return null;
  function clickIn() {
    if (leaving) return;
    setLeaving(true);
    onEnter();
  }
  const threadPathA = "M0 32 C82 8 168 8 250 32 S418 56 500 32 S668 8 750 32 S918 56 1000 32";
  const threadPathB = "M0 32 C82 56 168 56 250 32 S418 8 500 32 S668 56 750 32 S918 8 1000 32";
  const threadPathC = "M0 32 C82 25 168 39 250 32 S418 25 500 32 S668 39 750 32 S918 25 1000 32";
  const threadBand = (position) => h("div", { className: `splash-helix-band ${position}` },
    h("svg", { className: "splash-helix-svg", viewBox: "0 0 1000 64", preserveAspectRatio: "none", "aria-hidden": "true" },
      h("path", { className: "splash-helix-line helix-a", d: threadPathA }),
      h("path", { className: "splash-helix-line helix-b", d: threadPathB }),
      h("path", { className: "splash-helix-line helix-c", d: threadPathC })
    )
  );
  return h("div", { className: `enter-splash ${leaving ? "leaving" : ""}` },
    threadBand("top"),
    h("button", { className: "enter-word", onClick: clickIn, "aria-label": "Enter TAPESTRY" },
      "TAPESTRY".split("").map((letter, idx) => h("span", { key: idx, style: { "--i": idx } }, letter))
    ),
    h("p", null, "the country, woven together"),
    threadBand("bottom")
  );
}

function Tooltip({ hovered }) {
  if (!hovered) return null;
  return h("div", { className: "tooltip", style: { left: hovered.x + 14, top: hovered.y + 14 } },
    h("b", null, hovered.title),
    h("span", null, hovered.subtitle),
    hovered.uncertainty !== null && hovered.uncertainty !== undefined && h("div", { style: { fontSize: "11px", color: "#64748b" } }, `+/-${Math.round(Number(hovered.uncertainty))}pts`)
  );
}

function PoliticianOverlay({ district, detail, transparency, onClose }) {
  if (!district) return null;
  const verifiedOfficeholder = Boolean(district.liveDistrict?.incumbent_name) || isVerifiedOfficeholder(district.incumbent);
  const committees = detail?.incumbent_committees || district.liveDistrict?.incumbent_committees || [];
  const candidates = detail?.candidates_2026 || district.liveDistrict?.candidates_2026 || [];
  const challengers = detail?.major_challengers_2026 || district.liveDistrict?.major_challengers_2026 || candidates.filter((c) => !c.is_incumbent && c.is_major_challenger !== false && c.active_2026 !== false);
  const overlayCandidateNames = new Set([detail?.incumbent_name, displayOfficeholderName(district), ...challengers.map((c) => c.candidate_name)].filter(Boolean).map((name) => name.toLowerCase()));
  const fundraising = (detail?.fundraising || []).filter((f) => overlayCandidateNames.has(String(f.candidate_name || "").toLowerCase()));
  return h("div", { className: "bio-scrim", onClick: onClose },
    h("aside", { className: "bio-overlay", onClick: (e) => e.stopPropagation() },
      h("button", { className: "bio-close", onClick: onClose }, "X"),
      h("div", { className: "avatar" }),
      h("h2", null, displayOfficeholderName(district)),
      h("p", { className: "subhead" }, verifiedOfficeholder ? `${district.party || "Party pending"} - ${district.homeBase || district.districtLabel}` : "Roster not yet available"),
      h(Section, { title: "FULL BIO" }, h("p", { className: "muted bio-copy" }, "Not yet available")),
      h(Section, { title: "COMMITTEES" }, committees.length ? committees.map((c) => h("div", { className: "mini-row", key: c.committee_name || c }, c.committee_name ? `${c.committee_name}${c.role ? ` - ${c.role}` : ""}` : c)) : h("p", { className: "muted" }, "Not yet available")),
      h(Section, { title: "FEC FUNDRAISING" },
        (fundraising.length ? fundraising.map((f) => [`${f.candidate_name || f.party || "Candidate"}`, `${money(f.total_receipts)} raised - ${money(f.cash_on_hand)} cash`]) : [["Fundraising", "Data pending"]]).map(([k, v]) =>
          h("div", { className: "detail-row", key: k }, h("span", null, k), h("b", null, v))
        )
      ),
      h(Section, { title: "TRANSPARENCY" },
        h("div", { className: "detail-row" }, h("span", null, "Source"), h("b", null, transparency?.source_name || "OpenSecrets/FEC pending")),
        h("div", { className: "detail-row" }, h("span", null, "Top donor sector"), h("b", null, transparency?.top_donor_sector || "Data pending")),
        h("div", { className: "detail-row" }, h("span", null, "Top sector amount"), h("b", null, money(transparency?.top_donor_amount))),
        h("div", { className: "detail-row" }, h("span", null, "AIPAC-related"), h("b", null, money(transparency?.aipac_related_amount))),
        h("div", { className: "detail-row" }, h("span", null, "Pro-Israel PACs"), h("b", null, money(transparency?.pro_israel_pac_amount))),
        h("div", { className: "detail-row" }, h("span", null, "Defense sector"), h("b", null, money(transparency?.defense_sector_amount))),
        h("div", { className: "detail-row" }, h("span", null, "Small-dollar share"), h("b", null, transparency?.small_dollar_share == null ? "pending" : `${Math.round(transparency.small_dollar_share * 100)}%`)),
        h("div", { className: "position-row" }, h("b", null, "Medicare"), h("span", null, transparency?.medicare_posture || "Pending voting-record ingest")),
        h("div", { className: "position-row" }, h("b", null, "Israel"), h("span", null, transparency?.israel_posture || "Pending voting-record ingest")),
        h("div", { className: "position-row" }, h("b", null, "Military industry"), h("span", null, transparency?.defense_industry_posture || "Pending defense-sector ingest")),
        transparency?.notes && h("p", { className: "muted tight" }, transparency.notes)
      ),
      h(Section, { title: "VOTING RECORD" }, h("p", { className: "muted" }, "Not yet available")),
      h(Section, { title: "RECENT FLOOR STATEMENTS" }, h("p", { className: "muted" }, "Not yet available")),
      challengers.length > 0 && h(Section, { title: "CHALLENGER" },
        challengers.map((c) => h("div", { className: "detail-row", key: c.candidate_id || c.candidate_name }, h("span", null, c.party || "Party pending"), h("b", null, c.candidate_name)))
      )
    )
  );
}
function ElectionMap({ activeState, setActiveState, activeDistrict, setActiveDistrict, geoMode, setGeoMode, hovered, setHovered, focusedDistrictId, districts, chambers, loading, newsActiveState, newsCompleteStates, entered, stateSummaries }) {
  const [zoomState, setZoomState] = useState({ coordinates: [-97, 39], zoom: 1 });
  const selected = activeState ? stateData[activeState] : null;
  const position = zoomState;
  const fipsToAbbr = useMemo(() => Object.fromEntries(Object.values(stateData).map((s) => [s.fipsCode, s.abbreviation])), []);
  const stateLean = useMemo(() => Object.fromEntries((stateSummaries || []).map((s) => [s.state_abbr, s.avg_win_probability_d])), [stateSummaries]);
  const districtById = useMemo(() => Object.fromEntries((districts || []).map((d) => [d.district_id, d])), [districts]);

  function selectState(abbr) {
    const s = stateData[abbr];
    setActiveDistrict(null);
    setActiveState(abbr);
    setGeoMode("districts");
    setZoomState({ coordinates: s.center, zoom: abbr === "AK" ? 2.3 : abbr === "HI" ? 4.8 : 5.2 });
  }
  function resetMap() {
    setActiveDistrict(null);
    setActiveState(null);
    setZoomState({ coordinates: [-97, 39], zoom: 1 });
  }
  function stateLevel() {
    setActiveDistrict(null);
  }
  function backOneLevel() {
    if (activeDistrict) stateLevel();
    else resetMap();
  }
  function zoomBy(multiplier) {
    setZoomState((current) => ({
      coordinates: current.coordinates,
      zoom: Math.max(activeState ? 2.2 : 1, Math.min(activeState ? 85 : 4, current.zoom * multiplier)),
    }));
  }
  function districtFromGeo(geo) {
    const props = geo.properties || {};
    if (String(props.STATEFP20 || props.STATEFP || "").padStart(2, "0") !== selected.fipsCode) return null;
    const districtId = geoidToDistrictId(props);
    if (!districtId) return null;
    const live = districtById[districtId] || { district_id: districtId, win_probability_d: null };
    return districtFromApiRow(live, selected);
  }
  function updateZoomPosition(p) {
    if (!p?.coordinates || !Array.isArray(p.coordinates) || p.coordinates.length < 2 || !Number.isFinite(p.zoom)) return;
    setZoomState({ coordinates: p.coordinates, zoom: p.zoom });
  }
  const tossupHouse = (districts || []).filter((d) => d.win_probability_d >= 0.45 && d.win_probability_d <= 0.55).length;
  const senateProb = chamberByName(chambers, "senate")?.d_control_probability;
  const tossupSenate = senateProb >= 0.45 && senateProb <= 0.55 ? 1 : 0;
  const mapZoom = Math.max(position.zoom || 1, 1);
  const districtStroke = Math.max(0.002, 0.18 / Math.pow(mapZoom, 1.25));
  const districtFocusedStroke = Math.max(0.004, 0.42 / Math.pow(mapZoom, 1.2));
  const districtSelectedStroke = Math.max(0.005, 0.55 / Math.pow(mapZoom, 1.18));
  const interactive = entered && !loading;

  return h("main", { className: `map-panel ${interactive ? "" : "locked"}`.trim(), onDoubleClick: interactive && activeState ? resetMap : undefined },
    activeState && h("div", { className: "map-nav" },
      h("button", { className: "back-button", onClick: backOneLevel, "aria-label": activeDistrict ? `Back to ${selected.stateName}` : "Back to United States" }, activeDistrict ? `\u2190 ${selected.stateName}` : "\u2190 United States"),
      h("div", { className: "breadcrumb" },
        h("button", { onClick: resetMap }, "United States"),
        h("span", null, ">"),
        h("button", { onClick: stateLevel }, selected.stateName),
        activeDistrict && h("span", null, ">"),
        activeDistrict && h("b", null, activeDistrict.districtLabel)
      ),
      h("div", { className: "zoom-buttons" },
        h("button", { onClick: () => zoomBy(1.55), "aria-label": "Zoom in" }, "+"),
        h("button", { onClick: () => zoomBy(1 / 1.55), "aria-label": "Zoom out" }, "-")
      )
    ),
    h(ComposableMap, { projection: "geoAlbersUsa", width: 980, height: 720 },
      h(ZoomableGroup, {
        center: position.coordinates,
        zoom: position.zoom,
        minZoom: activeState ? 2.2 : 1,
        maxZoom: activeState ? 85 : 4,
        onMoveEnd: updateZoomPosition,
        translateExtent: activeState ? [[-4200, -3200], [5200, 4200]] : [[-120, -80], [1100, 820]]
      },
        !activeState && h(Geographies, { geography: STATES_URL },
          ({ geographies }) => geographies.map((geo) => {
            const abbr = fipsToAbbr[String(geo.id).padStart(2, "0")];
            const s = stateData[abbr];
            if (!s) return null;
            const newsLit = newsCompleteStates?.has(abbr);
            const newsActive = newsActiveState === abbr;
            const liveStateProb = stateLean[abbr] ?? null;
            const stateFill = loading ? "#1e2130" : colorFromProb(liveStateProb);
            const stateOpacity = newsActive ? 1 : newsLit ? 1 : 0.88;
            return h(Geography, {
              key: geo.rsmKey, geography: geo,
              "data-abbr": abbr,
              "data-name": s.stateName,
              fill: stateFill, stroke: "#1e2130", strokeWidth: 0.5,
              opacity: stateOpacity,
              className: `geo state-geo ${loading ? "loading-map" : ""} ${newsActive ? "news-active" : ""} ${newsLit ? "news-lit" : ""}`,
              onMouseMove: interactive ? (e) => setHovered({ x: e.clientX, y: e.clientY, title: s.stateName, subtitle: liveStateProb == null ? "Forecast data pending" : `Average House lean - ${ratingFromProb(liveStateProb)}` }) : undefined,
              onMouseLeave: interactive ? () => setHovered(null) : undefined,
              onClick: interactive ? () => selectState(abbr) : undefined,
              onWheel: interactive ? (e) => { if (e.deltaY < 0) selectState(abbr); } : undefined,
              onDoubleClick: interactive ? (e) => { e.stopPropagation(); selectState(abbr); } : undefined
            });
          })
        ),
        activeState && h(Geographies, { geography: DISTRICTS_URL },
          ({ geographies }) => h(React.Fragment, null,
          [...geographies].sort((a, b) => {
            const aId = geoidToDistrictId(a.properties || {});
            const bId = geoidToDistrictId(b.properties || {});
            const aLift = (activeDistrict?.districtLabel === aId ? 3 : 0) + (focusedDistrictId === aId ? 2 : 0) + (hovered?.title === aId ? 1 : 0);
            const bLift = (activeDistrict?.districtLabel === bId ? 3 : 0) + (focusedDistrictId === bId ? 2 : 0) + (hovered?.title === bId ? 1 : 0);
            if (aLift !== bLift) return aLift - bLift;
            return geoArea(b) - geoArea(a);
          }).map((geo) => {
            const d = districtFromGeo(geo);
            if (!d) return null;
            const isSelected = activeDistrict?.districtLabel === d.districtLabel;
            const isFocused = focusedDistrictId === d.districtLabel;
            const liveProb = d.liveDistrict?.win_probability_d ?? d.dPct / 100;
            const liveRating = ratingFromProb(liveProb);
            const fill = colorFromProb(liveProb);
            const opacity = isSelected || isFocused ? 1 : 0.68;
            const strokeWidth = isSelected ? districtSelectedStroke : isFocused ? districtFocusedStroke : districtStroke;
            return h(React.Fragment, { key: geo.rsmKey },
              h(Geography, {
                geography: geo,
                fill: "none",
                stroke: fill,
                strokeWidth: 1.2,
                vectorEffect: "non-scaling-stroke",
                strokeLinejoin: "round",
                strokeLinecap: "round",
                opacity: Math.min(1, opacity + 0.1),
                pointerEvents: "none",
                className: "district-seam"
              }),
              h(Geography, {
                geography: geo,
                "data-geoid": geo.properties.GEOID20 || geo.properties.GEOID,
                "data-district": d.districtLabel,
                fill,
                stroke: isSelected || isFocused ? ACCENT : "rgba(226,232,240,.12)",
                strokeWidth,
                strokeLinejoin: "round",
                strokeLinecap: "round",
                style: { "--district-hover-stroke": `${districtFocusedStroke}` },
                opacity,
                className: `geo district-geo ${isFocused ? "district-focused" : ""} ${d.rating === "Toss-Up" ? "pulse-ring" : ""}`,
                onMouseMove: interactive ? (e) => setHovered({ x: e.clientX, y: e.clientY, title: d.districtLabel, subtitle: `${displayOfficeholder(d)} - ${liveRating} - D ${Math.round(liveProb * 100)}% / R ${Math.round((1 - liveProb) * 100)}%`, uncertainty: d.liveDistrict?.uncertainty }) : undefined,
                onMouseLeave: interactive ? () => setHovered(null) : undefined,
                onClick: interactive ? (e) => {
                  e.stopPropagation();
                  setActiveDistrict(d);
                } : undefined,
                onDoubleClick: interactive ? (e) => e.stopPropagation() : undefined
              })
            );
          }))
        )
      )
    ),
    h("div", { className: "map-overlay" }, h("h4", null, "TOSS-UP SEATS"), h("span", null, `Senate: ${loading ? "-" : tossupSenate}`), h("span", null, `House: ${loading ? "-" : tossupHouse}`), h("small", null, `Updated: ${new Date().toLocaleDateString([], { month: "short", day: "numeric", year: "numeric" })}`)),
    interactive && h(Tooltip, { hovered })
  );
}

function App() {
  const [activeState, setActiveState] = useState(null);
  const [activeDistrict, setActiveDistrict] = useState(null);
  const [geoMode, setGeoMode] = useState("districts");
  const [moreInfoOpen, setMoreInfoOpen] = useState(false);
  const [hoveredRegion, setHoveredRegion] = useState(null);
  const [focusedDistrictId, setFocusedDistrictId] = useState(null);
  const [morningBrief, setMorningBrief] = useState(null);
  const [chambers, setChambers] = useState(null);
  const [national, setNational] = useState(null);
  const [districts, setDistricts] = useState([]);
  const [houseRoster, setHouseRoster] = useState([]);
  const [stateSummaries, setStateSummaries] = useState([]);
  const [conflicts, setConflicts] = useState([]);
  const [events, setEvents] = useState([]);
  const [scopedEvents, setScopedEvents] = useState([]);
  const [kalshiGaps, setKalshiGaps] = useState([]);
  const [marketGaps, setMarketGaps] = useState(null);
  const [districtDetail, setDistrictDetail] = useState(null);
  const [districtNews, setDistrictNews] = useState(null);
  const [districtTransparency, setDistrictTransparency] = useState(null);
  const [districtNewsCache, setDistrictNewsCache] = useState(() => new Map());
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [coldStart, setColdStart] = useState(false);
  const [warmupSeconds, setWarmupSeconds] = useState(0);
  const [warmupFactIndex, setWarmupFactIndex] = useState(0);
  const [showBrief, setShowBrief] = useState(false);
  const [entered, setEntered] = useState(false);
  const [newsCompleteStates, setNewsCompleteStates] = useState(() => new Set());
  const bootState = useRef({ done: false, receivedCore: false });

  function clearMediaSession() {
    try {
      if ("mediaSession" in navigator) {
        navigator.mediaSession.metadata = null;
        navigator.mediaSession.playbackState = "none";
        navigator.mediaSession.setActionHandler("play", null);
        navigator.mediaSession.setActionHandler("pause", null);
        navigator.mediaSession.setActionHandler("previoustrack", null);
        navigator.mediaSession.setActionHandler("nexttrack", null);
        navigator.mediaSession.setActionHandler("seekbackward", null);
        navigator.mediaSession.setActionHandler("seekforward", null);
        navigator.mediaSession.setActionHandler("stop", null);
      }
    } catch {}
  }

  useEffect(() => {
    clearMediaSession();
    return () => clearMediaSession();
  }, []);

  useEffect(() => {
    if (!loading) return;
    const id = setInterval(() => {
      setWarmupFactIndex((current) => (current + 1) % WARMUP_FACTS.length);
    }, 5000);
    return () => clearInterval(id);
  }, [loading]);

  useEffect(() => {
    let cancelled = false;
    const markReady = (hasCoreData = false) => {
      if (cancelled) return;
      if (hasCoreData) bootState.current.receivedCore = true;
      if (!bootState.current.done && (bootState.current.receivedCore || hasCoreData)) {
        bootState.current.done = true;
        setColdStart(false);
        setError(false);
        setLoading(false);
      }
    };

    const loadCore = () => {
      getMorningBrief().then((brief) => {
        if (cancelled || !brief) return;
        setMorningBrief(brief);
        setShowBrief(Boolean(brief));
      });
      getChambers().then((rows) => {
        if (cancelled || !rows) return;
        setChambers(rows || []);
        markReady(Array.isArray(rows) && rows.length > 0);
      });
      getNational().then((rows) => {
        if (cancelled || !rows) return;
        setNational(rows);
        markReady(Boolean(rows));
      });
      getDistrictSummaries().then((rows) => {
        if (cancelled || !rows) return;
        setDistricts(rows || []);
        markReady(Array.isArray(rows) && rows.length > 0);
      });
      getStates().then((rows) => {
        if (cancelled || !rows) return;
        setStateSummaries(rows || []);
        markReady(Array.isArray(rows) && rows.length > 0);
      });
      getMarketGaps().then((rows) => {
        if (cancelled) return;
        if (rows) setMarketGaps(rows);
      });
    };

    loadCore();

    const warmupTimer = setTimeout(() => {
      if (!cancelled && !bootState.current.receivedCore) setColdStart(true);
    }, 4000);

    const warmupTicker = setInterval(() => {
      if (!cancelled && !bootState.current.done) setWarmupSeconds((current) => current + 1);
    }, 1000);

    const loadingWatchdog = setTimeout(() => {
      if (cancelled || bootState.current.done) return;
      bootState.current.done = true;
      setError(!bootState.current.receivedCore);
      setLoading(false);
      setColdStart(false);
    }, 75000);

    const bootstrapRetry = setInterval(() => {
      if (cancelled || bootState.current.receivedCore) return;
      loadCore();
    }, 8000);

    Promise.all([
      getConflicts(),
      getEvents(),
      getNewsReadiness()
    ]).then(([conflictRows, eventRows, readiness]) => {
      if (cancelled) return;
      setConflicts(conflictRows || []);
      setEvents(eventRows || []);
      if (readiness) {
        setNewsCompleteStates(new Set(Object.entries(readiness).filter(([, count]) => Number(count) >= INFO_READY_THRESHOLD).map(([state]) => state)));
      }
    });
    return () => {
      cancelled = true;
      clearTimeout(warmupTimer);
      clearInterval(warmupTicker);
      clearTimeout(loadingWatchdog);
      clearInterval(bootstrapRetry);
    };
  }, []);

  useEffect(() => {
    getHouseRoster().then((rows) => rows && setHouseRoster(rows));
  }, []);

  function handleEnter() {
    clearMediaSession();
    setTimeout(() => {
      setEntered(true);
    }, 650);
  }

  function selectDistrictFromRow(row) {
    if (!row?.district_id) return;
    const abbr = row.district_id.split("-")[0];
    const rosterRow = houseRoster.find((r) => r.district_id === row.district_id);
    const state = stateData[abbr] || stateData[activeState];
    if (!state) return;
    setActiveState(abbr);
    setFocusedDistrictId(row.district_id);
    setActiveDistrict(districtFromApiRow({ ...(rosterRow || {}), ...row }, state));
  }

  useEffect(() => {
    const id = setInterval(() => {
      getChambers().then((rows) => rows && setChambers(rows));
      getKalshiGaps().then((rows) => rows && setKalshiGaps(rows));
      getMarketGaps().then((rows) => rows && setMarketGaps(rows));
    }, 5 * 60 * 1000);
    return () => clearInterval(id);
  }, []);

  useEffect(() => {
    if (!activeDistrict) {
      setDistrictDetail(null);
      setDistrictNews(null);
      setDistrictTransparency(null);
      return;
    }
    const districtId = activeDistrict.liveDistrict?.district_id || activeDistrict.districtLabel;
    setDistrictDetail(activeDistrict.liveDistrict || null);
    setDistrictNews(districtNewsCache.get(districtId) || null);
    setDistrictTransparency(null);
    getDistrict(districtId).then((row) => setDistrictDetail(row || activeDistrict.liveDistrict || null));
    getDistrictTransparency(districtId).then((row) => setDistrictTransparency(row));
    getDistrictNews(districtId, { limit: 5 }).then((rows) => {
      const finalNews = rows?.articles?.length ? rows : { seeded: false, articles: [], source_coverage: "pending" };
      setDistrictNewsCache((prev) => new Map(prev).set(districtId, finalNews));
      setDistrictNews(finalNews);
    });
  }, [activeDistrict]);

  useEffect(() => {
    let cancelled = false;
    if (activeDistrict) {
      const districtId = activeDistrict.liveDistrict?.district_id || activeDistrict.districtLabel;
      setScopedEvents([]);
      getEvents({ district: districtId, limit: 10 }).then((rows) => {
        if (!cancelled) setScopedEvents(rows || []);
      });
      return () => { cancelled = true; };
    }
    if (activeState) {
      setScopedEvents([]);
      getEvents({ state: activeState, limit: 10 }).then((rows) => {
        if (!cancelled) setScopedEvents(rows || []);
      });
      return () => { cancelled = true; };
    }
    setScopedEvents([]);
    return () => { cancelled = true; };
  }, [activeDistrict, activeState]);

  React.useEffect(() => { setMoreInfoOpen(false); }, [activeDistrict, activeState]);
  React.useEffect(() => { if (!activeState) setFocusedDistrictId(null); }, [activeState]);
  const visibleEvents = (activeDistrict || activeState) ? scopedEvents : events;
  const warmupFact = WARMUP_FACTS[warmupFactIndex % WARMUP_FACTS.length];
  return h("div", { className: "app-shell" },
    coldStart && loading && h("div", { className: "warmup-banner" },
      h("div", { className: "warmup-copy" },
        h("strong", null, "Waking the backend"),
        h("span", null, `Render cold start in progress. Live data should appear shortly. ${warmupSeconds}s`)
      ),
      h("div", { className: "warmup-progress" }, h("div", { className: "warmup-progress-fill" })),
      h("div", { className: "warmup-inline-fact" }, h("strong", null, "Fun fact:"), h("span", null, warmupFact))
    ),
    error && h("div", { className: "offline-banner" }, "The backend did not wake up in time. It may still be starting on Render."),
    showBrief && morningBrief && h(MorningBriefModal, { brief: morningBrief, marketGaps, onDismiss: () => setShowBrief(false) }),
    h(TopBar, { national, loading }),
    h("div", { className: "workspace" },
      h(LeftPanel, { activeState, activeDistrict, onMoreInfo: () => setMoreInfoOpen(true), chambers, districts, loading, districtDetail, districtNews, events: visibleEvents, marketGaps, national }),
      h(ElectionMap, { activeState, setActiveState, activeDistrict, setActiveDistrict, geoMode, setGeoMode, hovered: hoveredRegion, setHovered: setHoveredRegion, focusedDistrictId, districts, chambers, loading, newsActiveState: null, newsCompleteStates, entered, stateSummaries }),
      h(RightPanel, { morningBrief, activeState, activeDistrict, districtDetail, districtNews, districts, roster: houseRoster, focusedDistrictId, onRaceHover: setFocusedDistrictId, onRaceLeave: () => setFocusedDistrictId(activeDistrict?.districtLabel || null), onSelectDistrict: selectDistrictFromRow, events: visibleEvents, loading, warmupFact })
    ),
    moreInfoOpen && h(PoliticianOverlay, { district: activeDistrict, detail: districtDetail, transparency: districtTransparency, onClose: () => setMoreInfoOpen(false) }),
    h(EnterSplash, { entered, onEnter: handleEnter })
  );
}

createRoot(document.getElementById("root")).render(h(App));




