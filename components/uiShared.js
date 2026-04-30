import React from "https://esm.sh/react@18.3.1";
import { scaleOrdinal } from "https://esm.sh/d3-scale@4.0.2";

const h = React.createElement;
const STATES_URL = "https://cdn.jsdelivr.net/npm/us-atlas@3/states-10m.json";
const DISTRICTS_URL = "https://cdn.jsdelivr.net/gh/civic-interconnect/civic-data-boundaries-us-cd118@main/data-out/national/cd118_us.geojson";
const ELECTION_DAY = new Date("2026-11-03T00:00:00");
const ACCENT = "#7c3aed";
const LABS_AUDIO = "/assets/labs-consolidated.wav";
const ENTER_AUDIO = "/assets/track-5-consolidated.wav";
const INFO_READY_THRESHOLD = 3;

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


export { h, ratingColors, colorForRating, ratingFromProb, colorFromProb, stateDistricts, geoidToDistrictId, stateProbability, chamberByName, fmtPct, fmtMargin, money, eventTypeLabel, eventMeta, isVerifiedOfficeholder, displayOfficeholder, displayOfficeholderName, probBarRace, districtFromApiRow, ratingMoveText, stateData, STATE_DISTRICT_COUNT, daysToElection, clsRating };
