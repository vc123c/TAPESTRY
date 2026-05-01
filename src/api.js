const DEPLOY_API_URL = "__TAPESTRY_API_URL__";
const ENV_API_URL = import.meta.env?.VITE_API_URL;
const BASE_URL = (
  ENV_API_URL ||
  (DEPLOY_API_URL.startsWith("__") ? "" : DEPLOY_API_URL) ||
  ""
).replace(/\/$/, "");
const MINUTE = 60 * 1000;
const HOUR = 60 * MINUTE;
const MONTH = 30 * 24 * HOUR;
const REQUEST_TIMEOUT = 45 * 1000;

function buildUrl(path, params = {}) {
  const query = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== "") query.set(key, value);
  });
  return `${BASE_URL}${path}${query.toString() ? `?${query}` : ""}`;
}

function readCache(key) {
  if (typeof localStorage === "undefined") return null;
  try {
    const cached = JSON.parse(localStorage.getItem(key) || "null");
    if (!cached || Date.now() > cached.expiresAt) return null;
    return cached.data;
  } catch {
    return null;
  }
}

function writeCache(key, data, ttlMs) {
  if (!ttlMs || typeof localStorage === "undefined" || data === null || data === undefined) return;
  try {
    localStorage.setItem(key, JSON.stringify({ expiresAt: Date.now() + ttlMs, data }));
  } catch {
    // Storage can be unavailable or full; the app should keep working live.
  }
}

async function getJson(path, params = {}, options = {}) {
  const url = buildUrl(path, params);
  const cacheKey = `tapestry:api:${url}`;
  if (options.ttlMs) {
    const cached = readCache(cacheKey);
    if (cached) return cached;
  }
  let timeout = null;
  try {
    const controller = new AbortController();
    timeout = setTimeout(() => controller.abort(), REQUEST_TIMEOUT);
    const res = await fetch(url, {
      headers: { Accept: "application/json" },
      signal: controller.signal,
    });
    clearTimeout(timeout);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    const data = await res.json();
    writeCache(cacheKey, data, options.ttlMs);
    return data;
  } catch (err) {
    if (timeout) clearTimeout(timeout);
    console.error(`API request failed: ${path}`, err);
    if (options.ttlMs) return readCache(cacheKey);
    return null;
  }
}

export async function getMorningBrief() {
  return getJson("/api/morning-brief");
}

export async function getNational() {
  return getJson("/api/national");
}

export async function getAllDistricts(params = {}) {
  return getJson("/api/districts", params, { ttlMs: 15 * MINUTE });
}

export async function getDistrictSummaries(params = {}) {
  return getJson("/api/districts/summary", params, { ttlMs: 15 * MINUTE });
}

export async function getDistrict(districtId) {
  return getJson(`/api/districts/${encodeURIComponent(districtId)}`);
}

export async function getHouseRoster(params = {}) {
  return getJson("/api/districts/roster", params, { ttlMs: MONTH });
}

export async function getStates() {
  return getJson("/api/states", {}, { ttlMs: 15 * MINUTE });
}

export async function getChambers() {
  return getJson("/api/chambers");
}

export async function getConflicts() {
  return getJson("/api/conflicts");
}

export async function getEvents(params = {}) {
  return getJson("/api/events", params);
}

export async function getDistrictNews(districtId, params = {}) {
  return getJson(`/api/districts/${encodeURIComponent(districtId)}/news`, params);
}

export async function getDistrictTransparency(districtId) {
  return getJson(`/api/districts/${encodeURIComponent(districtId)}/transparency`, {}, { ttlMs: MONTH });
}

export async function getKalshiGaps() {
  return getJson("/api/kalshi/gaps");
}

export async function getMarketGaps() {
  return getJson("/api/market/gaps");
}

export async function refreshLocalNews() {
  try {
    const res = await fetch(buildUrl("/api/admin/scrape-local-news"), { method: "POST" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return await res.json();
  } catch (err) {
    console.error("API request failed: /api/admin/scrape-local-news", err);
    return null;
  }
}

export async function refreshLocalNewsState(state) {
  try {
    const res = await fetch(buildUrl(`/api/admin/scrape-local-news/${encodeURIComponent(state)}`, { fast: "true" }), { method: "POST" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return await res.json();
  } catch (err) {
    console.error(`API request failed: /api/admin/scrape-local-news/${state}`, err);
    return null;
  }
}

export async function getNewsReadiness() {
  return getJson("/api/admin/news-readiness");
}
