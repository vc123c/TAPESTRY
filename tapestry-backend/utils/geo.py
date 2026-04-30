from __future__ import annotations

import re

STATE_FIPS = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "FL": "12", "GA": "13", "HI": "15", "ID": "16",
    "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21", "LA": "22",
    "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27", "MS": "28",
    "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33", "NJ": "34",
    "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39", "OK": "40",
    "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46", "TN": "47",
    "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54",
    "WI": "55", "WY": "56",
}

STATE_NAME_TO_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY",
}

STATE_DISTRICT_COUNT = {
    "AL": 7, "AK": 1, "AZ": 9, "AR": 4, "CA": 52, "CO": 8, "CT": 5, "DE": 1,
    "FL": 28, "GA": 14, "HI": 2, "ID": 2, "IL": 17, "IN": 9, "IA": 4, "KS": 4,
    "KY": 6, "LA": 6, "ME": 2, "MD": 8, "MA": 9, "MI": 13, "MN": 8, "MS": 4,
    "MO": 8, "MT": 2, "NE": 3, "NV": 4, "NH": 2, "NJ": 12, "NM": 3, "NY": 26,
    "NC": 14, "ND": 1, "OH": 15, "OK": 5, "OR": 6, "PA": 17, "RI": 2, "SC": 7,
    "SD": 1, "TN": 9, "TX": 38, "UT": 4, "VT": 1, "VA": 11, "WA": 10, "WV": 2,
    "WI": 8, "WY": 1,
}

COMPETITIVE_DISTRICTS = [
    "AZ-01", "AZ-06", "CA-13", "CA-22", "CO-03", "ME-02", "MI-07", "MI-08",
    "NC-01", "NE-02", "NV-03", "NY-17", "OH-09", "OH-13", "OR-05", "PA-07",
    "PA-10", "TX-15", "VA-02", "WI-03",
]


def state_from_district(district_id: str) -> str:
    return normalize_district_id(district_id).split("-")[0].upper()


def normalize_district_id(raw: str) -> str:
    """
    Normalize any district ID to XX-## format.
    CA-1 -> CA-01, CA01 -> CA-01, California-1 -> CA-01.
    At-large districts stay XX-AL.
    """
    if not raw:
        return raw
    raw = str(raw).strip()
    if not raw:
        return raw
    normalized = raw.replace("_", "-").strip()
    upper = normalized.upper()
    at_large = re.match(r"^([A-Z]{2})[-\s]?(?:AL|00|0|AT[-\s]?LARGE)$", upper)
    if at_large:
        return f"{at_large.group(1)}-AL"
    lower = normalized.lower()
    for name, abbr in STATE_NAME_TO_ABBR.items():
        if lower.startswith(name):
            rest = normalized[len(name):].strip().lstrip("-").strip()
            if rest.lower() in {"al", "at large", "at-large", "0", "00"}:
                return f"{abbr}-AL"
            try:
                return f"{abbr}-{int(rest):02d}"
            except ValueError:
                pass
    match = re.match(r"^([A-Za-z]{2})[-\s]?(\d+)", normalized)
    if match:
        return f"{match.group(1).upper()}-{int(match.group(2)):02d}"
    match = re.match(r"^([A-Za-z]{2})[-\s]?([A-Za-z]+)$", normalized)
    if match and match.group(2).upper() in {"AL", "AT", "AT-LARGE"}:
        return f"{match.group(1).upper()}-AL"
    return upper
