from __future__ import annotations

from getpass import getpass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

KEYS = [
    "POLYMARKET_API_KEY",
    "NEWSAPI_KEY",
    "CENSUS_API_KEY",
    "FEC_API_KEY",
]


def parse_env() -> tuple[list[str], dict[str, str]]:
    lines = ENV_PATH.read_text(encoding="utf-8", errors="ignore").splitlines() if ENV_PATH.exists() else []
    values: dict[str, str] = {}
    for line in lines:
        if "=" not in line or line.strip().startswith("#"):
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return lines, values


def write_env(lines: list[str], updates: dict[str, str]) -> None:
    seen = set()
    out = []
    for line in lines:
        if "=" not in line or line.strip().startswith("#"):
            out.append(line)
            continue
        key, _value = line.split("=", 1)
        clean_key = key.strip()
        if clean_key in updates:
            out.append(f"{clean_key}={updates[clean_key]}")
            seen.add(clean_key)
        else:
            out.append(line)
    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={value}")
    ENV_PATH.write_text("\n".join(out) + "\n", encoding="utf-8")


def main() -> int:
    lines, values = parse_env()
    updates: dict[str, str] = {}
    print(f"Editing: {ENV_PATH}")
    print("Input is hidden. Press Enter to keep an existing non-empty value.")
    for key in KEYS:
        current = values.get(key, "")
        status = f"currently {len(current)} chars" if current else "currently empty"
        entered = getpass(f"{key} ({status}): ").strip()
        if entered:
            updates[key] = entered
        elif current:
            updates[key] = current
    if updates:
        write_env(lines, updates)
    _, after = parse_env()
    print("Saved lengths:")
    for key in KEYS:
        print(f"  {key}: {len(after.get(key, ''))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
