from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parent
ENV_PATH = ROOT / ".env"
SECRETS_DIR = ROOT / "secrets"
KEY_PATH = SECRETS_DIR / "kalshi_private_key.pem"


def _read_private_key() -> str:
    print("\nPaste the whole Kalshi private key below.")
    print("Start with -----BEGIN RSA PRIVATE KEY-----")
    print("End with   -----END RSA PRIVATE KEY-----")
    print("Then press Enter one extra time.\n")
    lines: list[str] = []
    while True:
        line = input()
        if not line and lines:
            break
        if line or lines:
            lines.append(line)
        if "END RSA PRIVATE KEY" in line or "END PRIVATE KEY" in line:
            break
    key = "\n".join(lines).strip()
    if "BEGIN" not in key or "END" not in key:
        raise SystemExit("That did not look like a private key. Nothing was changed.")
    return key + "\n"


def _write_env(key_id: str) -> None:
    existing = ENV_PATH.read_text(encoding="utf-8").splitlines() if ENV_PATH.exists() else []
    remove_keys = {"KALSHI_API_KEY", "KALSHI_ACCESS_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"}
    kept: list[str] = []
    in_pem_block = False
    for line in existing:
        stripped = line.strip()
        if "BEGIN " in stripped and "PRIVATE KEY" in stripped:
            in_pem_block = True
            continue
        if "END " in stripped and "PRIVATE KEY" in stripped:
            in_pem_block = False
            continue
        if in_pem_block:
            continue
        if not stripped or stripped.startswith("#"):
            kept.append(line)
            continue
        if "=" not in stripped:
            continue
        key = stripped.split("=", 1)[0].strip()
        if key in remove_keys:
            continue
        kept.append(line)
    kept.insert(0, "KALSHI_PRIVATE_KEY_PATH=./secrets/kalshi_private_key.pem")
    kept.insert(0, f"KALSHI_ACCESS_KEY_ID={key_id}")
    ENV_PATH.write_text("\n".join(kept).rstrip() + "\n", encoding="utf-8")


def main() -> None:
    print("TAPESTRY Kalshi setup")
    print("--------------------")
    key_id = input("Paste your Kalshi API Key ID, not the private key: ").strip()
    if not key_id:
        raise SystemExit("No Key ID entered. Nothing was changed.")
    private_key = _read_private_key()
    SECRETS_DIR.mkdir(exist_ok=True)
    KEY_PATH.write_text(private_key, encoding="utf-8")
    _write_env(key_id)
    print("\nDone.")
    print(f"Private key saved to: {KEY_PATH}")
    print(f".env updated at:       {ENV_PATH}")
    print("\nNow run:")
    print(r"  .\.venv\Scripts\python.exe -m scrapers.kalshi_scraper")


if __name__ == "__main__":
    main()
