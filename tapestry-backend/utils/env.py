from __future__ import annotations

import os
from pathlib import Path


def load_local_env(path: str | Path = ".env") -> None:
    """
    Small permissive .env loader.

    python-dotenv is strict and warns when users paste PEM blocks directly into
    .env files. This loader keeps normal KEY=value behavior, ignores comments,
    and tolerates continuation lines by appending them to the previous key.
    """
    env_path = Path(path)
    if not env_path.exists():
        return

    current_key: str | None = None
    current_parts: list[str] = []

    def flush() -> None:
        nonlocal current_key, current_parts
        if current_key and current_parts:
            value = "\n".join(current_parts).strip().strip('"').strip("'")
            if current_key not in os.environ or os.environ.get(current_key, "") == "":
                os.environ[current_key] = value
        current_key = None
        current_parts = []

    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line and not line.startswith("-----"):
            flush()
            key, value = line.split("=", 1)
            current_key = key.strip().lstrip("\ufeff")
            current_parts = [value.strip()]
        elif current_key:
            current_parts.append(raw_line.rstrip())
    flush()
