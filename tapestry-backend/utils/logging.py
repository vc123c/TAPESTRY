from __future__ import annotations

import logging
import os
from datetime import date
from pathlib import Path


def setup_logging(name: str = "tapestry") -> logging.Logger:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logger = logging.getLogger(name)
    if logger.handlers:
      return logger

    root = Path(__file__).resolve().parents[1]
    log_dir = root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    console.setLevel(level)
    file_handler = logging.FileHandler(log_dir / f"tapestry_{date.today().isoformat()}.log", encoding="utf-8")
    file_handler.setFormatter(fmt)
    file_handler.setLevel(level)

    logger.setLevel(level)
    logger.addHandler(console)
    logger.addHandler(file_handler)
    logger.propagate = False
    return logger
