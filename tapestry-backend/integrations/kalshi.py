from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

import httpx
from utils.logging import setup_logging

logger = setup_logging(__name__)


@dataclass
class KalshiMarket:
    market_id: str
    ticker: str
    yes_price: Optional[float]
    volume: Optional[float]
    open_interest: Optional[float]
    district_id: Optional[str] = None


class KalshiClient:
    base_url = "https://trading-api.kalshi.com/trade-api/v2"

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("KALSHI_API_KEY")

    async def get_markets(self) -> list[KalshiMarket]:
        if not self.api_key:
            logger.warning("KALSHI_API_KEY not set; returning no Kalshi markets")
            return []
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{self.base_url}/markets",
                params={"limit": 100, "category": "politics"},
                headers={"Authorization": f"Bearer {self.api_key}"},
            )
            if response.status_code == 401:
                logger.warning("Kalshi authentication failed; returning no markets")
                return []
            response.raise_for_status()
            return [
                KalshiMarket(
                    m.get("id") or m.get("ticker"),
                    m.get("ticker"),
                    m.get("yes_bid") or m.get("last_price"),
                    m.get("volume_24h") or m.get("volume"),
                    m.get("open_interest"),
                )
                for m in response.json().get("markets", [])
            ]

    @staticmethod
    def detect_gap(model_price: float, kalshi_price: float | None, threshold: float = 0.04) -> dict:
        if kalshi_price is None:
            return {"gap": None, "flag": False, "explanation": None}
        gap = model_price - kalshi_price
        return {"gap": gap, "flag": abs(gap) > threshold, "explanation": f"TAPESTRY differs from Kalshi by {gap:.1%}."}
