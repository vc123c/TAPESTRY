from __future__ import annotations

import math
import uuid
from datetime import date

import numpy as np

SEED_EVENTS = [
    ("financial_crisis_2008", "economic_shock", "", "incumbent", 0.9, -0.4, 120),
    ("tea_party_emergence", "policy_shock", "", "D", 1.0, -0.8, 240),
    ("bp_oil_spill", "scandal", "abuse_of_power", "incumbent", 0.6, -0.3, 80),
    ("romney_47_percent", "scandal", "candidate_personal", "R", 0.4, -0.7, 45),
    ("hurricane_sandy", "natural_disaster", "", "incumbent", -0.1, 0.2, 35),
    ("va_scandal", "scandal", "elite_corruption", "incumbent", 0.8, -0.5, 90),
    ("ebola_response", "policy_shock", "", "incumbent", 0.5, -0.2, 40),
    ("comey_letter_oct", "legal", "", "D", 0.7, -0.9, 18),
    ("clinton_emails", "scandal", "elite_corruption", "D", 0.9, -0.7, 180),
    ("trump_access_hollywood", "scandal", "candidate_personal", "R", 0.8, -0.8, 25),
    ("kavanaugh_hearings", "policy_shock", "", "both", 0.8, 0.0, 55),
    ("family_separation", "policy_shock", "", "R", 0.8, -0.5, 75),
    ("metoo_wave", "scandal", "abuse_of_power", "incumbent", 0.7, -0.4, 120),
    ("covid_response", "policy_shock", "", "incumbent", 1.0, -1.0, 240),
    ("george_floyd", "policy_shock", "", "both", 0.9, 0.0, 110),
    ("trump_covid_diagnosis", "natural_disaster", "", "R", 0.3, -0.2, 18),
    ("dobbs_decision", "policy_shock", "", "R", 0.9, -0.7, 220),
    ("jan6_hearings", "legal", "", "R", 0.8, -0.5, 150),
    ("fetterman_stroke", "scandal", "candidate_personal", "D", 0.4, -0.3, 45),
    ("biden_debate_performance", "scandal", "candidate_personal", "D", 0.9, -1.0, 60),
    ("trump_conviction", "legal", "", "R", 0.9, -0.6, 90),
    ("harris_substitution", "policy_shock", "", "D", 0.4, 0.7, 80),
    ("epstein_files_release", "scandal", "elite_corruption", "both", 1.0, -0.2, 120),
    ("iran_war_start_feb28", "conflict", "", "incumbent", 0.8, -0.6, 90),
]


class ScandalTokenizer:
    def __init__(self) -> None:
        self.model = None
        try:
            from sentence_transformers import SentenceTransformer
            self.model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu")
        except Exception:
            self.model = None
        self.seed_tokens = SEED_EVENTS

    def _embed(self, text: str) -> np.ndarray:
        if self.model:
            return np.asarray(self.model.encode([text])[0], dtype=np.float32)
        rng = np.random.default_rng(abs(hash(text)) % 2**32)
        return rng.normal(size=384).astype(np.float32)

    def tokenize_event(self, event_description: str, event_date: date, target_party: str, event_type: str) -> dict:
        embedding = self._embed(event_description)
        similar = [event_id for event_id, *_ in self.seed_tokens[:3]]
        return {
            "event_id": str(uuid.uuid4()),
            "event_name": event_description[:80],
            "event_date": event_date,
            "event_type": event_type,
            "primary_target_party": target_party,
            "anti_establishment_loading": 0.7,
            "partisan_loading": -0.4 if target_party in {"D", "R"} else 0.0,
            "embedding": embedding.tobytes(),
            "half_life_days": 90,
            "similar_event_ids": similar,
        }

    def compute_current_salience(self, event_id: str, as_of: date, initial_salience: float = 1.0, half_life_days: int = 90) -> float:
        days = max(0, (as_of - date(as_of.year, 1, 1)).days)
        return float(initial_salience * math.exp(-days / half_life_days))

    def compute_event_effect(self, event_id: str, district_features: dict) -> float:
        anti = float(district_features.get("independent_media_penetration", 50)) / 100
        return -1.2 * anti
