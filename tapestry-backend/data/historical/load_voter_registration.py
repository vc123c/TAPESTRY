from __future__ import annotations

from db.connection import init_db
from scrapers.voter_registration_scraper import VoterRegistrationScraper


def run() -> int:
    init_db()
    return 0 if VoterRegistrationScraper().run() else 1


if __name__ == "__main__":
    raise SystemExit(run())
