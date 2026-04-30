from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler

from model.retrainer import TapestryRetrainer
from scrapers.ballotpedia_scraper import BallotpediaScraper
from scrapers.bls_grocery_scraper import BLSGroceryScraper
from scrapers.eia_gas_scraper import EIAGasScraper
from scrapers.fec_scraper import FECScraper
from scrapers.fred_scraper import FredScraper
from scrapers.gdelt_scraper import GDELTScraper
from scrapers.google_trends_scraper import GoogleTrendsScraper
from scrapers.house_roster_scraper import HouseRosterScraper
from scrapers.kalshi_scraper import KalshiScraper
from scrapers.local_news_scraper import LocalNewsScraper
from scrapers.pape_scraper import PapeScraper
from utils.logging import setup_logging

logger = setup_logging(__name__)


def anomaly_check() -> None:
    logger.info("Anomaly check completed")


def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone="America/Los_Angeles")
    retrainer = TapestryRetrainer()
    scheduler.add_job(retrainer.fast_update, "cron", hour=5, minute=30, id="daily_update", replace_existing=True)
    scheduler.add_job(retrainer.slow_update, "cron", day_of_week="sun", hour=3, minute=0, id="weekly_slow_update", replace_existing=True)
    scheduler.add_job(KalshiScraper().run, "interval", minutes=60, id="kalshi_refresh", replace_existing=True)
    scheduler.add_job(FredScraper().run, "cron", hour=5, minute=0, id="scraper_fred", replace_existing=True)
    scheduler.add_job(EIAGasScraper().run, "cron", day_of_week="mon", hour=6, minute=0, id="scraper_gas", replace_existing=True)
    scheduler.add_job(BLSGroceryScraper().run, "cron", day=1, hour=6, minute=0, id="scraper_grocery", replace_existing=True)
    scheduler.add_job(FECScraper().run, "cron", day_of_week="sun", hour=7, minute=0, id="scraper_fec", replace_existing=True)
    scheduler.add_job(BallotpediaScraper().run, "cron", day_of_week="sun", hour=7, minute=30, id="scraper_ballotpedia", replace_existing=True)
    scheduler.add_job(HouseRosterScraper().run, "cron", day=1, hour=3, minute=45, id="scraper_house_roster", replace_existing=True)
    scheduler.add_job(PapeScraper().run, "interval", hours=6, id="scraper_pape", replace_existing=True)
    scheduler.add_job(GDELTScraper().run, "cron", hour=4, minute=0, id="scraper_gdelt", replace_existing=True)
    scheduler.add_job(GoogleTrendsScraper().run, "cron", hour=4, minute=30, id="scraper_trends", replace_existing=True)
    scheduler.add_job(LocalNewsScraper().run, "cron", hour=4, minute=45, id="scraper_local_news", replace_existing=True)
    scheduler.add_job(anomaly_check, "interval", minutes=30, id="anomaly_check", replace_existing=True)
    return scheduler


if __name__ == "__main__":
    sched = create_scheduler()
    sched.start()
    logger.info("TAPESTRY scheduler started")
    try:
        import time
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        sched.shutdown()
