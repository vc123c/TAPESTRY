from __future__ import annotations

import hashlib
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.connection import write_connection
from scrapers.race_web_scraper import RaceWebScraper


CURATED_ROWS = {
    "AZ-09": [
        {
            "headline": "Arizona 9th House Race 2026",
            "url": "https://www.cookpolitical.com/custom_entity/481796",
            "source_name": "Cook Political Report",
            "published_at": "2026-04-16",
            "summary": "Cook Political Report lists AZ-09 as an incumbent-running Solid R House race with Paul Gosar as incumbent.",
        },
        {
            "headline": "Who Is Running in Arizona Congressional District 9th in 2026?",
            "url": "https://battlegroundvote.com/arizona/house/9",
            "source_name": "Battleground Vote",
            "published_at": "2026-03-02",
            "summary": "Battleground Vote tracks the 2026 Arizona 9th Congressional District race and candidate field.",
        },
    ],
    "RI-01": [
        {
            "headline": "Amo Marks United States and United Kingdom Special Relationship During King Charles III Visit to Congress",
            "url": "https://amo.house.gov/",
            "source_name": "Congressman Gabe Amo",
            "published_at": "2026-04-28",
            "summary": "Rep. Gabe Amo's official office latest-news page includes April 2026 activity for Rhode Island's 1st Congressional District.",
        },
        {
            "headline": "Rhode Island's 1st congressional district profile",
            "url": "https://en.wikipedia.org/wiki/Rhode_Island%27s_1st_congressional_district",
            "source_name": "Wikipedia",
            "published_at": "2026-04-29",
            "summary": "Reference profile for Rhode Island's 1st Congressional District, represented by Gabe Amo.",
        },
    ],
    "SC-03": [
        {
            "headline": "South Carolina congresswoman Sheri Biggs files for reelection",
            "url": "https://www.wyff4.com/article/south-carolina-congresswoman-sheri-biggs-reelection/70804827",
            "source_name": "WYFF 4",
            "published_at": "2026-03-20",
            "summary": "WYFF reports Rep. Sheri Biggs filed for reelection in South Carolina's 3rd Congressional District.",
        },
        {
            "headline": "Clemson Democrat Eunice Lehmacher announces challenge to unseat GOP Rep. Sheri Biggs",
            "url": "https://nationaltoday.com/us/sc/clemson/news/2026/03/12/clemson-democrat-eunice-lehmacher-announces-challenge-to-unseat-gop-rep-sheri-biggs/",
            "source_name": "Clemson Today",
            "published_at": "2026-03-12",
            "summary": "Clemson Today covers Democrat Eunice Lehmacher's challenge to Rep. Sheri Biggs in SC-03.",
        },
    ],
    "SC-04": [
        {
            "headline": "Rep. Timmons officially files for re-election, facing 2 challengers in GOP primary",
            "url": "https://www.foxcarolina.com/2026/03/25/rep-timmons-officially-files-re-election-gop-primary/",
            "source_name": "Fox Carolina",
            "published_at": "2026-03-25",
            "summary": "Fox Carolina reports William Timmons filed for reelection in South Carolina's 4th Congressional District.",
        },
        {
            "headline": "4th Congressional District candidate could make state history in 2026 election",
            "url": "https://www.southcarolinapublicradio.org/sc-news/2026-03-20/4th-congressional-district-candidate-could-make-state-history-in-2026-election",
            "source_name": "South Carolina Public Radio",
            "published_at": "2026-03-20",
            "summary": "South Carolina Public Radio covers Courtney McClain's 2026 campaign in SC-04.",
        },
    ],
    "SC-05": [
        {
            "headline": "Race for SC's 5th Congressional District heats up with new candidates",
            "url": "https://www.heraldonline.com/news/local/article312353321.html",
            "source_name": "Rock Hill Herald",
            "published_at": "2025-10-02",
            "summary": "The Rock Hill Herald reports on the developing open-seat race in South Carolina's 5th Congressional District.",
        },
        {
            "headline": "Ralph Norman set to launch South Carolina gubernatorial bid",
            "url": "https://www.politico.com/news/2025/07/25/ralph-norman-south-carolina-governor-00477022",
            "source_name": "Politico",
            "published_at": "2025-07-25",
            "summary": "Politico reports Rep. Ralph Norman's governor bid, creating the SC-05 open-seat context.",
        },
    ],
    "SC-06": [
        {
            "headline": "US Rep. Jim Clyburn announces campaign for re-election",
            "url": "https://www.foxcarolina.com/2026/03/12/live-us-rep-jim-clyburn-make-campaign-announcement/",
            "source_name": "Fox Carolina",
            "published_at": "2026-03-12",
            "summary": "Fox Carolina reports Rep. Jim Clyburn will seek another term representing South Carolina's 6th Congressional District.",
        },
        {
            "headline": "Rep. Clyburn touts $50 million secured for projects in SC's 6th Congressional District",
            "url": "https://www.abccolumbia.com/2026/01/27/rep-clyburn-touts-50-million-secured-for-projects-in-scs-6th-congressional-district/",
            "source_name": "ABC Columbia",
            "published_at": "2026-01-27",
            "summary": "ABC Columbia reports on federal project funding Rep. Clyburn secured for SC-06.",
        },
    ],
    "SC-07": [
        {
            "headline": "Congressman Fry, President Trump Deliver $10.8 Million in Funding for South Carolina's Seventh District",
            "url": "https://fry.house.gov/news/documentsingle.aspx?DocumentID=1029",
            "source_name": "Congressman Russell Fry",
            "published_at": "2026-01-27",
            "summary": "Rep. Russell Fry's office reports funding secured for South Carolina's 7th Congressional District.",
        },
        {
            "headline": "Latest News",
            "url": "https://fry.house.gov/news/",
            "source_name": "Congressman Russell Fry",
            "published_at": "2026-02-04",
            "summary": "Rep. Russell Fry's latest-news page tracks district-facing updates for SC-07.",
        },
    ],
    "SD-AL": [
        {
            "headline": "Fact brief: Has South Dakota always had 1 U.S. House seat?",
            "url": "https://www.sdnewswatch.org/fact-brief-sd-us-house-seats-census-2026-election/",
            "source_name": "South Dakota News Watch",
            "published_at": "2026-04-23",
            "summary": "South Dakota News Watch explains the state's at-large U.S. House representation and reapportionment history.",
        },
        {
            "headline": "South Dakota congressional districts: from three seats to one at-large district",
            "url": "https://www.kotatv.com/2026/04/26/fact-brief-has-south-dakota-always-had-1-us-house-seat/",
            "source_name": "KOTA TV",
            "published_at": "2026-04-26",
            "summary": "KOTA carries South Dakota News Watch's explainer on the state's single at-large House district.",
        },
    ],
}


def article_id(district_id: str, url: str, headline: str) -> str:
    return hashlib.sha1(f"{district_id}:{url or headline}".encode("utf-8")).hexdigest()


def main() -> None:
    RaceWebScraper()._ensure_schema()
    keep = [
        "article_id", "district_id", "candidate_name", "query", "published_at", "headline", "url",
        "source_name", "source_type", "event_type", "incumbent_relevant", "topic_tags", "ideology_tags",
        "salience_score", "sentiment", "race_specific", "summary", "fetched_at",
    ]
    placeholders = ",".join(["?"] * len(keep))
    inserted = 0
    with write_connection() as con:
        for district_id, articles in CURATED_ROWS.items():
            for article in articles:
                text = f"{article['headline']} {article['summary']}"
                row = {
                    "article_id": article_id(district_id, article["url"], article["headline"]),
                    "district_id": district_id,
                    "candidate_name": None,
                    "query": "curated remaining district coverage",
                    "published_at": datetime.fromisoformat(article["published_at"]),
                    "headline": article["headline"],
                    "url": article["url"],
                    "source_name": article["source_name"],
                    "source_type": "CURATED COVERAGE",
                    "event_type": "race",
                    "incumbent_relevant": True,
                    "topic_tags": RaceWebScraper._tags(text),
                    "ideology_tags": [],
                    "salience_score": 0.38,
                    "sentiment": RaceWebScraper._sentiment(text),
                    "race_specific": True,
                    "summary": article["summary"],
                    "fetched_at": datetime.utcnow(),
                }
                con.execute(
                    f"INSERT OR REPLACE INTO race_web_articles ({', '.join(keep)}) VALUES ({placeholders})",
                    [row[col] for col in keep],
                )
                inserted += 1
        con.commit()
    print(f"Seeded curated remaining district news rows: {inserted}")


if __name__ == "__main__":
    main()
