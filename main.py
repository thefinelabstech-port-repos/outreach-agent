"""
Outreach Agent — top-level orchestrator.

Stages:
  1. scrape   — pull company details from LinkedIn via Apify and store to Google Sheet
  2. enrich   — (planned) AI-enrich keywords / pain points per lead
  3. outreach — (planned) send cold email sequences via Brevo

Usage:
  python main.py           # run full pipeline (scrape only for now)
  python main.py scrape    # run scraper stage explicitly
"""

import asyncio
import sys
from scraper.company_details_extractor import main as run_scraper


def scrape():
    asyncio.run(run_scraper())


def enrich():
    print("[Enrich] Not implemented yet.")


def outreach():
    print("[Outreach] Not implemented yet.")


STAGES = {
    "scrape":   scrape,
    "enrich":   enrich,
    "outreach": outreach,
}

if __name__ == "__main__":
    stage = sys.argv[1] if len(sys.argv) > 1 else "scrape"

    if stage not in STAGES:
        print(f"Unknown stage '{stage}'. Choose from: {', '.join(STAGES)}")
        sys.exit(1)

    STAGES[stage]()
