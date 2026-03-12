import csv
import random
import logging
from typing import Dict, Any, List, Optional
from apify_client import ApifyClient

logger = logging.getLogger(__name__)

def get_random_industry_id(csv_path: str) -> str:
    """Reads the industry_id.csv and returns a random industry id."""
    industry_ids = []
    try:
        with open(csv_path, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get('id'):
                    industry_ids.append(row['id'])
    except Exception as e:
        logger.error(f"Error reading {csv_path}: {e}")
        raise e
    
    if not industry_ids:
        raise ValueError("No industry IDs found in the CSV file.")
    
    return random.choice(industry_ids)

def search_companies(api_key: str, industry_id: str, keywords: str, location: str, max_items: int = 1) -> List[Dict[str, Any]]:
    """Uses LinkedIn Search Scraper to find companies based on keywords and industry ID."""
    logger.info(f"Starting LinkedIn search for industry ID: {industry_id}")
    client = ApifyClient(api_key)
    
    run_input = {
        "profileScraperMode": "Full",
        "searchQuery": keywords,
        "maxItems": max_items, # we just need one company usually, but maybe we fetch a few
        "industryIds": [industry_id], # Pass industry ID as a list
        "startPage": 1,
    }
    
    if location:
        run_input["locations"] = [location]
    else:
        run_input["locations"] = []

    try:
        # Run the LinkedIn Search Scraper
        run = client.actor("M2FMdjRVeF1HPGFcc").call(run_input=run_input)
        
        # Check if hitting a soft limit (since Apify doesn't throw exceptions here)
        status_msg = run.get("statusMessage", "").lower()
        if "free user run limit reached" in status_msg or "quota" in status_msg or "limit" in status_msg:
            logger.warning(f"Apify key limit reached for {api_key[:10]}... (Status Msg: {status_msg})")
            raise RuntimeError("APIFY_LIMIT_REACHED")
            
        results = []
        # We need to extract the company url. Depending on the exact output of this actor:
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            results.append(item)
            
        logger.info(f"Found {len(results)} profile items from search.")
        return results
    except Exception as e:
        error_msg = str(e).lower()
        if "free user run limit reached" in error_msg or "quota" in error_msg or "limit" in error_msg:
            logger.warning(f"Apify key limit reached for {api_key[:10]}...")
            raise RuntimeError("APIFY_LIMIT_REACHED")
        logger.error(f"Error during search_companies: {e}")
        return []

def get_company_details(api_key: str, company_url: str) -> Optional[Dict[str, Any]]:
    """Uses Company Details Scraper to get information about a company given its LinkedIn URL."""
    if not company_url:
        logger.warning("Empty company URL provided to get_company_details.")
        return None
        
    logger.info(f"Fetching company details for URL: {company_url}")
    client = ApifyClient(api_key)
    
    run_input = {
        "action": "get-profiles",
        "keywords": [company_url], # The notes say keywords can be url? "isUrl": True
        "isUrl": True,
        "isName": False,
        "limit": 1,
        "location": None,
    }

    try:
        # Run the Company Details Scraper
        run = client.actor("od6RadQV98FOARtrp").call(run_input=run_input)
        
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            # Expecting to find company details here
            return item
            
        return None
    except Exception as e:
        logger.error(f"Error during get_company_details: {e}")
        return None
