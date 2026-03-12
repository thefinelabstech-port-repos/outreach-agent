import asyncio
import logging
from datetime import datetime

from sheets import SheetsManager
from scraper import search_companies, get_company_details
from email_extractor import extract_contact_info

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

APIFY_SHEET_URL = "https://docs.google.com/spreadsheets/d/1uRxPzWt5JH7gb5Kz0mqM4oCeink6h73tjF3_q641bI8/edit?gid=0#gid=0"
OUTREACH_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UfXT9v3Tw8TAco-MFZSXQQwXOYxCrbGQCgp4QFkhTZA/edit?gid=0#gid=0"
CREDENTIALS_PATH = "credentials.json"


async def run_sample_scraping(keywords: str, location: str, scrap_count: int, industry_id: str):
    logger.info(f"Starting sample scraping for {scrap_count} entries. Industry: {industry_id}, Keywords: {keywords}, Location: {location}")
    
    try:
        sheets_manager = SheetsManager(CREDENTIALS_PATH)
        apify_keys = sheets_manager.get_apify_keys(APIFY_SHEET_URL)
        current_key_index = 0
    except Exception as e:
        logger.error(f"Failed to initialize sheets/key: {e}")
        return

    successful_entries = 0

    # 1. Search LinkedIn for profiles
    search_results = []
    
    while current_key_index < len(apify_keys):
        current_api_key = apify_keys[current_key_index]
        try:
            search_results = search_companies(current_api_key, industry_id, keywords, location, max_items=5)
            break
        except RuntimeError as e:
            if str(e) == "APIFY_LIMIT_REACHED":
                logger.warning(f"Key {current_key_index} reached limit. Attempting to rotate...")
                current_key_index += 1
            else:
                logger.error(f"Failed to search: {e}")
                return
                
    if not search_results:
        logger.warning("No profiles found or all API keys exhausted for this search criteria.")
        return
        
    for profile_summary in search_results:
        if successful_entries >= scrap_count:
            break
            
        company_url = None
        
        # Extract their current company URL from the profile
        if "companyUrl" in profile_summary and profile_summary["companyUrl"]:
            company_url = profile_summary["companyUrl"]
        elif "positions" in profile_summary and profile_summary["positions"]:
            for pos in profile_summary["positions"]:
                if pos.get("companyUrl"):
                    company_url = pos["companyUrl"]
                    break
        elif "experience" in profile_summary and profile_summary["experience"]:
            for exp in profile_summary["experience"]:
                if exp.get("companyLinkedinUrl"):
                    company_url = exp["companyLinkedinUrl"]
                    break
                elif exp.get("companyUrl"):
                    company_url = exp["companyUrl"]
                    break
        
        if not company_url:
            logger.info(f"Could not find a company URL for profile {profile_summary.get('url')}")
            continue
            
        logger.info(f"==> Found company URL: {company_url}")
        
        # 2. Get detailed company info
        details = get_company_details(apify_keys[current_key_index], company_url)
        if not details:
            logger.info(f"Could not fetch details for company {company_url}")
            continue
            
        website_url = details.get("websiteUrl")
        if not website_url:
            logger.info(f"No website URL found for {details.get('name')}. Skipping contact extraction.")
        
        # 3. Extract Emails & Phones
        emails = []
        phones = []
        if website_url:
            try:
                emails, phones = await extract_contact_info(website_url)
            except Exception as e:
                logger.error(f"Email extraction failed for {website_url}: {e}")
        
        # 4. Prepare Data for Sheet
        company_name = details.get("name", "")
        linkedin_company = details.get("url", "")
        industry_list = details.get("industry", [])
        industry = ", ".join(industry_list) if isinstance(industry_list, list) else str(industry_list)
        company_size = str(details.get("employeeCount", ""))
        
        hq = details.get("headquarter", {})
        loc = f"{hq.get('city', '')}, {hq.get('country', '')}".strip(', ')
        
        description = details.get("description", "")
        scraped_at = datetime.now().isoformat()
        
        emails_str = ", ".join(emails)
        phones_str = ", ".join(phones)
        
        row_data = [
            company_name,
            website_url or "",
            linkedin_company,
            industry,
            company_size,
            loc,
            description,
            scraped_at,
            emails_str,
            phones_str,
            "", # company_keywords
            "", # pain_points
            "New", # status
            "" # email_sent_at
        ]
        
        # 5. Append to Google Sheet
        try:
            sheets_manager.append_company_data(OUTREACH_SHEET_URL, row_data)
            successful_entries += 1
            logger.info(f"Successfully added entry {successful_entries}/{scrap_count}: {company_name}")
        except Exception as e:
            logger.error(f"Failed to append row for {company_name}: {e}")

    logger.info("Sample scraping finished.")

if __name__ == "__main__":
    # Parameters that you can control manually
    PARAMS = {
        "keywords": "Founders and CEO and CTO",
        "location": "Bangalore",
        "scrap_count": 1,
        "industry_id": "11" # We hardcode this one to avoid randomness during testing
    }
    
    asyncio.run(run_sample_scraping(
        keywords=PARAMS["keywords"],
        location=PARAMS["location"],
        scrap_count=PARAMS["scrap_count"],
        industry_id=PARAMS["industry_id"]
    ))
