import os
import asyncio
from typing import List, Optional
from datetime import datetime
import logging

from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel

from sheets import SheetsManager
from scraper import get_random_industry_id, search_companies, get_company_details
from email_extractor import extract_contact_info

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Outreach Agent API")

APIFY_SHEET_URL = "https://docs.google.com/spreadsheets/d/1uRxPzWt5JH7gb5Kz0mqM4oCeink6h73tjF3_q641bI8/edit?gid=0#gid=0"
OUTREACH_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UfXT9v3Tw8TAco-MFZSXQQwXOYxCrbGQCgp4QFkhTZA/edit?gid=0#gid=0"
CREDENTIALS_PATH = "credentials.json"
INDUSTRY_CSV_PATH = "industry_id.csv"

# In-memory status for long-running scrape jobs
scraping_jobs = {}

class ScrapeRequest(BaseModel):
    keywords: str
    location: str
    scrap_count: int

class ScrapeResponse(BaseModel):
    message: str
    job_id: str

async def perform_scraping_loop(job_id: str, request: ScrapeRequest):
    """Background task to run the scraping loop until target_count is reached."""
    target_count = request.scrap_count
    logger.info(f"Starting scraping job {job_id} for {target_count} entries.")
    
    try:
        sheets_manager = SheetsManager(CREDENTIALS_PATH)
        apify_keys = sheets_manager.get_apify_keys(APIFY_SHEET_URL)
        current_key_index = 0
    except Exception as e:
        logger.error(f"Job {job_id} failed to initialize sheets/key: {e}")
        scraping_jobs[job_id] = {"status": "failed", "error": str(e)}
        return

    successful_entries = 0
    scraping_jobs[job_id] = {"status": "in_progress", "progress": 0, "target": target_count}

    while successful_entries < target_count:
        try:
            # 1. Pick a random industry ID
            industry_id = get_random_industry_id(INDUSTRY_CSV_PATH)
            
            # 2. Search LinkedIn for companies (we only need enough to satisfy the remaining count)
            items_to_fetch = min(20, target_count - successful_entries + 5) # Fetch a few extra to account for failures
            
            current_api_key = apify_keys[current_key_index]
            
            try:
                search_results = search_companies(current_api_key, industry_id, request.keywords, request.location, max_items=items_to_fetch)
            except RuntimeError as e:
                if str(e) == "APIFY_LIMIT_REACHED":
                    logger.warning(f"Key {current_key_index} reached limit. Attempting to rotate...")
                    current_key_index += 1
                    if current_key_index >= len(apify_keys):
                        logger.error("All Apify API keys have exhausted their limits!")
                        scraping_jobs[job_id]["status"] = "failed"
                        scraping_jobs[job_id]["error"] = "All Apify API keys exhausted."
                        return
                    continue # Retry with the next key!
                else:
                    raise e
            
            if not search_results:
                logger.info("No companies found for this industry. Retrying...")
                continue
                
            for company_summary in search_results:
                if successful_entries >= target_count:
                    break
                # The search scraper returns people profiles.
                # We need to extract their current company URL.
                company_url = None
                
                # Check if there is past/current company data
                # Typically apify.linkedin-profile-search returns something like:
                # { "position": [...], "company": "...", "companyUrl": "..." }
                # Or it might be nested under experience.
                
                # Let's try some common paths based on typical Apify outputs:
                if "companyUrl" in company_summary and company_summary["companyUrl"]:
                    company_url = company_summary["companyUrl"]
                elif "positions" in company_summary and company_summary["positions"]:
                    # Try to get the first position's company url
                    for pos in company_summary["positions"]:
                        if pos.get("companyUrl"):
                            company_url = pos["companyUrl"]
                            break
                            
                elif "experience" in company_summary and company_summary["experience"]:
                    for exp in company_summary["experience"]:
                        if exp.get("companyLinkedinUrl"):
                            company_url = exp["companyLinkedinUrl"]
                            break
                        elif exp.get("companyUrl"):
                            company_url = exp["companyUrl"]
                            break
                
                if not company_url:
                    logger.info(f"Could not find a company URL for profile {company_summary.get('url')}")
                    continue
                    
                logger.info(f"Found company URL: {company_url}")
                # 3. Get detailed company info
                # The prompt explicitly asked to use the first api key (or keep company details on working api key).
                # But since we're rotating keys, we can just use the currently active one or keep an array of keys just for details if required.
                # Let's use the current rotating key, but typically they share the same Apify account quota anyway unless from different accounts.
                details = get_company_details(current_api_key, company_url)
                if not details:
                    continue
                    
                website_url = details.get("websiteUrl")
                if not website_url:
                    logger.info(f"No website URL found for {details.get('name')}. Skipping.")
                    continue
                    
                # 4. Extract Emails & Phones
                emails = []
                phones = []
                try:
                    emails, phones = await extract_contact_info(website_url)
                except Exception as e:
                    logger.error(f"Email extraction failed for {website_url}: {e}")
                
                # 5. Prepare Data for Sheet
                # cols: company_name website linkedin_company industry company_size location description scraped_at emails phones company_keywords pain_points status email_sent_at
                company_name = details.get("name", "")
                linkedin_company = details.get("url", "")
                industry_list = details.get("industry", [])
                industry = ", ".join(industry_list) if isinstance(industry_list, list) else str(industry_list)
                company_size = str(details.get("employeeCount", ""))
                
                hq = details.get("headquarter", {})
                location = f"{hq.get('city', '')}, {hq.get('country', '')}".strip(', ')
                
                description = details.get("description", "")
                scraped_at = datetime.now().isoformat()
                
                emails_str = ", ".join(emails)
                phones_str = ", ".join(phones)
                
                # We leave some custom cols blank or default
                company_keywords = ""
                pain_points = ""
                status = "New"
                email_sent_at = ""
                
                row_data = [
                    company_name,
                    website_url,
                    linkedin_company,
                    industry,
                    company_size,
                    location,
                    description,
                    scraped_at,
                    emails_str,
                    phones_str,
                    company_keywords,
                    pain_points,
                    status,
                    email_sent_at
                ]
                
                # 6. Append to Google Sheet
                try:
                    sheets_manager.append_company_data(OUTREACH_SHEET_URL, row_data)
                    successful_entries += 1
                    scraping_jobs[job_id]["progress"] = successful_entries
                    logger.info(f"Successfully added entry {successful_entries}/{target_count}: {company_name}")
                except Exception as e:
                    logger.error(f"Failed to append row for {company_name}: {e}")
                    
        except Exception as e:
            logger.error(f"Error in scraping loop: {e}")
            await asyncio.sleep(5) # Delay on error
            continue

    scraping_jobs[job_id]["status"] = "completed"
    logger.info(f"Job {job_id} finished successfully.")


@app.post("/scrap", response_model=ScrapeResponse)
async def start_scraping(request: ScrapeRequest, background_tasks: BackgroundTasks):
    """
    Endpoint to start scraping a given number of new companies.
    Runs asynchronously since scraping takes a long time.
    """
    if request.scrap_count <= 0:
        raise HTTPException(status_code=400, detail="Count must be greater than 0")
        
    job_id = f"job_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    # Run the scraping loop in the background
    background_tasks.add_task(perform_scraping_loop, job_id, request)
    
    return {"message": f"Started scraping {request.scrap_count} companies in the background.", "job_id": job_id}


@app.get("/job/{job_id}")
async def get_job_status(job_id: str):
    """Check the status of a scraping job."""
    if job_id not in scraping_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return scraping_jobs[job_id]

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
