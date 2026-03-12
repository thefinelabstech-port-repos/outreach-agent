import csv
import logging
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from apify_client import ApifyClient
from sheets import SheetsManager
from email_extractor import extract_contact_info

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
CREDENTIALS_FILE = "credentials.json"
SETTINGS_SHEET_URL = "https://docs.google.com/spreadsheets/d/1uRxPzWt5JH7gb5Kz0mqM4oCeink6h73tjF3_q641bI8/edit?gid=0#gid=0"
OUTREACH_SHEET_URL = "https://docs.google.com/spreadsheets/d/1UfXT9v3Tw8TAco-MFZSXQQwXOYxCrbGQCgp4QFkhTZA/edit?gid=0#gid=0"
COMPANY_URL_FILE = "company_url.csv"
ACTOR_ID = "od6RadQV98FOARtrp"
CONCURRENCY = 2   # Reduced to 2 for better stability with large datasets (30k+ URLs)
BATCH_SIZE = 5
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds

# Thread pool for blocking Apify / gspread calls
_thread_pool = ThreadPoolExecutor(max_workers=CONCURRENCY)


# ── Helpers ──────────────────────────────────────────────────────────────────

def normalize_url(url: str) -> str:
    """Normalize a URL for duplicate comparison (strip trailing slash, lowercase)."""
    return url.strip().rstrip('/').lower()


def normalize_website_url(url: str) -> str:
    """
    Ensure a website URL has a scheme.
    Apify sometimes returns bare domains like 'headsin.co' without http/https.
    """
    url = url.strip()
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


# ── Apify (blocking → async via executor) ───────────────────────────────────

def _fetch_company_details_sync(api_key: str, company_urls: list[str]) -> list[dict]:
    """Blocking Apify call with batched URLs — runs in a thread pool."""
    client = ApifyClient(api_key)
    run_input = {
        "action": "get-companies",
        "keywords": company_urls,  # Now accepts multiple URLs
        "isUrl": True,
        "isName": False,
        "limit": len(company_urls),  # Request all results
        "location": [],
    }
    run = client.actor(ACTOR_ID).call(run_input=run_input)
    results = []
    for item in client.dataset(run["defaultDatasetId"]).iterate_items():
        results.append(item)
    return results


async def extract_company_details(loop, api_key: str, company_urls: list[str]) -> list[dict]:
    """Async wrapper: runs blocking batched Apify call off the event loop."""
    logger.info(f"[Apify] Fetching batch of {len(company_urls)} URLs: {company_urls}")
    try:
        results = await loop.run_in_executor(
            _thread_pool,
            _fetch_company_details_sync,
            api_key,
            company_urls,
        )
        return results if results else []
    except Exception as e:
        msg = str(e).lower()
        if "free user run limit reached" in msg or "quota" in msg or "limit" in msg:
            logger.warning(f"[Apify] Key limit reached for {api_key[:10]}...")
            raise RuntimeError("APIFY_LIMIT_REACHED")
        logger.error(f"[Apify] Error for batch {company_urls}: {e}")
        return []


# ── Email / phone extraction (already async) ────────────────────────────────

async def extract_emails_and_phones(website_url: str) -> tuple[list, list]:
    """Crawl company website for emails and phone numbers."""
    if not website_url:
        logger.warning("No website URL — skipping contact extraction.")
        return [], []

    website_url = normalize_website_url(website_url)
    logger.info(f"[Web] Crawling: {website_url}")

    try:
        emails, phones = await extract_contact_info(website_url)
        logger.info(f"  → {len(emails)} email(s): {emails}")
        logger.info(f"  → {len(phones)} phone(s): {phones}")

        # Fallback: try http:// if https:// returns nothing
        if not emails and not phones and website_url.startswith("https://"):
            fallback = "http://" + website_url[len("https://"):]
            logger.info(f"  Retrying with: {fallback}")
            emails, phones = await extract_contact_info(fallback)
            logger.info(f"  Fallback → {len(emails)} email(s), {len(phones)} phone(s)")

        return emails, phones
    except Exception as ex:
        logger.warning(f"[Web] Could not extract from {website_url}: {ex}", exc_info=True)
        return [], []


# ── Formatting ───────────────────────────────────────────────────────────────

def format_company_data(company_data: dict, emails: list, phones: list) -> list:
    """
    Returns a row matching the Outreach Sheet columns:
        company_name | website | linkedin_company | industry | company_size |
        location | description | scraped_at | emails | phones |
        company_keywords | pain_points | status | email_sent_at
    """
    name        = company_data.get("name", "")
    website     = company_data.get("websiteUrl", "")
    linkedin    = company_data.get("url", "")

    industry_raw = company_data.get("industry", [])
    industry = ", ".join(industry_raw) if isinstance(industry_raw, list) else str(industry_raw)

    company_size = str(company_data.get("employeeCount", ""))

    hq = company_data.get("headquarter", {}) or {}
    parts = [p for p in [hq.get("city", ""), hq.get("country", "")] if p]
    location = ", ".join(parts)

    description = company_data.get("description", "")
    scraped_at  = datetime.now().isoformat()

    return [
        name, website, linkedin, industry, company_size,
        location, description, scraped_at,
        ", ".join(emails),  # emails
        ", ".join(phones),  # phones
        "",                 # company_keywords
        "",                 # pain_points
        "Extracted",        # status
        "",                 # email_sent_at
    ]


# ── Per-URL worker ───────────────────────────────────────────────────────────

async def process_url_batch(
    url_batch: list[str],
    apify_keys: list[str],
    key_index_ref: list[int],
    existing_linkedin_urls: set,
    sheets_manager: SheetsManager,
    semaphore: asyncio.Semaphore,
    sheet_lock: asyncio.Lock,
    loop,
):
    """Process a batch of URLs (up to 5): Apify → email extract → sheet append."""
    async with semaphore:
        logger.info(f"\n{'─'*50}\n[Batch Worker] Processing batch of {len(url_batch)} URLs: {url_batch[:2]}...")
        
        for attempt in range(MAX_RETRIES):
            key_index = key_index_ref[0]
            if key_index >= len(apify_keys):
                logger.error(f"[Batch Worker] All Apify keys exhausted. Batch failed.")
                return
            
            current_key = apify_keys[key_index]

            try:
                # ── A. Apify (batched) ─────────────────────────────
                company_details_list = await extract_company_details(loop, current_key, url_batch)

                if not company_details_list:
                    logger.warning(f"[Batch Worker] No Apify data for batch (attempt {attempt + 1}). Retrying...")
                    if attempt < MAX_RETRIES - 1:
                        await asyncio.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                        continue
                    else:
                        logger.warning(f"[Batch Worker] Failed after {MAX_RETRIES} attempts. Skipping batch.")
                        return

                logger.info(f"[Batch Worker] Got {len(company_details_list)} result(s) from Apify (attempt {attempt + 1})")

                # ── B. Process each result ─────────────────────────
                processed_count = 0
                skipped_count = 0
                
                for company_details in company_details_list:
                    # Get LinkedIn URL from Apify response
                    apify_linkedin = company_details.get("url", "").strip()
                    
                    if not apify_linkedin:
                        logger.debug("[Batch Worker] No LinkedIn URL in response, skipping.")
                        continue
                    
                    apify_linkedin_normalized = normalize_url(apify_linkedin)
                    
                    async with sheet_lock:
                        # Check if already exists in sheet
                        if apify_linkedin_normalized in existing_linkedin_urls:
                            logger.info(f"[Batch Worker] DUPLICATE (LinkedIn): {apify_linkedin} — skipping.")
                            skipped_count += 1
                            continue

                        # Add to existing set to prevent duplicates within this batch
                        existing_linkedin_urls.add(apify_linkedin_normalized)

                        # Email / phone extraction
                        website_url = company_details.get("websiteUrl", "")
                        emails, phones = await extract_emails_and_phones(website_url)

                        # Format & append
                        row = format_company_data(company_details, emails, phones)
                        await loop.run_in_executor(
                            _thread_pool,
                            lambda r=row: sheets_manager.append_company_data(OUTREACH_SHEET_URL, r)
                        )

                        logger.info(f"[Batch Worker] ✓ Appended: {row[0]} | LinkedIn: {apify_linkedin}")
                        processed_count += 1

                logger.info(f"[Batch Worker] Batch complete: {processed_count} added, {skipped_count} duplicates")
                return

            except RuntimeError as e:
                if str(e) == "APIFY_LIMIT_REACHED":
                    logger.warning(f"[Batch Worker] Rate limit hit (attempt {attempt + 1}/{MAX_RETRIES}). Backing off...")
                    
                    # Only rotate key if we have multiple keys
                    if len(apify_keys) > 1 and attempt < MAX_RETRIES - 1:
                        async with sheet_lock:
                            if key_index_ref[0] == key_index:
                                key_index_ref[0] += 1
                        logger.warning(f"[Batch Worker] Rotated to key index {key_index_ref[0]}")
                    
                    # Exponential backoff before retry
                    await asyncio.sleep(RETRY_DELAY * (2 ** attempt))
                    
                else:
                    logger.error(f"[Batch Worker] RuntimeError: {e}")
                    return
            except Exception as e:
                logger.error(f"[Batch Worker] Unexpected error: {e}", exc_info=True)
                return


# ── Backfill (rows already in sheet with blank emails) ───────────────────────

async def backfill_missing_contacts(
    sheets_manager: SheetsManager,
    sheet_lock: asyncio.Lock,
    semaphore: asyncio.Semaphore,
    loop,
):
    """Find existing sheet rows with blank emails and fill them in."""
    logger.info("[Backfill] Scanning for rows with missing contact info...")
    rows_to_fill = await loop.run_in_executor(
        _thread_pool,
        lambda: sheets_manager.get_rows_missing_contacts(OUTREACH_SHEET_URL)
    )

    if not rows_to_fill:
        logger.info("[Backfill] Nothing to backfill.")
        return

    logger.info(f"[Backfill] {len(rows_to_fill)} row(s) to update.")

    async def _fill_one(row_num: int, website_url: str):
        async with semaphore:
            logger.info(f"[Backfill] Row {row_num}: {website_url}")
            emails, phones = await extract_emails_and_phones(website_url)
            emails_str = ", ".join(emails)
            phones_str = ", ".join(phones)
            async with sheet_lock:
                await loop.run_in_executor(
                    _thread_pool,
                    lambda: sheets_manager.update_contact_info(
                        OUTREACH_SHEET_URL, row_num, emails_str, phones_str
                    )
                )
            logger.info(f"[Backfill] ✓ Row {row_num} updated: emails={emails_str}")

    await asyncio.gather(*[_fill_one(r, w) for r, w in rows_to_fill])


# ── Main ─────────────────────────────────────────────────────────────────────

async def main():
    loop = asyncio.get_running_loop()

    logger.info("=" * 60)
    logger.info("Starting Company Details Extractor (async, optimized for large datasets)")
    logger.info(f"Batch Size: {BATCH_SIZE} | Concurrency: {CONCURRENCY} | Max Retries: {MAX_RETRIES}")
    logger.info("=" * 60)

    # ── 1. Init Sheets ───────────────────────────────────────────
    try:
        sheets_manager = SheetsManager(CREDENTIALS_FILE)
        logger.info("[Init] SheetsManager ready.")
    except Exception as e:
        logger.error(f"Failed to init Sheets: {e}")
        return

    # ── 2. Apify keys ────────────────────────────────────────────
    try:
        apify_keys = await loop.run_in_executor(
            _thread_pool, lambda: sheets_manager.get_apify_keys(SETTINGS_SHEET_URL)
        )
        logger.info(f"[Init] Loaded {len(apify_keys)} Apify key(s).")
    except Exception as e:
        logger.error(f"Cannot load Apify keys: {e}")
        return

    # ── 3. Existing LinkedIn URLs ────────────────────────────────
    try:
        raw_urls = await loop.run_in_executor(
            _thread_pool, lambda: sheets_manager.get_existing_linkedin_urls(OUTREACH_SHEET_URL)
        )
        existing_linkedin_urls: set = {normalize_url(u) for u in raw_urls}
        logger.info(f"[Init] {len(existing_linkedin_urls)} existing LinkedIn URL(s) in sheet.")
    except Exception as e:
        logger.warning(f"Could not load existing URLs: {e}")
        existing_linkedin_urls = set()

    semaphore  = asyncio.Semaphore(CONCURRENCY)
    sheet_lock = asyncio.Lock()

    # ── 4. Backfill existing rows with missing contacts ──────────
    await backfill_missing_contacts(sheets_manager, sheet_lock, semaphore, loop)

    # ── 5. Load CSV ──────────────────────────────────────────────
    urls_to_process = []
    try:
        with open(COMPANY_URL_FILE, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            next(reader, None)
            for row in reader:
                if row and row[0].strip():
                    u = row[0].strip()
                    if normalize_url(u) not in existing_linkedin_urls:
                        urls_to_process.append(u)
                    else:
                        logger.info(f"[CSV] SKIP (already in sheet): {u}")
    except Exception as e:
        logger.error(f"Failed to read {COMPANY_URL_FILE}: {e}")
        return

    logger.info(f"[CSV] {len(urls_to_process)} new URL(s) to process.")

    if not urls_to_process:
        logger.info("Nothing new to process. Done.")
        return

    # ── 6. Batch URLs and process concurrently ──────────────────
    key_index_ref = [0]   # shared mutable reference for Apify key rotation

    # Group URLs into batches
    url_batches = [urls_to_process[i:i + BATCH_SIZE] for i in range(0, len(urls_to_process), BATCH_SIZE)]
    logger.info(f"[Main] Total URLs: {len(urls_to_process)}")
    logger.info(f"[Main] Created {len(url_batches)} batches of max {BATCH_SIZE} URLs each")
    logger.info(f"[Main] Concurrency: {CONCURRENCY}, Max retries: {MAX_RETRIES}")

    # Process batches with progress tracking
    tasks = [
        process_url_batch(
            batch, apify_keys, key_index_ref,
            existing_linkedin_urls, sheets_manager,
            semaphore, sheet_lock, loop,
        )
        for batch in url_batches
    ]

    await asyncio.gather(*tasks)
    logger.info("\n" + "="*60)
    logger.info("All batches processed. Done.")
    logger.info("="*60)


if __name__ == "__main__":
    asyncio.run(main())
