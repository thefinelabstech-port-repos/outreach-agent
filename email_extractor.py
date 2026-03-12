import asyncio
import re
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# ── Default test URL (change as needed) ──────────────────────
url = "https://headsin.co"

# Contact-related subpages to check
CONTACT_KEYWORDS = ["contact", "about", "team", "support", "help", "reach", "connect", "get-in-touch"]

# Email regex
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# Phone regex — handles Indian (+91), US, and generic international formats
PHONE_RE = re.compile(
    r"(?:"
    r"\+?[\d]{1,3}[\s.\-]?"          # optional country code
    r")?(?:\(?\d{3,5}\)?[\s.\-]?)"   # area code
    r"\d{3,5}[\s.\-]?"               # first block
    r"\d{3,5}"                        # second block
)


def _clean_phone(raw: str) -> str:
    """Strip everything except digits and leading +."""
    clean = re.sub(r"[^\d+]", "", raw)
    return clean


def _is_valid_phone(clean: str) -> bool:
    """Accept phone strings between 7 and 15 digits."""
    digits = re.sub(r"\D", "", clean)
    return 7 <= len(digits) <= 15


async def extract_from_page(page, target_url: str, emails: set, phone_numbers: set):
    """
    Navigate to target_url, wait for full JS render, then scrape
    emails and phone numbers from the fully rendered DOM.
    """
    try:
        # networkidle waits until no network requests for 500ms — catches SPA renders
        await page.goto(target_url, wait_until="networkidle", timeout=30000)
    except Exception:
        try:
            # Fallback: domcontentloaded if networkidle times out
            await page.goto(target_url, wait_until="domcontentloaded", timeout=20000)
        except Exception as e:
            print(f"  [ERROR] Cannot load {target_url}: {e}")
            return None

    # Extra wait for JS frameworks to finish rendering
    await asyncio.sleep(2)

    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    # ── 1. mailto: links ────────────────────────────────────────
    for a in soup.select("a[href^='mailto:']"):
        raw = a["href"].replace("mailto:", "").strip()
        match = EMAIL_RE.search(raw)
        if match:
            emails.add(match.group(0).lower())

    # ── 2. Regex over all visible text ─────────────────────────
    text = soup.get_text(separator=" ", strip=True)
    for m in EMAIL_RE.findall(text):
        emails.add(m.lower())

    # ── 3. Regex over raw HTML (catches obfuscated emails in attributes) ─
    for m in EMAIL_RE.findall(html):
        emails.add(m.lower())

    # ── 4. tel: links ──────────────────────────────────────────
    for a in soup.select("a[href^='tel:']"):
        raw = a["href"].replace("tel:", "").strip()
        clean = _clean_phone(raw)
        if _is_valid_phone(clean):
            phone_numbers.add(clean)

    # ── 5. Phone regex over page text ──────────────────────────
    for raw in PHONE_RE.findall(text):
        clean = _clean_phone(raw)
        if _is_valid_phone(clean):
            phone_numbers.add(clean)

    # Filter out obviously bad "emails" (image paths, version strings, etc.)
    bad_domains = {".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp", ".css", ".js"}
    emails_to_remove = {e for e in emails if any(e.endswith(d) for d in bad_domains)}
    emails -= emails_to_remove

    return soup


async def extract_contact_info(start_url: str):
    """
    Crawl start_url and linked contact/about pages.
    Returns (list_of_emails, list_of_phones).
    """
    emails: set = set()
    phone_numbers: set = set()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            )
        )

        page = await context.new_page()

        # ── Step 1: Homepage ───────────────────────────────────
        print(f"  Scraping homepage: {start_url}")
        soup = await extract_from_page(page, start_url, emails, phone_numbers)

        # ── Step 2: Find contact/about subpages ────────────────
        urls_to_visit: set = set()
        if soup:
            base_domain = urlparse(start_url).netloc
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"].strip()
                link_text = a_tag.get_text().lower()

                if any(kw in href.lower() or kw in link_text for kw in CONTACT_KEYWORDS):
                    full_url = urljoin(start_url, href)
                    parsed = urlparse(full_url)
                    if parsed.netloc == base_domain and parsed.scheme in ("http", "https"):
                        clean_url = full_url.split("#")[0].rstrip("/")
                        if clean_url != start_url.rstrip("/"):
                            urls_to_visit.add(clean_url)

        await page.close()

        # ── Step 3: Crawl subpages concurrently ────────────────
        if urls_to_visit:
            print(f"  Found {len(urls_to_visit)} subpage(s) to crawl: {urls_to_visit}")

            async def fetch(sub_url: str):
                try:
                    p2 = await context.new_page()
                    await extract_from_page(p2, sub_url, emails, phone_numbers)
                    await p2.close()
                except Exception as ex:
                    print(f"  [WARN] Subpage error ({sub_url}): {ex}")

            await asyncio.gather(*[fetch(u) for u in urls_to_visit])
        else:
            print("  No contact subpages found; results from homepage only.")

        await context.close()
        await browser.close()

    return list(emails), list(phone_numbers)


if __name__ == "__main__":
    found_emails, found_phones = asyncio.run(extract_contact_info(url))
    print("\n--- Final Results ---")
    print("Found emails:", found_emails)
    print("Found phone numbers:", found_phones)