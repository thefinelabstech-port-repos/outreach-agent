import os
import gspread
from google.oauth2.service_account import Credentials
import logging

logger = logging.getLogger(__name__)

# Scopes for Google Sheets and Drive
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

class SheetsManager:
    def __init__(self, credentials_path: str):
        self.credentials_path = credentials_path
        self.client = self._authenticate()

    def _authenticate(self):
        if not os.path.exists(self.credentials_path):
            raise FileNotFoundError(f"Credentials file not found at {self.credentials_path}")
        
        credentials = Credentials.from_service_account_file(
            self.credentials_path, scopes=SCOPES
        )
        return gspread.authorize(credentials)

    def get_apify_keys(self, sheet_url: str) -> list[str]:
        """Fetch all Apify keys from the settings Google Sheet."""
        keys = []
        try:
            sheet = self.client.open_by_url(sheet_url).sheet1
            values = sheet.get_all_values()
            
            # Look for anything that looks like an apify token (apify_api_...)
            for row in values:
                for cell in row:
                    if str(cell).strip().startswith("apify_api_"):
                        keys.append(str(cell).strip())
                        
            # If no obvious token, maybe grab column B assuming A="Apify Key"? 
            if not keys and len(values) > 1:
                # Fallback to column B if there's no "apify_api_" prefix
                for i in range(1, len(values)):
                     if len(values[i]) > 1 and values[i][1].strip():
                         keys.append(values[i][1].strip())
                 
            if not keys:
                raise ValueError("Could not find any Apify Keys in the sheet.")
                
            return keys
            
        except Exception as e:
            logger.error(f"Failed to fetch Apify keys: {e}")
            raise e

    def append_company_data(self, sheet_url: str, data: list):
        """Append the extracted company data to the Outreach sheet."""
        try:
            sheet = self.client.open_by_url(sheet_url).sheet1
            # data should be a 1D list matching columns:
            # company_name, website, linkedin_company, industry, company_size, location, description, scraped_at, emails, phones, company_keywords, pain_points, status, email_sent_at
            sheet.append_row(data)
            logger.info(f"Successfully appended data for {data[0]}")
        except Exception as e:
            logger.error(f"Failed to append row to Outreach Sheet: {e}")
            raise e

    def get_existing_linkedin_urls(self, sheet_url: str) -> set[str]:
        """Fetch all existing LinkedIn company URLs from the Outreach sheet."""
        existing_urls = set()
        try:
            sheet = self.client.open_by_url(sheet_url).sheet1
            # Column C (index 3) is 'linkedin_company'
            # We get all values in that column except the header
            col_values = sheet.col_values(3)
            if len(col_values) > 1:
                # Add normalized URLs (strip trailing slashes, enforce lowercase if needed)
                for url in col_values[1:]:
                    if url.strip():
                        existing_urls.add(url.strip().rstrip('/'))
            logger.info(f"Loaded {len(existing_urls)} existing LinkedIn URLs from the sheet.")
            return existing_urls
        except Exception as e:
            logger.error(f"Failed to fetch existing LinkedIn URLs: {e}")
            raise e

    def get_rows_missing_contacts(self, sheet_url: str) -> list[tuple[int, str]]:
        """
        Scan the Outreach sheet for rows where the emails column (col I = index 9)
        is blank but the website column (col B = index 2) has a value.
        EXCLUDES rows that have already been scraped (status='Extracted' or scraped_at is not empty).
        Returns list of (1-based row number, website_url) tuples.
        """
        results = []
        try:
            sheet = self.client.open_by_url(sheet_url).sheet1
            all_rows = sheet.get_all_values()
            # Row 0 is the header, so data starts at index 1 (sheet row 2)
            for i, row in enumerate(all_rows[1:], start=2):
                website     = row[1].strip() if len(row) > 1 else ""   # col B (index 1)
                emails      = row[8].strip() if len(row) > 8 else ""   # col I (index 8)
                scraped_at  = row[7].strip() if len(row) > 7 else ""   # col H (index 7)
                status      = row[12].strip() if len(row) > 12 else "" # col M (index 12)
                
                # Only backfill if:
                # - Website exists
                # - Emails are missing
                # - Row has NOT been scraped yet (no scraped_at timestamp and status != 'Extracted')
                if website and not emails and not scraped_at and status != "Extracted":
                    results.append((i, website))
            logger.info(f"Found {len(results)} row(s) with missing contact info (excluding already scraped rows).")
        except Exception as e:
            logger.error(f"Failed to scan for missing contacts: {e}")
        return results

    def update_contact_info(self, sheet_url: str, row_number: int, emails_str: str, phones_str: str):
        """
        Update the emails (col I) and phones (col J) cells for a specific row.
        row_number is 1-based (same as Google Sheets numbering).
        """
        try:
            sheet = self.client.open_by_url(sheet_url).sheet1
            sheet.update_cell(row_number, 9, emails_str)   # col I = emails
            sheet.update_cell(row_number, 10, phones_str)  # col J = phones
            logger.info(f"Updated contact info for row {row_number}: {emails_str} | {phones_str}")
        except Exception as e:
            logger.error(f"Failed to update row {row_number}: {e}")
            raise e
