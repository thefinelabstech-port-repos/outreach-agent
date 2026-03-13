# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

B2B outreach automation pipeline: scrape LinkedIn company data → extract emails → send cold email sequences.

## Environment Setup

Uses `uv` for dependency management (Python 3.13 required).

```bash
uv sync
.venv\Scripts\activate          # Windows
playwright install chromium     # required for email_extractor
```

## Running the Pipeline

```bash
python main.py              # runs full pipeline (scrape stage by default)
python main.py scrape       # explicit scrape stage
python main.py enrich       # (not yet implemented)
python main.py outreach     # (not yet implemented)
```

## Required Credentials

These are gitignored and must be provided manually:

- **`data/credentials.json`** — Google Service Account JSON for Sheets/Drive access
- **`.env`** — Must contain `BREVO_SMTP_KEY` and `SENDER_EMAIL`

## Architecture

### Data Flow

```
data/company_url.csv
        ↓
main.py  (orchestrator: scrape → enrich → outreach)
        ↓
scraper/company_details_extractor.py   (async, batched Apify calls)
    ├── core/sheets.py                 (Google Sheets read/write)
    └── scraper/email_extractor.py     (Playwright crawler)
        ↓
Outreach Google Sheet  →  outreach/sequence_manager.py  →  Brevo SMTP
```

### Package Structure

```
scraper/        — data acquisition (Apify + web crawling)
core/           — shared infrastructure (Sheets client, Pydantic models)
outreach/       — email sending and sequence logic
data/           — CSV input queue, credentials, reference CSVs
```

### Key Files

- **`main.py`** — Entry point. Dispatches to `scrape`, `enrich`, or `outreach` stage.
- **`scraper/company_details_extractor.py`** — Async orchestrator: batches LinkedIn URLs (`BATCH_SIZE=5`, `CONCURRENCY=2`), calls Apify actor `od6RadQV98FOARtrp`, extracts contacts, writes to Google Sheet. Removes processed URLs from `data/company_url.csv` immediately after save. Also backfills existing sheet rows missing email/phone.
- **`scraper/email_extractor.py`** — Headless Chromium (Playwright) crawler. Scrapes homepage + contact/about subpages for emails (mailto + regex) and phones (tel + regex).
- **`core/sheets.py`** — `SheetsManager`: Google Sheets auth, reads Apify keys from APIFY SHEET, reads/writes Outreach Sheet.
- **`core/models.py`** — Pydantic `Company` and `Lead` models. `Lead.to_sheet_row()` serializes to flat list for Google Sheets.
- **`outreach/email_sender.py`** — Brevo SMTP email sender.
- **`outreach/sequence_manager.py`** — Loads templates, renders with lead data, calls `email_sender`.
- **`outreach/templates/cold_email.txt`** — Cold email template. Placeholders: `{{company_name}}`, `{{industry}}`, `{{website}}`, `{{location}}`.

### Google Sheets

- **APIFY SHEET** — Stores Apify API keys (cells starting with `apify_api_`). Keys are rotated on rate-limit.
- **Outreach Sheet** — Columns: `company_name | website | linkedin_company | industry | company_size | location | description | scraped_at | emails | phones | company_keywords | pain_points | status | email_sent_at`

### CSV Queue

`data/company_url.csv` is the input queue. Each URL is removed from the file immediately after being written to the sheet, so the file always represents remaining work.
