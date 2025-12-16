# ITNB RAG Content Engine

A lightweight RAG pipeline that scrapes `itnb.ch`, indexes it into GroundX, and serves a CLI chat interface powered by Llama 4.

## Quick Start

### 1. Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Required for crawl4ai (Playwright browser automation)
playwright install
```

### 2. Config
```bash
cp .env.example .env
# Add your GROUNDX_API_KEY and OPENAI creds to .env
```

### 3. Run Pipeline
The pipeline is split into 3 steps:

1. **Scrape**: Crawls the site and saves raw content to `data/`.
   ```bash
   python -m src.scraper
   ```

2. **Ingest**: Reads local scraped data and uploads to GroundX.
   ```bash
   python -m src.ingest
   ```
   - Per-document success/failure is logged to `logs/ingestion_audit.json`.

3. **Chat**: Run the interactive CLI.
   ```bash
   python -m src.chat
   ```

## Stack

- **Scraping**: `crawl4ai` (Playwright-based async crawler)
- **Vector Store**: `GroundX` (managed RAG infrastructure)
- **LLM**: `OpenAI` client (connecting to Llama 4 endpoint)
- **CLI**: `rich`, `loguru`

## Notes

- **Security**: Keys are loaded from `.env` via `pydantic-settings`. Do not commit this file.
- **Scraper**: Configured to ignore non-English pages and static assets (PDF/images).
- **Audit Trail**: All ingestion results are logged per-document in `logs/ingestion_audit.json`.
