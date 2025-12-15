# ITNB RAG Content Engine

A lightweight RAG pipeline that scrapes `itnb.ch`, indexes it into GroundX, and serves a CLI chat interface powered by Llama 4.

## Quick Start

### 1. Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Config
```bash
cp .env.example .env
# Add your GROUNDX_API_KEY and OPENAI creds to .env
```

### 3. Run Pipeline
The pipeline is split into 3 steps:

1. **Scrape**: Crawls the site and saves raw content.
   ```bash
   python -m src.scraper
   ```

2. **Ingest**: Uploads/indexes content to GroundX.
   ```bash
   python -m src.ingest
   ```

3. **Chat**: Run the interactive CLI.
   ```bash
   python -m src.chat
   ```

## Stack

- **Ingestion**: `crawl4ai` (scraping), `GroundX` (vector store & retrieval)
- **RAG**: `OpenAI` client (connecting to Llama 4 endpoint)
- **CLI**: `rich`, `loguru`

## Notes

- **Security**: Keys are loaded from `.env` via `pydantic-settings`. Do not commit this file.
- **Scraper**: Configured to ignore non-English pages and static assets (PDF/images) to keep the index clean.
- **Rate Limits**: The ingest script has a small delay between batches if running in local mode, though the GroundX crawler handles most of this.

- **Rate Limits**: The ingest script has a small delay between batches if running in local mode, though the GroundX crawler handles most of this.
