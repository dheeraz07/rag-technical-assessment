"""
GroundX document ingestion pipeline.
"""

import json
import time
from pathlib import Path
from typing import Optional

from groundx import GroundX
from groundx.types import WebsiteSource
from loguru import logger
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TimeElapsedColumn
from rich.table import Table

from .config import get_config

console = Console()


class GroundXIngestor:
    """Pipelines documents into GroundX."""
    
    def __init__(self, api_key: Optional[str] = None):
        config = get_config()
        self.api_key = api_key or config.groundx_api_key
        
        if not self.api_key:
            raise ValueError("Missing GROUNDX_API_KEY. Check your .env file.")
        
        self.client = GroundX(api_key=self.api_key)
        self.bucket_id: Optional[int] = None
        self.process_id: Optional[str] = None
        
        self.stats = {"total": 0, "success": 0, "failed": 0}
    
    def get_or_create_bucket(self, bucket_name: str = "itnb-website-content") -> int:
        """Finds or creates a working bucket."""
        try:
            # Check existing first
            for bucket in self.client.buckets.list().buckets:
                if bucket.name == bucket_name:
                    logger.info(f"Using existing bucket: {bucket_name} ({bucket.bucket_id})")
                    return bucket.bucket_id
            
            # Create new
            response = self.client.buckets.create(name=bucket_name)
            logger.info(f"Created bucket: {bucket_name} ({response.bucket.bucket_id})")
            return response.bucket.bucket_id
            
        except Exception as e:
            logger.error(f"Bucket op failed: {e}")
            raise
    
    def ingest_website(self, base_url: str = "https://www.itnb.ch/en", depth: int = 3, cap: int = 50) -> dict:
        """
        Ingest website using GroundX crawler.
        Uses their native crawl API which handles the scraping internally.
        """
        console.print(f"\n[bold blue]GroundX Website Ingestion[/bold blue]")
        console.print(f"URL: {base_url}")
        console.print(f"Depth: {depth}, Cap: {cap} pages\n")
        
        self.bucket_id = self.get_or_create_bucket()
        
        try:
            website = WebsiteSource(
                bucket_id=self.bucket_id,
                source_url=base_url,
                depth=depth,
                cap=cap,
                search_data={
                    "source": "itnb_website",
                    "language": "en",
                }
            )
            
            console.print("Starting crawl...")
            response = self.client.documents.crawl_website(websites=[website])
            
            if response and response.ingest:
                self.process_id = response.ingest.process_id
                console.print(f"Crawl started. Process ID: {self.process_id}")
                
                # poll for completion
                self._wait_for_completion()
                self.stats["success"] = 1
                return self.stats
            else:
                logger.error("No response from crawl API")
                self.stats["failed"] = 1
                return self.stats
                
        except Exception as e:
            logger.error(f"Crawl failed: {e}")
            self.stats["failed"] = 1
            raise
    
    def _wait_for_completion(self, timeout_minutes: int = 10):
        """Poll for crawl completion."""
        if not self.process_id:
            return
        
        start = time.time()
        timeout = timeout_minutes * 60
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Processing...", total=None)
            
            while time.time() - start < timeout:
                try:
                    status = self.client.documents.get_processing_status_by_id(self.process_id)
                    
                    if status and status.ingest:
                        state = status.ingest.status
                        progress.update(task, description=f"Status: {state}")
                        
                        if state == "complete":
                            console.print("[green]✓ Crawl complete[/green]")
                            return
                        elif state == "error":
                            console.print("[red]✗ Crawl failed[/red]")
                            return
                        elif state in ("cancelled",):
                            console.print(f"[yellow]Crawl {state}[/yellow]")
                            return
                    
                    time.sleep(5)
                    
                except Exception as e:
                    logger.warning(f"Status check error: {e}")
                    time.sleep(5)
        
        console.print("[yellow]Timeout waiting for completion[/yellow]")
    
    def ingest_from_json(self, json_path: Path) -> dict:
        """
        Alternative: ingest URLs from scraped JSON using crawl API.
        Batches URLs for crawl.
        """
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        documents = data.get("documents", [])
        urls = [doc["url"] for doc in documents if doc["url"].startswith("https://www.itnb.ch")]
        
        self.stats["total"] = len(urls)
        
        console.print(f"\n[bold blue]GroundX Ingestion[/bold blue]")
        console.print(f"Source: {json_path}")
        console.print(f"URLs: {len(urls)}\n")
        
        self.bucket_id = self.get_or_create_bucket()
        
        # batch by 10
        batch_size = 10
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i+batch_size]
            
            websites = [
                WebsiteSource(
                    bucket_id=self.bucket_id,
                    source_url=url,
                    depth=0,  # no crawl, just this page
                    cap=1,
                )
                for url in batch
            ]
            
            try:
                console.print(f"Ingesting batch {i//batch_size + 1}...")
                response = self.client.documents.crawl_website(websites=websites)
                
                if response and response.ingest:
                    self.stats["success"] += len(batch)
                else:
                    self.stats["failed"] += len(batch)
                    
                time.sleep(2)  # rate limit
                
            except Exception as e:
                logger.error(f"Batch failed: {e}")
                self.stats["failed"] += len(batch)
        
        return self.stats
    
    def print_summary(self):
        """Display results."""
        console.print("\n" + "=" * 50)
        console.print("[bold green]Ingestion Complete[/bold green]")
        console.print("=" * 50)
        
        table = Table(show_header=True, header_style="bold")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        
        table.add_row("Bucket ID", str(self.bucket_id))
        table.add_row("Process ID", str(self.process_id) if self.process_id else "N/A")
        table.add_row("Status", "[green]Success[/green]" if self.stats["success"] else "[red]Failed[/red]")
        
        console.print(table)


def main():
    """Entry point."""
    import sys
    
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/ingestion.log", rotation="1 MB", level="DEBUG")
    
    try:
        ingestor = GroundXIngestor()
        
        # use GroundX native crawler
        ingestor.ingest_website(
            base_url="https://www.itnb.ch/en",
            depth=3,
            cap=50
        )
        ingestor.print_summary()
        
    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise


if __name__ == "__main__":
    main()
