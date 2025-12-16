"""
GroundX document ingestion pipeline.
Reads locally scraped content and uploads to GroundX.
"""

import json
import time
from pathlib import Path
from typing import Optional

from groundx import GroundX
from groundx.types import Document
from loguru import logger
from rich.console import Console
from rich.progress import Progress, BarColumn, TextColumn, TaskProgressColumn
from rich.table import Table

from .config import get_config

console = Console()


class GroundXIngestor:
    """Ingests locally scraped documents into GroundX."""
    
    def __init__(self, api_key: Optional[str] = None):
        config = get_config()
        self.api_key = api_key or config.groundx_api_key
        
        if not self.api_key:
            raise ValueError("Missing GROUNDX_API_KEY. Check your .env file.")
        
        self.client = GroundX(api_key=self.api_key)
        self.bucket_id: Optional[int] = None
        
        self.stats = {"total": 0, "success": 0, "failed": 0}
        self.results: list[dict] = []  # Per-document audit trail
    
    def get_or_create_bucket(self, bucket_name: str = "itnb-website-content") -> int:
        """Finds or creates a working bucket."""
        try:
            for bucket in self.client.buckets.list().buckets:
                if bucket.name == bucket_name:
                    logger.info(f"Using existing bucket: {bucket_name} ({bucket.bucket_id})")
                    return bucket.bucket_id
            
            response = self.client.buckets.create(name=bucket_name)
            logger.info(f"Created bucket: {bucket_name} ({response.bucket.bucket_id})")
            return response.bucket.bucket_id
            
        except Exception as e:
            logger.error(f"Bucket op failed: {e}")
            raise
    
    def ingest_from_json(self, json_path: Path = Path("data/itnb_scraped_content.json")) -> dict:
        """
        Ingest documents from locally scraped JSON file.
        Creates temp .txt files and uploads via GroundX SDK.
        """
        if not json_path.exists():
            raise FileNotFoundError(f"Scraped data not found: {json_path}. Run scraper first.")
        
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        documents = data.get("documents", [])
        self.stats["total"] = len(documents)
        
        console.print(f"\n[bold blue]GroundX Document Ingestion[/bold blue]")
        console.print(f"Source: {json_path}")
        console.print(f"Documents: {len(documents)}\n")
        
        self.bucket_id = self.get_or_create_bucket()
        
        # Create temp directory for document files
        temp_dir = Path("data/.ingest_temp")
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Ingesting...", total=len(documents))
            
            for doc in documents:
                doc_id = doc.get("doc_id", "unknown")
                url = doc.get("url", "unknown")
                content = doc.get("content", "")
                title = doc.get("title", "Untitled")
                
                result = {
                    "doc_id": doc_id,
                    "url": url,
                    "title": title,
                    "status": "pending",
                    "error": None,
                }
                
                try:
                    # Write content to temp file
                    temp_file = temp_dir / f"{doc_id}.txt"
                    with open(temp_file, "w", encoding="utf-8") as f:
                        f.write(f"# {title}\n")
                        f.write(f"Source: {url}\n\n")
                        f.write(content)
                    
                    # Create Document object
                    gx_doc = Document(
                        bucket_id=self.bucket_id,
                        file_path=str(temp_file.absolute()),
                        file_name=f"{doc_id}.txt",
                        file_type="txt",
                        search_data={
                            "source_url": url,
                            "title": title,
                            "doc_id": doc_id,
                        }
                    )
                    
                    # Upload to GroundX
                    response = self.client.ingest(
                        documents=[gx_doc],
                        wait_for_complete=True,
                    )
                    
                    if response and response.ingest:
                        result["status"] = "success"
                        result["process_id"] = response.ingest.process_id
                        self.stats["success"] += 1
                        logger.info(f"✓ Ingested: {doc_id} ({url})")
                    else:
                        result["status"] = "failed"
                        result["error"] = "No response from API"
                        self.stats["failed"] += 1
                        logger.warning(f"✗ No response: {doc_id}")
                    
                except Exception as e:
                    result["status"] = "failed"
                    result["error"] = str(e)
                    self.stats["failed"] += 1
                    logger.error(f"✗ Failed: {doc_id} - {e}")
                
                self.results.append(result)
                progress.update(task, advance=1)
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
        
        # Cleanup temp files
        for f in temp_dir.glob("*.txt"):
            f.unlink()
        temp_dir.rmdir()
        
        # Save audit log
        self._save_audit_log()
        
        return self.stats
    
    def _save_audit_log(self):
        """Save per-document ingestion results to log file."""
        log_path = Path("logs/ingestion_audit.json")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        audit = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "bucket_id": self.bucket_id,
            "stats": self.stats,
            "documents": self.results,
        }
        
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(audit, f, indent=2)
        
        logger.info(f"Audit log saved: {log_path}")
    
    def print_summary(self):
        """Display ingestion results."""
        console.print("\n" + "=" * 50)
        console.print("[bold green]Ingestion Complete[/bold green]")
        console.print("=" * 50)
        
        table = Table(show_header=True, header_style="bold")
        table.add_column("Metric", style="cyan")
        table.add_column("Value", justify="right")
        
        table.add_row("Bucket ID", str(self.bucket_id))
        table.add_row("Total Documents", str(self.stats["total"]))
        table.add_row("Successful", f"[green]{self.stats['success']}[/green]")
        table.add_row("Failed", f"[red]{self.stats['failed']}[/red]")
        
        success_rate = (self.stats["success"] / self.stats["total"] * 100) if self.stats["total"] > 0 else 0
        table.add_row("Success Rate", f"{success_rate:.1f}%")
        
        console.print(table)
        
        # Show failed documents if any
        failed = [r for r in self.results if r["status"] == "failed"]
        if failed:
            console.print("\n[bold red]Failed Documents:[/bold red]")
            for r in failed[:5]:  # Show first 5
                console.print(f"  • {r['doc_id']}: {r['error']}")
            if len(failed) > 5:
                console.print(f"  ... and {len(failed) - 5} more")


def main():
    """Entry point."""
    import sys
    
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/ingestion.log", rotation="1 MB", level="DEBUG")
    
    try:
        ingestor = GroundXIngestor()
        ingestor.ingest_from_json()
        ingestor.print_summary()
        
    except FileNotFoundError as e:
        console.print(f"[bold yellow]{e}[/bold yellow]")
        console.print("[dim]Run 'python -m src.scraper' first to scrape website content.[/dim]")
    except Exception as e:
        logger.exception(f"Ingestion failed: {e}")
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise


if __name__ == "__main__":
    main()
