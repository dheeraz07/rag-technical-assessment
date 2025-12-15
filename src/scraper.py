"""
ITNB website scraper using crawl4ai.
Target: https://www.itnb.ch/en
"""

import asyncio
import json
import hashlib
import re
from datetime import datetime
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from loguru import logger
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


class ITNBScraper:
    """Handles crawling and content extraction."""
    
    BASE_URL = "https://www.itnb.ch/en"
    
    # Skip binaries/assets
    SKIP_EXTENSIONS = ('.pdf', '.doc', '.docx', '.xls', '.xlsx', '.zip', 
                       '.png', '.jpg', '.jpeg', '.gif', '.svg')
    
    # Seed URLs to help with deep discovery
    SEED_URLS = [
        "https://www.itnb.ch/en",
        "https://www.itnb.ch/en/about",
        "https://www.itnb.ch/en/services",
        "https://www.itnb.ch/en/solutions",
        "https://www.itnb.ch/en/contact",
        "https://www.itnb.ch/en/partners",
        "https://www.itnb.ch/en/careers",
        "https://www.itnb.ch/en/news",
        "https://www.itnb.ch/en/blog",
        "https://www.itnb.ch/en/team",
        "https://www.itnb.ch/en/privacy",
        "https://www.itnb.ch/en/impressum",
        "https://www.itnb.ch/en/sovereign-orchestrator",
    ]
    
    def __init__(self, output_dir: Path = Path("data")):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.scraped_data: list[dict] = []
        self.visited_urls: set[str] = set()
        self.failed_urls: list[dict] = []
        
    def _is_valid_url(self, url: str) -> bool:
        """Keep it to English pages on itnb.ch only."""
        parsed = urlparse(url)
        is_domain = "itnb.ch" in parsed.netloc
        is_english = "/en" in parsed.path or parsed.path == "/en"
        return is_domain and is_english and not parsed.path.lower().endswith(self.SKIP_EXTENSIONS)
    
    def _generate_doc_id(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()[:12]
    
    def _clean_content(self, content: str) -> str:
        """Nuke cookie banners and excess whitespace."""
        patterns = [
            r'How you want to deal with cookies.*?Allow All\n?',
            r'Manage CookiesAllow All\n?',
        ]
        
        cleaned = content
        for pattern in patterns:
            cleaned = re.sub(pattern, '', cleaned, flags=re.DOTALL | re.IGNORECASE)
        
        return re.sub(r'\n{3,}', '\n\n', cleaned).strip()
    
    def _extract_links(self, markdown_content: str, base_url: str) -> list[str]:
        """Extract links from markdown and normalize them."""
        links = []
        matches = re.findall(r'\[([^\]]*)\]\(([^)]+)\)', markdown_content)
        
        for _, url in matches:
            if url.startswith(('javascript:', '#')):
                continue
                
            if url.startswith('/'):
                full_url = f"https://www.itnb.ch{url}"
            elif url.startswith('http'):
                full_url = url
            else:
                full_url = urljoin(base_url, url)
            
            # clean anchors/params
            parsed = urlparse(full_url)
            clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip('/')
            
            if self._is_valid_url(clean_url) and clean_url not in self.visited_urls:
                links.append(clean_url)
        
        return list(set(links))
    
    async def scrape_page(self, crawler: AsyncWebCrawler, url: str) -> Optional[dict]:
        """Fetch single page, return structured doc or None."""
        # normalize url
        url = url.rstrip('/')
        
        if url in self.visited_urls:
            return None
        
        self.visited_urls.add(url)
        
        try:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                remove_overlay_elements=True,
                wait_until="networkidle",
                page_timeout=30000,
            )
            
            result = await crawler.arun(url=url, config=config)
            
            if not result.success:
                logger.warning(f"Failed: {url} - {result.error_message}")
                self.failed_urls.append({"url": url, "error": result.error_message})
                return None
            
            content = result.markdown or ""
            content = self._clean_content(content)
            
            # skip very thin pages (just navigation/footer)
            if len(content.split()) < 20:
                logger.debug(f"Skipping {url}: too short ({len(content.split())} words)")
                return None
            
            doc = {
                "doc_id": self._generate_doc_id(url),
                "url": url,
                "title": result.metadata.get("title", "Untitled") if result.metadata else "Untitled",
                "content": content,
                "word_count": len(content.split()),
                "scraped_at": datetime.now().isoformat(),
            }
            
            logger.info(f"✓ {url} ({doc['word_count']} words)")
            return doc
            
        except Exception as e:
            logger.error(f"Error: {url} - {str(e)}")
            self.failed_urls.append({"url": url, "error": str(e)})
            return None
    
    async def crawl_website(self, max_pages: int = 50) -> list[dict]:
        """
        BFS crawl starting from seed URLs.
        Follows internal links up to max_pages limit.
        """
        console.print(f"\n[bold blue]ITNB Website Scraper[/bold blue]")
        console.print(f"Target: {self.BASE_URL}")
        console.print(f"Limit: {max_pages} pages\n")
        
        # start with seed URLs
        urls_to_visit = list(self.SEED_URLS)
        
        browser_config = BrowserConfig(
            headless=True,
            verbose=False,
            extra_args=["--disable-blink-features=AutomationControlled"],
        )
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                task = progress.add_task("Scraping...", total=None)
                
                while urls_to_visit and len(self.scraped_data) < max_pages:
                    url = urls_to_visit.pop(0)
                    progress.update(task, description=f"Scraping: {url[:60]}...")
                    
                    doc = await self.scrape_page(crawler, url)
                    
                    if doc:
                        self.scraped_data.append(doc)
                        
                        # queue up new links
                        new_links = self._extract_links(doc["content"], url)
                        for link in new_links:
                            if link not in self.visited_urls and link not in urls_to_visit:
                                urls_to_visit.append(link)
        
        return self.scraped_data
    
    def save_results(self, filename: str = "itnb_scraped_content.json") -> Path:
        """Dump results to JSON."""
        output_path = self.output_dir / filename
        
        output = {
            "scrape_timestamp": datetime.now().isoformat(),
            "base_url": self.BASE_URL,
            "total_documents": len(self.scraped_data),
            "failed_urls": self.failed_urls,
            "documents": self.scraped_data,
        }
        
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Saved {len(self.scraped_data)} docs to {output_path}")
        return output_path
    
    def print_summary(self):
        """Show stats."""
        console.print("\n" + "=" * 50)
        console.print("[bold green]Scraping Complete[/bold green]")
        console.print("=" * 50)
        console.print(f"✓ Scraped: [bold]{len(self.scraped_data)}[/bold]")
        console.print(f"✓ Visited: [bold]{len(self.visited_urls)}[/bold]")
        console.print(f"✗ Failed: [bold red]{len(self.failed_urls)}[/bold red]")
        
        if self.scraped_data:
            total_words = sum(doc["word_count"] for doc in self.scraped_data)
            console.print(f"✓ Words: [bold]{total_words:,}[/bold]")


async def main():
    """Entry point."""
    import sys
    
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("logs/scraper.log", rotation="1 MB", level="DEBUG")
    
    scraper = ITNBScraper()
    
    try:
        await scraper.crawl_website(max_pages=50)
        output_path = scraper.save_results()
        scraper.print_summary()
        console.print(f"\n[dim]Saved: {output_path}[/dim]")
        
    except Exception as e:
        logger.exception(f"Scraper failed: {e}")
        console.print(f"[bold red]Error: {e}[/bold red]")
        raise


if __name__ == "__main__":
    asyncio.run(main())
