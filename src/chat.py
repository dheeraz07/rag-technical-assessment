"""
CLI chat interface for ITNB knowledge base.
"""

import sys
from typing import Optional

from groundx import GroundX
from openai import OpenAI
from loguru import logger
from rich.console import Console
from rich.panel import Panel
from rich.markdown import Markdown
from rich.table import Table

from .config import get_config

console = Console()


class ChatInterface:
    def __init__(self, api_key: Optional[str] = None, bucket_id: Optional[int] = None):
        self.config = get_config()
        self.api_key = api_key or self.config.groundx_api_key
        
        if not self.api_key:
            raise ValueError("GroundX API key missing.")
        
        self.client = GroundX(api_key=self.api_key)
        self.bucket_id = bucket_id or self._find_bucket()
        
        # Setup OpenAI if enabled
        self.llm = None
        if self.config.openai_api_key:
            try:
                self.llm = OpenAI(
                    api_key=self.config.openai_api_key,
                    base_url=self.config.openai_api_base
                )
                logger.info(f"Connected to LLM: {self.config.openai_model_name}")
            except Exception as e:
                logger.warning(f"LLM connection failed: {e}")
    
    def _find_bucket(self) -> Optional[int]:
        """Discovery logic for the ITNB bucket."""
        try:
            buckets = self.client.buckets.list().buckets
            
            # 1. Try exact/fuzzy match
            for b in buckets:
                if "itnb" in b.name.lower():
                    return b.bucket_id
            
            # 2. Fallback to newest/first
            if buckets:
                return buckets[0].bucket_id
                
            return None
            
        except Exception as e:
            logger.error(f"Failed to list buckets: {e}")
            return None

    def search(self, query: str, n_results: int = 5) -> list[dict]:
        try:
            response = self.client.search.content(
                query=query,
                id=self.bucket_id,
                n=n_results,
            )
            
            results = []
            if response and response.search:
                for r in response.search.results:
                    url = r.source_url or "Unknown"
                    # Quick hack to get a readable title from the URL path
                    title = url.split("/")[-1].replace("-", " ").title() if url != "Unknown" else "Untitled"
                    
                    results.append({
                        "content": r.text,
                        "source_url": url,
                        "title": title,
                        "score": getattr(r, 'score', None),
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []

    def generate_answer(self, query: str, context: list[dict]):
        if not self.llm:
            return None
            
        context_str = "\n\n".join([
            f"Source: {c['title']} ({c['source_url']})\nContent: {c['content']}"
            for c in context
        ])
        
        try:
            stream = self.llm.chat.completions.create(
                model=self.config.openai_model_name,
                messages=[
                    {
                        "role": "system", 
                        "content": (
                            "You are a helpful assistant for ITNB AG. "
                            "Answer based on the context provided. Cite sources."
                        )
                    },
                    {"role": "user", "content": f"Context:\n{context_str}\n\nQuestion: {query}"}
                ],
                stream=True
            )
            
            console.print("[bold green]Answer[/bold green]")
            console.print(Panel("", border_style="green", padding=(1, 2)), end="")
            
            full_response = ""
            for chunk in stream:
                if chunk.choices[0].delta.content:
                    content = chunk.choices[0].delta.content
                    full_response += content
                    console.print(content, end="", style="bright_white")
            
            console.print()
            return full_response

        except Exception as e:
            logger.error(f"LLM Generation failed: {e}")
            console.print(f"[red]LLM Error: {e}[/red]")
            return None

    def display_sources(self, results: list[dict]):
        """Show sources table."""
        console.print("\n[bold cyan]📚 Sources:[/bold cyan]")
        
        table = Table(show_header=False, box=None, padding=(0, 2))
        table.add_column("", style="dim")
        table.add_column("")
        
        seen = set()
        for i, r in enumerate(results, 1):
            url = r["source_url"]
            if url not in seen:
                seen.add(url)
                table.add_row(f"[{i}]", f"{r['title']}\n    [dim]{url}[/dim]")
        
        console.print(table)
    
    def run(self):
        """Main chat loop."""
        mode = "LLM + RAG" if self.llm else "Search Only"
        
        console.print("\n" + "=" * 60)
        console.print(Panel(
            f"[bold blue]ITNB Knowledge Base[/bold blue]\n[dim]Mode: {mode}[/dim]\n\n"
            "Ask questions about ITNB AG.\n"
            "Type [bold]quit[/bold] to exit.",
            border_style="blue",
        ))
        console.print("=" * 60 + "\n")
        
        if not self.bucket_id:
            console.print("[bold red]No bucket found. Run ingestion first.[/bold red]")
            return
        
        query_count = 0
        
        while True:
            try:
                console.print("\n[bold green]You:[/bold green] ", end="")
                query = input().strip()
                
                if query.lower() in ("quit", "exit", "q"):
                    console.print("\n[dim]Goodbye![/dim]\n")
                    break
                
                if not query:
                    continue
                
                query_count += 1
                console.print()
                
                with console.status("[bold blue]Searching...[/bold blue]"):
                    results = self.search(query)
                
                if not results:
                    console.print(Panel("[yellow]No relevant content found.[/yellow]", border_style="yellow"))
                    continue
                
                # Try LLM generation first
                if self.llm:
                    console.print(Panel("Generating answer...", style="dim"))
                    self.generate_answer(query, results)
                else:
                    # Fallback to displaying top chunk
                    main = results[0]
                    content = main["content"]
                    if len(content) > 2500:
                        content = content[:2500] + "..."
                    
                    console.print(Panel(
                        Markdown(content),
                        title="[bold green]Top Match[/bold green]",
                        border_style="green",
                    ))
                
                self.display_sources(results)
                console.print("\n" + "-" * 60)
                
            except KeyboardInterrupt:
                console.print("\n\n[dim]Interrupted.[/dim]\n")
                break
            except EOFError:
                break
        
        console.print(f"[dim]Queries: {query_count}[/dim]")


def main():
    """Entry point."""
    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    logger.add("logs/chat.log", rotation="1 MB", level="DEBUG")
    
    try:
        chat = ChatInterface()
        chat.run()
        
    except ValueError as e:
        console.print(f"[bold red]Config Error: {e}[/bold red]")
    except Exception as e:
        logger.exception(f"Chat error: {e}")
        console.print(f"[bold red]Error: {e}[/bold red]")


if __name__ == "__main__":
    main()
