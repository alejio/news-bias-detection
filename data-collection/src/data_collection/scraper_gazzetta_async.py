import typer
import aiohttp
import asyncio
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin
import json
from typing import List, Dict, Set, Optional
from pathlib import Path
from rich import print as rprint
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn
import aiofiles
from functools import partial
import signal

app = typer.Typer()

class GazzettaBloggerScraper:
    def __init__(self, target_bloggers=None, max_articles_per_blogger=None, output_dir="scraped_data"):
        self.base_url = "https://www.gazzetta.gr"
        self.bloggers_url = f"{self.base_url}/bloggers"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        self.data_dir = Path(output_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.scraped_articles_file = self.data_dir / 'scraped_articles.json'
        self.scraped_urls = self._load_scraped_urls()
        self.target_bloggers = target_bloggers
        self.max_articles = max_articles_per_blogger
        self.progress_file = self.data_dir / 'scraping_progress.json'
        self.completed_bloggers = self._load_progress()
        self.semaphore = asyncio.Semaphore(10)  # Limit concurrent requests

    def _load_scraped_urls(self) -> Set[str]:
        if self.scraped_articles_file.exists():
            with open(self.scraped_articles_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {article['article_url'] for article in data}
        return set()

    def _load_progress(self) -> Set[str]:
        if self.progress_file.exists():
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        return set()

    async def _save_progress(self, blogger_name: str):
        self.completed_bloggers.add(blogger_name)
        async with aiofiles.open(self.progress_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(list(self.completed_bloggers)))

    async def _save_article(self, article: Dict, blogger_name: str):
        articles = []
        if self.scraped_articles_file.exists():
            async with aiofiles.open(self.scraped_articles_file, 'r', encoding='utf-8') as f:
                content = await f.read()
                articles = json.loads(content) if content else []
        
        article['blogger_name'] = blogger_name
        articles.append(article)
        
        async with aiofiles.open(self.scraped_articles_file, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(articles, ensure_ascii=False, indent=2))
        
        self.scraped_urls.add(article['article_url'])

    async def _get_article_content(self, session: aiohttp.ClientSession, article_url: str) -> str:
        async with self.semaphore:
            try:
                async with session.get(article_url, headers=self.headers) as response:
                    if response.status != 200:
                        return ''
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    content_div = soup.select_one('div.content.is-relative')
                    if not content_div:
                        return ''
                    
                    content_parts = []
                    
                    lead = soup.select_one('div.content__lead')
                    if lead:
                        content_parts.append(lead.get_text(strip=True))
                    
                    for p in content_div.find_all(['p', 'blockquote']):
                        if p.select_one('.admanager-content'):
                            continue
                        
                        text = p.get_text(strip=True)
                        if text:
                            content_parts.append(text)
                    
                    return '\n\n'.join(content_parts)
                
            except Exception as e:
                rprint(f"[red]Error fetching article content from {article_url}: {e}[/red]")
                return ''

    async def get_blogger_articles(self, session: aiohttp.ClientSession, profile_url: str, blogger_name: str) -> List[Dict]:
        articles = []
        page = 0
        
        while True:
            if self.max_articles and len(articles) >= self.max_articles:
                rprint(f"[yellow]Reached maximum articles limit ({self.max_articles}) for {blogger_name}[/yellow]")
                break

            try:
                url = f"{profile_url}?page={page}"
                async with session.get(url, headers=self.headers) as response:
                    if response.status != 200:
                        break
                    
                    html = await response.text()
                    soup = BeautifulSoup(html, 'html.parser')
                    
                    all_articles = []
                    all_articles.extend(soup.select('article.list-article-promo'))
                    all_articles.extend(soup.select('article.is-flex'))
                    
                    if not all_articles:
                        break
                    
                    tasks = []
                    for element in all_articles:
                        try:
                            if 'list-article-promo' in element.get('class', []):
                                article_link = element.select_one('h2 a')['href']
                                title = element.select_one('h2 a').text.strip()
                                date = element.select_one('time').text.strip()
                                categories = []
                                cat_elem = element.select_one('a.is-category')
                                if cat_elem:
                                    categories.append(cat_elem.text.strip())
                            else:
                                article_link = element.select_one('.list-article__info h3 a')['href']
                                title = element.select_one('.list-article__info h3 a').text.strip()
                                date = element.select_one('time.is-category-light').text.strip()
                                categories = [cat.text.strip() for cat in element.select('.is-category.whubcategory, .is-category.whubteam')]
                            
                            article_url = urljoin(self.base_url, article_link)
                            
                            if article_url in self.scraped_urls:
                                continue
                            
                            tasks.append({
                                'categories': categories,
                                'date': date,
                                'title': title,
                                'article_url': article_url,
                                'task': self._get_article_content(session, article_url)
                            })
                            
                        except (AttributeError, KeyError) as e:
                            rprint(f"[red]Error parsing article: {e}[/red]")
                    
                    # Fetch all article contents concurrently
                    contents = await asyncio.gather(*(task['task'] for task in tasks))
                    
                    # Save articles
                    for task, content in zip(tasks, contents):
                        article = {
                            'categories': task['categories'],
                            'date': task['date'],
                            'title': task['title'],
                            'article_url': task['article_url'],
                            'content': content
                        }
                        await self._save_article(article, blogger_name)
                        articles.append(article)
                        rprint(f"[green]Scraped article: {task['title']}[/green]")
                    
                    page += 1
                    await asyncio.sleep(0.5)  # Reduced delay
                    
            except Exception as e:
                rprint(f"[red]Error fetching blogger articles page: {e}[/red]")
                break
                    
        return articles

    async def get_bloggers(self, session: aiohttp.ClientSession, page: int = 0) -> List[Dict]:
        try:
            url = f"{self.bloggers_url}?page={page}"
            async with session.get(url, headers=self.headers) as response:
                if response.status != 200:
                    return []
                
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                
                blogger_elements = soup.select('div.bloggers .list-article__blogger')
                
                if not blogger_elements:
                    return []
                
                bloggers = []
                for element in blogger_elements:
                    try:
                        name = element.select_one('h3').text.strip()
                        
                        if self.target_bloggers and name not in self.target_bloggers:
                            continue
                            
                        if name in self.completed_bloggers:
                            rprint(f"[yellow]Skipping {name} - already scraped[/yellow]")
                            continue
                            
                        profile_url = element.select_one('a')['href']
                        
                        blogger_articles = await self.get_blogger_articles(
                            session,
                            urljoin(self.base_url, profile_url),
                            name
                        )
                        
                        bloggers.append({
                            'name': name,
                            'profile_url': urljoin(self.base_url, profile_url),
                            'articles': blogger_articles
                        })
                        
                        await self._save_progress(name)
                        
                    except (AttributeError, KeyError) as e:
                        rprint(f"[red]Error parsing blogger: {e}[/red]")
                        continue
                        
                return bloggers
                
        except Exception as e:
            rprint(f"[red]Error fetching bloggers page: {e}[/red]")
            return []

    async def save_to_json(self, data: List[Dict], filename: str):
        async with aiofiles.open(filename, 'w', encoding='utf-8') as f:
            await f.write(json.dumps(data, ensure_ascii=False, indent=2))

    def save_to_csv(self, data: List[Dict], filename: str):
        flattened_data = []
        for blogger in data:
            for article in blogger['articles']:
                flattened_data.append({
                    'blogger_name': blogger['name'],
                    'blogger_url': blogger['profile_url'],
                    **article
                })
        
        df = pd.DataFrame(flattened_data)
        df.to_csv(filename, index=False, encoding='utf-8')

async def run_scraper(
    bloggers: Optional[List[str]] = None,
    max_articles: Optional[int] = None,
    output_dir: str = "scraped_data",
    format: List[str] = ["json", "csv"]
):
    scraper = GazzettaBloggerScraper(
        target_bloggers=bloggers,
        max_articles_per_blogger=max_articles,
        output_dir=output_dir
    )

    async with aiohttp.ClientSession() as session:
        all_bloggers = []
        page = 0
        
        while True:
            rprint(f"[yellow]Fetching bloggers page {page}...[/yellow]")
            bloggers = await scraper.get_bloggers(session, page)
            
            if not bloggers:
                break
                
            all_bloggers.extend(bloggers)
            rprint(f"[green]Found {len(bloggers)} bloggers on page {page}[/green]")
            
            page += 1
            await asyncio.sleep(0.5)

        if "json" in format:
            output_file = Path(output_dir) / 'gazzetta_bloggers_articles.json'
            await scraper.save_to_json(all_bloggers, output_file)
            rprint(f"[bold green]Saved JSON output to {output_file}[/bold green]")

        if "csv" in format:
            output_file = Path(output_dir) / 'gazzetta_bloggers_articles.csv'
            scraper.save_to_csv(all_bloggers, output_file)
            rprint(f"[bold green]Saved CSV output to {output_file}[/bold green]")

        rprint(f"[bold blue]Scraping completed![/bold blue]")
        rprint(f"Total bloggers scraped: {len(all_bloggers)}")
        rprint(f"Total articles scraped: {sum(len(b['articles']) for b in all_bloggers)}")

@app.command()
def scrape(
    bloggers: Optional[List[str]] = typer.Option(None, "--blogger", "-b", help="Target specific bloggers"),
    max_articles: Optional[int] = typer.Option(None, "--max-articles", "-m", help="Maximum articles per blogger"),
    output_dir: str = typer.Option("scraped_data", "--output-dir", "-o", help="Output directory for scraped data"),
    format: List[str] = typer.Option(["json", "csv"], "--format", "-f", help="Output formats")
):
    """Scrape articles from Gazzetta.gr bloggers"""
    asyncio.run(run_scraper(bloggers, max_articles, output_dir, format))

@app.command()
async def list_bloggers():
    """List all available bloggers"""
    scraper = GazzettaBloggerScraper()
    async with aiohttp.ClientSession() as session:
        bloggers = await scraper.get_bloggers(session, 0)
        
        table = Table(title="Available Bloggers")
        table.add_column("Name")
        table.add_column("Profile URL")
        
        for blogger in bloggers:
            table.add_row(blogger['name'], blogger['profile_url'])
        
        rprint(table)

if __name__ == "__main__":
    app()