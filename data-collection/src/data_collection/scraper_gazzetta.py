import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import json
from typing import List, Dict

class GazzettaScraper:
    def __init__(self):
        self.base_url = "https://www.gazzetta.gr"
        self.bloggers_url = f"{self.base_url}/bloggers"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_bloggers(self, page: int = 0) -> List[Dict]:
        """
        Get list of bloggers from a specific page
        
        Args:
            page: Page number to scrape
            
        Returns:
            List of dictionaries containing blogger information
        """
        try:
            url = f"{self.bloggers_url}?page={page}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            bloggers = []
            blogger_elements = soup.select('div.bloggers .list-article__blogger')
            
            for element in blogger_elements:
                try:
                    name = element.select_one('h3').text.strip()
                    profile_url = element.select_one('a')['href']
                    
                    bloggers.append({
                        'name': name,
                        'profile_url': urljoin(self.base_url, profile_url)
                    })
                except (AttributeError, KeyError) as e:
                    print(f"Error parsing blogger: {e}")
                    continue
                    
            return bloggers
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching bloggers page: {e}")
            return []

    def get_article_content(self, article_url: str) -> str:
        """
        Get the full content of an article
        
        Args:
            article_url: URL of the article to scrape
            
        Returns:
            String containing the article text
        """
        try:
            response = requests.get(article_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find the main content div
            content_div = soup.find('div', class_='content is-relative')
            if not content_div:
                return ""
                
            # Get all paragraphs and headings from the content
            paragraphs = content_div.find_all(['p', 'h2'])
            
            # Join all paragraphs with newlines, excluding empty ones
            full_text = '\n\n'.join(p.get_text().strip() for p in paragraphs if p.get_text().strip())
            
            return full_text
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching article content: {e}")
            return ""

    def get_articles(self, page: int = 0) -> List[Dict]:
        """
        Get articles from a specific page
        
        Args:
            page: Page number to scrape
            
        Returns:
            List of dictionaries containing article information
        """
        try:
            url = f"{self.bloggers_url}?page={page}"
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            articles = []
            article_elements = soup.select('div.bloggers .list-article__blogger-wrap')
            
            for element in article_elements:
                try:
                    article_url = urljoin(self.base_url, element.select_one('h2.h3 a')['href'])
                    
                    # Get article metadata
                    article_info = {
                        'category': element.select_one('.is-category').text.strip(),
                        'date': element.select_one('time').text.strip(),
                        'title': element.select_one('h2.h3 a').text.strip(),
                        'article_url': article_url,
                        'description': element.select_one('p.mb-16').text.strip(),
                        'blogger_name': element.select_one('.list-article__blogger h3').text.strip(),
                        'blogger_url': urljoin(self.base_url, element.select_one('.list-article__blogger a')['href']),
                        'content': self.get_article_content(article_url)  # Get full article content
                    }
                    articles.append(article_info)
                    
                    # Add a small delay between article requests
                    time.sleep(1)
                    
                except (AttributeError, KeyError) as e:
                    print(f"Error parsing article: {e}")
                    continue
                    
            return articles
        
        except requests.exceptions.RequestException as e:
            print(f"Error fetching articles page: {e}")
            return []
                

    def save_to_json(self, data: List[Dict], filename: str):
        """Save scraped data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, ensure_ascii=False, indent=2, fp=f)

    def save_to_csv(self, data: List[Dict], filename: str):
        """Save scraped data to CSV file"""
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8')

    def run(self, max_pages: int = 5):
        """
        Main method to run the scraper
        
        Args:
            max_pages: Maximum number of pages to scrape
        """
        print("Starting scraper...")
        
        all_bloggers = []
        all_articles = []
        
        for page in range(max_pages):
            print(f"Fetching page {page + 1}...")
            
            # Get bloggers
            bloggers = self.get_bloggers(page)
            if bloggers:
                all_bloggers.extend(bloggers)
                print(f"Found {len(bloggers)} bloggers on page {page + 1}")
            
            # Get articles
            articles = self.get_articles(page)
            if articles:
                all_articles.extend(articles)
                print(f"Found {len(articles)} articles on page {page + 1}")
            
            # If neither bloggers nor articles were found, we've reached the end
            if not bloggers and not articles:
                break
                
            time.sleep(1)  # Be nice to the server
        
        # Save data
        self.save_to_json(all_bloggers, 'gazzetta_bloggers.json')
        self.save_to_json(all_articles, 'gazzetta_articles.json')
        self.save_to_csv(all_bloggers, 'gazzetta_bloggers.csv')
        self.save_to_csv(all_articles, 'gazzetta_articles.csv')
        
        print(f"Scraping completed!")
        print(f"Total bloggers scraped: {len(all_bloggers)}")
        print(f"Total articles scraped: {len(all_articles)}")

if __name__ == "__main__":
    scraper = GazzettaScraper()
    scraper.run()