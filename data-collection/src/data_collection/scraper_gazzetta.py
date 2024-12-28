import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from urllib.parse import urljoin
import json

class GazzettaScraper:
    def __init__(self):
        self.base_url = "https://www.gazzetta.gr"
        self.bloggers_url = f"{self.base_url}/bloggers"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def get_journalists(self):
        """Get list of journalists and their profile URLs"""
        try:
            response = requests.get(self.bloggers_url, headers=self.headers)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            journalists = []
            # You'll need to adjust these selectors based on the actual HTML structure
            journalist_cards = soup.select('.blogger-card')  # Adjust selector as needed
            
            for card in journalist_cards:
                name = card.select_one('.blogger-name').text.strip()
                profile_url = urljoin(self.base_url, card.select_one('a')['href'])
                journalists.append({
                    'name': name,
                    'profile_url': profile_url
                })
            
            return journalists
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching journalists: {e}")
            return []

    def get_articles_for_journalist(self, journalist):
        """Get all articles for a specific journalist"""
        articles = []
        page = 1
        
        while True:
            try:
                # Adjust URL pattern based on actual website structure
                url = f"{journalist['profile_url']}?page={page}"
                response = requests.get(url, headers=self.headers)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.text, 'html.parser')
                # Adjust selector based on actual HTML structure
                article_elements = soup.select('.article-card')
                
                if not article_elements:
                    break
                
                for article in article_elements:
                    title = article.select_one('.article-title').text.strip()
                    url = urljoin(self.base_url, article.select_one('a')['href'])
                    date = article.select_one('.article-date').text.strip()
                    
                    articles.append({
                        'journalist': journalist['name'],
                        'title': title,
                        'url': url,
                        'date': date
                    })
                
                page += 1
                time.sleep(1)  # Be nice to the server
                
            except requests.exceptions.RequestException as e:
                print(f"Error fetching articles for {journalist['name']}: {e}")
                break
                
        return articles

    def save_to_json(self, data, filename):
        """Save scraped data to JSON file"""
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, ensure_ascii=False, indent=2, fp=f)

    def save_to_csv(self, data, filename):
        """Save scraped data to CSV file"""
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False, encoding='utf-8')

    def run(self):
        """Main method to run the scraper"""
        print("Starting scraper...")
        
        # Get all journalists
        journalists = self.get_journalists()
        print(f"Found {len(journalists)} journalists")
        
        # Get articles for each journalist
        all_articles = []
        for journalist in journalists:
            print(f"Fetching articles for {journalist['name']}...")
            articles = self.get_articles_for_journalist(journalist)
            all_articles.extend(articles)
            print(f"Found {len(articles)} articles")
        
        # Save data
        self.save_to_json(all_articles, 'gazzetta_articles.json')
        self.save_to_csv(all_articles, 'gazzetta_articles.csv')
        print("Scraping completed!")

if __name__ == "__main__":
    scraper = GazzettaScraper()
    scraper.run()