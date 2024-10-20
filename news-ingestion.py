import requests
import json
from google.cloud import bigquery
import os
from datetime import datetime
from newsapi import NewsApiClient



# Initialise News API
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
NEWS_API_URL = "https://newsapi.org/v2/top-headlines"
newsapi = NewsApiClient(api_key=NEWS_API_KEY)


# Initialize BigQuery client
client = bigquery.Client(project="news-bias-detection-439208")

# BigQuery table details
dataset_id = "news_data"
table_id = "articles"

def fetch_news():
    """
    Fetch news articles from NewsAPI
    """
    params = {
        "topic": "AI",
        "language": "en"
    }
    print(params)
    #all_articles = newsapi.get_everything(q=params["topic"],
     #                                 language=params["language"])
                                      # sources='bbc-news,the-verge',
                                      # domains='bbc.co.uk,techcrunch.com',
                                     #   from_param=datetime(2023, 9, 1, hour=5),
                                      #  to=datetime(2024, 9, 1, hour=15),,
                                      #country=params["country"],
                                      # sort_by='relevancy',
                                      #page=2)

    response = requests.get(NEWS_API_URL, params=params)
    all_articles = response.json().get("articles", [])
    print(f"Articles {len(all_articles)}")
    return all_articles

def insert_into_bigquery(rows):
    """
    Insert rows of articles into BigQuery
    """
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    table = client.get_table(table_ref)  # API call

    # Insert rows into BigQuery
    errors = client.insert_rows_json(table, rows)
    if errors:
        print(f"Errors: {errors}")
    else:
        print(f"{len(rows)} rows inserted into BigQuery")

def transform_article(article):
    try:
        published_at = article.get("publishedAt")
        if published_at:
            published_at = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
        else:
            published_at = None
    except Exception as e:
        print(f"Error processing article: {article}")
        published_at = None

    return {
        "source": article["source"]["name"],
        "author": article.get("author"),
        "title": article["title"],
        "description": article.get("description"),
        "url": article["url"],
        "published_at": published_at,
        "content": article.get("content")
    }




def newsapi_to_bigquery(request):
    """
    Cloud Function Entry Point: Fetches news from NewsAPI and inserts into BigQuery
    """
    try:
        # Fetch news articles
        articles = fetch_news()

        # Transform articles for BigQuery
        rows = [transform_article(article) for article in articles]

        # Insert data into BigQuery
        insert_into_bigquery(rows)

        return "Data ingestion complete", 200
    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}", 500
