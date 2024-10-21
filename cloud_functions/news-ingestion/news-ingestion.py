import requests
import json
from google.cloud import bigquery
import os
from datetime import datetime


# Initialise News API
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
if not NEWS_API_KEY:
    raise ValueError("News API Key is missing from environment variables")
NEWS_API_URL = "https://newsapi.org/v2/everything"


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
      "apiKey": NEWS_API_KEY,
      "q": "AI",
      "language": "en",
      "from": "2024-09-20",
      }

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
    """
    Transform the article into the desired format for BigQuery
    """
    return {
        "source": article["source"]["name"],  # Extract source name
        "author": article.get("author"),  # Author might be missing, so .get() is used
        "title": article["title"],  # Article title is mandatory
        "description": article.get("description"),  # Optional
        "url": article["url"],  # Mandatory
        "published_at": article["publishedAt"],  # Keep this as a string in ISO8601 format
        "content": article.get("content")  # Optional content field
    }




def newsapi_to_bigquery(request):
    """
    Cloud Function Entry Point: Fetches news from NewsAPI and inserts into BigQuery
    """
    try:
        # Fetch news articles
        articles = fetch_news()
        print("fetched news")

        # Transform articles for BigQuery
        rows = [transform_article(article) for article in articles]
        print("transformed rows")
        # Insert data into BigQuery
        insert_into_bigquery(rows)
        print("inserted into bq")

        return "Data ingestion complete", 200
    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}", 500
