import requests
import json
from google.cloud import bigquery
import os
from datetime import datetime, timedelta


# Initialise News API
NEWS_API_KEY = os.getenv("NEWS_API_KEY")
if not NEWS_API_KEY:
    raise ValueError("News API Key is missing from environment variables")

NEWS_API_URL = "https://newsapi.org/v2/everything"


# Initialize BigQuery client globally
client = bigquery.Client(project="news-bias-detection-439208")

# BigQuery table details
dataset_id = "news_data"
table_id = "articles"


def fetch_news():
    """
    Fetch news articles from NewsAPI
    """
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=30)

    params = {
        "apiKey": NEWS_API_KEY,
        "q": "AI",
        "language": "en",
        "from": start_date.isoformat(),
        "to": end_date.isoformat(),
    }

    response = requests.get(NEWS_API_URL, params=params)
    all_articles = response.json().get("articles", [])
    print(f"Articles fetched: {len(all_articles)}")
    return all_articles


def insert_new_rows_into_bigquery(rows):
    """
    Insert only new rows of articles into BigQuery
    """
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    table = client.get_table(table_ref)

    # Attempt to insert rows
    errors = client.insert_rows_json(table, rows)

    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Inserted {len(rows)} rows into BigQuery")

    return len(errors) == 0


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
        "content": article.get("content"),  # Optional content field
        # Optional fields below
        "category": article.get("category", None),  # Optional category, fallback to "General"
        "category_score": article.get("category_score", None)  # Optional category score
    }


def newsapi_to_bigquery(request):
    """
    Cloud Function Entry Point: Fetches news from NewsAPI and inserts new articles into BigQuery
    """
    try:
        # Fetch news articles
        articles = fetch_news()
        print("Fetched news articles")

        # Transform articles for BigQuery
        rows = [transform_article(article) for article in articles]
        print("Transformed rows")

        # Insert new data into BigQuery
        insert_new_rows_into_bigquery(rows)
        print("Inserted new rows into BigQuery")

        return ("Data ingestion complete", 200)
    except Exception as e:
        print(f"Error: {str(e)}")
        return (f"Error: {str(e)}", 500)
