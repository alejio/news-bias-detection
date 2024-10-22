import functions_framework
from google.cloud import bigquery
from transformers import pipeline
import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize BigQuery and transformer model
client = bigquery.Client(project="news-bias-detection-439208")
# BigQuery table details
dataset_id = "news_data"
table_id = "articles"
# Zero-shot classifier
classifier = pipeline("zero-shot-classification", model="facebook/bart-large-mnli")

def check_and_add_columns():
    """
    Check if the 'category' and 'category_score' columns exist in BigQuery, and add them if they don't.
    """
    logger.info("Checking and adding columns if necessary")
    table_id = "news-bias-detection-439208.news_data.articles"
    
    # Get the latest table schema and metadata, including the etag
    table = client.get_table(table_id)  # API request to get the table
    logger.info(f"Retrieved table schema for {table_id}")

    # Check if 'category' and 'category_score' columns exist
    existing_columns = [field.name for field in table.schema]
    schema_changed = False

    new_schema = table.schema[:]  # Copy the existing schema
    
    if "category" not in existing_columns:
        logger.info("Category column does not exist. Adding it...")
        new_schema.append(bigquery.SchemaField("category", "STRING"))
        schema_changed = True
    
    if "category_score" not in existing_columns:
        logger.info("Category Score column does not exist. Adding it...")
        new_schema.append(bigquery.SchemaField("category_score", "FLOAT"))
        schema_changed = True
    
    if schema_changed:
        # Update the table schema with the correct etag
        table.schema = new_schema
        client.update_table(table, ["schema"])  # This will automatically use the latest etag
        logger.info("Schema updated successfully.")
    else:
        logger.info("Schema already contains 'category' and 'category_score'. No changes needed.")

def fetch_uncategorized_data():
    """
    Fetch uncategorized articles from BigQuery.
    """
    logger.info("Fetching uncategorized data from BigQuery")
    query = """
        SELECT url, description
        FROM `news-bias-detection-439208.news_data.articles`
        WHERE category IS NULL
    """
    query_job = client.query(query)
    rows = query_job.result().to_dataframe()  # Load results into a pandas DataFrame
    logger.info(f"Fetched {len(rows)} uncategorized articles")
    return rows

def classify_content(row):
    """
    Classify description using zero-shot classification.
    """
    labels = ["AI Boomer", "AI Doomer"]
    result = classifier(row['description'], labels)
    # Return the most likely label (highest score) and its corresponding score
    return result['labels'][0], result['scores'][0]  # (category, score)

def update_bigquery_with_classification(rows):
    """
    Update BigQuery with classification results (category and score).
    """
    logger.info("Classifying content and updating BigQuery")
    # Prepare data for updating back to BigQuery
    classifications = rows.apply(lambda row: classify_content(row), axis=1)
    rows['category'] = classifications.map(lambda x: x[0])
    rows['category_score'] = classifications.map(lambda x: x[1])

    # Prepare a list of dictionaries for each row to update
    rows_to_update = rows[['url', 'category', 'category_score']].to_dict(orient='records')

    # Define the update query using 'url' as the unique identifier
    update_query = """
        UPDATE `news-bias-detection-439208.news_data.articles`
        SET category = @category, category_score = @category_score
        WHERE url = @url
    """
    
    # Execute the update query for each row
    for i, row in enumerate(rows_to_update):
        query_job = client.query(
            update_query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("category", "STRING", row['category']),
                    bigquery.ScalarQueryParameter("category_score", "FLOAT64", row['category_score']),
                    bigquery.ScalarQueryParameter("url", "STRING", row['url']),
                ]
            ),
        )
        query_job.result()  # Wait for the job to complete
        if (i + 1) % 10 == 0:  # Log progress every 10 rows
            logger.info(f"Updated {i + 1} rows in BigQuery")
    
    logger.info(f"Finished updating {len(rows_to_update)} rows in BigQuery")

@functions_framework.http
def classify_articles(request):
    """
    Cloud Function entry point. This function fetches uncategorized articles from BigQuery,
    checks if the 'category' and 'category_score' columns exist, adds them if needed,
    classifies the articles, and writes back the classification results.
    """
    logger.info("Starting classify_articles function")
    try:
        # Check if the 'category' and 'category_score' columns exist, add if missing
        check_and_add_columns()

        # Fetch uncategorized data
        rows = fetch_uncategorized_data()

        # Classify the content using zero-shot classification
        update_bigquery_with_classification(rows)

        logger.info("Classification completed successfully")
        return "Classification completed and data updated in BigQuery.", 200

    except Exception as e:
        logger.error(f"Error in classify_articles: {str(e)}", exc_info=True)
        return f"Error: {str(e)}", 500
