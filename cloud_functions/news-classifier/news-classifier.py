import functions_framework
from google.cloud import bigquery
from transformers import pipeline
import pandas as pd

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
    table_id = "news-bias-detection-439208.news_data.articles"
    
    # Get the latest table schema and metadata, including the etag
    table = client.get_table(table_id)  # API request to get the table

    # Check if 'category' and 'category_score' columns exist
    existing_columns = [field.name for field in table.schema]
    schema_changed = False

    new_schema = table.schema[:]  # Copy the existing schema
    
    if "category" not in existing_columns:
        print("Category column does not exist. Adding it...")
        new_schema.append(bigquery.SchemaField("category", "STRING"))
        schema_changed = True
    
    if "category_score" not in existing_columns:
        print("Category Score column does not exist. Adding it...")
        new_schema.append(bigquery.SchemaField("category_score", "FLOAT"))
        schema_changed = True
    
    if schema_changed:
        # Update the table schema with the correct etag
        table.schema = new_schema
        client.update_table(table, ["schema"])  # This will automatically use the latest etag
        print("Schema updated successfully.")
    else:
        print("Schema already contains 'category' and 'category_score'. No changes needed.")


def fetch_uncategorized_data():
    """
    Fetch uncategorized articles from BigQuery.
    """
    query = """
        SELECT url, content
        FROM `news-bias-detection-439208.news_data.articles`
        WHERE category IS NULL
        LIMIT 100
    """
    query_job = client.query(query)
    rows = query_job.result().to_dataframe()  # Load results into a pandas DataFrame
    return rows

def classify_content(row):
    """
    Classify content using zero-shot classification.
    """
    labels = ["AI Boomer", "AI Doomer"]
    result = classifier(row['content'], labels)
    # Return the most likely label (highest score) and its corresponding score
    return result['labels'][0], result['scores'][0]  # (category, score)

def update_bigquery_with_classification(rows):
    """
    Update BigQuery with classification results (category and score).
    """
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
    for row in rows_to_update:
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


@functions_framework.http
def classify_articles(request):
    """
    Cloud Function entry point. This function fetches uncategorized articles from BigQuery,
    checks if the 'category' and 'category_score' columns exist, adds them if needed,
    classifies the articles, and writes back the classification results.
    """
    try:
        # Check if the 'category' and 'category_score' columns exist, add if missing
        check_and_add_columns()

        # Fetch uncategorized data
        rows = fetch_uncategorized_data()

        # Classify the content using zero-shot classification
        update_bigquery_with_classification(rows)

        return "Classification completed and data updated in BigQuery.", 200

    except Exception as e:
        print(f"Error: {str(e)}")
        return f"Error: {str(e)}", 500
