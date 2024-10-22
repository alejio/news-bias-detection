gcloud functions deploy news-api-ingest \
  --runtime python310 \
  --trigger-http \
  --entry-point newsapi_to_bigquery \
  --set-env-vars NEWS_API_KEY=$NEWS_API_KEY \
  --region europe-west2 \
  --project news-bias-detection-439208