sudo docker build -t streamlit-app:latest .
sudo docker tag streamlit-app:latest europe-west2-docker.pkg.dev/news-bias-detection-439208/streamlit-app-repo/streamlit-app:latest

gcloud builds submit --tag europe-west2-docker.pkg.dev/news-bias-detection-439208/streamlit-app-repo/streamlit-app:latest

  gcloud run deploy streamlit-app \
  --image europe-west2-docker.pkg.dev/news-bias-detection-439208/streamlit-app-repo/streamlit-app:latest \
  --platform managed \
  --region europe-west2 \
  --allow-unauthenticated \
  --service-account news-bias-detection-439208@appspot.gserviceaccount.com \
  --port 8080
