from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import streamlit as st

# Initialize BigQuery client
client = bigquery.Client(project="news-bias-detection-439208")

# Run a query
query = """
SELECT * FROM `news-bias-detection-439208.news_data.articles`
"""
query_job = client.query(query)

# Convert the query result to a Pandas DataFrame
df = query_job.to_dataframe()

# Convert 'published_at' to datetime
df['published_at'] = pd.to_datetime(df['published_at'])

# Interactive DataFrame Preview
st.title("News Bias Detection Dashboard")
st.subheader("DataFrame Preview")
num_rows = st.slider("Number of rows to display", min_value=5, max_value=50, value=10)
st.dataframe(df.head(num_rows))

# Interactive Plotly Chart
st.subheader("Average Category Score Over Time")

# Prepare data for plotting
df_grouped = df.groupby(['source', 'published_at'])['category_score'].mean().reset_index()

# Create source selector
sources = df['source'].unique()
selected_source = st.selectbox("Select a source", sources)

# Filter data based on selected source
filtered_df = df_grouped[df_grouped['source'] == selected_source]

# Create the plot
fig = px.line(filtered_df, x='published_at', y='category_score', 
              title=f"Average Category Score for {selected_source}")
fig.update_layout(xaxis_title="Date", yaxis_title="Average Category Score")

# Display the plot
st.plotly_chart(fig)

# columns are ['source', 'author', 'title', 'description', 'url', 'published_at','content', 'category', 'category_score']
