from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt

# Define a consistent color palette at the top of the file
color_palette = {
    'AI Boomer': '#4682B4',  # Steel Blue
    'AI Doomer': '#CD5C5C',  # Indian Red
    'neutral': '#8B8B83',    # Sage Gray
}

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

# category_score is the score for the predicted category.
# we create category_score_v2 to put a quantitative score on the doomer-boomer spectrum.
df["category_score_v2"] = df.apply(lambda x: x["category_score"] if x["category"] == "AI Boomer" else 1-x["category_score"], axis=1)

# Set page config at the very beginning
st.set_page_config(layout="wide", page_title="AI News Bias Detector", page_icon="🤖")

# Custom CSS for improved styling
st.markdown("""
<style>
    .reportview-container .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    h1 {
        color: #1E1E1E;
    }
    h2 {
        color: #3366cc;
        border-bottom: 1px solid #3366cc;
        padding-bottom: 10px;
    }
    .stMetric {
        background-color: transparent;
        border: 1px solid #e0e0e0;
        border-radius: 5px;
        padding: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }
    .stMetric .metric-label {
        color: #3366cc;
        font-weight: bold;
    }
    .stMetric .metric-value {
        color: #1E1E1E;
        font-size: 1.5em;
    }
</style>
""", unsafe_allow_html=True)

# Title and introduction
col1, col2 = st.columns([1, 3])

with col1:
    st.image("black-mirror-brand-key-visual.jpg", width=200)  # Adjust width as needed

with col2:
    st.title("Detecting Bias towards AI in the News")
    st.markdown("""
        <p style='font-size: 1.2em; font-weight: 300;'>
        A data-driven approach to understanding the narrative shift towards AI
        </p>
    """, unsafe_allow_html=True)
    st.markdown("*By [Alex](https://www.alexiospanos.com). Built with Streamlit and Plotly on Google Cloud in October 2024. [Github](https://github.com/alejio/news-bias-detection/tree/main/streamlit)*")


st.markdown("---")

st.markdown("### Context")
st.markdown("Prototype aiming to detect optimism or [doomerism](https://www.wired.com/story/geoffrey-hinton-ai-chatgpt-dangers/) bias in news articles mentioning AI. Retrieved using [News API](https://newsapi.org/). Bias detection using [zero-shot classification with `bart-large-mnli`](https://huggingface.co/facebook/bart-large-mnli). Will be updated with more data as it becomes available.")

st.markdown("---")


# Dataset Overview section
st.header("Dataset Overview")
col1, col2 = st.columns(2)

with col1:
    st.metric("Total Articles", f"{len(df):,}")
    st.metric("Unique Sources", f"{df['source'].nunique():,}")

with col2:
    st.metric("Unique Journalists", f"{df['author'].nunique():,}")
    date_range = f"{df['published_at'].min().date()} to {df['published_at'].max().date()}"
    st.metric("Date Range", date_range)

st.markdown("---")

# New section: Examples of strong AI Boomer and AI Doomer posts
st.header("Examples of Strong AI Stances")

# Create two columns for Boomer and Doomer examples
col1, col2 = st.columns(2)

# Find strong AI Boomer and AI Doomer examples
strong_boomer = df[df['category'] == 'AI Boomer'].nlargest(3, 'category_score')
strong_doomer = df[df['category'] == 'AI Doomer'].nlargest(3, 'category_score')

with col1:
    st.subheader("Strong AI Optimist Examples")
    for _, article in strong_boomer.iterrows():
        with st.expander(f"{article['title'][:50]}..."):
            st.markdown(f"**[{article['title']}]({article['url']})**")
            st.markdown(f"*{article['published_at'].strftime('%Y-%m-%d')}* - {article['source']}")
            st.markdown(f"{article['description'][:200]}...")
            st.markdown(f"**Score:** {article['category_score']:.2f}")

with col2:
    st.subheader("Strong AI Doomer Examples")
    for _, article in strong_doomer.iterrows():
        with st.expander(f"{article['title'][:50]}..."):
            st.markdown(f"**[{article['title']}]({article['url']})**")
            st.markdown(f"*{article['published_at'].strftime('%Y-%m-%d')}* - {article['source']}")
            st.markdown(f"{article['description'][:200]}...")
            st.markdown(f"**Score:** {article['category_score']:.2f}")

# Date range select

# Create a row for both charts
st.header("Narrative Shift and Bias Comparison")
col1, col2 = st.columns(2)

with col1:
    # 1. Narrative shift over weeks
    st.subheader("Narrative Shift Over Weeks")

    # Add source filter for this chart
    sources_for_shift = st.multiselect("Select sources for narrative shift", df['source'].unique().tolist(), default=df['source'].unique().tolist())

    # Filter data based on selected sources
    df_shift = df[df['source'].isin(sources_for_shift)]

    # Group by week and category
    df_shift['week'] = df_shift['published_at'].dt.to_period('W').apply(lambda r: r.start_time)
    df_shift_grouped = df_shift.groupby([df_shift['week'], 'category']).size().unstack(fill_value=0)
    df_shift_grouped['total'] = df_shift_grouped.sum(axis=1)
    df_shift_grouped['AI Boomer %'] = df_shift_grouped['AI Boomer'] / df_shift_grouped['total'] * 100
    df_shift_grouped['AI Doomer %'] = df_shift_grouped['AI Doomer'] / df_shift_grouped['total'] * 100

    # Create the narrative shift plot
    fig_shift = go.Figure()

    fig_shift.add_trace(go.Bar(
        x=df_shift_grouped.index,
        y=df_shift_grouped['AI Boomer %'],
        name='AI Boomer',
        marker_color=color_palette['AI Boomer']
    ))

    fig_shift.add_trace(go.Bar(
        x=df_shift_grouped.index,
        y=df_shift_grouped['AI Doomer %'],
        name='AI Doomer',
        marker_color=color_palette['AI Doomer']
    ))

    fig_shift.update_layout(
        title="Weekly Narrative Shift: AI Doomer vs AI Boomer",
        xaxis_title="Week",
        yaxis_title="Percentage",
        barmode='stack',
        legend_title="Category",
        hovermode="x unified"
    )

    st.plotly_chart(fig_shift, use_container_width=True)

with col2:
    # 2. Comparison of bias across different news outlets
    st.subheader("Bias Comparison Across News Outlets")

    # Group by source and calculate average category score
    df_source_bias = df.groupby('source')['category_score_v2'].mean().sort_values(ascending=False).reset_index()

    # Create the bias comparison plot
    fig_bias = go.Figure(go.Bar(
        x=df_source_bias['source'],
        y=df_source_bias['category_score_v2'],
        marker_color=color_palette['neutral'],
    ))

    fig_bias.update_layout(
        title="Average Bias Score by News Outlet",
        xaxis_title="News Outlet",
        yaxis_title="Average Category Score",
        yaxis_range=[0, 1]
    )

    st.plotly_chart(fig_bias, use_container_width=True)

# 3. Ranking journalists by their boomer or doomer predisposition
st.header("Journalist Ranking by AI Optimist/Doomer Predisposition")
# Calculate the average category score for each journalist
df_journalist_bias = df.groupby('author')['category_score_v2'].agg(['mean', 'count']).reset_index()
df_journalist_bias = df_journalist_bias[df_journalist_bias['count'] >= 3]  # Filter out journalists with less than 3 articles
df_journalist_bias = df_journalist_bias.sort_values('mean', ascending=False)

# Create a color scale based on the mean score
colors = [color_palette['AI Boomer'] if score > 0.5 else color_palette['AI Doomer'] for score in df_journalist_bias['mean']]

# Create the journalist ranking plot
fig_journalist = go.Figure(go.Bar(
    x=df_journalist_bias['author'],
    y=df_journalist_bias['mean'],
    marker_color=colors,
    text=df_journalist_bias['count'],
    textposition='auto',
    hovertemplate='<b>%{x}</b><br>Average Score: %{y:.2f}<br>Article Count: %{text}<extra></extra>'
))

fig_journalist.update_layout(
    title="Journalists Ranked by AI Optimist/Doomer Predisposition",
    xaxis_title="Journalist",
    yaxis_title="Average Category Score",
    yaxis_range=[0, 1],
    showlegend=False
)

st.plotly_chart(fig_journalist, use_container_width=True)

# Add explanation for the chart
st.markdown("""
This chart ranks journalists based on their average category score across their articles. 
- Scores above 0.5 (red) indicate a tendency towards AI Optimist content.
- Scores below 0.5 (blue) indicate a tendency towards AI Doomer content.
- The number on each bar represents the count of articles written by that journalist.
- Only journalists with 3 or more articles are included to ensure a meaningful average.
""")

# Add journalist selection and article preview
selected_journalist = st.selectbox("Select a journalist to preview their articles:", df_journalist_bias['author'].tolist())

if selected_journalist:
    journalist_articles = df[df['author'] == selected_journalist].sort_values('published_at', ascending=False)
    st.subheader(f"Recent articles by {selected_journalist}")
    
    # Create a 3-column grid
    cols = st.columns(3)
    
    for idx, (_, article) in enumerate(journalist_articles.head(5).iterrows()):
        with cols[idx % 3]:
            st.markdown(f"**[{article['title']}]({article['url']})**")
            st.markdown(f"*{article['published_at'].strftime('%Y-%m-%d')}* - [{article['source']}]({article['url']})")
            st.markdown(f"{article['description'][:200]}...")
            st.markdown(f"Category: {article['category']} (Score: {article['category_score']:.2f})")
            st.markdown(f"[Read full article]({article['url']})")
            st.markdown("---")
