from google.cloud import bigquery
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import numpy as np
import matplotlib.pyplot as plt


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


st.set_page_config(layout="wide")

st.title("News Bias Detection Dashboard")

# New section: Examples of strong AI Boomer and AI Doomer posts
st.header("Examples of Strong AI Stances")

# Create two columns for Boomer and Doomer examples
col1, col2 = st.columns(2)

# Find strong AI Boomer and AI Doomer examples
strong_boomer = df[df['category'] == 'AI Boomer'].nlargest(3, 'category_score')
strong_doomer = df[df['category'] == 'AI Doomer'].nlargest(3, 'category_score')

with col1:
    st.subheader("Strong AI Boomer Examples")
    for _, article in strong_boomer.iterrows():
        st.markdown(f"**[{article['title']}]({article['url']})**")
        st.markdown(f"*{article['published_at'].strftime('%Y-%m-%d')}* - {article['source']}")
        st.markdown(f"{article['description'][:200]}...")
        st.markdown(f"**Score:** {article['category_score']:.2f}")
        st.markdown("---")

with col2:
    st.subheader("Strong AI Doomer Examples")
    for _, article in strong_doomer.iterrows():
        st.markdown(f"**[{article['title']}]({article['url']})**")
        st.markdown(f"*{article['published_at'].strftime('%Y-%m-%d')}* - {article['source']}")
        st.markdown(f"{article['description'][:200]}...")
        st.markdown(f"**Score:** {article['category_score']:.2f}")
        st.markdown("---")

# Date range select

# Create a row for both charts
st.header("Narrative Shift and Bias Comparison")
col1, col2 = st.columns(2)

with col1:
    # 1. Narrative shift over days
    st.subheader("Narrative Shift Over Days")

    # Add source filter for this chart
    sources_for_shift = st.multiselect("Select sources for narrative shift", df['source'].unique().tolist(), default=df['source'].unique().tolist())

    # Filter data based on selected sources
    df_shift = df[df['source'].isin(sources_for_shift)]

    # Group by date and category
    df_shift_grouped = df_shift.groupby([df_shift['published_at'].dt.date, 'category']).size().unstack(fill_value=0)
    df_shift_grouped['total'] = df_shift_grouped.sum(axis=1)
    df_shift_grouped['AI Boomer %'] = df_shift_grouped['AI Boomer'] / df_shift_grouped['total'] * 100
    df_shift_grouped['AI Doomer %'] = df_shift_grouped['AI Doomer'] / df_shift_grouped['total'] * 100

    # Create the narrative shift plot
    fig_shift = go.Figure()

    fig_shift.add_trace(go.Bar(
        x=df_shift_grouped.index,
        y=df_shift_grouped['AI Boomer %'],
        name='AI Boomer',
        marker_color='blue'
    ))

    fig_shift.add_trace(go.Bar(
        x=df_shift_grouped.index,
        y=df_shift_grouped['AI Doomer %'],
        name='AI Doomer',
        marker_color='red'
    ))

    fig_shift.update_layout(
        title="Daily Narrative Shift: AI Doomer vs AI Boomer",
        xaxis_title="Date",
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
    df_source_bias = df.groupby('source')['category_score'].mean().sort_values(ascending=False).reset_index()

    # Create the bias comparison plot
    fig_bias = go.Figure(go.Bar(
        x=df_source_bias['source'],
        y=df_source_bias['category_score'],
        marker_color=df_source_bias['category_score'],
        marker_colorscale='RdBu_r',
        marker_colorbar=dict(title="Avg. Category Score")
    ))

    fig_bias.update_layout(
        title="Average Bias Score by News Outlet",
        xaxis_title="News Outlet",
        yaxis_title="Average Category Score",
        yaxis_range=[0, 1]
    )

    st.plotly_chart(fig_bias, use_container_width=True)

# 3. Ranking journalists by their boomer or doomer predisposition
st.header("Journalist Ranking by AI Boomer/Doomer Predisposition")
# Calculate the average category score for each journalist
df_journalist_bias = df.groupby('author')['category_score_v2'].agg(['mean', 'count']).reset_index()
df_journalist_bias = df_journalist_bias[df_journalist_bias['count'] >= 5]  # Filter out journalists with less than 5 articles
df_journalist_bias = df_journalist_bias.sort_values('mean', ascending=False)

# Create a color scale based on the mean score
colors = ['red' if score > 0.5 else 'blue' for score in df_journalist_bias['mean']]

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
    title="Journalists Ranked by AI Boomer/Doomer Predisposition",
    xaxis_title="Journalist",
    yaxis_title="Average Category Score",
    yaxis_range=[0, 1],
    showlegend=False
)

st.plotly_chart(fig_journalist, use_container_width=True)

# Add explanation for the chart
st.markdown("""
This chart ranks journalists based on their average category score across their articles. 
- Scores above 0.5 (red) indicate a tendency towards AI Boomer content.
- Scores below 0.5 (blue) indicate a tendency towards AI Doomer content.
- The number on each bar represents the count of articles written by that journalist.
- Only journalists with 5 or more articles are included to ensure a meaningful average.
""")

# Add journalist selection and article preview
selected_journalist = st.selectbox("Select a journalist to preview their articles:", df_journalist_bias['author'].tolist())

if selected_journalist:
    journalist_articles = df[df['author'] == selected_journalist].sort_values('published_at', ascending=False)
    st.subheader(f"Recent articles by {selected_journalist}")
    for _, article in journalist_articles.head(5).iterrows():
        st.markdown(f"**[{article['title']}]({article['url']})**")
        st.markdown(f"*{article['published_at'].strftime('%Y-%m-%d')}* - {article['source']}")
        st.markdown(f"{article['description'][:200]}...")
        st.markdown(f"Category: {article['category']} (Score: {article['category_score']:.2f})")
        st.markdown("---")
