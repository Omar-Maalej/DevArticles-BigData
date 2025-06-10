import pandas as pd
import streamlit as st
from utils.db import get_latest_tag_analysis
from datetime import datetime

# Load data from MongoDB
analysis_data = get_latest_tag_analysis()

if not analysis_data:
    st.error("No tag analysis data found in MongoDB")
    st.stop()

# Convert to DataFrame
df = pd.DataFrame(analysis_data["tag_counts"])

# Sort by count (descending) and take top 20
top_tags = df.sort_values("count", ascending=False).head(20)

# Ensure tags are ordered properly in the index
top_tags["tag"] = pd.Categorical(
    top_tags["tag"], 
    categories=top_tags["tag"], 
    ordered=True
)
top_tags = top_tags.sort_values("tag")  # Sort by categorical order

# Display the analysis
st.title("Tag Popularity Analysis")
st.caption(f"Last updated: {analysis_data['analysis_date'].strftime('%Y-%m-%d %H:%M:%S UTC')}")
st.caption(f"Total unique tags analyzed: {analysis_data['total_tags']}")

# Show the bar chart
st.bar_chart(
    data=top_tags.set_index("tag"),
    height=500,
    use_container_width=True
)

# Optional: Show raw data
with st.expander("Show raw data"):
    st.dataframe(top_tags.sort_values("count", ascending=False))