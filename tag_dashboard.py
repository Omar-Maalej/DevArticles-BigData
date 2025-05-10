import pandas as pd
import streamlit as st

# Load the data
df = pd.read_csv("tag_counts.csv")

# Take top 20 tags (already sorted in CSV)
top_tags = df.head(20)

# Ensure tags are ordered properly in the index
top_tags["Tag"] = pd.Categorical(top_tags["Tag"], categories=top_tags["Tag"], ordered=True)
top_tags = top_tags.sort_values("Tag")  # Sort by categorical order, not alphabetically

# Set Tag as index and plot
st.title("Tag Popularity - Batch Analysis")
st.bar_chart(data=top_tags.set_index("Tag"))
