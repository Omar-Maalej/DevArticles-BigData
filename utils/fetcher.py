import requests
import os
import time
from dotenv import load_dotenv

load_dotenv()

def fetch_articles(page=1, per_page=30):
    url = f"https://dev.to/api/articles?page={page}&per_page={per_page}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch articles: {response.status_code}")
        return []


def fetch_article_details(article_id):
    url = f"https://dev.to/api/articles/{article_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch article details: {response.status_code}")
        return {}