from utils.fetcher import fetch_articles
from utils.db import upsert_article
import time

TOTAL_ARTICLES = 5000
ARTICLES_PER_PAGE = 300

def main():
    articles_fetched = 0
    page = 1
    print("Starting to fetch articles...")
    
    while articles_fetched < TOTAL_ARTICLES:
        articles = fetch_articles(page=page, per_page=ARTICLES_PER_PAGE)
        
        if not articles:
            print("No more articles to fetch.")
            break
        
        for article in articles:
            upsert_article(article)
            articles_fetched += 1
            if articles_fetched >= TOTAL_ARTICLES:
                break
        
        print(f"Fetched {articles_fetched}/{TOTAL_ARTICLES} articles so far...")
        print("Hello world")
        page += 1
        time.sleep(1)  # simple rate limit protection

if __name__ == "__main__":
    main()
