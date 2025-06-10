from kafka import KafkaProducer
import json
from utils.fetcher import fetch_articles
import time
from utils.db import upsert_article


TOTAL_ARTICLES = 5000
ARTICLES_PER_PAGE = 300
KAFKA_TOPIC = "articles"
KAFKA_BROKER = "localhost:9093"  

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    articles_fetched = 0
    page = 1
    print("Starting to fetch and stream articles...")
    
    while articles_fetched < TOTAL_ARTICLES:
        articles = fetch_articles(page=page, per_page=ARTICLES_PER_PAGE)
        
        if not articles:
            print("No more articles to fetch.")
            break
        
        for article in articles:
            # Send to Kafka
            upsert_article(article)
            producer.send(KAFKA_TOPIC, value=article)
            articles_fetched += 1
            if articles_fetched >= TOTAL_ARTICLES:
                break
        
        print(f"Streamed {articles_fetched}/{TOTAL_ARTICLES} articles so far...")
        page += 1
        time.sleep(1)  # simple rate limit protection
    
    producer.flush()
    producer.close()

if __name__ == "__main__":
    main()