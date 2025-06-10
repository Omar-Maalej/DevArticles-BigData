from pymongo import MongoClient
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN
import numpy as np
import collections
from tqdm import tqdm

# MongoDB setup
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "dev_articles_db"
ARTICLE_COLLECTION = "articles"
CLUSTER_COLLECTION = "article_clusters"

def load_articles():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[ARTICLE_COLLECTION]
    articles = list(collection.find({}, {"id": 1, "title": 1, "description": 1, "_id": 0}))
    return articles

def embed_articles(texts, model_name='all-MiniLM-L6-v2'):
    model = SentenceTransformer(model_name)
    embeddings = model.encode(texts, show_progress_bar=True)
    return embeddings

def cluster_embeddings(embeddings, eps=1.0, min_samples=5):
    clustering_model = DBSCAN(eps=eps, min_samples=min_samples, metric='cosine')
    labels = clustering_model.fit_predict(embeddings)
    return labels

def save_clusters(articles, labels):
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    cluster_collection = db[CLUSTER_COLLECTION]

    for idx, label in enumerate(labels):
        article = articles[idx]
        cluster_collection.update_one(
            {"article_id": article["id"]},
            {"$set": {
                "cluster_id": int(label),
                "title": article.get("title", ""),
                "description": article.get("description", "")
            }},
            upsert=True
        )

def print_cluster_summary(labels):
    counter = collections.Counter(labels)
    print("\nCluster Summary:")
    for cluster_id, size in sorted(counter.items()):
        print(f"Cluster {cluster_id:>3}: {size} articles")

def main():
    print("Loading articles from MongoDB...")
    articles = load_articles()
    if not articles:
        print("No articles found.")
        return

    texts = [f"{a.get('title', '')}. {a.get('description', '')}" for a in articles]
    print(f"Loaded {len(texts)} articles.")

    print("\nEmbedding articles...")
    embeddings = embed_articles(texts)

    print("\nClustering with DBSCAN...")
    labels = cluster_embeddings(embeddings)

    print("\nSaving clustering results...")
    save_clusters(articles, labels)

    print_cluster_summary(labels)

    print("\nDone.")

if __name__ == "__main__":
    main()
