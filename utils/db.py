from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URI = os.getenv('MONGO_URI', "mongodb://localhost:27017/dev_articles_db")
MONGO_DB = os.getenv('MONGO_DB')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION')

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]

def upsert_article(article):
    collection.update_one(
        {"id": article["id"]},
        {"$set": article},
        upsert=True
    )
