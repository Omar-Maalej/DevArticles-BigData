from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, when, array
from utils.db import collection

def main():
    spark = SparkSession.builder \
        .appName("ArticleAnalyzer") \
        .master("local[*]") \
        .getOrCreate()

    print("Starting Spark session...")

    # Fetching articles with ID and tag list
    articles = list(collection.find({}, {"id": 1, "tags": 1, "published_at": 1, "_id": 0}))

    print(f"Size of articles to process : {len(articles)}")

    if not articles:
        print("No articles found.")
        return

    # Create DataFrame without strict schema first
    df = spark.createDataFrame(articles)

    # Clean and transform the tags column to handle all cases:
    # 1. Already arrays
    # 2. Comma-separated strings
    # 3. Null values
    # 4. Single tags as strings
    
    # First check if tags exists
    if "tags" not in df.columns:
        print("No tags field found in documents")
        spark.stop()
        return

    # Handle all tag format cases
    df = df.withColumn(
        "processed_tags",
        when(
            col("tags").isNull(), 
            array()  # empty array for nulls
        ).when(
            col("tags").cast("string").contains(","),
            split(col("tags").cast("string"), ",\s*")
        ).otherwise(
            array(col("tags").cast("string"))  # wrap single tags in array
        )
    )

    # Explode the processed tags
    tags_df = df.select(explode(col("processed_tags")).alias("tag"))
    
    # Filter out empty strings if any
    tags_df = tags_df.filter(col("tag") != "")
    
    # Count tag occurrences
    tag_counts_df = tags_df.groupBy("tag").count().orderBy(col("count").desc())

    # Collect the results and print
    tag_counts = tag_counts_df.collect()

    print("Tag popularity analysis:")
    for row in tag_counts:
        print(f"Tag: {row['tag']} | Count: {row['count']}")

    spark.stop()

if __name__ == "__main__":
    main()