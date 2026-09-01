"""
stream_joiner.py
────────────────
Spark Structured Streaming job that:
  1. Reads scored-sentiment and raw-prices from Kafka
  2. Aggregates sentiment per ticker in 5-min tumbling windows
  3. Joins with price aggregates on (ticker, window)
  4. Generates BULLISH / BEARISH / NEUTRAL signals
  5. Writes to Postgres (local Delta Lake equivalent)

Run locally:
  spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
               org.postgresql:postgresql:42.7.1 \
    processing/stream_joiner.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count,
    when, expr, to_timestamp, explode
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, LongType, DoubleType, ArrayType
)
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
PG_URL  = "jdbc:postgresql://localhost:5432/sentiment_db"
PG_OPTS = {"user": "nithya", "password": "pipeline123", "driver": "org.postgresql.Driver"}

# ── Spark session ─────────────────────────────
spark = SparkSession.builder \
    .appName("MarketSentimentJoiner") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ── Schemas ───────────────────────────────────
sentiment_schema = StructType([
    StructField("id",         StringType()),
    StructField("ticker",     StringType()),
    StructField("title",      StringType()),
    StructField("score",      IntegerType()),
    StructField("timestamp",  LongType()),
    StructField("sentiment",  StructType([
        StructField("label",      StringType()),
        StructField("confidence", DoubleType()),
        StructField("positive",   DoubleType()),
        StructField("negative",   DoubleType()),
        StructField("neutral",    DoubleType()),
    ]))
])

price_schema = StructType([
    StructField("ticker",    StringType()),
    StructField("price",     DoubleType()),
    StructField("volume",    DoubleType()),
    StructField("timestamp", StringType()),
])

# ── Read sentiment stream ─────────────────────
sentiment_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "scored-sentiment") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

sentiment_df = sentiment_raw \
    .select(from_json(col("value").cast("string"), sentiment_schema).alias("d")) \
    .select(
        col("d.ticker"),
        col("d.sentiment.positive").alias("positive"),
        col("d.sentiment.negative").alias("negative"),
        col("d.score").alias("reddit_score"),
        to_timestamp(expr("d.timestamp")).alias("event_time")
    ) \
    .withWatermark("event_time", "5 minutes")

# 5-minute window aggregation per ticker
sentiment_agg = sentiment_df \
    .groupBy(window("event_time", "5 minutes"), col("ticker")) \
    .agg(
        avg("positive").alias("avg_positive"),
        avg("negative").alias("avg_negative"),
        count("*").alias("mention_count"),
        avg("reddit_score").alias("avg_reddit_score")
    ) \
    .withColumn("sentiment_score",
                col("avg_positive") - col("avg_negative"))

# ── Read price stream ─────────────────────────
price_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", "raw-prices") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

price_df = price_raw \
    .select(from_json(col("value").cast("string"), price_schema).alias("d")) \
    .select(
        col("d.ticker"),
        col("d.price"),
        col("d.volume"),
        to_timestamp(col("d.timestamp")).alias("event_time")
    ) \
    .withWatermark("event_time", "5 minutes")

price_agg = price_df \
    .groupBy(window("event_time", "5 minutes"), col("ticker")) \
    .agg(
        avg("price").alias("avg_price"),
        avg("volume").alias("avg_volume")
    )

# ── Join sentiment + price on (window, ticker) ─
joined = sentiment_agg.join(
    price_agg,
    ["window", "ticker"],
    "inner"
).select(
    col("window.start").alias("window_start"),
    col("ticker"),
    col("sentiment_score"),
    col("mention_count"),
    col("avg_price"),
    col("avg_volume"),
    # Signal logic: high-confidence sentiment + volume threshold
    when(
        (col("sentiment_score") > 0.25) & (col("mention_count") >= 3),
        "BULLISH"
    ).when(
        (col("sentiment_score") < -0.25) & (col("mention_count") >= 3),
        "BEARISH"
    ).otherwise("NEUTRAL").alias("signal")
)

# ── Write to Postgres ─────────────────────────
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df.write \
        .jdbc(PG_URL, "sentiment_signals", mode="append", properties=PG_OPTS)
    print(f"[Batch {batch_id}] Written {batch_df.count()} signal rows to Postgres")


query = joined.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints/signals") \
    .trigger(processingTime="30 seconds") \
    .start()

print("Spark stream joiner running. Press Ctrl+C to stop.")
print("Watch signals appear at: http://localhost:3000 (Grafana)")
query.awaitTermination()
