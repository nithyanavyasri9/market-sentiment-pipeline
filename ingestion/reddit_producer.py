"""
reddit_producer.py
──────────────────
Streams Reddit posts from r/wallstreetbets and r/stocks into Kafka.
Uses PRAW (free Reddit API - no payment needed).

Setup:
  1. Go to https://www.reddit.com/prefs/apps
  2. Create a "script" app (free)
  3. Copy client_id and client_secret into .env
"""

import os, json, time, logging
from datetime import datetime
from confluent_kafka import Producer
import praw
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [REDDIT] %(message)s")

# ── Kafka setup ───────────────────────────────
producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
    'client.id': 'reddit-producer',
    'acks': 'all',
    'retries': 3
})

# ── Reddit setup ──────────────────────────────
reddit = praw.Reddit(
    client_id=os.getenv('REDDIT_CLIENT_ID'),
    client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
    user_agent="market-sentiment-bot/1.0 by Nithya"
)

TICKERS = [
    "AAPL", "TSLA", "NVDA", "SPY", "MSFT",
    "AMZN", "GOOGL", "META", "AMD", "PLTR"
]

SUBREDDITS = ["wallstreetbets", "stocks", "investing"]


def extract_tickers(text: str) -> list:
    """Find which tickers are mentioned in the post."""
    text_upper = text.upper()
    return [t for t in TICKERS if f" {t} " in f" {text_upper} "]


def delivery_report(err, msg):
    if err:
        logging.error(f"Delivery failed: {err}")
    else:
        logging.debug(f"Delivered to {msg.topic()} [{msg.partition()}]")


def publish_post(post, subreddit_name: str):
    full_text = post.title + " " + (post.selftext or "")
    tickers = extract_tickers(full_text)

    if not tickers:
        return   # skip posts with no ticker mentions

    message = {
        "id":          post.id,
        "source":      "reddit",
        "subreddit":   subreddit_name,
        "title":       post.title[:300],
        "text":        (post.selftext or "")[:500],
        "score":       post.score,
        "num_comments":post.num_comments,
        "tickers":     tickers,
        "timestamp":   int(post.created_utc),
        "ingested_at": datetime.utcnow().isoformat()
    }

    producer.produce(
        topic="raw-sentiment",
        key=post.id,
        value=json.dumps(message),
        callback=delivery_report
    )
    producer.poll(0)

    logging.info(
        f"[{subreddit_name}] {post.title[:60]}... "
        f"→ tickers: {tickers} | upvotes: {post.score}"
    )


def stream_subreddits():
    """Stream new posts from multiple subreddits simultaneously."""
    logging.info(f"Starting stream for: {SUBREDDITS}")
    subreddit = reddit.subreddit("+".join(SUBREDDITS))

    while True:
        try:
            for post in subreddit.stream.submissions(skip_existing=True):
                publish_post(post, post.subreddit.display_name)
                producer.flush()

        except Exception as e:
            logging.error(f"Stream error: {e}. Reconnecting in 10s...")
            time.sleep(10)


if __name__ == "__main__":
    stream_subreddits()
