"""
sentiment_scorer.py
───────────────────
Reads raw posts from Kafka, scores them with FinBERT (runs 100% locally,
no API key needed), and writes scored events back to Kafka + Postgres.

FinBERT is a BERT model fine-tuned on financial news — much more accurate
than general-purpose sentiment models for stock-related text.

First run will download the model (~440MB). Cached after that.
"""

import os, json, logging, time
from datetime import datetime
import psycopg2
from confluent_kafka import Consumer, Producer
from transformers import pipeline
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [SCORER] %(message)s")

# ── Kafka ─────────────────────────────────────
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")

consumer = Consumer({
    'bootstrap.servers': BOOTSTRAP,
    'group.id':          'sentiment-scorer-v1',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
})

producer = Producer({
    'bootstrap.servers': BOOTSTRAP,
    'client.id':         'sentiment-scorer',
})

# ── Postgres ──────────────────────────────────
pg = psycopg2.connect(
    host=os.getenv("PG_HOST", "localhost"),
    port=5433,
    dbname="sentiment_db",
    user="nithya",
    password="pipeline123"
)
pg_cursor = pg.cursor()

# ── FinBERT (downloads once, runs locally) ────
logging.info("Loading FinBERT model (first run downloads ~440MB)...")
finbert = pipeline(
    "text-classification",
    model="ProsusAI/finbert",
    return_all_scores=True,
    device=-1   # CPU; change to 0 if you have a GPU
)
logging.info("FinBERT loaded. Ready to score.")


def score(text: str) -> dict:
    truncated = text[:512]
    results = finbert(truncated)
    # Handle both old and new transformers output format
    if isinstance(results, list) and len(results) > 0:
        if isinstance(results[0], list):
            results = results[0]
        elif isinstance(results[0], dict):
            results = results
    scores = {r["label"]: round(r["score"], 4) for r in results}
    best = max(scores, key=scores.get)
    return {
        "label":      best,
        "confidence": scores[best],
        "positive":   scores.get("positive", 0.0),
        "negative":   scores.get("negative", 0.0),
        "neutral":    scores.get("neutral",  0.0),
    }


def write_to_postgres(event: dict, ticker: str, sentiment: dict):
    try:
        pg_cursor.execute("""
            INSERT INTO scored_sentiment
                (post_id, source, ticker, title, label, confidence,
                 positive, negative, neutral, reddit_score, event_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, (
            event["id"][:20],
            event.get("source", "simulated")[:20],
            ticker[:10],
            event["title"][:300],
            sentiment["label"],
            sentiment["confidence"],
            sentiment["positive"],
            sentiment["negative"],
            sentiment["neutral"],
            event.get("score", 0),
            datetime.utcfromtimestamp(event["timestamp"])
        ))
        pg.commit()
    except Exception as e:
        pg.rollback()
        logging.error(f"Postgres write error: {e}")

def run():
    consumer.subscribe(["raw-sentiment"])
    logging.info("Listening on raw-sentiment topic...")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logging.error(f"Consumer error: {msg.error()}")
            continue

        try:
            event     = json.loads(msg.value())
            text      = event["title"] + " " + event.get("text", "")
            sentiment = score(text)
            tickers   = event.get("tickers", [])

            scored_event = {
                **event,
                "sentiment":   sentiment,
                "scored_at":   datetime.utcnow().isoformat()
            }

            # Publish one scored message per ticker mentioned
            for ticker in tickers:
                producer.produce(
                    topic="scored-sentiment",
                    key=ticker,
                    value=json.dumps({**scored_event, "ticker": ticker})
                )
                write_to_postgres(event, ticker, sentiment)

            producer.flush()

            emoji = {"positive": "↑", "negative": "↓", "neutral": "→"}
            logging.info(
                f"{emoji.get(sentiment['label'], '?')} "
                f"[{sentiment['label'].upper()} {sentiment['confidence']:.2f}] "
                f"{tickers} | {event['title'][:55]}..."
            )

        except Exception as e:
            logging.error(f"Scoring error: {e}", exc_info=True)
            time.sleep(1)


if __name__ == "__main__":
    run()
