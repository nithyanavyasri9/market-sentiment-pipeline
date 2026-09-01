# Real-Time Market Sentiment Signal Pipeline

**Stack**: Apache Kafka · FinBERT (LLM) · Spark Structured Streaming · PostgreSQL · dbt · Apache Airflow · Redis · Grafana  
**Domain**: Finance / NLP  
**Pattern**: Event-driven streaming + ML inference + signal generation

---

## What it does

Streams Reddit posts mentioning stock tickers, scores them using FinBERT
(a finance-domain language model), joins the sentiment stream with a
simulated live price feed in 5-minute tumbling windows, and generates
BULLISH / BEARISH / NEUTRAL trading signals — all in real time,
entirely on your local machine.

---

## Architecture

```
Reddit (PRAW)          Simulated Prices
      │                       │
      ▼                       ▼
 Kafka Topic            Kafka Topic
 raw-sentiment          raw-prices
      │                       │
      ▼                       │
 FinBERT Scorer               │
 (runs locally)               │
      │                       │
      ▼                       │
 Kafka Topic                  │
 scored-sentiment             │
      │                       │
      └──────────┬────────────┘
                 ▼
        Spark Structured Streaming
        (5-min tumbling window join)
                 │
        ┌────────┴────────┐
        ▼                 ▼
    PostgreSQL           Redis
    (signals table)    (latest signal
        │               per ticker)
        ▼
       dbt
    (mart_signals)
        │
        ▼
     Grafana
   (live dashboard)
        │
     Airflow
  (orchestration +
    data quality)
```

---

## Local services (all free)

| Service       | URL                        | Credentials   |
|---------------|----------------------------|---------------|
| Kafka UI      | http://localhost:8090      | none          |
| Spark UI      | http://localhost:8080      | none          |
| Grafana       | http://localhost:3000      | admin / admin |
| Airflow       | http://localhost:8088      | admin / admin |
| Redis UI      | http://localhost:8091      | none          |

---

## Setup (one-time)

### 1. Prerequisites
```bash
# Make sure these are installed
docker --version        # Docker Desktop or Docker Engine
docker-compose --version
python --version        # Python 3.10+
```

### 2. Clone and configure
```bash
git clone https://github.com/YOUR_USERNAME/market-sentiment-pipeline
cd market-sentiment-pipeline

cp .env.example .env
# Edit .env and add your Reddit API credentials
# Get them free at: https://www.reddit.com/prefs/apps
```

### 3. Install Python dependencies
```bash
pip install -r requirements.txt
# Note: first run downloads FinBERT model (~440MB), cached after that
```

### 4. Start all infrastructure
```bash
docker-compose up -d
# Wait ~30 seconds for all services to be healthy
docker-compose ps     # all should show "Up"
```

### 5. Initialise Kafka topics
```bash
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw-sentiment --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic raw-prices --partitions 3 --replication-factor 1

docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic scored-sentiment --partitions 3 --replication-factor 1
```

---

## Running the pipeline

Open **4 terminal windows** and run one command in each:

### Terminal 1 — Price feed
```bash
python ingestion/price_producer.py
# Publishes simulated AAPL, TSLA, NVDA, SPY prices every second
```

### Terminal 2 — Reddit feed
```bash
python ingestion/reddit_producer.py
# Streams r/wallstreetbets and r/stocks posts to Kafka
```

### Terminal 3 — FinBERT sentiment scorer
```bash
python processing/sentiment_scorer.py
# Downloads FinBERT on first run, then scores posts in real time
```

### Terminal 4 — Spark stream joiner
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 \
  processing/stream_joiner.py
```

### Watch it live
- Open http://localhost:8090 → see messages flowing through Kafka topics
- Open http://localhost:3000 → Grafana dashboard updates every 10 seconds
- Open http://localhost:8088 → Airflow DAG runs every 30 minutes

---

## Key technical decisions (for interviews)

| Decision | Why |
|----------|-----|
| FinBERT over general BERT | Finance-domain fine-tuning; higher accuracy on ticker/earnings text |
| Tumbling window (not sliding) | Simpler state management; no duplicate signal counting |
| Watermark of 5 min | Handles Reddit API delay without holding state indefinitely |
| Write via `foreachBatch` | Allows JDBC upsert logic not available in native Kafka Spark sink |
| Redis for hot signals | Sub-millisecond reads for API layer without hitting Postgres |
| dbt incremental model | Avoids full table scan on every refresh; scales to months of history |

---

## Resume talking points

> "Built an end-to-end real-time pipeline that joins Reddit sentiment (scored
> with FinBERT) and live stock prices using Spark Structured Streaming with
> 5-minute tumbling windows and watermarking for late-data handling.
> Signals land in Postgres via a dbt semantic layer and surface in Grafana
> with 10-second refresh. Orchestrated with Airflow including data quality
> checks and Redis publishing for low-latency downstream reads."
