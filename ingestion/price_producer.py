"""
price_producer.py
─────────────────
Produces simulated real-time stock prices into Kafka.
No API key needed for local development/portfolio demo.

Simulates realistic price movements using geometric Brownian motion —
the same model used in Black-Scholes options pricing.

To use real prices later: swap simulate_prices() with
the Alpaca WebSocket or yfinance live feed.
"""

import os, json, time, random, math, logging
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [PRICES] %(message)s")

producer = Producer({
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
    'client.id': 'price-producer',
})

# Realistic base prices and volatility per ticker
TICKERS = {
    "AAPL":  {"price": 185.0,  "volatility": 0.015, "drift": 0.0001},
    "TSLA":  {"price": 245.0,  "volatility": 0.035, "drift": 0.0002},
    "NVDA":  {"price": 875.0,  "volatility": 0.030, "drift": 0.0003},
    "SPY":   {"price": 480.0,  "volatility": 0.008, "drift": 0.0001},
    "MSFT":  {"price": 415.0,  "volatility": 0.012, "drift": 0.0001},
    "AMZN":  {"price": 185.0,  "volatility": 0.018, "drift": 0.0001},
    "GOOGL": {"price": 175.0,  "volatility": 0.016, "drift": 0.0001},
    "META":  {"price": 510.0,  "volatility": 0.022, "drift": 0.0002},
    "AMD":   {"price": 165.0,  "volatility": 0.028, "drift": 0.0002},
    "PLTR":  {"price": 22.0,   "volatility": 0.040, "drift": 0.0001},
}

# Maintain current prices across ticks
current_prices = {t: v["price"] for t, v in TICKERS.items()}


def next_price(ticker: str) -> float:
    """Geometric Brownian Motion: dS = S*(mu*dt + sigma*dW)"""
    config = TICKERS[ticker]
    dt = 1 / (252 * 6.5 * 60)   # 1-second increment in trading-year fraction
    drift   = config["drift"]
    sigma   = config["volatility"]
    shock   = random.gauss(0, 1)
    current = current_prices[ticker]
    new_price = current * math.exp(
        (drift - 0.5 * sigma**2) * dt + sigma * math.sqrt(dt) * shock
    )
    current_prices[ticker] = round(new_price, 4)
    return current_prices[ticker]


def delivery_report(err, msg):
    if err:
        logging.error(f"Delivery failed: {err}")


def simulate_prices(interval_seconds: float = 1.0):
    """Publish one price tick per ticker every interval_seconds."""
    logging.info(f"Simulating prices for: {list(TICKERS.keys())}")
    logging.info("Open http://localhost:8090 to see messages in Kafka UI")

    while True:
        for ticker in TICKERS:
            price  = next_price(ticker)
            volume = random.randint(100, 50000)

            payload = {
                "ticker":    ticker,
                "price":     price,
                "volume":    volume,
                "bid":       round(price - random.uniform(0.01, 0.05), 4),
                "ask":       round(price + random.uniform(0.01, 0.05), 4),
                "timestamp": datetime.utcnow().isoformat()
            }

            producer.produce(
                topic="raw-prices",
                key=ticker,
                value=json.dumps(payload),
                callback=delivery_report
            )

        producer.flush()
        logging.info(
            "  ".join(f"{t}: ${current_prices[t]:.2f}" for t in list(TICKERS)[:5])
        )
        time.sleep(interval_seconds)


if __name__ == "__main__":
    simulate_prices(interval_seconds=1.0)
