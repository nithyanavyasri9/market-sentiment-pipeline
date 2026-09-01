import json, time, random, logging
from datetime import datetime
from confluent_kafka import Producer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SIMULATOR] %(message)s")
producer = Producer({"bootstrap.servers": "localhost:9092"})

TICKERS = ["AAPL", "TSLA", "NVDA", "SPY", "MSFT", "AMD", "PLTR"]
BULLISH = ["{t} going to the moon bought more calls", "Loading up on {t} fundamentals solid", "{t} breaking out this is the move", "{t} revenue beat expectations buying dip"]
BEARISH = ["{t} is overvalued taking profits now", "Sold all my {t} rally is over", "{t} missed earnings guidance cut going lower", "{t} losing market share fast"]
NEUTRAL = ["What do you think about {t} at current levels", "Anyone holding {t} through earnings", "{t} trading sideways waiting for direction", "Watching {t} closely could go either way"]
BIAS = {"NVDA":[0.6,0.1,0.3],"TSLA":[0.4,0.4,0.2],"AAPL":[0.5,0.2,0.3],"SPY":[0.4,0.3,0.3],"MSFT":[0.5,0.2,0.3],"AMD":[0.5,0.2,0.3],"PLTR":[0.4,0.4,0.2]}

def generate_post(ticker):
    bias = BIAS.get(ticker, [0.33, 0.33, 0.34])
    bucket = random.choices(["bullish","bearish","neutral"], weights=bias, k=1)[0]
    templates = {"bullish":BULLISH,"bearish":BEARISH,"neutral":NEUTRAL}[bucket]
    title = random.choice(templates).format(t=ticker)
    return {
        "id": "sim_" + ticker + "_" + str(int(time.time()*1000)),
        "source": "simulated",
        "subreddit": "wallstreetbets",
        "title": title,
        "text": "",
        "score": random.randint(1, 5000),
        "tickers": [ticker],
        "timestamp": int(datetime.utcnow().timestamp()),
        "_sentiment": bucket
    }

def run():
    logging.info("Simulating posts to Kafka raw-sentiment topic")
    while True:
        ticker = random.choice(TICKERS)
        post = generate_post(ticker)
        producer.produce(topic="raw-sentiment", key=post["id"], value=json.dumps(post))
        producer.flush()
        logging.info("[%s] %s: %s" % (post["_sentiment"].upper(), ticker, post["title"]))
        time.sleep(3)

if __name__ == "__main__":
    run()
