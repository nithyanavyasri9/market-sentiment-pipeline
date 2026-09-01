-- ─────────────────────────────────────────────
--  Postgres schema for local sentiment pipeline
-- ─────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS raw_prices (
    id          SERIAL PRIMARY KEY,
    ticker      VARCHAR(10) NOT NULL,
    price       NUMERIC(12,4),
    volume      NUMERIC(20,4),
    event_time  TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS scored_sentiment (
    id            SERIAL PRIMARY KEY,
    post_id       VARCHAR(20),
    source        VARCHAR(20),
    ticker        VARCHAR(10),
    title         TEXT,
    label         VARCHAR(10),
    confidence    NUMERIC(6,4),
    positive      NUMERIC(6,4),
    negative      NUMERIC(6,4),
    neutral       NUMERIC(6,4),
    reddit_score  INTEGER,
    event_time    TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sentiment_signals (
    id              SERIAL PRIMARY KEY,
    window_start    TIMESTAMPTZ,
    ticker          VARCHAR(10),
    sentiment_score NUMERIC(8,4),
    mention_count   INTEGER,
    avg_price       NUMERIC(12,4),
    signal          VARCHAR(10),   -- BULLISH / BEARISH / NEUTRAL
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Index for fast Grafana queries
CREATE INDEX IF NOT EXISTS idx_signals_ticker_time
    ON sentiment_signals (ticker, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_sentiment_ticker_time
    ON scored_sentiment (ticker, event_time DESC);

-- View used by dbt mart
CREATE OR REPLACE VIEW v_latest_signals AS
SELECT DISTINCT ON (ticker)
    ticker,
    signal,
    sentiment_score,
    avg_price,
    mention_count,
    window_start
FROM sentiment_signals
ORDER BY ticker, window_start DESC;
