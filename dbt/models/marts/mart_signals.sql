-- mart_signals.sql
-- Final signal mart consumed by Grafana dashboard.
-- Refreshed every 5 minutes via Airflow.

{{ config(
    materialized='incremental',
    unique_key='signal_id',
    on_schema_change='sync_all_columns'
) }}

with signals as (
    select
        window_start,
        ticker,
        round(sentiment_score::numeric, 4)  as sentiment_score,
        mention_count,
        round(avg_price::numeric, 4)         as avg_price,
        signal,
        processed_at,
        md5(ticker || window_start::text)    as signal_id
    from {{ source('raw', 'sentiment_signals') }}

    {% if is_incremental() %}
    where window_start > (select max(window_start) from {{ this }})
    {% endif %}
),

-- Rolling 1-hour sentiment average per ticker
hourly_avg as (
    select
        ticker,
        date_trunc('hour', window_start) as hour,
        avg(sentiment_score)             as hourly_sentiment,
        sum(mention_count)               as hourly_mentions
    from signals
    group by 1, 2
)

select
    s.*,
    h.hourly_sentiment,
    h.hourly_mentions,
    -- Strength label for dashboard coloring
    case
        when s.signal = 'BULLISH' and s.sentiment_score > 0.5  then 'STRONG BUY'
        when s.signal = 'BULLISH'                               then 'BUY'
        when s.signal = 'BEARISH' and s.sentiment_score < -0.5 then 'STRONG SELL'
        when s.signal = 'BEARISH'                               then 'SELL'
        else 'HOLD'
    end as strength_label
from signals s
left join hourly_avg h
    on s.ticker = h.ticker
    and date_trunc('hour', s.window_start) = h.hour
