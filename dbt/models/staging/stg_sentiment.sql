-- stg_sentiment.sql
-- Cleans and standardises raw scored_sentiment rows

{{ config(materialized='view') }}

select
    id,
    post_id,
    source,
    upper(ticker)                       as ticker,
    left(title, 200)                    as title,
    upper(label)                        as sentiment_label,
    round(confidence::numeric, 4)       as confidence,
    round(positive::numeric,   4)       as positive_score,
    round(negative::numeric,   4)       as negative_score,
    round(neutral::numeric,    4)       as neutral_score,
    reddit_score,
    event_time::timestamptz             as event_time,
    date_trunc('hour', event_time)      as event_hour,
    date_trunc('day',  event_time)      as event_date
from {{ source('raw', 'scored_sentiment') }}
where ticker is not null
  and label  is not null
