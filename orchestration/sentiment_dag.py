"""
sentiment_dag.py
────────────────
Airflow DAG that:
  1. Validates data quality in Postgres every 30 min
  2. Runs dbt to refresh the mart_signals model
  3. Publishes latest signals to Redis for low-latency reads
  4. Sends a Slack/log alert if any ticker goes strongly BEARISH

Open Airflow UI at: http://localhost:8088 (admin/admin)
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2, redis, json, logging

default_args = {
    "owner":            "nithya",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}


def validate_data_quality(**ctx):
    """Check that fresh signals are arriving within the last 10 minutes."""
    conn = psycopg2.connect(
        host="postgres", dbname="sentiment_db",
        user="nithya", password="pipeline123"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*) FROM sentiment_signals
        WHERE processed_at > NOW() - INTERVAL '10 minutes'
    """)
    count = cur.fetchone()[0]
    conn.close()

    if count == 0:
        raise ValueError(
            "DATA QUALITY ALERT: No new signals in the last 10 minutes. "
            "Check Kafka producers and Spark job."
        )
    logging.info(f"Data quality OK — {count} fresh signals found.")
    return count


def publish_to_redis(**ctx):
    """Push latest signal per ticker to Redis for fast API reads."""
    r   = redis.Redis(host="redis", port=6379, decode_responses=True)
    conn = psycopg2.connect(
        host="postgres", dbname="sentiment_db",
        user="nithya", password="pipeline123"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT ON (ticker)
            ticker, signal, sentiment_score, avg_price,
            mention_count, window_start
        FROM sentiment_signals
        ORDER BY ticker, window_start DESC
    """)
    rows = cur.fetchall()
    conn.close()

    pipeline = r.pipeline()
    for row in rows:
        ticker, signal, score, price, mentions, ts = row
        payload = {
            "signal":          signal,
            "sentiment_score": float(score),
            "avg_price":       float(price),
            "mention_count":   mentions,
            "as_of":           ts.isoformat()
        }
        pipeline.setex(f"signal:{ticker}", 600, json.dumps(payload))
    pipeline.execute()

    logging.info(f"Published {len(rows)} signals to Redis.")
    return len(rows)


def check_bearish_alerts(**ctx):
    """Log a warning if any ticker has strong bearish signal."""
    conn = psycopg2.connect(
        host="postgres", dbname="sentiment_db",
        user="nithya", password="pipeline123"
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT ticker, sentiment_score, mention_count, window_start
        FROM sentiment_signals
        WHERE signal = 'BEARISH'
          AND sentiment_score < -0.4
          AND window_start > NOW() - INTERVAL '30 minutes'
        ORDER BY sentiment_score ASC
    """)
    alerts = cur.fetchall()
    conn.close()

    for ticker, score, mentions, ts in alerts:
        logging.warning(
            f"BEARISH ALERT: {ticker} | score={score:.3f} | "
            f"mentions={mentions} | at {ts}"
        )

    if not alerts:
        logging.info("No strong bearish signals detected.")

    return len(alerts)


with DAG(
    dag_id="sentiment_pipeline_orchestrator",
    default_args=default_args,
    description="Validate, refresh, and alert on sentiment signals",
    schedule_interval="*/30 * * * *",   # every 30 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["finance", "realtime", "sentiment", "mlops"]
) as dag:

    t1_validate = PythonOperator(
        task_id="validate_data_quality",
        python_callable=validate_data_quality,
    )

    t2_dbt = BashOperator(
        task_id="dbt_refresh_mart",
        bash_command="cd /opt/airflow/dags/../dbt && dbt run --select mart_signals --profiles-dir .",
        # If dbt not installed in Airflow image, skip this task locally
        # and run:  cd dbt && dbt run  manually
    )

    t3_redis = PythonOperator(
        task_id="publish_to_redis",
        python_callable=publish_to_redis,
    )

    t4_alerts = PythonOperator(
        task_id="check_bearish_alerts",
        python_callable=check_bearish_alerts,
    )

    t1_validate >> t2_dbt >> t3_redis >> t4_alerts
