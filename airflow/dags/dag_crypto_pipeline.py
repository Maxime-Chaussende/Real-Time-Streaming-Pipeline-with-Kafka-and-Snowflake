from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from producers.coingecko_producer import fetch_markets, clean_coin, publish_to_kafka
from utils.snowflake_connection import get_snowflake_connection
import os

def fetch_and_publish():
    coins = fetch_markets()
    for coin in coins:
        clean = clean_coin(coin)
        publish_to_kafka(clean)
    print(f"Published {len(coins)} coins to Kafka")

def verify_snowflake():


    conn = get_snowflake_connection()

    # 4. Vérifier les données récentes
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) FROM CRYPTO_PRICES
        WHERE INGESTED_AT >= DATEADD(minute, -10, CURRENT_TIMESTAMP())
    """)
    count = cursor.fetchone()[0]
    conn.close()

    if count == 0:
        raise Exception("No data received in Snowflake in the last 10 minutes!")
    
    print(f"Snowflake verification OK — {count} rows received")

with DAG(
    dag_id="crypto_pipeline",
    schedule="*/5 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
) as dag:

    task_fetch = PythonOperator(
        task_id="fetch_and_publish",
        python_callable=fetch_and_publish,
    )

    task_verify = PythonOperator(
        task_id="verify_snowflake",
        python_callable=verify_snowflake,
    )

    task_fetch >> task_verify