from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from producers.coingecko_producer import fetch_markets, clean_coin, publish_to_kafka
import snowflake.connector
import os
from cryptography.hazmat.primitives import serialization

def fetch_and_publish():
    coins = fetch_markets()
    for coin in coins:
        clean = clean_coin(coin)
        publish_to_kafka(clean)
    print(f"Published {len(coins)} coins to Kafka")

def verify_snowflake():
     # 1. Lire la clé privée
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    with open(private_key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None
        )
    
    # 2. Sérialiser la clé pour snowflake-connector
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    print(f"Type of private_key_bytes: {type(private_key_bytes)}")
    print(f"private_key_path: {private_key_path}")

    # 3. Se connecter à Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=private_key_bytes,
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    )

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