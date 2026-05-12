import os
import requests
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer
import socket


load_dotenv()

API_KEY = os.getenv("COINGECKO_API_KEY")
BASE_URL = "https://api.coingecko.com/api/v3"
KAFKA_TOPIC = "crypto-prices"
POLL_INTERVAL_SECONDS = 300 #5 minutes
KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

def fetch_markets():

    url = BASE_URL + "/coins/markets"

    params = {
        "vs_currency" : "usd",
        "order" : "market_cap_desc",
        "per_page" : 100,
        "page" : 1 ,
        "sparkline" : False,
        "price_change_percentage" : "24h"
    }

    headers = {
        "x-cg-demo-api-key" : API_KEY
    }

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    coin_data = response.json()

    return coin_data


def clean_coin(raw):

    return {
        "id" : raw["id"],
        "symbol" : raw["symbol"],
        "name" : raw["name"],
        "current_price" : raw["current_price"],
        "market_cap" : raw["market_cap"],
        "market_cap_rank" : raw["market_cap_rank"],
        "total_volume" : raw["total_volume"],
        "high_24h" : raw["high_24h"],
        "low_24h" : raw["low_24h"],
        "price_change_24h" : raw["price_change_24h"],
        "price_change_pct_24h" : raw["price_change_percentage_24h"],
        "circulating_supply" : raw["circulating_supply"],
        "max_supply" : raw["max_supply"],
        "ath" : raw["ath"],
        "ath_date" : raw["ath_date"],
        "last_updated" : raw["last_updated"],
        "ingested_at" : datetime.now(timezone.utc).isoformat(),
    }



def publish_to_kafka(coins):

    #Kafka Producer Initialization 
    conf = {'bootstrap.servers': KAFKA_SERVERS,
        'client.id': socket.gethostname()}

    producer = Producer(conf)
    json_coins = json.dumps(coins)
    producer.produce(KAFKA_TOPIC, key=coins["symbol"], value=json_coins)
    producer.flush()



def main():
    while True:

        coins = fetch_markets()
        for coin in coins:
            clean = clean_coin(coin)
            publish_to_kafka(clean)
            print(f"Published: {clean['symbol']} - {clean['current_price']} USD")
        print(f"Batch terminé. Prochain fetch dans {POLL_INTERVAL_SECONDS}s")
        time.sleep(POLL_INTERVAL_SECONDS)    

if __name__ == "__main__":
    main()