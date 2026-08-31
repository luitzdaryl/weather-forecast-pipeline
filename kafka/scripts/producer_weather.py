# producer_weather.py

import json
import os
import sys
from confluent_kafka import Producer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "data-pipeline", "scripts"))
from fetch_weather import fetch_weather

producer = Producer({"bootstrap.servers": "localhost:9092"})


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")


def main():
    reading = fetch_weather()
    print("Fetched:", reading)

    producer.produce(
        "weather-readings",
        value=json.dumps(reading, default=str).encode("utf-8"),
       
        callback=delivery_report,
    )
    producer.flush()

if __name__ == "__main__":
    main()