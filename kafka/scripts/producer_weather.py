# producer_weather.py

import json
import os
import sys
from confluent_kafka import Producer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "data-pipeline", "scripts"))
from fetch_weather import fetch_weather

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")   # ← ADD THIS LINE

producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})   # ← CHANGED from the hardcoded string

delivery_failed = False

def delivery_report(err, msg):
    global delivery_failed
    if err is not None:
        print(f"Delivery failed: {err}")
        delivery_failed = True
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
    remaining = producer.flush(timeout=10)

    if remaining > 0 or delivery_failed:
        raise RuntimeError(
            f"Kafka delivery failed (undelivered={remaining}, error_seen={delivery_failed})"
        )

if __name__ == "__main__":
    main()