import json
import os
import sys
from confluent_kafka import Consumer

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(SCRIPT_DIR, "..", "..", "data-pipeline", "scripts"))
from fetch_weather import insert_into_snowflake

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "weather-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # commit manually, only after a successful Snowflake insert
}


def main():
    consumer = Consumer(conf)
    consumer.subscribe(["weather-readings"])

    processed = 0
    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            if msg is None:
                break  # nothing new arrived within 5s — treat the topic as drained for now
            if msg.error():
                print("Error:", msg.error())
                continue

            reading = json.loads(msg.value().decode("utf-8"))
            insert_into_snowflake(reading)
            consumer.commit(msg)  # only commit AFTER the insert actually succeeds
            processed += 1
            print(f"Processed offset {msg.offset()}: {reading}")
    finally:
        consumer.close()

    print(f"Done. Processed {processed} message(s).")


if __name__ == "__main__":
    main()