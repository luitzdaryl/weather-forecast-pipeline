import json
from confluent_kafka import Consumer

conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "weather-test-group",
    "auto.offset.reset": "earliest",  # start from the beginning if no prior offset exists
}
consumer = Consumer(conf)
consumer.subscribe(["weather-readings"])

print("Listening for messages... (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print("Error:", msg.error())
            continue

        data = json.loads(msg.value().decode("utf-8"))
        print(f"Received: {data} (offset {msg.offset()})")
finally:
    consumer.close()