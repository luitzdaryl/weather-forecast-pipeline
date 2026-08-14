import json
from confluent_kafka import Producer

conf = {"bootstrap.servers": "localhost:9092"}
producer = Producer(conf)


def delivery_report(err, msg):
    """Called once per message, confirming it was actually accepted by the broker."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Delivered to {msg.topic()} [partition {msg.partition()}] at offset {msg.offset()}")


message = {"city": "Taipei", "temperature_c": 30.5}

producer.produce(
    "weather-readings",
    value=json.dumps(message).encode("utf-8"),
    callback=delivery_report,
)

producer.flush()  # blocks until all pending messages are actually sent