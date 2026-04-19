from kafka import KafkaProducer
import json
from datetime import datetime


def serializer(v):
    def default(o):
        if isinstance(o, datetime):
            return o.isoformat()  # ✅ clean + standard
        raise TypeError(f"Type not serializable: {type(o)}")

    return json.dumps(v, default=default).encode("utf-8")


producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=serializer
)


def send_event(event):
    producer.send("events", event)
    producer.flush()  # ✅ ensures it's actually sent