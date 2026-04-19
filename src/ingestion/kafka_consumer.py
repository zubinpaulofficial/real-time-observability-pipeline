from kafka import KafkaConsumer
import json
import time

def create_consumer():
    for i in range(10):
        try:
            consumer = KafkaConsumer(
                "events",
                bootstrap_servers="kafka:9092",   # ✅ IMPORTANT
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="event-group"
            )
            print("✅ Connected to Kafka")
            return consumer
        except Exception:
            print(f"⏳ Waiting for Kafka... ({i+1}/10)")
            time.sleep(3)

    raise Exception("❌ Kafka not available")

def consume_events():
    consumer = create_consumer()
    for message in consumer:
        yield message.value