from ingestion.generator import generate_event
from ingestion.kafka_producer import send_event
import time

print("Kafka Producer started...")

while True:
    event = generate_event()
    send_event(event)
    print(f"Sent: {event}")
    time.sleep(1)