import time
import yaml
import logging

from ingestion.generator import generate_event
from ingestion.kafka_consumer import consume_events

from processing.validator import validate_event
from processing.transformer import transform_event
from processing.anomaly import detect_anomaly
from processing.metrics import update_metrics, print_metrics, metrics

from storage.db import insert_event, create_table, get_connection

from utils.logger import setup_logger


def load_config():
    with open("configs/pipeline.yaml", "r") as f:
        return yaml.safe_load(f)

def normalize_event(event):
    event["is_anomaly"] = bool(event.get("is_anomaly", False))
    return event

# Wait for DB properly
def wait_for_db(retries=15, delay=2):
    for i in range(retries):
        try:
            conn = get_connection()
            conn.close()
            print("✅ Database is ready")
            return
        except Exception:
            print(f"⏳ Waiting for DB... ({i+1}/{retries})")
            time.sleep(delay)

    raise Exception("❌ Database connection failed after retries")


def run_pipeline():
    setup_logger()
    print("🚀 Pipeline starting...")

    # 🔥 FIX 1: Wait BEFORE anything
    wait_for_db()

    # 🔥 FIX 2: Ensure table exists
    create_table()

    config = load_config()
    sleep_time = config["pipeline"]["sleep_time"]

    print("✅ Pipeline started successfully")

    try:
        for event in consume_events():
            logging.info(f"Received: {event}")

            # VALIDATION
            validated_event = validate_event(event)

            if not validated_event:
                logging.warning("Invalid event skipped")
                update_metrics(event, is_valid=False, is_anomaly=False)
                continue

            # TRANSFORMATION
            event = transform_event(validated_event)

            # ANOMALY DETECTION
            is_anomaly = detect_anomaly(event, config)
            event["is_anomaly"] = is_anomaly

            # ALERTING
            if is_anomaly:
                try:
                    from alerting.alerts import send_alert
                    send_alert(event)
                except Exception as e:
                    logging.error(f"Alert failed: {e}")

            # STORAGE
            event = normalize_event(event)
            insert_event(event)
            logging.info(f"Stored: {event}")

            # METRICS (single update only)
            update_metrics(event, is_valid=True, is_anomaly=is_anomaly)

            # PRINT METRICS EVERY 10 EVENTS
            if metrics["total_events"] % 10 == 0:
                print_metrics()

            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Shutting down pipeline gracefully...")
        print_metrics()