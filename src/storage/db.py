import psycopg2
import os
import time

def get_connection():
    retries = 10
    delay = 3

    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "postgres"),
                database=os.getenv("DB_NAME", "observability"),
                user=os.getenv("DB_USER", "postgres"),
                password=os.getenv("DB_PASSWORD", "postgres")
            )
            return conn
        except psycopg2.OperationalError:
            print(f"DB not ready, retrying in {delay}s... ({i+1}/{retries})")
            time.sleep(delay)

    raise Exception("Database connection failed after retries")

def insert_event(event):
    conn = get_connection()
    cur = conn.cursor()

    # ✅ Fix numpy types
    event["is_anomaly"] = bool(event["is_anomaly"])

    cur.execute("""
        INSERT INTO events (service, status, latency_ms, timestamp, latency_bucket, is_anomaly)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (
        event["service"],
        event["status"],
        event["latency_ms"],
        event["timestamp"],
        event["latency_bucket"],
        event["is_anomaly"]
    ))

    conn.commit()
    cur.close()
    conn.close()

def create_table():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS events (
        id SERIAL PRIMARY KEY,
        service TEXT,
        status TEXT,
        latency_ms INT,
        latency_bucket TEXT,
        is_anomaly BOOLEAN,
        timestamp TIMESTAMP
    );
    """)

    conn.commit()
    cur.close()
    conn.close()