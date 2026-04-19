from fastapi import FastAPI
import psycopg2
import os

app = FastAPI()

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "observability"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )

@app.get("/")
def home():
    return {"message": "Observability API running"}


@app.get("/metrics")
def get_metrics():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT service,
                   COUNT(*) as total_requests,
                   SUM(CASE WHEN status='error' THEN 1 ELSE 0 END) as errors,
                   AVG(latency_ms) as avg_latency
            FROM events
            GROUP BY service
        """)

        data = cur.fetchall()

        cur.close()
        conn.close()

        return [
            {
                "service": row[0],
                "total_requests": row[1],
                "errors": row[2],
                "avg_latency": float(row[3]) if row[3] else 0
            }
            for row in data
        ]

    except Exception as e:
        return {
            "message": "No data yet",
            "error": str(e)
        }