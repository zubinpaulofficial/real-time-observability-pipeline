from storage.db import get_connection

def run_analytics():
    conn = get_connection()
    cur = conn.cursor()

    print("\n--- Analytics ---")

    # Error rate
    cur.execute("""
        SELECT service,
        COUNT(*) FILTER (WHERE status='error') * 1.0 / COUNT(*)
        FROM events
        GROUP BY service;
    """)
    print("Error Rates:", cur.fetchall())

    # Avg latency
    cur.execute("""
        SELECT service, AVG(latency_ms)
        FROM events
        GROUP BY service;
    """)
    print("Avg Latency:", cur.fetchall())

    cur.close()
    conn.close()