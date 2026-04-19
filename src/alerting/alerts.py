def send_alert(event):
    print(f"🚨 ALERT: Anomaly detected in {event['service']} | latency={event['latency_ms']}")