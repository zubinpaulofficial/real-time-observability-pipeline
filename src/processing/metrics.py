metrics = {
    "total_events": 0,
    "invalid_events": 0,
    "anomalies": 0
}

def update_metrics(event, is_valid, is_anomaly):
    metrics["total_events"] += 1

    if not is_valid:
        metrics["invalid_events"] += 1

    if is_anomaly:
        metrics["anomalies"] += 1


def print_metrics():
    print("\n--- Pipeline Metrics ---")
    for k, v in metrics.items():
        print(f"{k}: {v}")