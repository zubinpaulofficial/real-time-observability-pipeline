def transform_event(event):
    latency = event["latency_ms"]

    if latency < 200:
        bucket = "low"
    elif latency < 1000:
        bucket = "medium"
    else:
        bucket = "high"

    event["latency_bucket"] = bucket
    return event