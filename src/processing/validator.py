def validate_event(event):
    required_fields = ["service", "status", "latency_ms", "timestamp"]

    for field in required_fields:
        if field not in event:
            return None

    return event