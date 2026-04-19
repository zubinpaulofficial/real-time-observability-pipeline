from collections import defaultdict
import numpy as np

latency_history = defaultdict(list)

def detect_anomaly(event, config):
    service = event["service"]
    latency = event["latency_ms"]

    history = latency_history[service]
    history.append(latency)

    # Only detect anomaly after enough data
    if len(history) < 20:
        return False

    mean = np.mean(history)
    std = np.std(history)

    if std == 0:
        return False

    z_score = (latency - mean) / std

    return abs(z_score) > config["anomaly"]["z_threshold"]