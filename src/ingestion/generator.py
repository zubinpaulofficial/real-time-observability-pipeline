import random
from datetime import datetime

services = ["auth", "payment", "search", "checkout"]
statuses = ["success", "error"]

def generate_event():
    return {
        "service": random.choice(services),
        "status": random.choice(statuses),
        "latency_ms": random.randint(50, 2000),
        "timestamp": datetime.utcnow()
    }