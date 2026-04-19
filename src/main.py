from pipeline import run_pipeline
from analytics import run_analytics
import threading
import time


def run_analytics_job():
    while True:
        time.sleep(30)  # every 30 sec
        run_analytics()


if __name__ == "__main__":
    threading.Thread(target=run_analytics_job, daemon=True).start()
    run_pipeline()