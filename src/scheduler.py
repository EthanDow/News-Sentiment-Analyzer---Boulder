import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler

from .analyzer import analyze_pending
from .collector import collect_all_topics

log = logging.getLogger(__name__)

DEFAULT_TOPICS = ["artificial intelligence", "climate change", "stock market", "elections"]
TOPICS = [t.strip() for t in os.environ.get("NEWS_TOPICS", ",".join(DEFAULT_TOPICS)).split(",") if t.strip()]
INTERVAL_MIN = int(os.environ.get("NEWS_INTERVAL_MIN", "30"))


def run_cycle():
    try:
        collect_all_topics(TOPICS)
        analyze_pending()
    except Exception:
        log.exception("scheduler: cycle failed")


def start_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(run_cycle, "interval", minutes=INTERVAL_MIN, next_run_time=None)
    scheduler.start()
    log.info("scheduler: started, interval=%d min, topics=%s", INTERVAL_MIN, TOPICS)
    return scheduler
