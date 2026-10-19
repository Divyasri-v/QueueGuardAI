"""
QueueGuard AI — RQ Scheduler Entry Point
Required for delayed retries (exponential backoff).
Run in a separate terminal alongside the worker.

Usage:
    python scripts/start_scheduler.py

Note:
    The scheduler requires the rq-scheduler package.
    Install it with: pip install rq-scheduler
"""

import sys
import os
import logging

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger("queueguard.scheduler")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def run_scheduler():
    try:
        from rq_scheduler import Scheduler
    except ImportError:
        logger.error(
            "rq-scheduler is not installed. Install it with: pip install rq-scheduler"
        )
        sys.exit(1)

    # get_rq_redis_client() returns decode_responses=False — required for RQ/Scheduler
    # internals that round-trip pickle-serialised bytes through Redis.
    from queueguard.redis_connection import get_rq_redis_client, check_redis_health
    from config import QUEUE_NAME

    health = check_redis_health()
    if not health["connected"]:
        logger.error(
            "Cannot connect to Redis at %s:%s - %s",
            health["host"], health["port"], health.get("error"),
        )
        sys.exit(1)

    logger.info(
        "Redis connected (%s:%s, latency=%.1fms)",
        health["host"], health["port"], health.get("latency_ms", 0),
    )

    r = get_rq_redis_client()   # bytes client — required for RQ Scheduler internals

    logger.info("Starting RQ Scheduler on queue: %r (interval=1s)", QUEUE_NAME)
    logger.info("This process moves scheduled retries into the active queue.")
    logger.info("Press Ctrl+C to stop.")

    scheduler = Scheduler(queue_name=QUEUE_NAME, connection=r, interval=1)
    scheduler.run()


if __name__ == "__main__":
    run_scheduler()
