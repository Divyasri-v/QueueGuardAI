"""
QueueGuard AI — RQ Worker Entry Point
Run this script in a separate terminal to process background jobs.

NOTE — Windows compatibility:
    The standard RQ `Worker` uses os.fork() to spawn a child process per job.
    os.fork() is NOT available on Windows, which causes every job to remain
    permanently stuck in 'queued' state.

    This script automatically uses `SimpleWorker` on Windows, which executes
    jobs in the same process without forking. On Linux/macOS the standard
    Worker is used, which provides better isolation.

Usage:
    python scripts/start_worker.py
"""

import sys
import os
import platform
import logging

# Ensure the project root is on the path so all imports work
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rq import Queue, SimpleWorker, Worker
from queueguard.redis_connection import get_redis_client, get_rq_redis_client, check_redis_health
from config import QUEUE_NAME

logger = logging.getLogger("queueguard.worker")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# On Windows, os.fork() is unavailable — SimpleWorker runs jobs in-process.
# On POSIX systems the full Worker is used (better isolation via fork).
_IS_WINDOWS = platform.system() == "Windows"
WorkerClass = SimpleWorker if _IS_WINDOWS else Worker


def run_worker():
    health = check_redis_health()
    if not health["connected"]:
        logger.error(
            "Cannot connect to Redis at %s:%s - %s",
            health["host"], health["port"], health.get("error"),
        )
        logger.error("Start Redis with: docker-compose up -d")
        sys.exit(1)

    logger.info(
        "Redis connected (%s:%s, latency=%.1fms)",
        health["host"], health["port"], health.get("latency_ms", 0),
    )

    r = get_rq_redis_client()   # bytes client — required for RQ internals
    q = Queue(QUEUE_NAME, connection=r)

    logger.info(
        "Starting %s on queue %r  [platform=%s]",
        WorkerClass.__name__, QUEUE_NAME, platform.system(),
    )
    if _IS_WINDOWS:
        logger.info(
            "Using SimpleWorker (in-process) — os.fork() is not available on Windows."
        )
    logger.info("Press Ctrl+C to stop the worker.")

    # Pre-import task module so RQ can resolve function paths
    import queueguard.worker_tasks  # noqa: F401

    worker = WorkerClass([q], connection=r, name="queueguard-worker")
    worker.work(with_scheduler=True, burst=False)


if __name__ == "__main__":
    run_worker()

