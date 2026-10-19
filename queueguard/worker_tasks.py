"""
QueueGuard AI — Worker Task Definitions
All job functions that can be safely enqueued and executed by RQ workers.
These are the ONLY functions the system will execute — no arbitrary user code.
"""

import time
import random
import logging
import traceback

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Exception types used by failing tasks (importable for test matching)
# ──────────────────────────────────────────────────────────────────────────────

class TaskTimeoutError(TimeoutError):
    """Raised when a task exceeds its time budget."""


class TaskConnectionError(ConnectionError):
    """Raised when a task cannot reach a required external resource."""


class TaskValueError(ValueError):
    """Raised when task input parameters are invalid."""


class TaskRuntimeError(RuntimeError):
    """Raised for unexpected runtime failures."""


# ──────────────────────────────────────────────────────────────────────────────
# Task implementations
# ──────────────────────────────────────────────────────────────────────────────

def successful_task(duration: float = 1.0, payload: str = "hello") -> dict:
    """
    A task that always completes successfully.

    Args:
        duration: How long to simulate work (seconds, capped at 30).
        payload:  Arbitrary string to echo back in the result.

    Returns:
        Result dict with timing and payload.
    """
    duration = max(0.1, min(float(duration), 30.0))
    logger.info("successful_task started | duration=%.1fs payload=%r", duration, payload)

    t0 = time.perf_counter()
    # Simulate work
    checkpoints = []
    steps = max(1, int(duration))
    for i in range(steps):
        time.sleep(duration / steps)
        checkpoints.append(f"step_{i+1}_done")

    elapsed = time.perf_counter() - t0
    result = {
        "status": "success",
        "payload_echo": payload,
        "duration_s": round(elapsed, 3),
        "checkpoints": checkpoints,
        "message": f"Task completed successfully in {elapsed:.2f}s",
    }
    logger.info("successful_task finished | %s", result)
    return result


def failing_task(error_type: str = "runtime") -> dict:
    """
    A task that intentionally fails for testing failure detection.

    Args:
        error_type: One of 'timeout', 'connection', 'value', 'runtime'.

    Raises:
        TaskTimeoutError | TaskConnectionError | TaskValueError | TaskRuntimeError
    """
    error_type = error_type.lower().strip()
    logger.info("failing_task started | error_type=%r", error_type)

    # Brief pause so the job appears in the "started" state
    time.sleep(0.5)

    if error_type == "timeout":
        raise TaskTimeoutError(
            "Operation timed out after 30 seconds — upstream service did not respond."
        )
    elif error_type == "connection":
        raise TaskConnectionError(
            "Failed to connect to database host 'db.internal:5432' — "
            "connection refused. Check network configuration."
        )
    elif error_type == "value":
        raise TaskValueError(
            "Invalid parameter 'batch_size=-1' — must be a positive integer greater than zero."
        )
    else:
        raise TaskRuntimeError(
            "Unexpected internal error during processing step 3 of 5 — "
            "division by zero in aggregation pipeline."
        )


def slow_task(duration: float = 15.0) -> dict:
    """
    A long-running task that simulates heavy computation.

    Args:
        duration: Total runtime in seconds (capped at 120s).

    Returns:
        Result dict with progress summary.
    """
    duration = max(1.0, min(float(duration), 120.0))
    logger.info("slow_task started | duration=%.1fs", duration)

    t0 = time.perf_counter()
    progress_log = []

    # Report progress every ~10% of total duration
    intervals = 10
    interval_sleep = duration / intervals
    for i in range(intervals):
        time.sleep(interval_sleep)
        pct = (i + 1) * 10
        progress_log.append(f"{pct}% complete")
        logger.debug("slow_task progress: %s%%", pct)

    elapsed = time.perf_counter() - t0
    result = {
        "status": "success",
        "duration_s": round(elapsed, 3),
        "progress_log": progress_log,
        "message": f"Long-running task completed in {elapsed:.2f}s",
    }
    logger.info("slow_task finished | %.2fs", elapsed)
    return result


def data_processing_task(dataset_size: int = 1000) -> dict:
    """
    Processes a synthetic in-memory dataset to simulate real computation.

    Args:
        dataset_size: Number of records to generate and process (max 50_000).

    Returns:
        Result dict with aggregated statistics.
    """
    dataset_size = max(10, min(int(dataset_size), 50_000))
    logger.info("data_processing_task started | size=%d", dataset_size)

    t0 = time.perf_counter()

    # Generate synthetic records
    random.seed(42)
    data = [
        {
            "id": i,
            "value": random.uniform(0, 1000),
            "category": random.choice(["A", "B", "C", "D"]),
            "active": random.choice([True, False]),
        }
        for i in range(dataset_size)
    ]

    # Process: filter, aggregate
    active_records = [r for r in data if r["active"]]
    by_category: dict[str, list[float]] = {}
    for r in active_records:
        by_category.setdefault(r["category"], []).append(r["value"])

    stats = {
        cat: {
            "count": len(vals),
            "mean": round(sum(vals) / len(vals), 2),
            "min": round(min(vals), 2),
            "max": round(max(vals), 2),
        }
        for cat, vals in by_category.items()
    }

    elapsed = time.perf_counter() - t0
    result = {
        "status": "success",
        "total_records": dataset_size,
        "active_records": len(active_records),
        "by_category": stats,
        "duration_s": round(elapsed, 3),
        "message": f"Processed {dataset_size} records in {elapsed:.3f}s",
    }
    logger.info("data_processing_task finished | %s", result)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# Registry — maps task name string to callable (whitelist only)
# ──────────────────────────────────────────────────────────────────────────────

TASK_REGISTRY: dict[str, callable] = {
    "successful_task": successful_task,
    "failing_task": failing_task,
    "slow_task": slow_task,
    "data_processing_task": data_processing_task,
}

TASK_DESCRIPTIONS: dict[str, str] = {
    "successful_task": "Completes successfully after a configurable delay. Good for testing the happy path.",
    "failing_task": "Intentionally raises an exception. Use to test failure detection and retry logic.",
    "slow_task": "Simulates a long-running heavy computation. Good for testing worker timeouts.",
    "data_processing_task": "Generates and aggregates a synthetic dataset. Simulates real ETL work.",
}

# Which tasks are safe to auto-retry (idempotent)
RETRYABLE_TASKS: set[str] = {"successful_task", "slow_task", "data_processing_task"}
