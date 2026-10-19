"""
QueueGuard AI — Recovery Module
Implements retry logic with exponential backoff, dead-letter tracking,
and audit history for every attempt.

Retry flow:
  1. Check eligibility (is retryable? under max retries? task in whitelist?)
  2. Calculate backoff delay
  3. Re-enqueue in RQ (using rq.Retry for automatic RQ-level retries, or
     manual re-enqueue for user-triggered retries)
  4. Update job history record
  5. Save attempt audit entry

Note on scheduling: For delayed retries (backoff > 0), rq-scheduler is used
if available.  If it is not running, retries are enqueued immediately with a
warning.  Start the scheduler with: python scripts/start_scheduler.py
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any

import redis
from rq import Queue
from rq.job import Job

from config import (
    QUEUE_NAME,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    RETRY_BACKOFF_MAX,
)
from queueguard.history import (
    load_job_record,
    update_job_field,
    save_attempt,
    list_all_jobs,
)
from queueguard.redis_connection import get_rq_redis_client
from queueguard.worker_tasks import TASK_REGISTRY, RETRYABLE_TASKS
from queueguard.failure_analyzer import analyze_failure, analysis_to_dict

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Backoff calculation
# ──────────────────────────────────────────────────────────────────────────────

def calculate_backoff(attempt: int, base: float = RETRY_BACKOFF_BASE, cap: int = RETRY_BACKOFF_MAX) -> float:
    """
    Exponential backoff with jitter.

    delay = min(base ** attempt, cap)

    Returns delay in seconds (float).
    """
    delay = min(base ** attempt, cap)
    return float(delay)


# ──────────────────────────────────────────────────────────────────────────────
# Eligibility check
# ──────────────────────────────────────────────────────────────────────────────

def check_retry_eligibility(record: dict) -> tuple[bool, str]:
    """
    Determine whether a job is eligible for retry.

    Returns:
        (eligible: bool, reason: str)
    """
    task_name = record.get("task_name", "")
    status = record.get("status", "")
    retry_count = int(record.get("retry_count", 0))
    is_retryable = record.get("is_retryable", True)

    if status not in ("failed", "retrying"):
        return False, f"Job is in '{status}' state — only 'failed' or 'retrying' jobs can be retried."

    if retry_count >= MAX_RETRIES:
        return False, (
            f"Job has reached the maximum retry limit ({MAX_RETRIES}). "
            "It has been moved to the dead-letter queue."
        )

    if task_name not in RETRYABLE_TASKS:
        return False, (
            f"Task '{task_name}' is not marked as retryable (non-idempotent). "
            "Manual intervention required."
        )

    if not is_retryable:
        return False, (
            "Failure analysis determined this job is not safe to retry "
            "(e.g., invalid input). Fix the underlying issue first."
        )

    return True, "Eligible for retry."


# ──────────────────────────────────────────────────────────────────────────────
# Core retry function
# ──────────────────────────────────────────────────────────────────────────────

def retry_job(
    r: redis.Redis,
    job_id: str,
    triggered_by: str = "manual",
) -> dict:
    """
    Retry a failed job.

    Args:
        r:            Redis client.
        job_id:       The job_id from our history store.
        triggered_by: 'manual' (user click) or 'auto' (auto-retry policy).

    Returns:
        {
            "success": bool,
            "new_job_id": str | None,
            "message": str,
            "attempt": int,
            "backoff_s": float,
        }
    """
    record = load_job_record(r, job_id)
    if not record:
        return {
            "success": False,
            "new_job_id": None,
            "message": f"No history record found for job_id={job_id}",
            "attempt": 0,
            "backoff_s": 0,
        }

    eligible, reason = check_retry_eligibility(record)
    if not eligible:
        # Mark as dead-letter if max retries exceeded
        retry_count = int(record.get("retry_count", 0))
        if retry_count >= MAX_RETRIES:
            update_job_field(r, job_id, status="dead", dead_reason=reason)
        return {
            "success": False,
            "new_job_id": None,
            "message": reason,
            "attempt": retry_count,
            "backoff_s": 0,
        }

    task_name = record["task_name"]
    task_params = record.get("task_params", {}) or {}
    retry_count = int(record.get("retry_count", 0))
    new_attempt = retry_count + 1
    backoff_s = calculate_backoff(new_attempt)

    # Attempt to use rq-scheduler for delayed retries
    new_job = _enqueue_with_backoff(r, task_name, task_params, backoff_s)

    if new_job is None:
        return {
            "success": False,
            "new_job_id": None,
            "message": "Failed to enqueue retry job — check Redis connection.",
            "attempt": new_attempt,
            "backoff_s": backoff_s,
        }

    # Update original job record
    now = datetime.now(timezone.utc).isoformat()
    update_job_field(
        r, job_id,
        status="retrying",
        retry_count=new_attempt,
        last_retry_at=now,
        retry_job_id=new_job.id,
    )

    # Save attempt audit
    attempt_record = {
        "attempt_index": new_attempt,
        "triggered_by": triggered_by,
        "retry_job_id": new_job.id,
        "backoff_s": backoff_s,
        "scheduled_at": now,
        "status": "scheduled",
    }
    save_attempt(r, job_id, attempt_record)

    logger.info(
        "Retry scheduled | original=%s new=%s attempt=%d backoff=%.1fs by=%s",
        job_id, new_job.id, new_attempt, backoff_s, triggered_by,
    )

    return {
        "success": True,
        "new_job_id": new_job.id,
        "message": (
            f"Retry #{new_attempt} scheduled with {backoff_s:.0f}s backoff "
            f"(job_id: {new_job.id})."
        ),
        "attempt": new_attempt,
        "backoff_s": backoff_s,
    }


def _enqueue_with_backoff(
    r: redis.Redis,
    task_name: str,
    task_params: dict,
    backoff_s: float,
) -> Any | None:
    """
    Enqueue task using rq-scheduler if backoff > 0, else enqueue immediately.
    Falls back to immediate enqueue if scheduler is unavailable.
    r is the decoded (history) client; we obtain the RQ raw client internally.
    """
    func = TASK_REGISTRY.get(task_name)
    if func is None:
        logger.error("Task %r not found in registry", task_name)
        return None

    r_raw = get_rq_redis_client()   # bytes client for RQ
    try:
        if backoff_s > 0:
            return _try_scheduled_enqueue(r_raw, func, task_params, backoff_s)
        else:
            q = Queue(QUEUE_NAME, connection=r_raw)
            return q.enqueue(func, kwargs=task_params or {})
    except Exception as exc:
        logger.error("Enqueue failed: %s", exc)
        return None


def _try_scheduled_enqueue(r_raw: redis.Redis, func, task_params: dict, delay_s: float):
    """Try rq-scheduler; fall back to immediate enqueue if unavailable."""
    try:
        from rq_scheduler import Scheduler
        scheduler = Scheduler(queue_name=QUEUE_NAME, connection=r_raw)
        run_at = datetime.now(timezone.utc) + timedelta(seconds=delay_s)
        job = scheduler.enqueue_at(run_at, func, kwargs=task_params or {})
        logger.info("Scheduled retry at %s via rq-scheduler", run_at.isoformat())
        return job
    except ImportError:
        logger.warning("rq-scheduler not installed; enqueuing immediately")
    except Exception as exc:
        logger.warning("rq-scheduler unavailable (%s); enqueuing immediately", exc)

    # Fallback: immediate enqueue
    q = Queue(QUEUE_NAME, connection=r_raw)
    return q.enqueue(func, kwargs=task_params or {})


# ──────────────────────────────────────────────────────────────────────────────
# Auto-retry policy
# ──────────────────────────────────────────────────────────────────────────────

def auto_retry_failed_jobs(r: redis.Redis) -> list[dict]:
    """
    Scan all failed jobs and automatically retry eligible ones.

    Returns a list of retry result dicts.
    """
    results = []
    failed_jobs = [j for j in list_all_jobs(r, limit=200) if j.get("status") == "failed"]

    for record in failed_jobs:
        job_id = record.get("job_id")
        if not job_id:
            continue

        eligible, reason = check_retry_eligibility(record)
        if eligible:
            result = retry_job(r, job_id, triggered_by="auto")
            results.append({**result, "original_job_id": job_id})
            logger.info("Auto-retry triggered for %s", job_id)
        else:
            logger.debug("Auto-retry skipped for %s: %s", job_id, reason)

    return results


# ──────────────────────────────────────────────────────────────────────────────
# Recovery confirmation (called after a retried job completes)
# ──────────────────────────────────────────────────────────────────────────────

def confirm_recovery(r: redis.Redis, original_job_id: str, retried_job_id: str) -> bool:
    """
    Check if a retried job completed successfully and mark the original as recovered.
    r is the decoded (history) client.
    Returns True if recovery is confirmed.
    """
    try:
        r_raw = get_rq_redis_client()   # bytes client for Job.fetch
        rq_job = Job.fetch(retried_job_id, connection=r_raw)
        if rq_job.is_finished:
            update_job_field(
                r, original_job_id,
                status="finished",
                recovered=True,
                recovery_job_id=retried_job_id,
                recovery_time=datetime.now(timezone.utc).isoformat(),
            )
            logger.info("Recovery confirmed | original=%s via=%s", original_job_id, retried_job_id)
            return True
    except Exception as exc:
        logger.debug("Could not confirm recovery for %s: %s", original_job_id, exc)
    return False
