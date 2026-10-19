"""
QueueGuard AI — Job Service
Central interface between the Streamlit dashboard and the Redis/RQ backend.
Handles job enqueue, status fetching, listing, and metrics aggregation.
"""

import logging
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import redis
from rq import Queue
from rq.job import Job, JobStatus
from rq.registry import (
    StartedJobRegistry,
    FinishedJobRegistry,
    FailedJobRegistry,
    DeferredJobRegistry,
    ScheduledJobRegistry,
)

from config import QUEUE_NAME, ALLOWED_TASKS, JOB_TIMEOUT
from queueguard.redis_connection import (
    get_redis_client,
    get_rq_redis_client,
    check_redis_health,
)
from queueguard.worker_tasks import TASK_REGISTRY, TASK_DESCRIPTIONS, RETRYABLE_TASKS
from queueguard.history import (
    save_job_record,
    load_job_record,
    list_all_jobs,
    count_jobs_by_status,
    update_job_field,
)
from queueguard.failure_analyzer import (
    analyze_failure_from_rq_job,
    analyze_failure,
    analysis_to_dict,
)

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Parameter validation
# ──────────────────────────────────────────────────────────────────────────────

_PARAM_CONSTRAINTS: dict[str, dict] = {
    "successful_task": {
        "duration": {"type": float, "min": 0.1, "max": 30.0, "default": 1.0},
        "payload":  {"type": str,   "max_len": 256, "default": "hello"},
    },
    "failing_task": {
        "error_type": {
            "type": str,
            "choices": ["timeout", "connection", "value", "runtime"],
            "default": "runtime",
        },
    },
    "slow_task": {
        "duration": {"type": float, "min": 1.0, "max": 120.0, "default": 15.0},
    },
    "data_processing_task": {
        "dataset_size": {"type": int, "min": 10, "max": 50_000, "default": 1_000},
    },
}


def validate_params(task_name: str, params: dict) -> dict:
    """
    Validate and coerce job parameters.

    Returns cleaned params dict.
    Raises ValueError with a descriptive message on invalid input.
    """
    if task_name not in ALLOWED_TASKS:
        raise ValueError(
            f"Task '{task_name}' is not in the allowed task list. "
            f"Allowed: {', '.join(ALLOWED_TASKS)}"
        )

    constraints = _PARAM_CONSTRAINTS.get(task_name, {})
    cleaned: dict = {}

    for field, rules in constraints.items():
        raw = params.get(field, rules.get("default"))

        # Type coercion
        try:
            expected_type = rules["type"]
            value = expected_type(raw)
        except (ValueError, TypeError):
            raise ValueError(
                f"Parameter '{field}' must be of type {rules['type'].__name__}"
            )

        # Range / length checks
        if "min" in rules and value < rules["min"]:
            raise ValueError(
                f"Parameter '{field}' must be >= {rules['min']} (got {value})"
            )
        if "max" in rules and value > rules["max"]:
            raise ValueError(
                f"Parameter '{field}' must be <= {rules['max']} (got {value})"
            )
        if "max_len" in rules and len(str(value)) > rules["max_len"]:
            raise ValueError(
                f"Parameter '{field}' must be <= {rules['max_len']} characters"
            )
        if "choices" in rules and value not in rules["choices"]:
            raise ValueError(
                f"Parameter '{field}' must be one of: {', '.join(rules['choices'])}"
            )

        cleaned[field] = value

    return cleaned


# ──────────────────────────────────────────────────────────────────────────────
# Job enqueue
# ──────────────────────────────────────────────────────────────────────────────

def enqueue_job(task_name: str, params: dict | None = None) -> dict:
    """
    Validate parameters and enqueue a job in RQ.

    Returns a result dict with job_id, status, and queue position.
    """
    try:
        r_raw = get_rq_redis_client()   # bytes client for RQ
        r_raw.ping()                    # fail fast if Redis is down
        r = get_redis_client()          # decoded client for our history store
    except redis.ConnectionError as exc:
        return {
            "success": False,
            "error": f"Cannot connect to Redis: {exc}",
        }

    # Validate
    try:
        cleaned_params = validate_params(task_name, params or {})
    except ValueError as exc:
        return {"success": False, "error": str(exc)}

    func = TASK_REGISTRY.get(task_name)
    if func is None:
        return {"success": False, "error": f"Task '{task_name}' not found in registry."}

    # Enqueue via the raw (bytes) client so RQ can serialise the job correctly
    q = Queue(QUEUE_NAME, connection=r_raw)
    job_id = str(uuid.uuid4())
    enqueue_time = datetime.now(timezone.utc)

    try:
        rq_job = q.enqueue(
            func,
            kwargs=cleaned_params,
            job_id=job_id,
            timeout=JOB_TIMEOUT,
            result_ttl=86400,     # keep result 24h
            failure_ttl=86400,    # keep failure 24h
        )
    except Exception as exc:
        logger.error("Enqueue failed: %s", exc)
        return {"success": False, "error": f"Enqueue failed: {exc}"}

    # Persist initial record
    record = {
        "job_id": job_id,
        "task_name": task_name,
        "task_params": cleaned_params,
        "status": "queued",
        "enqueue_time": enqueue_time.isoformat(),
        "enqueue_timestamp": enqueue_time.timestamp(),
        "start_time": None,
        "finish_time": None,
        "duration_s": None,
        "result": None,
        "error": None,
        "exc_type": None,
        "exc_message": None,
        "traceback": None,
        "retry_count": 0,
        "is_retryable": task_name in RETRYABLE_TASKS,
        "failure_analysis": None,
        "recovered": False,
        "retry_job_id": None,
    }
    try:
        save_job_record(r, record)
    except Exception as exc:
        logger.warning("Could not persist initial job record: %s", exc)

    logger.info("Enqueued job | id=%s task=%s params=%s", job_id, task_name, cleaned_params)

    return {
        "success": True,
        "job_id": job_id,
        "task_name": task_name,
        "params": cleaned_params,
        "queue_position": q.count,
        "enqueue_time": enqueue_time.isoformat(),
    }


# ──────────────────────────────────────────────────────────────────────────────
# Job status fetching + history sync
# ──────────────────────────────────────────────────────────────────────────────

def get_job_status(job_id: str) -> dict | None:
    """
    Fetch the current status of a job from RQ + our history store.
    Syncs the history record with the latest RQ state.

    Returns merged dict, or None if not found anywhere.
    """
    try:
        r = get_redis_client()          # decoded — for history
        r_raw = get_rq_redis_client()   # bytes   — for RQ Job.fetch
    except redis.ConnectionError:
        return None

    # Load from our history first
    record = load_job_record(r, job_id) or {}

    # Try to fetch from RQ using the raw client
    try:
        rq_job = Job.fetch(job_id, connection=r_raw)
        _sync_from_rq(r, r_raw, job_id, rq_job, record)
        record = load_job_record(r, job_id) or record
    except Exception:
        pass  # RQ job may have expired; use history record

    return record if record else None


def _sync_from_rq(r: redis.Redis, r_raw: redis.Redis, job_id: str, rq_job: Job, record: dict) -> None:
    """Update history record (via decoded r) from the live RQ job state."""
    # r     = decoded client  — for our history writes
    # r_raw = bytes client    — for any further RQ calls inside this function
    status_map = {
        JobStatus.QUEUED:    "queued",
        JobStatus.STARTED:   "started",
        JobStatus.FINISHED:  "finished",
        JobStatus.FAILED:    "failed",
        JobStatus.DEFERRED:  "queued",
        JobStatus.SCHEDULED: "queued",
        JobStatus.STOPPED:   "failed",
        JobStatus.CANCELED:  "failed",
    }
    # Handle CREATED status (RQ 2.x)
    if hasattr(JobStatus, "CREATED"):
        status_map[JobStatus.CREATED] = "queued"
    rq_status = rq_job.get_status()
    our_status = status_map.get(rq_status, str(rq_status))

    updates: dict = {"status": our_status}

    if rq_job.started_at and not record.get("start_time"):
        updates["start_time"] = rq_job.started_at.isoformat()

    if rq_job.ended_at and not record.get("finish_time"):
        updates["finish_time"] = rq_job.ended_at.isoformat()
        if rq_job.started_at:
            dur = (rq_job.ended_at - rq_job.started_at).total_seconds()
            updates["duration_s"] = round(dur, 3)

    if rq_status == JobStatus.FINISHED and rq_job.result is not None:
        updates["result"] = rq_job.result

    if rq_status == JobStatus.FAILED and rq_job.exc_info:
        raw_exc = rq_job.exc_info or ""
        lines = raw_exc.strip().splitlines()
        exc_type, exc_msg = "UnknownError", raw_exc[:200]
        if lines:
            last = lines[-1]
            if ":" in last:
                exc_type, exc_msg = last.split(":", 1)
                exc_type = exc_type.strip()
                exc_msg = exc_msg.strip()

        updates["exc_type"] = exc_type
        updates["exc_message"] = exc_msg
        updates["traceback"] = raw_exc

        retry_count = int(record.get("retry_count", 0))
        analysis = analyze_failure(
            exc_type=exc_type,
            exc_message=exc_msg,
            traceback_str=raw_exc,
            retry_count=retry_count,
            task_name=record.get("task_name"),
        )
        updates["failure_analysis"] = analysis_to_dict(analysis)
        updates["is_retryable"] = analysis.is_retryable

    update_job_field(r, job_id, **updates)


# ──────────────────────────────────────────────────────────────────────────────
# Listing and metrics
# ──────────────────────────────────────────────────────────────────────────────

def list_recent_jobs(limit: int = 50) -> list[dict]:
    """
    Return recent jobs (most recent first), merging RQ registry + history.
    Syncs RQ status to history for each job found.
    """
    try:
        r = get_redis_client()          # decoded — for history
        r_raw = get_rq_redis_client()   # bytes   — for RQ Job.fetch
    except redis.ConnectionError:
        return []

    # Start from history (always has our custom records)
    history_jobs = list_all_jobs(r, limit=limit)

    # Sync each with live RQ data
    for rec in history_jobs:
        jid = rec.get("job_id")
        if jid:
            try:
                rq_job = Job.fetch(jid, connection=r_raw)
                _sync_from_rq(r, r_raw, jid, rq_job, rec)
            except Exception:
                pass

    # Re-load after sync
    synced = list_all_jobs(r, limit=limit)

    # Also pull any RQ jobs not in our history (e.g. created externally)
    known_ids = {j["job_id"] for j in synced if j.get("job_id")}
    _backfill_rq_jobs(r, r_raw, known_ids, synced, limit)

    return synced


def _backfill_rq_jobs(
    r: redis.Redis, r_raw: redis.Redis, known_ids: set, jobs_list: list, limit: int
) -> None:
    """Add RQ jobs that aren't in our history store."""
    q = Queue(QUEUE_NAME, connection=r_raw)
    try:
        for registry_cls in [FinishedJobRegistry, FailedJobRegistry, StartedJobRegistry]:
            registry = registry_cls(QUEUE_NAME, connection=r_raw)
            for job_id in registry.get_job_ids()[:20]:
                if job_id not in known_ids and len(jobs_list) < limit:
                    try:
                        rq_job = Job.fetch(job_id, connection=r_raw)
                        rec = _job_to_dict(rq_job)
                        save_job_record(r, rec)
                        jobs_list.append(rec)
                        known_ids.add(job_id)
                    except Exception:
                        pass
    except Exception:
        pass


def _job_to_dict(rq_job: Job) -> dict:
    """Convert a raw RQ Job to our record format."""
    status_map = {
        "queued": "queued", "started": "started",
        "finished": "finished", "failed": "failed",
    }
    rq_status = str(rq_job.get_status().value) if rq_job.get_status() else "unknown"
    enqueue_ts = rq_job.enqueued_at.timestamp() if rq_job.enqueued_at else time.time()

    return {
        "job_id": rq_job.id,
        "task_name": (rq_job.func_name or "").split(".")[-1],
        "task_params": rq_job.kwargs or {},
        "status": status_map.get(rq_status, rq_status),
        "enqueue_time": rq_job.enqueued_at.isoformat() if rq_job.enqueued_at else None,
        "enqueue_timestamp": enqueue_ts,
        "start_time": rq_job.started_at.isoformat() if rq_job.started_at else None,
        "finish_time": rq_job.ended_at.isoformat() if rq_job.ended_at else None,
        "duration_s": None,
        "result": rq_job.result,
        "exc_type": None,
        "exc_message": None,
        "retry_count": 0,
        "is_retryable": False,
        "recovered": False,
    }


def get_queue_stats() -> dict:
    """
    Return real queue statistics from Redis/RQ + persisted history.
    """
    try:
        r = get_redis_client()          # decoded — for history queries
        r_raw = get_rq_redis_client()   # bytes   — for RQ registries
        r.ping()
    except redis.ConnectionError as exc:
        return {
            "error": str(exc),
            "connected": False,
            "total": 0, "queued": 0, "started": 0,
            "finished": 0, "failed": 0, "retrying": 0, "dead": 0,
        }

    q = Queue(QUEUE_NAME, connection=r_raw)
    history_counts = count_jobs_by_status(r)

    # RQ registry counts (real-time) — must use raw bytes client
    rq_counts = {
        "queued":   q.count,
        "started":  StartedJobRegistry(QUEUE_NAME, connection=r_raw).count,
        "finished": FinishedJobRegistry(QUEUE_NAME, connection=r_raw).count,
        "failed":   FailedJobRegistry(QUEUE_NAME, connection=r_raw).count,
    }

    # Use RQ counts for real-time states, history for retrying/dead
    total = (
        rq_counts["queued"] + rq_counts["started"] +
        rq_counts["finished"] + rq_counts["failed"]
    )

    # Recovery rate
    finished = max(history_counts.get("finished", 0), rq_counts["finished"])
    retrying = history_counts.get("retrying", 0)
    dead = history_counts.get("dead", 0)
    recovered = sum(
        1 for j in list_all_jobs(r, limit=200) if j.get("recovered")
    )
    failed_total = max(history_counts.get("failed", 0), rq_counts["failed"])
    recovery_rate = (
        round(recovered / (recovered + failed_total) * 100, 1)
        if (recovered + failed_total) > 0 else 0.0
    )

    return {
        "connected": True,
        "total": total,
        "queued": rq_counts["queued"],
        "started": rq_counts["started"],
        "finished": finished,
        "failed": failed_total,
        "retrying": retrying,
        "dead": dead,
        "recovered": recovered,
        "recovery_rate": recovery_rate,
    }


def get_connection_status() -> dict:
    """Return Redis connection health."""
    return check_redis_health()


def get_task_descriptions() -> dict[str, str]:
    return TASK_DESCRIPTIONS
