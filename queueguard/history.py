"""
QueueGuard AI — Redis-Backed Job History Module
Persists job records across application restarts using Redis Hashes and Sorted Sets.

Schema:
  Hash   qg:job:{job_id}          → full job metadata (JSON-encoded fields)
  ZSet   qg:jobs:timeline         → job_id → enqueue_timestamp (score)
  Hash   qg:job:{job_id}:attempts → attempt_index → attempt JSON
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import redis

from config import KEY_JOB_PREFIX, KEY_TIMELINE, KEY_ATTEMPTS_SUFFIX

logger = logging.getLogger(__name__)

# Maximum jobs to keep in history (oldest pruned automatically)
MAX_HISTORY_SIZE = 500


def _ts() -> float:
    """Current UTC Unix timestamp."""
    return time.time()


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _encode(value: Any) -> str:
    """Serialize value to JSON string for Redis storage."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, default=str)


def _decode(value: str | None) -> Any:
    """Deserialize a JSON string from Redis; return raw string if not JSON."""
    if value is None or value == "":
        return None
    try:
        return json.loads(value)
    except (json.JSONDecodeError, TypeError):
        return value


# ──────────────────────────────────────────────────────────────────────────────
# Write operations
# ──────────────────────────────────────────────────────────────────────────────

def save_job_record(r: redis.Redis, record: dict) -> None:
    """
    Persist a job record to Redis.
    ``record`` must contain at least: job_id, task_name, enqueue_time.

    Existing fields are overwritten; fields absent from ``record`` are left as-is.
    """
    job_id = record.get("job_id")
    if not job_id:
        raise ValueError("record must contain 'job_id'")

    key = f"{KEY_JOB_PREFIX}{job_id}"
    pipe = r.pipeline()

    # Store each field individually (HSET accepts mapping)
    mapping = {k: _encode(v) for k, v in record.items() if v is not None}
    pipe.hset(key, mapping=mapping)

    # Add to timeline sorted set
    score = record.get("enqueue_timestamp") or _ts()
    pipe.zadd(KEY_TIMELINE, {job_id: score})

    # Prune oldest entries beyond MAX_HISTORY_SIZE
    pipe.zremrangebyrank(KEY_TIMELINE, 0, -(MAX_HISTORY_SIZE + 1))

    pipe.execute()
    logger.debug("Saved job record %s", job_id)


def update_job_field(r: redis.Redis, job_id: str, **fields) -> None:
    """Update specific fields of an existing job record."""
    key = f"{KEY_JOB_PREFIX}{job_id}"
    mapping = {k: _encode(v) for k, v in fields.items()}
    r.hset(key, mapping=mapping)
    logger.debug("Updated job %s fields: %s", job_id, list(fields.keys()))


def save_attempt(r: redis.Redis, job_id: str, attempt: dict) -> None:
    """
    Append a retry/attempt record for a job.
    Stored as hash: qg:job:{job_id}:attempts, field = attempt index (auto-incremented).
    """
    key = f"{KEY_JOB_PREFIX}{job_id}{KEY_ATTEMPTS_SUFFIX}"
    # Use the length of the hash as the next index
    idx = r.hlen(key)
    r.hset(key, str(idx), _encode(attempt))
    logger.debug("Saved attempt %d for job %s", idx, job_id)


# ──────────────────────────────────────────────────────────────────────────────
# Read operations
# ──────────────────────────────────────────────────────────────────────────────

def load_job_record(r: redis.Redis, job_id: str) -> dict | None:
    """Load a single job record. Returns None if not found."""
    key = f"{KEY_JOB_PREFIX}{job_id}"
    raw = r.hgetall(key)
    if not raw:
        return None
    return {k: _decode(v) for k, v in raw.items()}


def load_job_attempts(r: redis.Redis, job_id: str) -> list[dict]:
    """Load all attempt records for a job, ordered by attempt index."""
    key = f"{KEY_JOB_PREFIX}{job_id}{KEY_ATTEMPTS_SUFFIX}"
    raw = r.hgetall(key)
    attempts = []
    for i in range(len(raw)):
        val = raw.get(str(i))
        if val:
            attempts.append(_decode(val))
    return attempts


def list_all_jobs(r: redis.Redis, limit: int = 100, offset: int = 0) -> list[dict]:
    """
    Return job records sorted by enqueue time (most recent first).

    Args:
        limit:  Maximum records to return.
        offset: Starting offset (for pagination).
    """
    # ZREVRANGE returns job_ids newest-first
    job_ids = r.zrevrange(KEY_TIMELINE, offset, offset + limit - 1)
    results = []
    for job_id in job_ids:
        record = load_job_record(r, job_id)
        if record:
            results.append(record)
    return results


def count_jobs_by_status(r: redis.Redis) -> dict[str, int]:
    """
    Return counts of jobs grouped by status from persisted history.
    """
    all_ids = r.zrange(KEY_TIMELINE, 0, -1)
    counts: dict[str, int] = {
        "queued": 0, "started": 0, "finished": 0,
        "failed": 0, "retrying": 0, "dead": 0,
    }
    for job_id in all_ids:
        record = load_job_record(r, job_id)
        if record:
            status = record.get("status", "unknown")
            if status in counts:
                counts[status] += 1
            else:
                counts["unknown"] = counts.get("unknown", 0) + 1
    return counts


def delete_job_record(r: redis.Redis, job_id: str) -> None:
    """Remove a job and its attempts from history."""
    pipe = r.pipeline()
    pipe.delete(f"{KEY_JOB_PREFIX}{job_id}")
    pipe.delete(f"{KEY_JOB_PREFIX}{job_id}{KEY_ATTEMPTS_SUFFIX}")
    pipe.zrem(KEY_TIMELINE, job_id)
    pipe.execute()
    logger.info("Deleted job record %s", job_id)


def clear_all_history(r: redis.Redis) -> int:
    """Delete all job records. Returns the number of records deleted."""
    job_ids = r.zrange(KEY_TIMELINE, 0, -1)
    count = 0
    for job_id in job_ids:
        delete_job_record(r, job_id)
        count += 1
    return count
