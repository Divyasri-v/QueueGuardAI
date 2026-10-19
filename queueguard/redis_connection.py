"""
QueueGuard AI — Redis Connection Module
Provides two thread-safe Redis clients:

  get_redis_client()     — decode_responses=True   (used by our history/JSON layer)
  get_rq_redis_client()  — decode_responses=False  (used by RQ Queue / Worker)

WHY TWO CLIENTS?
  RQ serialises job payloads with pickle, storing raw binary bytes in Redis.
  If the connection has decode_responses=True, redis-py tries to decode those
  bytes as UTF-8 and raises UnicodeDecodeError.  RQ's own internal cleanup code
  also calls .decode() on values it expects to be bytes; with decode_responses=True
  the values are already str objects, so it raises:
    AttributeError: 'str' object has no attribute 'decode'

  Our history module stores plain JSON strings, so it still benefits from
  decode_responses=True.  The RQ client must always have decode_responses=False.
"""

import logging
import redis
from redis import ConnectionPool
from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

logger = logging.getLogger(__name__)

# Pool for our history / JSON layer  (decode_responses=True)
_pool: ConnectionPool | None = None

# Pool for RQ Queue / Worker internals  (decode_responses=False — RQ uses pickle)
_rq_pool: ConnectionPool | None = None


def _get_pool() -> ConnectionPool:
    """Return or create the shared decoded connection pool (for our history layer)."""
    global _pool
    if _pool is None:
        _pool = ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,      # OK for our plain-text JSON keys
            max_connections=20,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _pool


def _get_rq_pool() -> ConnectionPool:
    """Return or create the raw (bytes) connection pool for RQ internals."""
    global _rq_pool
    if _rq_pool is None:
        _rq_pool = ConnectionPool(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=False,     # REQUIRED: RQ stores pickle bytes
            max_connections=20,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
    return _rq_pool


def get_redis_client() -> redis.Redis:
    """
    Return a decoded Redis client (decode_responses=True).
    Use this for our own history / JSON storage layer.
    Raises redis.ConnectionError if Redis is unavailable.
    """
    return redis.Redis(connection_pool=_get_pool())


def get_rq_redis_client() -> redis.Redis:
    """
    Return a raw-bytes Redis client (decode_responses=False).
    ALWAYS use this when creating an RQ Queue, Worker, or Job.fetch().
    Raises redis.ConnectionError if Redis is unavailable.
    """
    return redis.Redis(connection_pool=_get_rq_pool())


def check_redis_health() -> dict:
    """
    Ping Redis and return a status dictionary.

    Returns:
        {
            "connected": bool,
            "host": str,
            "port": int,
            "latency_ms": float | None,
            "error": str | None,
        }
    """
    import time

    status = {
        "connected": False,
        "host": REDIS_HOST,
        "port": REDIS_PORT,
        "latency_ms": None,
        "error": None,
    }
    try:
        client = get_redis_client()
        t0 = time.perf_counter()
        client.ping()
        status["latency_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        status["connected"] = True
    except redis.ConnectionError as exc:
        status["error"] = f"ConnectionError: {exc}"
        logger.warning("Redis health check failed - %s", exc)
    except redis.TimeoutError as exc:
        status["error"] = f"TimeoutError: {exc}"
        logger.warning("Redis health check timed out - %s", exc)
    except Exception as exc:  # noqa: BLE001
        status["error"] = f"Unexpected error: {exc}"
        logger.error("Redis health check unexpected error - %s", exc)
    return status


def reset_pool() -> None:
    """Force new connection pools (useful for testing or reconnection)."""
    global _pool, _rq_pool
    if _pool:
        _pool.disconnect()
    if _rq_pool:
        _rq_pool.disconnect()
    _pool = None
    _rq_pool = None
