"""
QueueGuard AI — Configuration Module
Loads all settings from environment variables (.env file).
"""

import os
import logging
from dotenv import load_dotenv

# Load .env file if it exists
load_dotenv()


def _get_int(key: str, default: int) -> int:
    try:
        return int(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


def _get_float(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except (ValueError, TypeError):
        return default


# ─── Redis Settings ───────────────────────────────────────────────────────────
REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT: int = _get_int("REDIS_PORT", 6379)
REDIS_DB: int = _get_int("REDIS_DB", 0)
REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD") or None

# ─── Queue Settings ───────────────────────────────────────────────────────────
QUEUE_NAME: str = os.getenv("QUEUE_NAME", "queueguard")
MAX_RETRIES: int = _get_int("MAX_RETRIES", 3)
RETRY_BACKOFF_BASE: float = _get_float("RETRY_BACKOFF_BASE", 2.0)
RETRY_BACKOFF_MAX: int = _get_int("RETRY_BACKOFF_MAX", 300)   # seconds cap
JOB_TIMEOUT: int = _get_int("JOB_TIMEOUT", 180)               # seconds

# ─── Redis Key Prefixes ───────────────────────────────────────────────────────
KEY_JOB_PREFIX: str = "qg:job:"
KEY_TIMELINE: str = "qg:jobs:timeline"
KEY_ATTEMPTS_SUFFIX: str = ":attempts"

# ─── Logging ──────────────────────────────────────────────────────────────────
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ─── Optional LLM Keys (never log or expose) ─────────────────────────────────
OPENAI_API_KEY: str | None = os.getenv("OPENAI_API_KEY") or None
GEMINI_API_KEY: str | None = os.getenv("GEMINI_API_KEY") or None
LLM_ENABLED: bool = bool(OPENAI_API_KEY or GEMINI_API_KEY)

# ─── Allowed task names (whitelist — prevents arbitrary code execution) ───────
ALLOWED_TASKS: list[str] = [
    "successful_task",
    "failing_task",
    "slow_task",
    "data_processing_task",
]
