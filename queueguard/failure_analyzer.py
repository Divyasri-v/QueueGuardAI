"""
QueueGuard AI — Local Failure Analyzer
Classifies job failures deterministically — no external API required.
Optionally enriches with an LLM if an API key is configured.

All explanations are labeled as DIAGNOSTIC SUGGESTIONS, not guaranteed root causes.
"""

import logging
import re
import traceback as tb
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Data structures
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class FailureAnalysis:
    category: str              # timeout | connection | invalid_input | resource | unexpected
    severity: str              # critical | high | medium | low
    summary: str               # one-liner
    explanation: str           # plain-English paragraph
    suggestions: list[str]     # actionable steps
    traceback_summary: str     # shortened traceback (safe to show)
    exc_type: str
    exc_message: str
    is_retryable: bool
    confidence: str            # high | medium | low
    ai_enriched: bool = False  # True if LLM was used


# ──────────────────────────────────────────────────────────────────────────────
# Category definitions
# ──────────────────────────────────────────────────────────────────────────────

_CATEGORIES = {
    "timeout": {
        "patterns": [
            r"timeout",
            r"timed? out",
            r"deadline exceeded",
            r"read timeout",
            r"connect timeout",
        ],
        "exc_types": ["TimeoutError", "TaskTimeoutError", "socket.timeout",
                      "requests.exceptions.Timeout", "concurrent.futures.TimeoutError"],
        "severity": "high",
        "is_retryable": True,
        "explanation": (
            "The job exceeded its allocated time budget before completing. "
            "This is commonly caused by slow downstream services, large data volumes, "
            "or insufficient worker resources."
        ),
        "suggestions": [
            "Increase the job timeout limit in configuration.",
            "Check if the upstream service or database is responding slowly.",
            "Consider splitting the task into smaller chunks.",
            "Verify that network latency is within acceptable bounds.",
            "Retry the job — transient timeouts often resolve on their own.",
        ],
    },
    "connection": {
        "patterns": [
            r"connection refused",
            r"connection reset",
            r"failed to connect",
            r"network unreachable",
            r"name or service not known",
            r"no route to host",
            r"broken pipe",
            r"eof occurred",
            r"ssl.*error",
        ],
        "exc_types": ["ConnectionError", "TaskConnectionError", "ConnectionRefusedError",
                      "ConnectionResetError", "BrokenPipeError",
                      "requests.exceptions.ConnectionError",
                      "redis.exceptions.ConnectionError"],
        "severity": "high",
        "is_retryable": True,
        "explanation": (
            "The job could not establish or maintain a connection to a required "
            "external resource (database, API, cache server, etc.). This is often "
            "a transient network issue or service downtime."
        ),
        "suggestions": [
            "Verify the target service is running and reachable.",
            "Check firewall rules and network security groups.",
            "Confirm environment variables for host/port are correct.",
            "Retry the job — connection failures are often transient.",
            "Implement a circuit breaker pattern if failures persist.",
        ],
    },
    "invalid_input": {
        "patterns": [
            r"invalid.*param",
            r"invalid.*input",
            r"invalid.*value",
            r"invalid.*argument",
            r"must be.*positive",
            r"must be.*integer",
            r"expected.*got",
            r"validation.*fail",
            r"schema.*error",
            r"type.*error",
            r"value.*error",
        ],
        "exc_types": ["ValueError", "TaskValueError", "TypeError",
                      "AttributeError", "KeyError", "IndexError",
                      "pydantic.ValidationError"],
        "severity": "medium",
        "is_retryable": False,
        "explanation": (
            "The job received parameters that are invalid, out of range, or of the "
            "wrong type. This is typically a programming or configuration error "
            "and will not succeed on retry without fixing the input."
        ),
        "suggestions": [
            "Review the job parameters that were submitted.",
            "Validate input data before submitting the job.",
            "Check the task function signature and parameter constraints.",
            "Add input validation in the job submission layer.",
            "Do NOT retry without fixing the underlying input issue.",
        ],
    },
    "resource": {
        "patterns": [
            r"out of memory",
            r"memory error",
            r"disk.*full",
            r"no space left",
            r"too many open files",
            r"resource.*unavailable",
            r"quota.*exceeded",
            r"rate.*limit",
        ],
        "exc_types": ["MemoryError", "OSError", "IOError", "ResourceWarning"],
        "severity": "critical",
        "is_retryable": True,
        "explanation": (
            "The job failed due to insufficient system resources — memory, disk space, "
            "file descriptors, or an external quota/rate limit. Retrying without "
            "addressing the underlying resource constraint will likely fail again."
        ),
        "suggestions": [
            "Check available memory and disk space on worker nodes.",
            "Reduce the job's data volume or batch size.",
            "Increase system resource limits (ulimit, cgroup quotas).",
            "If hitting a rate limit, implement exponential backoff between retries.",
            "Scale up worker infrastructure if resources are consistently exhausted.",
        ],
    },
}

_UNEXPECTED = {
    "severity": "medium",
    "is_retryable": True,
    "explanation": (
        "The job encountered an unexpected runtime error not covered by known failure "
        "categories. This may be a bug in the task code, an unhandled edge case, "
        "or a transient infrastructure issue."
    ),
    "suggestions": [
        "Review the full traceback in the diagnostic section.",
        "Add error handling for this case in the task code.",
        "Check recent code changes that may have introduced a regression.",
        "Retry once to rule out transient errors.",
        "If it consistently fails, treat as a code bug and fix before retrying.",
    ],
}


# ──────────────────────────────────────────────────────────────────────────────
# Core classifier
# ──────────────────────────────────────────────────────────────────────────────

def _classify(exc_type: str, exc_message: str) -> tuple[str, dict]:
    """Return (category_name, category_dict)."""
    exc_type_clean = exc_type.split(".")[-1] if exc_type else ""
    message_lower = exc_message.lower() if exc_message else ""

    for category, cfg in _CATEGORIES.items():
        # Check exception type
        if exc_type_clean in cfg["exc_types"] or any(
            exc_type_clean in et for et in cfg["exc_types"]
        ):
            return category, cfg

        # Check message patterns
        for pattern in cfg["patterns"]:
            if re.search(pattern, message_lower):
                return category, cfg

    return "unexpected", _UNEXPECTED


def _shorten_traceback(full_tb: str | None, max_lines: int = 8) -> str:
    """Return a shortened, safe version of a traceback."""
    if not full_tb:
        return "No traceback available."
    lines = [l for l in full_tb.splitlines() if l.strip()]
    if len(lines) <= max_lines:
        return "\n".join(lines)
    # Keep first 2 and last 6 lines
    head = lines[:2]
    tail = lines[-(max_lines - 2):]
    return "\n".join(head + [f"  ... ({len(lines) - max_lines} lines omitted) ..."] + tail)


def _severity_from_retry(retry_count: int, base_severity: str) -> str:
    """Escalate severity based on retry count."""
    order = ["low", "medium", "high", "critical"]
    base_idx = order.index(base_severity) if base_severity in order else 1
    escalated_idx = min(base_idx + (retry_count // 2), len(order) - 1)
    return order[escalated_idx]


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def analyze_failure(
    exc_type: str,
    exc_message: str,
    traceback_str: str | None = None,
    retry_count: int = 0,
    task_name: str | None = None,
) -> FailureAnalysis:
    """
    Analyze a job failure and return a structured FailureAnalysis.

    This is a DIAGNOSTIC SUGGESTION based on pattern matching.
    It does not guarantee a root cause.

    Args:
        exc_type:      Exception class name (e.g. 'ValueError').
        exc_message:   Exception message string.
        traceback_str: Full traceback string (optional).
        retry_count:   Number of retries already attempted.
        task_name:     Name of the task that failed (for context).

    Returns:
        FailureAnalysis dataclass instance.
    """
    category, cfg = _classify(exc_type, exc_message)
    severity = _severity_from_retry(retry_count, cfg["severity"])
    tb_summary = _shorten_traceback(traceback_str)

    summary = _build_summary(category, exc_type, exc_message, task_name)

    analysis = FailureAnalysis(
        category=category,
        severity=severity,
        summary=summary,
        explanation=cfg["explanation"],
        suggestions=list(cfg["suggestions"]),
        traceback_summary=tb_summary,
        exc_type=exc_type,
        exc_message=exc_message,
        is_retryable=cfg["is_retryable"],
        confidence="high" if category != "unexpected" else "medium",
        ai_enriched=False,
    )

    logger.info(
        "Failure analysis complete | job=%s category=%s severity=%s retryable=%s",
        task_name, category, severity, cfg["is_retryable"],
    )
    return analysis


def _build_summary(category: str, exc_type: str, exc_message: str, task_name: str | None) -> str:
    prefixes = {
        "timeout": "Timeout:",
        "connection": "Connection failure:",
        "invalid_input": "Invalid input:",
        "resource": "Resource exhaustion:",
        "unexpected": "Unexpected error:",
    }
    prefix = prefixes.get(category, "Error:")
    short_msg = exc_message[:80] + "…" if len(exc_message or "") > 80 else exc_message
    task_part = f" in {task_name}" if task_name else ""
    return f"{prefix}{task_part} — {exc_type}: {short_msg}"


def analyze_failure_from_rq_job(job: Any, retry_count: int = 0) -> FailureAnalysis:
    """
    Convenience wrapper: extract failure details from an RQ Job object.
    """
    exc_type = "UnknownError"
    exc_message = "No error information available."
    traceback_str = None

    if hasattr(job, "exc_info") and job.exc_info:
        # RQ stores exc_info as a string (formatted traceback)
        raw = job.exc_info
        if isinstance(raw, str):
            lines = raw.strip().splitlines()
            traceback_str = raw
            if lines:
                last_line = lines[-1]
                if ":" in last_line:
                    parts = last_line.split(":", 1)
                    exc_type = parts[0].strip()
                    exc_message = parts[1].strip()
                else:
                    exc_message = last_line

    task_name = None
    if hasattr(job, "func_name"):
        task_name = job.func_name.split(".")[-1] if job.func_name else None

    return analyze_failure(
        exc_type=exc_type,
        exc_message=exc_message,
        traceback_str=traceback_str,
        retry_count=retry_count,
        task_name=task_name,
    )


def analysis_to_dict(analysis: FailureAnalysis) -> dict:
    """Serialize FailureAnalysis to a JSON-safe dict for storage."""
    return {
        "category": analysis.category,
        "severity": analysis.severity,
        "summary": analysis.summary,
        "explanation": analysis.explanation,
        "suggestions": analysis.suggestions,
        "traceback_summary": analysis.traceback_summary,
        "exc_type": analysis.exc_type,
        "exc_message": analysis.exc_message,
        "is_retryable": analysis.is_retryable,
        "confidence": analysis.confidence,
        "ai_enriched": analysis.ai_enriched,
    }
