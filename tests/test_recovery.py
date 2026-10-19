"""
Tests for the recovery module.
Uses mock Redis so no live Redis is required.
"""

import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch, call

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queueguard.recovery import (
    calculate_backoff,
    check_retry_eligibility,
)
from config import MAX_RETRIES


# ──────────────────────────────────────────────────────────────────────────────
# Backoff calculation
# ──────────────────────────────────────────────────────────────────────────────

class TestCalculateBackoff:
    def test_first_attempt(self):
        delay = calculate_backoff(1, base=2.0)
        assert delay == 2.0

    def test_second_attempt(self):
        delay = calculate_backoff(2, base=2.0)
        assert delay == 4.0

    def test_third_attempt(self):
        delay = calculate_backoff(3, base=2.0)
        assert delay == 8.0

    def test_cap_is_respected(self):
        delay = calculate_backoff(100, base=2.0, cap=300)
        assert delay == 300.0

    def test_custom_base(self):
        delay = calculate_backoff(3, base=3.0)
        assert delay == 27.0

    def test_returns_float(self):
        delay = calculate_backoff(1)
        assert isinstance(delay, float)

    def test_backoff_is_monotonically_increasing(self):
        delays = [calculate_backoff(i, base=2.0, cap=1000) for i in range(1, 6)]
        for i in range(len(delays) - 1):
            assert delays[i] < delays[i + 1]


# ──────────────────────────────────────────────────────────────────────────────
# Retry eligibility
# ──────────────────────────────────────────────────────────────────────────────

class TestCheckRetryEligibility:
    def _make_record(self, **overrides):
        defaults = {
            "task_name": "successful_task",
            "status": "failed",
            "retry_count": 0,
            "is_retryable": True,
        }
        defaults.update(overrides)
        return defaults

    def test_eligible_failed_job(self):
        record = self._make_record()
        eligible, reason = check_retry_eligibility(record)
        assert eligible is True

    def test_ineligible_because_max_retries_reached(self):
        record = self._make_record(retry_count=MAX_RETRIES)
        eligible, reason = check_retry_eligibility(record)
        assert eligible is False
        assert "maximum retry limit" in reason.lower() or "max" in reason.lower()

    def test_ineligible_because_wrong_status(self):
        record = self._make_record(status="queued")
        eligible, reason = check_retry_eligibility(record)
        assert eligible is False
        assert "state" in reason.lower()

    def test_ineligible_because_task_not_retryable(self):
        record = self._make_record(task_name="failing_task")
        eligible, reason = check_retry_eligibility(record)
        assert eligible is False

    def test_ineligible_because_is_retryable_false(self):
        record = self._make_record(is_retryable=False)
        eligible, reason = check_retry_eligibility(record)
        assert eligible is False

    def test_eligible_at_max_retries_minus_one(self):
        record = self._make_record(retry_count=MAX_RETRIES - 1)
        eligible, reason = check_retry_eligibility(record)
        assert eligible is True

    def test_retrying_status_is_eligible(self):
        record = self._make_record(status="retrying", retry_count=1)
        eligible, reason = check_retry_eligibility(record)
        assert eligible is True

    def test_finished_status_is_ineligible(self):
        record = self._make_record(status="finished")
        eligible, reason = check_retry_eligibility(record)
        assert eligible is False


# ──────────────────────────────────────────────────────────────────────────────
# retry_job (with mocked Redis and RQ)
# ──────────────────────────────────────────────────────────────────────────────

class TestRetryJob:
    def _make_mock_redis(self, record: dict):
        """Create a mock Redis client that returns the given record."""
        r = MagicMock()

        # Mock hgetall to return the encoded record
        import json as _json

        def encode(v):
            if v is None:
                return ""
            if isinstance(v, str):
                return v
            return _json.dumps(v, default=str)

        raw = {k: encode(v) for k, v in record.items() if v is not None}
        r.hgetall.return_value = raw
        r.hlen.return_value = 0
        r.hset.return_value = True
        r.pipeline.return_value.__enter__ = MagicMock(return_value=r)
        r.pipeline.return_value.__exit__ = MagicMock(return_value=False)
        r.pipeline.return_value.execute = MagicMock(return_value=[True])
        pipe = r.pipeline.return_value
        pipe.hset = MagicMock()
        pipe.zadd = MagicMock()
        pipe.zremrangebyrank = MagicMock()
        return r

    def test_returns_failure_when_no_record(self):
        from queueguard.recovery import retry_job

        r = MagicMock()
        r.hgetall.return_value = {}  # No record found

        result = retry_job(r, "nonexistent-job-id")
        assert result["success"] is False
        assert "No history record" in result["message"]

    def test_returns_failure_when_max_retries_exceeded(self):
        from queueguard.recovery import retry_job

        record = {
            "job_id": "test-123",
            "task_name": "successful_task",
            "status": "failed",
            "retry_count": str(MAX_RETRIES),
            "is_retryable": "true",
            "task_params": "{}",
        }
        r = self._make_mock_redis(record)

        result = retry_job(r, "test-123")
        assert result["success"] is False

    def test_backoff_increases_with_retry_count(self):
        delays = [calculate_backoff(i) for i in range(1, MAX_RETRIES + 1)]
        for i in range(len(delays) - 1):
            assert delays[i] <= delays[i + 1], (
                f"Backoff should be non-decreasing: attempt {i+1}={delays[i]} >= attempt {i+2}={delays[i+1]}"
            )


# ──────────────────────────────────────────────────────────────────────────────
# auto_retry_failed_jobs (with mocked data)
# ──────────────────────────────────────────────────────────────────────────────

class TestAutoRetry:
    def test_auto_retry_skips_ineligible_tasks(self):
        """failing_task is not in RETRYABLE_TASKS, so auto-retry skips it."""
        from queueguard.recovery import auto_retry_failed_jobs

        r = MagicMock()
        # Patch list_all_jobs in the recovery module's namespace
        with patch("queueguard.recovery.list_all_jobs") as mock_list:
            mock_list.return_value = [{
                "job_id": "j1",
                "task_name": "failing_task",  # not retryable
                "status": "failed",
                "retry_count": 0,
                "is_retryable": False,
            }]
            results = auto_retry_failed_jobs(r)

        # Should return empty list (no eligible jobs)
        assert results == []

    def test_auto_retry_processes_eligible_jobs(self):
        """successful_task is retryable, so auto-retry should attempt it."""
        from queueguard.recovery import auto_retry_failed_jobs

        r = MagicMock()
        with patch("queueguard.recovery.list_all_jobs") as mock_list, \
             patch("queueguard.recovery.retry_job") as mock_retry:
            mock_list.return_value = [{
                "job_id": "j2",
                "task_name": "successful_task",
                "status": "failed",
                "retry_count": 0,
                "is_retryable": True,
            }]
            mock_retry.return_value = {
                "success": True,
                "new_job_id": "j2-retry",
                "message": "Retried",
                "attempt": 1,
                "backoff_s": 2.0,
            }

            results = auto_retry_failed_jobs(r)

        assert len(results) == 1
        assert results[0]["success"] is True
