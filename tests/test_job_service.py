"""
Tests for the job service module.
Redis-dependent tests use mocks; pure logic tests run without Redis.
"""

import sys
import os
import json
import pytest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queueguard.job_service import validate_params


# ──────────────────────────────────────────────────────────────────────────────
# Parameter validation (pure — no Redis required)
# ──────────────────────────────────────────────────────────────────────────────

class TestValidateParams:
    def test_valid_successful_task(self):
        params = validate_params("successful_task", {"duration": 2.0, "payload": "test"})
        assert params["duration"] == 2.0
        assert params["payload"] == "test"

    def test_successful_task_defaults(self):
        params = validate_params("successful_task", {})
        assert params["duration"] == 1.0
        assert params["payload"] == "hello"

    def test_duration_too_small_raises(self):
        with pytest.raises(ValueError, match=">="):
            validate_params("successful_task", {"duration": 0.0})

    def test_duration_too_large_raises(self):
        with pytest.raises(ValueError, match="<="):
            validate_params("successful_task", {"duration": 999.0})

    def test_payload_too_long_raises(self):
        with pytest.raises(ValueError, match="characters"):
            validate_params("successful_task", {"payload": "x" * 300})

    def test_failing_task_valid_types(self):
        for error_type in ["timeout", "connection", "value", "runtime"]:
            params = validate_params("failing_task", {"error_type": error_type})
            assert params["error_type"] == error_type

    def test_failing_task_invalid_type_raises(self):
        with pytest.raises(ValueError, match="one of"):
            validate_params("failing_task", {"error_type": "arbitrary_code"})

    def test_slow_task_defaults(self):
        params = validate_params("slow_task", {})
        assert params["duration"] == 15.0

    def test_slow_task_exceeds_max_raises(self):
        with pytest.raises(ValueError, match="<="):
            validate_params("slow_task", {"duration": 500.0})

    def test_data_processing_valid(self):
        params = validate_params("data_processing_task", {"dataset_size": 5000})
        assert params["dataset_size"] == 5000

    def test_data_processing_negative_raises(self):
        with pytest.raises(ValueError, match=">="):
            validate_params("data_processing_task", {"dataset_size": -1})

    def test_data_processing_exceeds_max_raises(self):
        with pytest.raises(ValueError, match="<="):
            validate_params("data_processing_task", {"dataset_size": 100_000})

    def test_unknown_task_raises(self):
        with pytest.raises(ValueError, match="not in the allowed"):
            validate_params("os.system", {"cmd": "rm -rf /"})

    def test_arbitrary_code_rejected(self):
        with pytest.raises(ValueError):
            validate_params("__import__('os').system", {})

    def test_type_coercion_string_to_float(self):
        params = validate_params("successful_task", {"duration": "3.5"})
        assert params["duration"] == 3.5

    def test_type_coercion_string_to_int(self):
        params = validate_params("data_processing_task", {"dataset_size": "2000"})
        assert params["dataset_size"] == 2000


# ──────────────────────────────────────────────────────────────────────────────
# Worker task functions (no Redis needed)
# ──────────────────────────────────────────────────────────────────────────────

class TestWorkerTasks:
    def test_successful_task_returns_dict(self):
        from queueguard.worker_tasks import successful_task
        result = successful_task(duration=0.1, payload="unit-test")
        assert isinstance(result, dict)
        assert result["status"] == "success"
        assert result["payload_echo"] == "unit-test"

    def test_successful_task_completes_within_time(self):
        import time
        from queueguard.worker_tasks import successful_task
        t0 = time.perf_counter()
        result = successful_task(duration=0.2)
        elapsed = time.perf_counter() - t0
        assert elapsed < 2.0  # Should complete well within 2 seconds

    def test_failing_task_raises_timeout(self):
        from queueguard.worker_tasks import failing_task, TaskTimeoutError
        with pytest.raises(TaskTimeoutError):
            failing_task(error_type="timeout")

    def test_failing_task_raises_connection(self):
        from queueguard.worker_tasks import failing_task, TaskConnectionError
        with pytest.raises(TaskConnectionError):
            failing_task(error_type="connection")

    def test_failing_task_raises_value(self):
        from queueguard.worker_tasks import failing_task, TaskValueError
        with pytest.raises(TaskValueError):
            failing_task(error_type="value")

    def test_failing_task_raises_runtime(self):
        from queueguard.worker_tasks import failing_task, TaskRuntimeError
        with pytest.raises(TaskRuntimeError):
            failing_task(error_type="runtime")

    def test_data_processing_returns_stats(self):
        from queueguard.worker_tasks import data_processing_task
        result = data_processing_task(dataset_size=100)
        assert result["status"] == "success"
        assert result["total_records"] == 100
        assert "by_category" in result

    def test_data_processing_category_stats(self):
        from queueguard.worker_tasks import data_processing_task
        result = data_processing_task(dataset_size=500)
        for cat, stats in result["by_category"].items():
            assert "count" in stats
            assert "mean" in stats
            assert stats["min"] <= stats["mean"] <= stats["max"]


# ──────────────────────────────────────────────────────────────────────────────
# enqueue_job with mocked Redis + RQ
# ──────────────────────────────────────────────────────────────────────────────

class TestEnqueueJob:
    def test_enqueue_fails_when_redis_down(self):
        import redis as redis_lib
        from queueguard.job_service import enqueue_job

        # enqueue_job pings via get_rq_redis_client — patch that one
        with patch("queueguard.job_service.get_rq_redis_client") as mock_rq_client, \
             patch("queueguard.job_service.get_redis_client"):
            mock_r = MagicMock()
            mock_r.ping.side_effect = redis_lib.ConnectionError("Connection refused")
            mock_rq_client.return_value = mock_r

            result = enqueue_job("successful_task", {"duration": 1.0})

        assert result["success"] is False
        assert "Cannot connect" in result["error"] or "redis" in result["error"].lower()

    def test_enqueue_rejects_invalid_task_name(self):
        from queueguard.job_service import enqueue_job

        # Invalid task name is caught by validate_params before Redis is touched
        with patch("queueguard.job_service.get_rq_redis_client") as mock_rq_client, \
             patch("queueguard.job_service.get_redis_client") as mock_client:
            mock_r = MagicMock()
            mock_r.ping.return_value = True
            mock_rq_client.return_value = mock_r
            mock_client.return_value = mock_r

            result = enqueue_job("rm_rf", {})

        assert result["success"] is False
        assert "allowed" in result["error"].lower()

    def test_enqueue_success_returns_job_id(self):
        from queueguard.job_service import enqueue_job

        mock_rq_job = MagicMock()
        mock_rq_job.id = "test-uuid-1234"

        with patch("queueguard.job_service.get_rq_redis_client") as mock_rq_client, \
             patch("queueguard.job_service.get_redis_client") as mock_client, \
             patch("queueguard.job_service.Queue") as mock_queue_cls, \
             patch("queueguard.job_service.save_job_record"):

            mock_r = MagicMock()
            mock_r.ping.return_value = True
            mock_rq_client.return_value = mock_r
            mock_client.return_value = mock_r

            mock_queue = MagicMock()
            mock_queue.enqueue.return_value = mock_rq_job
            mock_queue.count = 1
            mock_queue_cls.return_value = mock_queue

            result = enqueue_job("successful_task", {"duration": 1.0})

        assert result["success"] is True
        assert "job_id" in result

    def test_enqueue_validates_params_before_redis(self):
        from queueguard.job_service import enqueue_job

        # Param validation runs after the Redis ping; patch both clients
        with patch("queueguard.job_service.get_rq_redis_client") as mock_rq_client, \
             patch("queueguard.job_service.get_redis_client") as mock_client:
            mock_r = MagicMock()
            mock_r.ping.return_value = True
            mock_rq_client.return_value = mock_r
            mock_client.return_value = mock_r

            result = enqueue_job("slow_task", {"duration": 9999.0})

        assert result["success"] is False
        assert "<=" in result["error"] or "duration" in result["error"]


# ──────────────────────────────────────────────────────────────────────────────
# get_queue_stats with mocked Redis
# ──────────────────────────────────────────────────────────────────────────────

class TestGetQueueStats:
    def test_returns_error_dict_when_redis_down(self):
        import redis as redis_lib
        from queueguard.job_service import get_queue_stats

        with patch("queueguard.job_service.get_redis_client") as mock_client:
            mock_r = MagicMock()
            mock_r.ping.side_effect = redis_lib.ConnectionError("down")
            mock_client.return_value = mock_r

            stats = get_queue_stats()

        assert stats["connected"] is False
        assert "error" in stats

    def test_returns_connected_stats_structure(self):
        from queueguard.job_service import get_queue_stats

        with patch("queueguard.job_service.get_redis_client") as mock_client, \
             patch("queueguard.job_service.Queue") as mock_queue_cls, \
             patch("queueguard.job_service.StartedJobRegistry") as mock_started, \
             patch("queueguard.job_service.FinishedJobRegistry") as mock_finished, \
             patch("queueguard.job_service.FailedJobRegistry") as mock_failed, \
             patch("queueguard.job_service.count_jobs_by_status") as mock_counts, \
             patch("queueguard.job_service.list_all_jobs") as mock_list:

            mock_r = MagicMock()
            mock_r.ping.return_value = True
            mock_client.return_value = mock_r

            for mock_reg in [mock_started, mock_finished, mock_failed]:
                mock_reg.return_value.count = 0

            mock_q = MagicMock()
            mock_q.count = 0
            mock_queue_cls.return_value = mock_q

            mock_counts.return_value = {
                "queued": 0, "started": 0, "finished": 0,
                "failed": 0, "retrying": 0, "dead": 0,
            }
            mock_list.return_value = []

            stats = get_queue_stats()

        assert stats["connected"] is True
        for key in ["total", "queued", "started", "finished", "failed"]:
            assert key in stats
