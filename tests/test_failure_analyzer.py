"""
Tests for the failure analyzer module.
These tests run without Redis — pure unit tests.
"""

import sys
import os
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from queueguard.failure_analyzer import (
    analyze_failure,
    FailureAnalysis,
    analysis_to_dict,
    _classify,
    _shorten_traceback,
    _severity_from_retry,
)


# ──────────────────────────────────────────────────────────────────────────────
# Classification tests
# ──────────────────────────────────────────────────────────────────────────────

class TestClassification:
    def test_timeout_by_exc_type(self):
        cat, cfg = _classify("TaskTimeoutError", "timed out")
        assert cat == "timeout"

    def test_timeout_by_message_pattern(self):
        cat, cfg = _classify("Exception", "operation timed out after 30 seconds")
        assert cat == "timeout"

    def test_connection_by_exc_type(self):
        cat, cfg = _classify("ConnectionRefusedError", "connection refused")
        assert cat == "connection"

    def test_connection_by_message(self):
        cat, cfg = _classify("OSError", "failed to connect to db.internal:5432")
        assert cat == "connection"

    def test_invalid_input_by_exc_type(self):
        cat, cfg = _classify("ValueError", "invalid parameter")
        assert cat == "invalid_input"

    def test_invalid_input_by_message(self):
        cat, cfg = _classify("Exception", "value error: must be positive integer")
        assert cat == "invalid_input"

    def test_resource_by_message(self):
        cat, cfg = _classify("MemoryError", "out of memory")
        assert cat == "resource"

    def test_unexpected_fallback(self):
        cat, cfg = _classify("ZeroDivisionError", "division by zero")
        assert cat == "unexpected"

    def test_task_value_error(self):
        cat, cfg = _classify("TaskValueError", "invalid param batch_size=-1")
        assert cat == "invalid_input"


# ──────────────────────────────────────────────────────────────────────────────
# Severity escalation
# ──────────────────────────────────────────────────────────────────────────────

class TestSeverityEscalation:
    def test_no_retries_keeps_base_severity(self):
        result = _severity_from_retry(0, "medium")
        assert result == "medium"

    def test_two_retries_escalates_medium_to_high(self):
        result = _severity_from_retry(2, "medium")
        assert result == "high"

    def test_four_retries_escalates_to_critical(self):
        result = _severity_from_retry(4, "medium")
        assert result == "critical"

    def test_cannot_exceed_critical(self):
        result = _severity_from_retry(100, "critical")
        assert result == "critical"

    def test_low_severity_escalates(self):
        result = _severity_from_retry(2, "low")
        assert result == "medium"


# ──────────────────────────────────────────────────────────────────────────────
# Full analyze_failure()
# ──────────────────────────────────────────────────────────────────────────────

class TestAnalyzeFailure:
    def test_returns_failure_analysis_instance(self):
        result = analyze_failure("ValueError", "invalid input")
        assert isinstance(result, FailureAnalysis)

    def test_timeout_is_retryable(self):
        result = analyze_failure("TaskTimeoutError", "timed out")
        assert result.is_retryable is True
        assert result.category == "timeout"

    def test_invalid_input_is_not_retryable(self):
        result = analyze_failure("ValueError", "invalid parameter value")
        assert result.is_retryable is False
        assert result.category == "invalid_input"

    def test_connection_error_is_retryable(self):
        result = analyze_failure("ConnectionError", "connection refused")
        assert result.is_retryable is True

    def test_explanation_is_present_and_nonempty(self):
        result = analyze_failure("RuntimeError", "something unexpected")
        assert result.explanation
        assert len(result.explanation) > 20

    def test_suggestions_list_is_nonempty(self):
        result = analyze_failure("TimeoutError", "deadline exceeded")
        assert isinstance(result.suggestions, list)
        assert len(result.suggestions) >= 1

    def test_not_ai_enriched_by_default(self):
        result = analyze_failure("RuntimeError", "oops")
        assert result.ai_enriched is False

    def test_high_retry_count_escalates_severity(self):
        result_low = analyze_failure("ConnectionError", "refused", retry_count=0)
        result_high = analyze_failure("ConnectionError", "refused", retry_count=4)
        severities = ["low", "medium", "high", "critical"]
        assert severities.index(result_high.severity) >= severities.index(result_low.severity)

    def test_exc_type_and_message_captured(self):
        result = analyze_failure("MyCustomError", "something went wrong")
        assert result.exc_type == "MyCustomError"
        assert "something went wrong" in result.exc_message

    def test_traceback_shortening(self):
        long_tb = "\n".join([f"  line {i}" for i in range(50)])
        result = analyze_failure("RuntimeError", "err", traceback_str=long_tb)
        # Traceback summary should be shorter than the original
        assert len(result.traceback_summary.splitlines()) <= 10

    def test_task_name_in_summary(self):
        result = analyze_failure("ValueError", "bad param", task_name="my_task")
        assert "my_task" in result.summary


# ──────────────────────────────────────────────────────────────────────────────
# analysis_to_dict serialization
# ──────────────────────────────────────────────────────────────────────────────

class TestAnalysisToDict:
    def test_all_expected_keys_present(self):
        result = analyze_failure("RuntimeError", "err")
        d = analysis_to_dict(result)
        expected_keys = {
            "category", "severity", "summary", "explanation",
            "suggestions", "traceback_summary", "exc_type",
            "exc_message", "is_retryable", "confidence", "ai_enriched",
        }
        assert expected_keys.issubset(d.keys())

    def test_suggestions_is_list(self):
        result = analyze_failure("RuntimeError", "err")
        d = analysis_to_dict(result)
        assert isinstance(d["suggestions"], list)

    def test_serialization_roundtrip(self):
        import json
        result = analyze_failure("ConnectionError", "refused")
        d = analysis_to_dict(result)
        serialized = json.dumps(d)
        restored = json.loads(serialized)
        assert restored["category"] == "connection"
        assert restored["is_retryable"] is True


# ──────────────────────────────────────────────────────────────────────────────
# Traceback shortening
# ──────────────────────────────────────────────────────────────────────────────

class TestShortTraceback:
    def test_none_returns_placeholder(self):
        result = _shorten_traceback(None)
        assert "No traceback" in result

    def test_short_tb_unchanged(self):
        tb = "Traceback:\n  File foo.py, line 1\nRuntimeError: oops"
        result = _shorten_traceback(tb)
        assert "RuntimeError" in result

    def test_long_tb_truncated(self):
        long_tb = "\n".join([f"  line {i}" for i in range(100)])
        result = _shorten_traceback(long_tb)
        assert "omitted" in result
