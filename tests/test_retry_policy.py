"""
Unit tests for exponential backoff retry and circuit breaker.
"""

import unittest
import time
from unittest.mock import Mock, patch
from core.retry_policy import (
    CircuitBreaker,
    exponential_backoff_retry,
    retry_with_circuit_breaker,
    RetryPolicy,
    safe_execute_with_fallback
)


class TestCircuitBreaker(unittest.TestCase):
    """Test circuit breaker functionality."""

    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in CLOSED state (normal operation)."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)

        # Should be closed initially
        self.assertEqual(cb.state, "CLOSED")

        # Successful call
        result = cb.call(lambda: "success")
        self.assertEqual(result, "success")
        self.assertEqual(cb.state, "CLOSED")

    def test_circuit_breaker_opens_after_failures(self):
        """Test circuit breaker opens after failure threshold."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=1)

        failing_func = Mock(side_effect=Exception("error"))

        # First 3 failures should open circuit
        for i in range(3):
            with self.assertRaises(Exception):
                cb.call(failing_func)

        self.assertEqual(cb.state, "OPEN")
        self.assertEqual(cb.failure_count, 3)

    def test_circuit_breaker_fails_fast_when_open(self):
        """Test circuit breaker fails fast when OPEN."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=10)

        # Trigger failures to open circuit
        failing_func = Mock(side_effect=Exception("error"))
        for i in range(2):
            with self.assertRaises(Exception):
                cb.call(failing_func)

        self.assertEqual(cb.state, "OPEN")

        # Should fail fast without calling function
        with self.assertRaises(Exception) as context:
            cb.call(lambda: "should not run")

        self.assertIn("OPEN", str(context.exception))

    def test_circuit_breaker_half_open_recovery(self):
        """Test circuit breaker HALF_OPEN state and recovery."""
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout=0.1)

        # Open circuit
        failing_func = Mock(side_effect=Exception("error"))
        for i in range(2):
            with self.assertRaises(Exception):
                cb.call(failing_func)

        self.assertEqual(cb.state, "OPEN")

        # Wait for recovery timeout
        time.sleep(0.2)

        # Should transition to HALF_OPEN and allow call
        success_func = Mock(return_value="success")
        result = cb.call(success_func)

        self.assertEqual(result, "success")
        self.assertEqual(cb.state, "CLOSED")  # Should close on success

    def test_circuit_breaker_reset(self):
        """Test manual circuit breaker reset."""
        cb = CircuitBreaker(failure_threshold=2)

        # Open circuit
        failing_func = Mock(side_effect=Exception("error"))
        for i in range(2):
            with self.assertRaises(Exception):
                cb.call(failing_func)

        self.assertEqual(cb.state, "OPEN")

        # Manual reset
        cb.reset()

        self.assertEqual(cb.state, "CLOSED")
        self.assertEqual(cb.failure_count, 0)


class TestExponentialBackoffRetry(unittest.TestCase):
    """Test exponential backoff retry decorator."""

    def test_successful_call_no_retry(self):
        """Test successful call requires no retry."""
        @exponential_backoff_retry(max_retries=3, base_delay=0.01)
        def success_func():
            return "success"

        result = success_func()
        self.assertEqual(result, "success")

    def test_retry_on_failure(self):
        """Test retry on transient failure."""
        call_count = [0]

        @exponential_backoff_retry(max_retries=3, base_delay=0.01)
        def flaky_func():
            call_count[0] += 1
            if call_count[0] < 3:
                raise Exception("transient error")
            return "success"

        result = flaky_func()

        self.assertEqual(result, "success")
        self.assertEqual(call_count[0], 3)  # Should have retried 2 times

    def test_max_retries_exceeded(self):
        """Test exception raised when max retries exceeded."""
        @exponential_backoff_retry(max_retries=2, base_delay=0.01)
        def always_fails():
            raise ValueError("persistent error")

        with self.assertRaises(ValueError):
            always_fails()

    def test_exponential_delay(self):
        """Test that delays increase exponentially."""
        call_times = []

        @exponential_backoff_retry(max_retries=3, base_delay=0.05, exponential_base=2.0)
        def failing_func():
            call_times.append(time.time())
            raise Exception("error")

        with self.assertRaises(Exception):
            failing_func()

        # Check delays increased (approximately)
        self.assertEqual(len(call_times), 4)  # Initial + 3 retries

        # Delays should be roughly 0.05, 0.1, 0.2 seconds
        for i in range(1, len(call_times)):
            delay = call_times[i] - call_times[i-1]
            expected_delay = 0.05 * (2 ** (i-1))
            self.assertGreater(delay, expected_delay * 0.8)  # Allow some tolerance

    def test_specific_exception_catching(self):
        """Test catching only specific exception types."""
        @exponential_backoff_retry(max_retries=2, base_delay=0.01, exceptions=(ValueError,))
        def specific_error():
            raise KeyError("not caught")

        # KeyError should not be caught, should raise immediately
        with self.assertRaises(KeyError):
            specific_error()


class TestRetryPolicy(unittest.TestCase):
    """Test retry policy presets."""

    def test_get_database_policy(self):
        """Test database retry policy."""
        policy = RetryPolicy.get_policy("database")

        self.assertEqual(policy["max_retries"], 3)
        self.assertEqual(policy["base_delay"], 2.0)
        self.assertIn("exponential_base", policy)

    def test_get_api_policy(self):
        """Test API rate limit retry policy."""
        policy = RetryPolicy.get_policy("api")

        self.assertEqual(policy["max_retries"], 5)
        self.assertGreater(policy["max_delay"], 60)  # Longer delays for rate limits

    def test_get_network_policy(self):
        """Test network timeout retry policy."""
        policy = RetryPolicy.get_policy("network")

        self.assertEqual(policy["max_retries"], 3)
        self.assertIn("base_delay", policy)


class TestSafeExecuteWithFallback(unittest.TestCase):
    """Test safe execution with fallback."""

    def test_primary_success_no_fallback(self):
        """Test primary function succeeds, no fallback needed."""
        primary = Mock(return_value="primary result")
        fallback = Mock(return_value="fallback result")

        result, used_fallback = safe_execute_with_fallback(primary, fallback, max_retries=2)

        self.assertEqual(result, "primary result")
        self.assertFalse(used_fallback)
        fallback.assert_not_called()

    def test_primary_fails_uses_fallback(self):
        """Test fallback used when primary fails."""
        primary = Mock(side_effect=Exception("primary failed"))
        fallback = Mock(return_value="fallback result")

        result, used_fallback = safe_execute_with_fallback(primary, fallback, max_retries=2)

        self.assertEqual(result, "fallback result")
        self.assertTrue(used_fallback)
        self.assertEqual(primary.call_count, 2)  # Should retry
        fallback.assert_called_once()

    def test_both_fail_raises_exception(self):
        """Test exception raised when both primary and fallback fail."""
        primary = Mock(side_effect=Exception("primary failed"))
        fallback = Mock(side_effect=Exception("fallback failed"))

        with self.assertRaises(Exception):
            safe_execute_with_fallback(primary, fallback, max_retries=2)

    def test_no_fallback_raises_exception(self):
        """Test exception raised when no fallback provided."""
        primary = Mock(side_effect=Exception("primary failed"))

        with self.assertRaises(Exception):
            safe_execute_with_fallback(primary, fallback_func=None, max_retries=2)


if __name__ == "__main__":
    unittest.main()
