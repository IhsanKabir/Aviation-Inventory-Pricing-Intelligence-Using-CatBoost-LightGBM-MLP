"""
Exponential backoff retry logic with circuit breaker pattern.

Implements robust error recovery for transient failures:
- Database connection errors
- API rate limits
- Network timeouts
- Circuit breaker to prevent cascading failures
"""

import time
import functools
from typing import Callable, Any, Optional, Tuple, Type
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """
    Circuit breaker pattern to prevent cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failure threshold exceeded, requests fail fast
    - HALF_OPEN: Testing if service recovered, limited requests allowed
    """

    def __init__(self,
                 failure_threshold: int = 5,
                 recovery_timeout: int = 60,
                 expected_exception: Type[Exception] = Exception):
        """
        Initialize circuit breaker.

        Args:
            failure_threshold: Number of failures before opening circuit
            recovery_timeout: Seconds to wait before attempting recovery
            expected_exception: Exception type to catch
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exception = expected_exception

        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to call
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Function result

        Raises:
            Exception: If circuit is open or function fails
        """
        if self.state == "OPEN":
            if self._should_attempt_reset():
                self.state = "HALF_OPEN"
                logger.info("Circuit breaker entering HALF_OPEN state")
            else:
                raise Exception("Circuit breaker is OPEN - failing fast")

        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except self.expected_exception as e:
            self._on_failure()
            raise e

    def _on_success(self):
        """Handle successful call."""
        self.failure_count = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info("Circuit breaker CLOSED after successful recovery")

    def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"Circuit breaker OPEN after {self.failure_count} failures")

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True

        return (datetime.now() - self.last_failure_time).total_seconds() >= self.recovery_timeout

    def reset(self):
        """Manually reset circuit breaker."""
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"
        logger.info("Circuit breaker manually reset")


def exponential_backoff_retry(max_retries: int = 3,
                              base_delay: float = 1.0,
                              max_delay: float = 60.0,
                              exponential_base: float = 2.0,
                              exceptions: Tuple[Type[Exception], ...] = (Exception,),
                              on_retry: Optional[Callable] = None):
    """
    Decorator for exponential backoff retry logic.

    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay in seconds
        max_delay: Maximum delay in seconds (cap for exponential growth)
        exponential_base: Base for exponential calculation (default 2 = doubles each time)
        exceptions: Tuple of exception types to catch and retry
        on_retry: Optional callback function called on each retry

    Example:
        @exponential_backoff_retry(max_retries=3, base_delay=1.0)
        def fetch_data_from_db():
            # Database query that might fail transiently
            pass
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} retries: {str(e)}"
                        )
                        raise e

                    # Calculate delay with exponential backoff
                    delay = min(base_delay * (exponential_base ** attempt), max_delay)

                    logger.warning(
                        f"{func.__name__} failed on attempt {attempt + 1}/{max_retries + 1}. "
                        f"Retrying in {delay:.1f}s... Error: {str(e)}"
                    )

                    if on_retry:
                        on_retry(attempt, delay, e)

                    time.sleep(delay)

            # Should never reach here, but just in case
            raise last_exception

        return wrapper

    return decorator


def retry_with_circuit_breaker(circuit_breaker: CircuitBreaker,
                               max_retries: int = 3,
                               base_delay: float = 1.0,
                               exponential_base: float = 2.0):
    """
    Decorator combining exponential backoff with circuit breaker.

    Args:
        circuit_breaker: CircuitBreaker instance
        max_retries: Maximum retry attempts
        base_delay: Initial delay in seconds
        exponential_base: Base for exponential calculation

    Example:
        db_circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

        @retry_with_circuit_breaker(db_circuit_breaker, max_retries=3)
        def query_database():
            # Database query
            pass
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    # Use circuit breaker to call function
                    return circuit_breaker.call(func, *args, **kwargs)
                except Exception as e:
                    last_exception = e

                    if attempt == max_retries:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} retries: {str(e)}"
                        )
                        raise e

                    # Calculate delay
                    delay = base_delay * (exponential_base ** attempt)

                    logger.warning(
                        f"{func.__name__} failed on attempt {attempt + 1}/{max_retries + 1}. "
                        f"Retrying in {delay:.1f}s..."
                    )

                    time.sleep(delay)

            raise last_exception

        return wrapper

    return decorator


class RetryPolicy:
    """
    Configurable retry policy for different failure scenarios.
    """

    # Predefined policies for common scenarios
    DATABASE_RETRY = {
        "max_retries": 3,
        "base_delay": 2.0,
        "max_delay": 30.0,
        "exponential_base": 2.0,
    }

    API_RATE_LIMIT_RETRY = {
        "max_retries": 5,
        "base_delay": 5.0,
        "max_delay": 120.0,
        "exponential_base": 2.0,
    }

    NETWORK_TIMEOUT_RETRY = {
        "max_retries": 3,
        "base_delay": 1.0,
        "max_delay": 10.0,
        "exponential_base": 2.0,
    }

    @staticmethod
    def get_policy(policy_name: str) -> dict:
        """
        Get predefined retry policy by name.

        Args:
            policy_name: Name of policy (DATABASE_RETRY, API_RATE_LIMIT_RETRY, NETWORK_TIMEOUT_RETRY)

        Returns:
            Dictionary with retry parameters
        """
        policies = {
            "database": RetryPolicy.DATABASE_RETRY,
            "api": RetryPolicy.API_RATE_LIMIT_RETRY,
            "network": RetryPolicy.NETWORK_TIMEOUT_RETRY,
        }
        return policies.get(policy_name.lower(), RetryPolicy.DATABASE_RETRY)


def safe_execute_with_fallback(primary_func: Callable,
                               fallback_func: Optional[Callable] = None,
                               max_retries: int = 3,
                               log_prefix: str = "") -> Tuple[Any, bool]:
    """
    Execute function with retry logic and optional fallback.

    Args:
        primary_func: Primary function to execute
        fallback_func: Optional fallback function if primary fails
        max_retries: Maximum retry attempts for primary function
        log_prefix: Prefix for log messages

    Returns:
        Tuple of (result, used_fallback)
        - result: Function return value
        - used_fallback: True if fallback was used, False if primary succeeded
    """
    # Try primary function with retries
    for attempt in range(max_retries):
        try:
            result = primary_func()
            return result, False
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"{log_prefix} Primary function failed after {max_retries} attempts: {str(e)}")
                break
            else:
                delay = 2 ** attempt
                logger.warning(f"{log_prefix} Attempt {attempt + 1} failed, retrying in {delay}s...")
                time.sleep(delay)

    # Try fallback if available
    if fallback_func:
        try:
            logger.info(f"{log_prefix} Using fallback function")
            result = fallback_func()
            return result, True
        except Exception as e:
            logger.error(f"{log_prefix} Fallback function also failed: {str(e)}")
            raise e
    else:
        raise Exception(f"{log_prefix} No fallback available and primary function failed")
