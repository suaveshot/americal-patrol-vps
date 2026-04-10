"""
Shared retry decorator with exponential backoff.

Usage:
    from shared_utils.retry import with_retry

    @with_retry(max_attempts=3, base_delay=5, exceptions=(requests.Timeout, IOError))
    def fetch_data():
        ...

    # Or inline (without decorating):
    result = with_retry(max_attempts=3)(my_func)(arg1, arg2)
"""

import functools
import logging
import time


def with_retry(max_attempts=3, base_delay=5, exceptions=(Exception,), label=None):
    """
    Retry a function with exponential backoff on specified exceptions.

    Args:
        max_attempts: Total attempts (default 3)
        base_delay:   Seconds before first retry; doubles each attempt (5 → 10 → 20)
        exceptions:   Exception types that trigger a retry
        label:        Override the function name shown in log messages
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            name = label or func.__qualname__
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exc = e
                    if attempt < max_attempts:
                        delay = base_delay * (2 ** (attempt - 1))   # 5s, 10s, 20s
                        logging.warning(
                            f"[retry] {name} attempt {attempt}/{max_attempts} failed: {e}. "
                            f"Retrying in {delay}s..."
                        )
                        time.sleep(delay)
                    else:
                        logging.error(
                            f"[retry] {name} failed after {max_attempts} attempts: {e}"
                        )
            raise last_exc
        return wrapper
    return decorator
