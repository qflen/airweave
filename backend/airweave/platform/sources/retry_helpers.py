"""Retry helpers for source connectors.

Provides reusable retry strategies that handle both API rate limits
and Airweave's internal rate limiting (via AirweaveHttpClient).
"""

import httpx
from tenacity import retry_if_exception, wait_exponential
import random
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


def should_retry_on_rate_limit(exception: BaseException) -> bool:
    """Check if exception is a retryable rate limit (429).

    Handles both:
    - Real API 429 responses
    - Airweave internal rate limits (AirweaveHttpClient → 429)

    Args:
        exception: Exception to check

    Returns:
        True if this is a 429 that should be retried
    """
    if isinstance(exception, httpx.HTTPStatusError):
        return exception.response.status_code == 429
    return False


def should_retry_on_timeout(exception: BaseException) -> bool:
    """Check if exception is a timeout that should be retried.

    Args:
        exception: Exception to check

    Returns:
        True if this is a timeout exception
    """
    return isinstance(exception, (httpx.ConnectTimeout, httpx.ReadTimeout))


def should_retry_on_rate_limit_or_timeout(exception: BaseException) -> bool:
    """Combined retry condition for rate limits and timeouts.

    Use this as the retry condition for source API calls:

    Example:
        @retry(
            stop=stop_after_attempt(5),
            retry=should_retry_on_rate_limit_or_timeout,
            wait=wait_rate_limit_with_backoff,
            reraise=True,
        )
        async def _get_with_auth(self, client, url, params=None):
            ...
    """
    return should_retry_on_rate_limit(exception) or should_retry_on_timeout(exception)


def wait_rate_limit_with_backoff(retry_state) -> float:
    """Wait strategy that respects Retry-After header for 429s, exponential backoff for timeouts.

    For 429 errors:
    - Uses Retry-After header if present (set by AirweaveHttpClient)
    - Falls back to exponential backoff if no header

    For timeouts:
    - Uses exponential backoff: 2s, 4s, 8s, max 10s

    Args:
        retry_state: tenacity retry state

    Returns:
        Number of seconds to wait before retry
    """
    exception = retry_state.outcome.exception()

    attempt = getattr(retry_state, "attempt_number", 1) or 1
    exp_429 = min(30.0, 2.0 ** (attempt - 1))  # for 429 fallback path
    exp_to = min(10.0, 2.0 ** (attempt - 1))   # for timeout path

    # For 429 rate limits, check Retry-After header
    if isinstance(exception, httpx.HTTPStatusError) and exception.response.status_code == 429:
        retry_after = exception.response.headers.get("Retry-After")
        if retry_after:
            try:
                # Retry-After is in seconds (float)
                wait_seconds = float(retry_after)

                # CRITICAL: Add minimum wait of 1.0s to prevent rapid-fire retries
                # When Retry-After is < 1s (e.g., 0.3s), retries happen too fast and
                # burn through all attempts before the window actually expires.
                # This ensures we always wait long enough for the sliding window to clear.
                wait_seconds = max(wait_seconds, 1.0)

                # If header was actually a date, the float() branch above would fail; handled below.
            except (ValueError, TypeError):
                try:
                    dt = parsedate_to_datetime(retry_after)
                    # Convert to seconds from now (UTC-safe)
                    wait_seconds = max((dt - datetime.now(timezone.utc)).total_seconds(), 0.0)
                    wait_seconds = max(wait_seconds, 1.0)  # same minimum
                except Exception:
                    # fall through to exponential
                    wait_seconds = 0.0

            if wait_seconds and wait_seconds > 0.0:
                # Cap at 120 seconds to avoid indefinite waits
                wait = min(wait_seconds, 120.0)
                return wait + random.uniform(0.0, 0.1 * wait)

        # No Retry-After header or invalid - use exponential backoff
        # This shouldn't happen with AirweaveHttpClient (always sets header)
        # but might happen with real API 429s that don't include header
        base = exp_429 if exp_429 else wait_exponential(multiplier=1, min=2, max=30)(retry_state)
        return base + random.uniform(0.0, 0.1 * base)

    # For timeouts and other retryable errors, use exponential backoff
    base = exp_to if exp_to else wait_exponential(multiplier=1, min=2, max=10)(retry_state)
    return base + random.uniform(0.0, 0.1 * base)

# For sources that need simpler fixed-wait retry strategy
retry_if_rate_limit = retry_if_exception(should_retry_on_rate_limit)
retry_if_timeout = retry_if_exception(should_retry_on_timeout)
retry_if_rate_limit_or_timeout = retry_if_exception(should_retry_on_rate_limit_or_timeout)
