"""
Threading utilities for Kafka Smart Producer.

This module provides simplified threading patterns for background health monitoring
using standard library approaches instead of custom complex implementations.
"""

import asyncio
import logging
import threading
from typing import Any, Callable, Optional, Union

from cachetools import TTLCache

logger = logging.getLogger(__name__)


class ThreadSafeCache:
    """
    Thread-safe cache using cachetools.TTLCache with lock protection.

    Provides a simple, reliable caching solution using proven libraries
    instead of custom implementations.
    """

    def __init__(self, max_size: int = 1000, default_ttl: float = 300.0):
        """
        Initialize thread-safe cache.

        Args:
            max_size: Maximum number of entries in cache
            default_ttl: Default TTL in seconds for cache entries
        """
        self._cache: TTLCache[str, Any] = TTLCache(maxsize=max_size, ttl=default_ttl)
        self._lock = threading.RLock()

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache. Thread-safe."""
        with self._lock:
            return self._cache.get(key)

    def set(self, key: str, value: Any) -> None:
        """Set value in cache. Thread-safe."""
        with self._lock:
            self._cache[key] = value

    def delete(self, key: str) -> None:
        """Delete key from cache. Thread-safe."""
        with self._lock:
            self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cache entries. Thread-safe."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get current cache size. Thread-safe."""
        with self._lock:
            return len(self._cache)


async def run_periodic_async(
    callback: Union[Callable[[], Any], Any], interval: float = 5.0
) -> None:
    """
    Run a callback periodically in an async context.

    Simplified replacement for AsyncBackgroundRefreshManager using
    standard asyncio patterns.

    Args:
        callback: Function to call periodically (sync or async)
        interval: Interval between calls in seconds
    """
    while True:
        try:
            # Check if callback is async function or async callable
            if asyncio.iscoroutinefunction(callback) or (
                callable(callback) and asyncio.iscoroutinefunction(callback.__call__)  # type: ignore[operator]
            ):
                await callback()
            else:
                # Run blocking callback in executor to avoid blocking event loop
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(None, callback)
        except Exception as e:
            logger.warning(f"Background refresh error: {e}", exc_info=True)

        try:
            await asyncio.sleep(interval)
        except asyncio.CancelledError:
            logger.info("Async background refresh task cancelled")
            break


class SimpleBackgroundRefresh:
    """
    Simple background refresh using threading.Timer for sync contexts.

    Simplified replacement for ThreadingBackgroundRefreshManager using
    standard library patterns.
    """

    def __init__(self, callback: Callable[[], Any], interval: float = 5.0):
        """
        Initialize simple background refresh.

        Args:
            callback: Function to call periodically (must be sync)
            interval: Interval between calls in seconds
        """
        self._callback = callback
        self._interval = interval
        self._timer: Optional[threading.Timer] = None
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start background refresh."""
        with self._lock:
            if self._running:
                logger.debug("Background refresh already running")
                return
            self._running = True
            self._schedule_next()
        logger.info(f"Started background refresh with {self._interval}s interval")

    def stop(self) -> None:
        """Stop background refresh."""
        with self._lock:
            if not self._running:
                return
            self._running = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
        logger.info("Stopped background refresh")

    def is_running(self) -> bool:
        """Check if background refresh is running."""
        with self._lock:
            return self._running

    def get_refresh_interval(self) -> float:
        """Get refresh interval."""
        return self._interval

    def _schedule_next(self) -> None:
        """Schedule the next refresh call."""
        if not self._running:
            return

        def _execute_and_reschedule() -> None:
            try:
                self._callback()
            except Exception as e:
                logger.warning(f"Background refresh error: {e}", exc_info=True)

            with self._lock:
                if self._running:
                    self._timer = threading.Timer(
                        self._interval, _execute_and_reschedule
                    )
                    self._timer.daemon = True
                    self._timer.start()

        self._timer = threading.Timer(self._interval, _execute_and_reschedule)
        self._timer.daemon = True
        self._timer.start()


# Utility functions for explicit context-based usage
def create_async_background_task(
    callback: Callable[[], Any], interval: float = 5.0
) -> asyncio.Task[None]:
    """
    Create async background task using standard asyncio patterns.

    Args:
        callback: Function to call periodically
        interval: Interval in seconds

    Returns:
        asyncio.Task that can be cancelled
    """
    return asyncio.create_task(run_periodic_async(callback, interval))


def create_sync_background_refresh(
    callback: Callable[[], Any], interval: float = 5.0
) -> SimpleBackgroundRefresh:
    """
    Create sync background refresh using threading.Timer.

    Args:
        callback: Function to call periodically
        interval: Interval in seconds

    Returns:
        SimpleBackgroundRefresh instance
    """
    return SimpleBackgroundRefresh(callback, interval)
