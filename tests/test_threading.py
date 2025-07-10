"""
Tests for simplified threading utilities.

This module tests the simplified threading components using standard library patterns
instead of the previous complex custom implementations.
"""

import asyncio
import threading
import time

import pytest

from kafka_smart_producer.caching import CacheConfig, DefaultLocalCache
from kafka_smart_producer.threading import (
    SimpleBackgroundRefresh,
    create_async_background_task,
    create_sync_background_refresh,
    run_periodic_async,
)


class MockRefreshCallback:
    """Mock callback for testing background refresh."""

    def __init__(self, should_raise: bool = False, delay: float = 0.0):
        self.call_count = 0
        self.should_raise = should_raise
        self.delay = delay
        self.calls = []
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            self.call_count += 1
            self.calls.append(time.time())

        if self.delay > 0:
            time.sleep(self.delay)

        if self.should_raise:
            raise Exception(f"Mock error on call {self.call_count}")


class MockAsyncRefreshCallback:
    """Mock async callback for testing."""

    def __init__(self, delay: float = 0.01, should_raise: bool = False):
        self.call_count = 0
        self.delay = delay
        self.should_raise = should_raise
        self.calls = []
        self.lock = asyncio.Lock()

    async def __call__(self):
        async with self.lock:
            self.call_count += 1
            self.calls.append(time.time())

        await asyncio.sleep(self.delay)

        if self.should_raise:
            raise Exception(f"Mock async error on call {self.call_count}")


class TestSimpleBackgroundRefresh:
    """Test scenarios for SimpleBackgroundRefresh using threading.Timer."""

    def test_basic_lifecycle(self):
        """Test start and stop lifecycle."""
        callback = MockRefreshCallback()
        refresh = SimpleBackgroundRefresh(callback, interval=0.1)

        # Initially not running
        assert not refresh.is_running()

        # Start the refresh
        refresh.start()
        assert refresh.is_running()
        assert refresh.get_refresh_interval() == 0.1

        # Wait for callbacks
        time.sleep(0.25)
        assert callback.call_count >= 2

        # Stop the refresh
        refresh.stop()
        assert not refresh.is_running()

        # Verify callbacks stop
        old_count = callback.call_count
        time.sleep(0.15)
        assert callback.call_count == old_count

    def test_exception_handling(self):
        """Test exception handling in refresh."""
        callback = MockRefreshCallback(should_raise=True)
        refresh = SimpleBackgroundRefresh(callback, interval=0.1)

        refresh.start()
        assert refresh.is_running()

        # Wait for multiple callback attempts
        time.sleep(0.25)

        # Verify exceptions are caught and refresh continues
        assert callback.call_count >= 2
        assert refresh.is_running()

        refresh.stop()

    def test_multiple_start_calls(self):
        """Test idempotent start behavior."""
        callback = MockRefreshCallback()
        refresh = SimpleBackgroundRefresh(callback, interval=0.1)

        refresh.start()
        refresh.start()  # Second call should be ignored
        assert refresh.is_running()

        time.sleep(0.15)
        refresh.stop()

    def test_stop_without_start(self):
        """Test stop without start doesn't raise errors."""
        callback = MockRefreshCallback()
        refresh = SimpleBackgroundRefresh(callback, interval=0.1)

        refresh.stop()  # Should not raise
        assert not refresh.is_running()


class TestAsyncBackgroundRefresh:
    """Test scenarios for async background refresh utilities."""

    @pytest.mark.asyncio
    async def test_run_periodic_async_with_sync_callback(self):
        """Test run_periodic_async with sync callback."""
        callback = MockRefreshCallback()

        # Start the task
        task = asyncio.create_task(run_periodic_async(callback, interval=0.1))

        # Wait for some callbacks
        await asyncio.sleep(0.25)
        assert callback.call_count >= 2

        # Cancel the task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_run_periodic_async_with_async_callback(self):
        """Test run_periodic_async with async callback."""
        callback = MockAsyncRefreshCallback(delay=0.05)

        # Start the task
        task = asyncio.create_task(run_periodic_async(callback, interval=0.1))

        # Wait for some callbacks
        await asyncio.sleep(0.25)
        assert callback.call_count >= 2

        # Cancel the task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_exception_handling_async(self):
        """Test exception handling in async refresh."""
        callback = MockAsyncRefreshCallback(should_raise=True)

        # Start the task
        task = asyncio.create_task(run_periodic_async(callback, interval=0.1))

        # Wait for callbacks
        await asyncio.sleep(0.25)

        # Should continue despite exceptions
        assert callback.call_count >= 2

        # Cancel the task
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_create_async_background_task(self):
        """Test the create_async_background_task utility function."""
        callback = MockRefreshCallback()

        task = create_async_background_task(callback, interval=0.1)

        await asyncio.sleep(0.25)
        assert callback.call_count >= 2

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


class TestSyncUtilities:
    """Test sync utility functions."""

    def test_create_sync_background_refresh(self):
        """Test the create_sync_background_refresh utility function."""
        callback = MockRefreshCallback()

        refresh = create_sync_background_refresh(callback, interval=0.1)
        assert isinstance(refresh, SimpleBackgroundRefresh)
        assert refresh.get_refresh_interval() == 0.1

        refresh.start()
        time.sleep(0.25)
        assert callback.call_count >= 2
        refresh.stop()


class TestPerformanceRequirements:
    """Test that simplified implementation meets performance requirements."""

    @pytest.mark.asyncio
    async def test_async_task_overhead(self):
        """Verify async tasks don't block event loop."""
        callback = MockRefreshCallback(delay=0.05)  # 50ms blocking operation

        # Measure event loop responsiveness
        async def event_loop_test():
            start = time.time()
            await asyncio.sleep(0.01)
            return time.time() - start

        task = create_async_background_task(callback, interval=0.1)

        # Event loop should remain responsive
        elapsed = await event_loop_test()
        assert elapsed < 0.02  # Should be close to 0.01 + small overhead

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    def test_thread_resource_usage(self):
        """Verify minimal thread overhead."""
        callback = MockRefreshCallback()
        refresh = SimpleBackgroundRefresh(callback, interval=1.0)

        # Count active threads before
        initial_threads = threading.active_count()

        refresh.start()

        # Should only add one thread (Timer creates threads internally)
        # Note: Timer may create multiple threads, so we allow some variance
        current_threads = threading.active_count()
        assert current_threads >= initial_threads
        assert current_threads <= initial_threads + 3  # Allow for Timer internals

        refresh.stop()

        # Give some time for cleanup
        time.sleep(0.1)

    def test_cache_access_performance(self):
        """Test cache access time requirement (<1ms)."""
        cache = DefaultLocalCache(
            CacheConfig(local_max_size=1000, local_default_ttl_seconds=300.0)
        )

        # Pre-populate cache
        for i in range(100):
            cache.set(f"key_{i}", f"value_{i}")

        # Measure access times
        times = []
        for _ in range(1000):
            start = time.time()
            cache.get("key_50")
            times.append(time.time() - start)

        avg_time = sum(times) / len(times)
        max_time = max(times)

        # Performance requirement: < 1ms
        assert avg_time < 0.001
        assert max_time < 0.001


class TestIntegration:
    """Integration tests for simplified components."""

    @pytest.mark.asyncio
    async def test_async_refresh_with_cache(self):
        """Test async refresh updating cache."""
        cache = DefaultLocalCache(
            CacheConfig(local_max_size=10, local_default_ttl_seconds=300.0)
        )
        call_count = 0

        async def refresh_callback():
            nonlocal call_count
            call_count += 1
            cache.set(f"key_{call_count}", f"value_{call_count}")

        task = create_async_background_task(refresh_callback, interval=0.1)

        await asyncio.sleep(0.25)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify cache was updated
        assert cache.size() >= 2
        assert cache.get("key_1") == "value_1"

    def test_sync_refresh_with_cache(self):
        """Test sync refresh updating cache."""
        cache = DefaultLocalCache(
            CacheConfig(local_max_size=10, local_default_ttl_seconds=300.0)
        )
        call_count = 0
        lock = threading.Lock()

        def refresh_callback():
            nonlocal call_count
            with lock:
                call_count += 1
                cache.set(f"key_{call_count}", f"value_{call_count}")

        refresh = create_sync_background_refresh(refresh_callback, interval=0.1)

        refresh.start()
        time.sleep(0.25)
        refresh.stop()

        # Verify cache was updated
        assert cache.size() >= 2
        assert cache.get("key_1") == "value_1"
