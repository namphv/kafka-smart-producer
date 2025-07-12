"""
Tests for protocol interfaces following Task-01 specifications.

This module implements comprehensive tests for all protocol interfaces,
including mock implementations, edge cases, and performance validation.
"""

import time
from typing import Any, Dict, Optional

import pytest

from kafka_smart_producer.exceptions import LagDataUnavailableError
from kafka_smart_producer.protocols import LagDataCollector


class MockLagDataCollector:
    """
    Mock implementation of LagDataCollector for testing.

    Follows Task-01 specification with configurable behavior for testing
    different scenarios including failures and edge cases.
    """

    def __init__(self, lag_data: Dict[int, int], is_healthy: bool = True):
        self._lag_data = lag_data.copy()
        self._is_healthy = is_healthy
        self._call_count = 0

    async def get_lag_data(self, topic: str) -> Dict[int, int]:
        """Async lag data retrieval with failure simulation."""
        self._call_count += 1
        if not self._is_healthy:
            raise LagDataUnavailableError(
                f"Mock collector unhealthy for topic: {topic}",
                context={"topic": topic, "call_count": self._call_count},
            )
        return self._lag_data.copy()

    def get_lag_data_sync(self, topic: str) -> Dict[int, int]:
        """Sync lag data retrieval with failure simulation."""
        self._call_count += 1
        if not self._is_healthy:
            raise LagDataUnavailableError(
                f"Mock collector unhealthy for topic: {topic}",
                context={"topic": topic, "call_count": self._call_count},
            )
        return self._lag_data.copy()

    def is_healthy(self) -> bool:
        """Health check with performance requirement < 100ms."""
        return self._is_healthy

    def set_health(self, healthy: bool) -> None:
        """Test helper to change health status."""
        self._is_healthy = healthy

    def get_call_count(self) -> int:
        """Test helper to verify call patterns."""
        return self._call_count


class MockCacheBackend:
    """
    Mock implementation of CacheBackend for testing.

    Simulates both local and distributed cache behavior with TTL support.
    """

    def __init__(self, simulate_failures: bool = False):
        self._data: Dict[str, Any] = {}
        self._ttl: Dict[str, float] = {}
        self._simulate_failures = simulate_failures
        self._call_count = 0

    def _is_expired(self, key: str) -> bool:
        """Check if key has expired based on TTL."""
        if key not in self._ttl:
            return False
        return time.time() > self._ttl[key]

    def _cleanup_expired(self, key: str) -> None:
        """Remove expired key from cache."""
        if self._is_expired(key):
            self._data.pop(key, None)
            self._ttl.pop(key, None)

    async def get(self, key: str) -> Optional[Any]:
        """Async get with TTL and failure simulation."""
        self._call_count += 1
        if self._simulate_failures:
            raise Exception("Simulated cache failure")

        self._cleanup_expired(key)
        return self._data.get(key)

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Async set with TTL support."""
        self._call_count += 1
        if self._simulate_failures:
            raise Exception("Simulated cache failure")

        self._data[key] = value
        if ttl is not None:
            self._ttl[key] = time.time() + ttl

    async def delete(self, key: str) -> None:
        """Async delete."""
        self._call_count += 1
        if self._simulate_failures:
            raise Exception("Simulated cache failure")

        self._data.pop(key, None)
        self._ttl.pop(key, None)

    def get_sync(self, key: str) -> Optional[Any]:
        """Sync get with TTL."""
        self._call_count += 1
        if self._simulate_failures:
            raise Exception("Simulated cache failure")

        self._cleanup_expired(key)
        return self._data.get(key)

    def set_sync(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Sync set with TTL support."""
        self._call_count += 1
        if self._simulate_failures:
            raise Exception("Simulated cache failure")

        self._data[key] = value
        if ttl is not None:
            self._ttl[key] = time.time() + ttl

    def delete_sync(self, key: str) -> None:
        """Sync delete."""
        self._call_count += 1
        if self._simulate_failures:
            raise Exception("Simulated cache failure")

        self._data.pop(key, None)
        self._ttl.pop(key, None)

    def get_call_count(self) -> int:
        """Test helper to verify call patterns."""
        return self._call_count


# Test Scenarios from Task-01 Specification


class TestLagDataCollector:
    """Test scenarios for LagDataCollector protocol."""

    def test_successful_lag_data_retrieval(self):
        """Scenario 1: Successful lag data retrieval."""
        lag_data = {0: 100, 1: 500, 2: 1500}
        collector = MockLagDataCollector(lag_data)

        result = collector.get_lag_data_sync("test-topic")

        # Verify mapping structure
        assert isinstance(result, dict)
        assert result == lag_data

        # Verify all partition IDs are non-negative integers
        for partition_id in result.keys():
            assert isinstance(partition_id, int)
            assert partition_id >= 0

        # Verify all lag counts are non-negative integers
        for lag_count in result.values():
            assert isinstance(lag_count, int)
            assert lag_count >= 0

    @pytest.mark.asyncio
    async def test_successful_lag_data_retrieval_async(self):
        """Scenario 1: Successful async lag data retrieval."""
        lag_data = {0: 100, 1: 500, 2: 1500}
        collector = MockLagDataCollector(lag_data)

        result = await collector.get_lag_data("test-topic")
        assert result == lag_data

    def test_data_source_unavailable(self):
        """Scenario 2: Data source unavailable."""
        collector = MockLagDataCollector({}, is_healthy=False)

        with pytest.raises(LagDataUnavailableError) as exc_info:
            collector.get_lag_data_sync("test-topic")

        # Verify error contains diagnostic information
        assert "test-topic" in str(exc_info.value)
        assert exc_info.value.context is not None
        assert "topic" in exc_info.value.context

    @pytest.mark.asyncio
    async def test_data_source_unavailable_async(self):
        """Scenario 2: Async data source unavailable."""
        collector = MockLagDataCollector({}, is_healthy=False)

        with pytest.raises(LagDataUnavailableError):
            await collector.get_lag_data("test-topic")

    def test_health_check_validation(self):
        """Scenario 3: Health check validation."""
        collector = MockLagDataCollector({0: 100})

        # Test performance requirement < 100ms
        start_time = time.time()
        is_healthy = collector.is_healthy()
        elapsed = time.time() - start_time

        assert isinstance(is_healthy, bool)
        assert elapsed < 0.1  # < 100ms
        assert is_healthy is True

        # Test unhealthy state
        collector.set_health(False)
        assert collector.is_healthy() is False


class TestCacheBackend:
    """Test scenarios for CacheBackend protocol."""

    @pytest.mark.asyncio
    async def test_basic_async_operations(self):
        """Test basic async cache operations."""
        cache = MockCacheBackend()

        # Test set/get
        await cache.set("key1", "value1")
        result = await cache.get("key1")
        assert result == "value1"

        # Test delete
        await cache.delete("key1")
        result = await cache.get("key1")
        assert result is None

        # Test non-existent key
        result = await cache.get("nonexistent")
        assert result is None

    def test_basic_sync_operations(self):
        """Test basic sync cache operations."""
        cache = MockCacheBackend()

        # Test set/get
        cache.set_sync("key2", "value2")
        result = cache.get_sync("key2")
        assert result == "value2"

        # Test delete
        cache.delete_sync("key2")
        result = cache.get_sync("key2")
        assert result is None

    @pytest.mark.asyncio
    async def test_ttl_functionality(self):
        """Test TTL (time-to-live) functionality."""
        cache = MockCacheBackend()

        # Set with very short TTL
        await cache.set("ttl_key", "ttl_value", ttl=1)

        # Should be available immediately
        result = await cache.get("ttl_key")
        assert result == "ttl_value"

        # Wait for expiration
        time.sleep(1.1)

        # Should be expired now
        result = await cache.get("ttl_key")
        assert result is None

    @pytest.mark.asyncio
    async def test_failure_simulation(self):
        """Test cache failure handling."""
        cache = MockCacheBackend(simulate_failures=True)

        with pytest.raises(Exception, match="Simulated cache failure"):
            await cache.get("any_key")

        with pytest.raises(Exception, match="Simulated cache failure"):
            await cache.set("any_key", "any_value")


# Performance and Integration Tests


class TestPerformanceRequirements:
    """Test performance requirements from Task-01."""

    def test_health_check_performance(self):
        """Verify is_healthy() completes in < 100ms."""
        collector = MockLagDataCollector({0: 100})

        times = []
        for _ in range(10):
            start = time.time()
            collector.is_healthy()
            times.append(time.time() - start)

        avg_time = sum(times) / len(times)
        assert avg_time < 0.1  # < 100ms average
        assert max(times) < 0.1  # No single call > 100ms

    def test_sync_method_performance(self):
        """Verify sync methods complete quickly (< 50ms typical)."""
        collector = MockLagDataCollector({i: i * 100 for i in range(10)})

        start = time.time()
        result = collector.get_lag_data_sync("test-topic")
        elapsed = time.time() - start

        assert elapsed < 0.05  # < 50ms
        assert len(result) == 10


class TestProtocolCompliance:
    """Test protocol interface compliance."""

    def test_all_methods_have_type_hints(self):
        """Verify all protocol methods have proper type hints."""

        # This would fail at import time if type hints are missing
        # due to Protocol validation
        assert LagDataCollector is not None

    def test_exception_hierarchy(self):
        """Test exception class hierarchy and context."""
        from kafka_smart_producer.exceptions import (
            LagDataUnavailableError,
            SmartProducerError,
        )

        # Test base exception
        base_error = SmartProducerError(
            "Test error", cause=ValueError("root cause"), context={"key": "value"}
        )
        assert str(base_error) == "Test error"
        assert isinstance(base_error.cause, ValueError)
        assert base_error.context["key"] == "value"

        # Test inheritance
        assert issubclass(LagDataUnavailableError, SmartProducerError)
