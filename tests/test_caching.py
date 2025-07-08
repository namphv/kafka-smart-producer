"""
Tests for the caching system components.

This module tests L1Cache, L2Cache, and HybridCache implementations including
LRU eviction, TTL expiration, read-through patterns, and error handling.
"""

import time
from typing import Any, Dict, Optional

import pytest

from kafka_smart_producer.caching import (
    CacheConfig,
    CacheEntry,
    CacheStats,
    CacheTimeoutError,
    CacheUnavailableError,
    DefaultHybridCache,
    DefaultL1Cache,
)


class TestCacheEntry:
    """Test CacheEntry class functionality."""

    def test_cache_entry_creation(self):
        """Test basic cache entry creation."""
        value = "test_value"
        entry = CacheEntry(value, ttl_seconds=60.0)

        assert entry.value == value
        assert entry.ttl_seconds == 60.0
        assert entry.created_at > 0

    def test_cache_entry_expiration(self):
        """Test TTL expiration logic."""
        entry = CacheEntry("test", ttl_seconds=0.1)

        # Should not be expired immediately
        assert not entry.is_expired()

        # Manually advance time for deterministic testing
        entry.created_at = time.time() - 0.15  # Make it expired
        assert entry.is_expired()

    def test_cache_entry_no_ttl(self):
        """Test cache entry without TTL."""
        entry = CacheEntry("test", ttl_seconds=None)

        # Should never expire
        assert not entry.is_expired()
        # Even if we set created_at in the past
        entry.created_at = time.time() - 3600  # 1 hour ago
        assert not entry.is_expired()

    def test_cache_entry_touch(self):
        """Test that entry creation works without access tracking."""
        entry = CacheEntry("test")

        # Entry should be created successfully without access tracking
        assert entry.value == "test"
        assert entry.created_at > 0


class TestCacheStats:
    """Test CacheStats functionality."""

    def test_cache_stats_creation(self):
        """Test stats creation with default values."""
        stats = CacheStats()

        assert stats.hits_l1 == 0
        assert stats.hits_l2 == 0
        assert stats.misses == 0
        assert stats.sets == 0
        assert stats.deletes == 0
        assert stats.evictions == 0
        assert stats.errors == 0
        assert stats.total_latency_ms == 0.0

    def test_cache_stats_operations(self):
        """Test stats tracking operations."""
        stats = CacheStats()

        stats.hit_l1(1.5)
        stats.hit_l2(3.0)
        stats.miss(2.0)
        stats.set_operation()
        stats.delete_operation()
        stats.eviction()
        stats.error()

        assert stats.hits_l1 == 1
        assert stats.hits_l2 == 1
        assert stats.misses == 1
        assert stats.sets == 1
        assert stats.deletes == 1
        assert stats.evictions == 1
        assert stats.errors == 1
        assert stats.total_latency_ms == 6.5

    def test_cache_stats_calculations(self):
        """Test calculated metrics."""
        stats = CacheStats()

        # Test empty stats
        assert stats.get_hit_ratio() == 0.0
        assert stats.get_avg_latency_ms() == 0.0

        # Add some operations
        stats.hit_l1(2.0)
        stats.hit_l2(4.0)
        stats.miss(6.0)

        # Hit ratio should be 2/3
        assert stats.get_hit_ratio() == pytest.approx(2 / 3)
        # Average latency should be 4.0ms
        assert stats.get_avg_latency_ms() == pytest.approx(4.0)

    def test_cache_stats_reset(self):
        """Test stats reset functionality."""
        stats = CacheStats()

        stats.hit_l1(1.0)
        stats.set_operation()

        stats.reset()

        assert stats.hits_l1 == 0
        assert stats.sets == 0
        assert stats.total_latency_ms == 0.0


class TestDefaultL1Cache:
    """Test DefaultL1Cache implementation."""

    def create_l1_cache(self, max_size: int = 3, ttl: float = 60.0) -> DefaultL1Cache:
        """Create a test L1 cache with custom configuration."""
        config = CacheConfig(
            l1_max_size=max_size,
            l1_default_ttl_seconds=ttl,
            stats_collection_enabled=True,
        )
        return DefaultL1Cache(config)

    def test_basic_operations(self):
        """Test basic cache get/set/delete operations."""
        cache = self.create_l1_cache()

        # Test set and get
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"
        assert cache.size() == 1

        # Test delete
        cache.delete("key1")
        assert cache.get("key1") is None
        assert cache.size() == 0

    def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        cache = self.create_l1_cache(max_size=2)

        # Fill cache to capacity
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        assert cache.size() == 2

        # Add third item, should evict oldest
        cache.set("key3", "value3")
        assert cache.size() == 2
        assert cache.get("key1") is None  # Evicted
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"

    def test_lru_order_update(self):
        """Test that accessing items updates LRU order."""
        cache = self.create_l1_cache(max_size=2)

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        # Access key1 to make it most recently used
        cache.get("key1")

        # Add third item, should evict key2 (least recently used)
        cache.set("key3", "value3")
        assert cache.get("key1") == "value1"  # Still there
        assert cache.get("key2") is None  # Evicted
        assert cache.get("key3") == "value3"

    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        cache = self.create_l1_cache(ttl=0.1)

        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        # Manually expire the entry
        node = cache._data["key1"]
        node.entry.created_at = time.time() - 0.15  # Make it expired

        assert cache.get("key1") is None
        assert cache.size() == 0  # Expired entry should be removed

    def test_custom_ttl(self):
        """Test per-item TTL override."""
        cache = self.create_l1_cache(ttl=60.0)  # Default long TTL

        # Set with short TTL
        cache.set("key1", "value1", ttl_seconds=0.1)
        cache.set("key2", "value2")  # Uses default TTL

        # Manually expire the short TTL entry
        node1 = cache._data["key1"]
        node1.entry.created_at = time.time() - 0.15  # Make it expired

        assert cache.get("key1") is None  # Expired
        assert cache.get("key2") == "value2"  # Still valid

    def test_clear_operation(self):
        """Test cache clear functionality."""
        cache = self.create_l1_cache()

        cache.set("key1", "value1")
        cache.set("key2", "value2")
        assert cache.size() == 2

        cache.clear()
        assert cache.size() == 0
        assert cache.get("key1") is None
        assert cache.get("key2") is None

    def test_stats_collection(self):
        """Test cache statistics collection."""
        cache = self.create_l1_cache()
        stats = cache.get_stats()

        # Test miss
        cache.get("nonexistent")
        assert stats.misses == 1
        assert stats.hits_l1 == 0

        # Test hit
        cache.set("key1", "value1")
        cache.get("key1")
        assert stats.hits_l1 == 1
        assert stats.sets == 1

        # Test delete
        cache.delete("key1")
        assert stats.deletes == 1

    def test_stats_disabled(self):
        """Test cache with statistics disabled."""
        config = CacheConfig(l1_max_size=10, stats_collection_enabled=False)
        cache = DefaultL1Cache(config)

        cache.set("key1", "value1")
        cache.get("key1")
        cache.delete("key1")

        # Stats should remain at zero
        stats = cache.get_stats()
        assert stats.hits_l1 == 0
        assert stats.sets == 0
        assert stats.deletes == 0

    def test_update_existing_key(self):
        """Test updating existing key maintains LRU position."""
        cache = self.create_l1_cache(max_size=2)

        cache.set("key1", "value1")
        cache.set("key2", "value2")

        # Update key1
        cache.set("key1", "new_value1")
        assert cache.size() == 2

        # Add third key, should not evict key1 since it was updated
        cache.set("key3", "value3")
        assert cache.get("key1") == "new_value1"
        assert cache.get("key2") is None  # Evicted
        assert cache.get("key3") == "value3"


class MockL2Cache:
    """Mock L2 cache for testing hybrid functionality."""

    def __init__(self, available: bool = True, fail_operations: bool = False):
        self._data: Dict[str, Any] = {}
        self._available = available
        self._fail_operations = fail_operations
        self._stats = CacheStats()

    async def get(self, key: str) -> Optional[Any]:
        if self._fail_operations:
            raise CacheUnavailableError("Mock L2 failure")

        value = self._data.get(key)
        if value:
            self._stats.hit_l2(2.0)
        else:
            self._stats.miss(2.0)
        return value

    def get_sync(self, key: str) -> Optional[Any]:
        if self._fail_operations:
            raise CacheUnavailableError("Mock L2 failure")

        return self._data.get(key)

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        if self._fail_operations:
            raise CacheUnavailableError("Mock L2 failure")

        self._data[key] = value
        self._stats.set_operation()

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        if self._fail_operations:
            raise CacheUnavailableError("Mock L2 failure")

        self._data[key] = value

    async def delete(self, key: str) -> None:
        if self._fail_operations:
            raise CacheUnavailableError("Mock L2 failure")

        self._data.pop(key, None)

    def delete_sync(self, key: str) -> None:
        if self._fail_operations:
            raise CacheUnavailableError("Mock L2 failure")

        self._data.pop(key, None)

    async def ping(self) -> bool:
        return self._available

    def ping_sync(self) -> bool:
        return self._available

    def get_stats(self) -> CacheStats:
        return self._stats

    def set_available(self, available: bool) -> None:
        """Helper for testing availability changes."""
        self._available = available

    def set_fail_operations(self, fail: bool) -> None:
        """Helper for testing operation failures."""
        self._fail_operations = fail


class TestDefaultHybridCache:
    """Test DefaultHybridCache implementation."""

    def create_hybrid_cache(
        self, l2_available: bool = True, l2_enabled: bool = True
    ) -> tuple[DefaultHybridCache, DefaultL1Cache, MockL2Cache]:
        """Create hybrid cache with mock L2."""
        config = CacheConfig(
            l1_max_size=10,
            l1_default_ttl_seconds=60.0,
            l2_enabled=l2_enabled,
            l2_default_ttl_seconds=300.0,
        )
        l1_cache = DefaultL1Cache(config)
        l2_cache = MockL2Cache(available=l2_available)
        hybrid_cache = DefaultHybridCache(l1_cache, l2_cache, config)

        return hybrid_cache, l1_cache, l2_cache

    @pytest.mark.asyncio
    async def test_read_through_pattern(self):
        """Test read-through cache pattern: L1 -> L2 -> None."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Set value only in L2
        await l2.set("key1", "value1")

        # Get should find in L2 and promote to L1
        value = await hybrid.get("key1")
        assert value == "value1"

        # Now should be in L1
        assert l1.get("key1") == "value1"

    def test_read_through_pattern_sync(self):
        """Test synchronous read-through pattern."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Set value only in L2
        l2.set_sync("key1", "value1")

        # Get should find in L2 and promote to L1
        value = hybrid.get_sync("key1")
        assert value == "value1"

        # Now should be in L1
        assert l1.get("key1") == "value1"

    @pytest.mark.asyncio
    async def test_l1_cache_hit(self):
        """Test L1 cache hit (fastest path)."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Set value in L1
        l1.set("key1", "value1")

        # Get should hit L1 directly
        value = await hybrid.get("key1")
        assert value == "value1"

        # L2 should not have been consulted
        assert "key1" not in l2._data

    @pytest.mark.asyncio
    async def test_cache_miss(self):
        """Test complete cache miss."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Get non-existent key
        value = await hybrid.get("nonexistent")
        assert value is None

    @pytest.mark.asyncio
    async def test_set_both_caches(self):
        """Test setting value in both L1 and L2."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        await hybrid.set("key1", "value1")

        # Should be in both caches
        assert l1.get("key1") == "value1"
        assert l2._data["key1"] == "value1"

    def test_set_both_caches_sync(self):
        """Test synchronous set in both caches."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        hybrid.set_sync("key1", "value1")

        # Should be in both caches
        assert l1.get("key1") == "value1"
        assert l2._data["key1"] == "value1"

    @pytest.mark.asyncio
    async def test_delete_both_caches(self):
        """Test deleting from both caches."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Set in both
        await hybrid.set("key1", "value1")

        # Delete
        await hybrid.delete("key1")

        # Should be gone from both
        assert l1.get("key1") is None
        assert "key1" not in l2._data

    def test_delete_both_caches_sync(self):
        """Test synchronous delete from both caches."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Set in both
        hybrid.set_sync("key1", "value1")

        # Delete
        hybrid.delete_sync("key1")

        # Should be gone from both
        assert l1.get("key1") is None
        assert "key1" not in l2._data

    @pytest.mark.asyncio
    async def test_l2_failure_handling(self):
        """Test graceful handling of L2 failures."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Configure L2 to fail
        l2.set_fail_operations(True)

        # Set should still work (L1 succeeds)
        await hybrid.set("key1", "value1")
        assert l1.get("key1") == "value1"

        # Get should work from L1
        value = await hybrid.get("key1")
        assert value == "value1"

        # L2 should be marked unavailable
        assert not hybrid.is_l2_available()

    def test_l2_disabled(self):
        """Test hybrid cache with L2 disabled."""
        hybrid, l1, l2 = self.create_hybrid_cache(l2_enabled=False)

        # Operations should only use L1
        hybrid.set_sync("key1", "value1")
        assert l1.get("key1") == "value1"
        assert "key1" not in l2._data

        value = hybrid.get_sync("key1")
        assert value == "value1"

    def test_l2_health_recovery(self):
        """Test L2 health recovery detection."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Make L2 fail and become unavailable
        l2.set_fail_operations(True)
        hybrid.set_sync("key1", "value1")  # This will mark L2 as unavailable

        # Manually set last check to trigger health check
        hybrid._last_l2_check = time.time() - 35.0  # 35 seconds ago

        # Make L2 available again
        l2.set_fail_operations(False)
        l2.set_available(True)

        # This should trigger health check and recover L2
        hybrid.get_sync("test")

        # L2 should be available again (after health check)
        assert hybrid.is_l2_available()

    def test_pattern_invalidation(self):
        """Test pattern-based cache invalidation."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Set multiple keys in L1
        l1.set("topic:A:health", "health_a")
        l1.set("topic:B:health", "health_b")
        l1.set("other:key", "other_value")

        # Invalidate topic pattern
        hybrid.invalidate_pattern("topic:*")

        # Topic keys should be gone, other key should remain
        assert l1.get("topic:A:health") is None
        assert l1.get("topic:B:health") is None
        assert l1.get("other:key") == "other_value"

    def test_combined_stats(self):
        """Test combined statistics from both cache levels."""
        hybrid, l1, l2 = self.create_hybrid_cache()

        # Perform some operations
        hybrid.set_sync("key1", "value1")
        hybrid.get_sync("key1")  # L1 hit

        l2.set_sync("key2", "value2")
        hybrid.get_sync("key2")  # L2 hit, promoted to L1

        stats = hybrid.get_combined_stats()

        assert "l1" in stats
        assert "l2" in stats
        assert stats["l1"].hits_l1 >= 1
        assert stats["l1"].sets >= 1

    def test_l2_none(self):
        """Test hybrid cache with no L2 cache."""
        config = CacheConfig(l2_enabled=False)
        l1_cache = DefaultL1Cache(config)
        hybrid_cache = DefaultHybridCache(l1_cache, None, config)

        # Should work with L1 only
        hybrid_cache.set_sync("key1", "value1")
        assert hybrid_cache.get_sync("key1") == "value1"

        # L2 should not be available
        assert not hybrid_cache.is_l2_available()


class TestCacheConfig:
    """Test CacheConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = CacheConfig()

        assert config.l1_max_size == 1000
        assert config.l1_default_ttl_seconds == 300.0
        assert config.l2_enabled is True
        assert config.l2_default_ttl_seconds == 900.0
        assert config.stats_collection_enabled is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = CacheConfig(
            l1_max_size=500,
            l1_default_ttl_seconds=600.0,
            l2_enabled=False,
            stats_collection_enabled=False,
        )

        assert config.l1_max_size == 500
        assert config.l1_default_ttl_seconds == 600.0
        assert config.l2_enabled is False
        assert config.stats_collection_enabled is False

    def test_config_validation(self):
        """Test configuration validation."""
        # Test invalid max_size
        with pytest.raises(ValueError, match="l1_max_size must be positive"):
            CacheConfig(l1_max_size=0)

        # Test invalid TTL
        with pytest.raises(ValueError, match="l1_default_ttl_seconds must be positive"):
            CacheConfig(l1_default_ttl_seconds=-1.0)


class TestCacheExceptions:
    """Test cache exception classes."""

    def test_cache_unavailable_error(self):
        """Test CacheUnavailableError exception."""
        error = CacheUnavailableError("Cache backend unavailable")
        assert isinstance(error, CacheUnavailableError)
        assert str(error) == "Cache backend unavailable"

    def test_cache_timeout_error(self):
        """Test CacheTimeoutError exception."""
        error = CacheTimeoutError("Operation timed out")
        assert isinstance(error, CacheTimeoutError)
        assert str(error) == "Operation timed out"


class TestCacheIntegration:
    """Integration tests for cache components."""

    @pytest.mark.asyncio
    async def test_end_to_end_workflow(self):
        """Test complete caching workflow."""
        config = CacheConfig(
            l1_max_size=5, l1_default_ttl_seconds=60.0, l2_default_ttl_seconds=300.0
        )

        l1_cache = DefaultL1Cache(config)
        l2_cache = MockL2Cache()
        hybrid_cache = DefaultHybridCache(l1_cache, l2_cache, config)

        # 1. Cache miss
        value = await hybrid_cache.get("key1")
        assert value is None

        # 2. Set value
        await hybrid_cache.set("key1", "value1")

        # 3. L1 cache hit
        value = await hybrid_cache.get("key1")
        assert value == "value1"

        # 4. Clear L1, test L2 hit and promotion
        l1_cache.clear()
        value = await hybrid_cache.get("key1")
        assert value == "value1"
        assert l1_cache.get("key1") == "value1"  # Promoted back to L1

        # 5. Pattern invalidation (L1 only)
        await hybrid_cache.set("topic:A", "data_a")
        await hybrid_cache.set("topic:B", "data_b")
        await hybrid_cache.set("other", "data_other")

        # Clear L2 to test L1-only invalidation
        l2_cache._data.clear()

        hybrid_cache.invalidate_pattern("topic:*")

        # These should be gone from L1 and not in L2
        assert await hybrid_cache.get("topic:A") is None
        assert await hybrid_cache.get("topic:B") is None
        # This should still be available (was not invalidated and still in L2)
        assert l1_cache.get("other") == "data_other"

    def test_concurrent_access(self):
        """Test concurrent access to cache."""
        import threading

        cache = DefaultL1Cache(CacheConfig(l1_max_size=100))
        results = []
        errors = []

        def worker(thread_id: int):
            try:
                for i in range(50):
                    key = f"thread_{thread_id}_key_{i}"
                    value = f"thread_{thread_id}_value_{i}"

                    cache.set(key, value)
                    retrieved = cache.get(key)
                    results.append((key, retrieved))

                    if i % 10 == 0:
                        cache.delete(key)
            except Exception as e:
                errors.append(e)

        # Run multiple threads
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        assert len(results) == 250  # 5 threads * 50 operations each

        # Verify no corruption
        for key, value in results:
            if value is not None:  # Some might be deleted
                expected_value = key.replace("key", "value")
                assert value == expected_value
