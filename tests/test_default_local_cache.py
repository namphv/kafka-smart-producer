"""
Comprehensive test suite for DefaultLocalCache crucial logic.

This test suite verifies the refactored DefaultLocalCache implementation that uses
cachetools.LRUCache as storage engine while preserving custom features.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from kafka_smart_producer.caching import CacheConfig, DefaultLocalCache


class TestDefaultLocalCache:
    """Test suite for DefaultLocalCache core functionality."""

    def test_basic_set_and_get(self):
        """Test basic set and get operations."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Test setting and getting a value
        cache.set("test_key", "test_value")
        assert cache.get("test_key") == "test_value"

    def test_get_nonexistent_key(self):
        """Test getting a key that doesn't exist returns None."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        assert cache.get("nonexistent") is None

    def test_set_with_custom_ttl(self):
        """Test setting a value with custom TTL."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Set with custom TTL
        cache.set("ttl_key", "ttl_value", ttl_seconds=30.0)
        assert cache.get("ttl_key") == "ttl_value"

    def test_ttl_expiration_on_access(self):
        """Test that expired keys are removed when accessed and return None."""
        config = CacheConfig(
            local_max_size=100, local_default_ttl_seconds=0.1
        )  # 100ms TTL
        cache = DefaultLocalCache(config)

        # Set a value with short TTL
        cache.set("expire_key", "expire_value")
        assert cache.get("expire_key") == "expire_value"

        # Wait for expiration
        time.sleep(0.15)

        # Key should be expired and removed
        assert cache.get("expire_key") is None

        # Key should no longer be in cache
        assert cache.size() == 0

    def test_expired_keys_remain_until_accessed(self):
        """Test that expired keys stay in cache until accessed (lazy expiration)."""
        config = CacheConfig(
            local_max_size=100, local_default_ttl_seconds=0.1
        )  # 100ms TTL
        cache = DefaultLocalCache(config)

        # Set a value with short TTL
        cache.set("lazy_expire", "value")
        initial_size = cache.size()
        assert initial_size == 1

        # Wait for expiration but don't access the key
        time.sleep(0.15)

        # Size should still show the expired entry until accessed
        assert cache.size() == initial_size

        # Accessing the key should remove it
        assert cache.get("lazy_expire") is None
        assert cache.size() == 0

    def test_update_existing_key(self):
        """Test updating an existing key moves it to most recent position."""
        config = CacheConfig(local_max_size=3, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Fill cache
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # Update key1 (should move to most recent)
        cache.set("key1", "updated_value1")

        # Add another key to trigger eviction
        cache.set("key4", "value4")

        # key1 should still be present (most recent), key2 should be evicted (oldest)
        assert cache.get("key1") == "updated_value1"
        assert cache.get("key2") is None  # Should be evicted
        assert cache.get("key3") == "value3"
        assert cache.get("key4") == "value4"

    def test_lru_eviction_at_capacity(self):
        """Test LRU eviction when cache reaches max capacity."""
        config = CacheConfig(local_max_size=3, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Fill cache to capacity
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        assert cache.size() == 3

        # Add one more - should evict LRU (key1)
        cache.set("key4", "value4")
        assert cache.size() == 3

        # key1 should be evicted, others should remain
        assert cache.get("key1") is None
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"
        assert cache.get("key4") == "value4"

    def test_lru_promotion_on_access(self):
        """Test that accessing a key promotes it to most recent."""
        config = CacheConfig(local_max_size=3, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Fill cache
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # Access key1 to promote it
        assert cache.get("key1") == "value1"

        # Add another key - key2 should be evicted (now oldest)
        cache.set("key4", "value4")

        # key1 should still be present, key2 should be evicted
        assert cache.get("key1") == "value1"
        assert cache.get("key2") is None  # Should be evicted
        assert cache.get("key3") == "value3"
        assert cache.get("key4") == "value4"

    def test_delete_key(self):
        """Test deleting a specific key."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Set and delete
        cache.set("delete_me", "value")
        assert cache.get("delete_me") == "value"

        cache.delete("delete_me")
        assert cache.get("delete_me") is None
        assert cache.size() == 0

    def test_delete_nonexistent_key(self):
        """Test deleting a key that doesn't exist (should not raise error)."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Should not raise an exception
        cache.delete("nonexistent")

    def test_clear_cache(self):
        """Test clearing all entries from cache."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Add some entries
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")
        assert cache.size() == 3

        # Clear cache
        cache.clear()
        assert cache.size() == 0
        assert cache.get("key1") is None
        assert cache.get("key2") is None
        assert cache.get("key3") is None

    def test_size_tracking(self):
        """Test that size tracking is accurate."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        assert cache.size() == 0

        cache.set("key1", "value1")
        assert cache.size() == 1

        cache.set("key2", "value2")
        assert cache.size() == 2

        cache.delete("key1")
        assert cache.size() == 1

        cache.clear()
        assert cache.size() == 0

    def test_thread_safety_concurrent_access(self):
        """Test that cache operations are thread-safe."""
        config = CacheConfig(local_max_size=1000, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        def worker(worker_id: int, operations: int) -> List[str]:
            """Worker function that performs cache operations."""
            results = []
            for i in range(operations):
                key = f"worker_{worker_id}_key_{i}"
                value = f"worker_{worker_id}_value_{i}"

                # Set the value
                cache.set(key, value)

                # Get the value
                retrieved = cache.get(key)
                if retrieved == value:
                    results.append(f"success_{worker_id}_{i}")
                else:
                    results.append(f"failure_{worker_id}_{i}")

            return results

        # Run multiple workers concurrently
        num_workers = 10
        operations_per_worker = 100

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [
                executor.submit(worker, i, operations_per_worker)
                for i in range(num_workers)
            ]

            # Collect all results
            all_results = []
            for future in as_completed(futures):
                all_results.extend(future.result())

        # All operations should succeed
        successes = [r for r in all_results if r.startswith("success")]
        failures = [r for r in all_results if r.startswith("failure")]

        assert len(successes) == num_workers * operations_per_worker
        assert len(failures) == 0

    def test_thread_safety_concurrent_eviction(self):
        """Test thread safety during LRU eviction."""
        config = CacheConfig(local_max_size=10, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        def eviction_worker(worker_id: int) -> int:
            """Worker that adds entries to trigger eviction."""
            successful_operations = 0
            for i in range(50):  # More than cache capacity to force evictions
                try:
                    key = f"evict_worker_{worker_id}_key_{i}"
                    value = f"evict_worker_{worker_id}_value_{i}"
                    cache.set(key, value)

                    # Try to retrieve immediately
                    retrieved = cache.get(key)
                    if retrieved is not None:  # May be None due to eviction
                        successful_operations += 1
                except Exception as e:
                    # Log unexpected exceptions in thread-safe implementation
                    print(f"Unexpected exception in cache operation: {e}")

            return successful_operations

        # Run workers that will cause evictions
        num_workers = 5

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = [executor.submit(eviction_worker, i) for i in range(num_workers)]

            total_successful = sum(future.result() for future in as_completed(futures))

        # Cache should be at capacity
        assert cache.size() <= config.local_max_size

        # Should have had some successful operations despite evictions
        assert total_successful > 0

    def test_zero_ttl_handling(self):
        """Test handling of zero TTL (immediate expiration)."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Set with zero TTL
        cache.set("zero_ttl", "value", ttl_seconds=0.0)

        # Should be immediately expired when accessed
        assert cache.get("zero_ttl") is None

    def test_very_large_cache_performance(self):
        """Test performance with large number of entries."""
        config = CacheConfig(local_max_size=10000, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        start_time = time.time()

        # Add many entries
        for i in range(5000):
            cache.set(f"perf_key_{i}", f"perf_value_{i}")

        # Access entries to test LRU performance
        for i in range(0, 5000, 100):  # Access every 100th entry
            assert cache.get(f"perf_key_{i}") == f"perf_value_{i}"

        end_time = time.time()

        # Should complete reasonably quickly (under 1 second)
        assert end_time - start_time < 1.0
        assert cache.size() == 5000

    def test_compatibility_interface_methods(self):
        """Test unified interface compatibility methods."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Test get_sync (should be same as get)
        cache.set("sync_key", "sync_value")
        assert cache.get_sync("sync_key") == "sync_value"
        assert cache.get_sync("nonexistent") is None

        # Test delete_sync (should be same as delete)
        cache.delete_sync("sync_key")
        assert cache.get("sync_key") is None

        # Test set_with_ordered_ttl
        cache.set_with_ordered_ttl("ordered_key", "ordered_value")
        assert cache.get("ordered_key") == "ordered_value"

    def test_memory_management_with_mixed_operations(self):
        """Test memory management with mixed set/get/delete/eviction operations."""
        config = CacheConfig(
            local_max_size=5, local_default_ttl_seconds=0.2
        )  # Short TTL
        cache = DefaultLocalCache(config)

        # Fill cache
        for i in range(5):
            cache.set(f"mem_key_{i}", f"mem_value_{i}")

        assert cache.size() == 5

        # Add more to trigger evictions
        cache.set("overflow1", "value1")
        cache.set("overflow2", "value2")

        # Should still be at capacity
        assert cache.size() == 5

        # Wait for some entries to expire
        time.sleep(0.25)

        # Access to trigger cleanup of expired entries
        remaining_keys = []
        for i in range(10):  # Try various keys
            key = f"test_remaining_{i}"
            cache.set(key, f"value_{i}")
            if cache.get(key) is not None:
                remaining_keys.append(key)

        # Should maintain capacity limits
        assert cache.size() <= 5

    def test_edge_case_empty_cache_operations(self):
        """Test operations on empty cache don't cause issues."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        cache = DefaultLocalCache(config)

        # Operations on empty cache
        assert cache.get("anything") is None
        cache.delete("anything")  # Should not raise
        # Pattern operations removed - just test clear
        cache.clear()  # Should not raise
        cache.clear()  # Should not raise
        assert cache.size() == 0

        # Cache should handle empty state correctly
        assert cache.size() == 0
