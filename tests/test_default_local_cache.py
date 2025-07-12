"""
Tests for DefaultLocalCache implementation.
"""

import time

from kafka_smart_producer.caching import CacheConfig, DefaultLocalCache


class TestDefaultLocalCache:
    """Test DefaultLocalCache functionality."""

    def test_basic_operations(self):
        """Test basic cache operations."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=300.0)
        cache = DefaultLocalCache(config)

        # Test set and get
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        # Test get non-existent key
        assert cache.get("nonexistent") is None

        # Test delete
        cache.delete("key1")
        assert cache.get("key1") is None

        # Test delete non-existent key (should not error)
        cache.delete("nonexistent")

    def test_ttl_expiration(self):
        """Test TTL expiration functionality."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=300.0)
        cache = DefaultLocalCache(config)

        # Set with short TTL
        cache.set("temp_key", "temp_value", 0.1)  # 100ms TTL
        assert cache.get("temp_key") == "temp_value"

        # Wait for expiration
        time.sleep(0.15)
        assert cache.get("temp_key") is None

    def test_default_ttl(self):
        """Test default TTL from config."""
        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=0.1)
        cache = DefaultLocalCache(config)

        # Set without explicit TTL (should use default)
        cache.set("key1", "value1")
        assert cache.get("key1") == "value1"

        # Wait for expiration
        time.sleep(0.15)
        assert cache.get("key1") is None

    def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        config = CacheConfig(local_max_size=3, local_default_ttl_seconds=300.0)
        cache = DefaultLocalCache(config)

        # Fill cache to capacity
        cache.set("key1", "value1")
        cache.set("key2", "value2")
        cache.set("key3", "value3")

        # All should exist
        assert cache.get("key1") == "value1"
        assert cache.get("key2") == "value2"
        assert cache.get("key3") == "value3"

        # Access key1 to make it recently used
        cache.get("key1")

        # Add new key, should evict key2 (least recently used)
        cache.set("key4", "value4")

        assert cache.get("key1") == "value1"  # Still exists (recently used)
        assert cache.get("key2") is None  # Evicted
        assert cache.get("key3") == "value3"  # Still exists
        assert cache.get("key4") == "value4"  # New key

    def test_thread_safety(self):
        """Test basic thread safety."""
        import threading

        config = CacheConfig(local_max_size=100, local_default_ttl_seconds=300.0)
        cache = DefaultLocalCache(config)

        def worker(thread_id):
            for i in range(10):
                key = f"thread{thread_id}_key{i}"
                value = f"thread{thread_id}_value{i}"
                cache.set(key, value)
                assert cache.get(key) == value

        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # No assertion needed - test passes if no exceptions occurred
