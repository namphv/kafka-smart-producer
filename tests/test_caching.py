"""
Comprehensive tests for the caching module.

This module provides complete test coverage for all caching components:
- CacheEntry: TTL-based cache entry container
- CacheConfig: Configuration dataclass with validation
- Cache Exceptions: Error handling classes
- CacheFactory: Factory methods for cache creation
- DefaultLocalCache: LRU cache implementation
"""

import threading
import time
from unittest.mock import patch

import pytest

from kafka_smart_producer.caching import (
    CacheConfig,
    CacheEntry,
    CacheFactory,
    CacheTimeoutError,
    CacheUnavailableError,
    DefaultLocalCache,
)
from kafka_smart_producer.exceptions import CacheError


class TestCacheEntry:
    """Test CacheEntry functionality."""

    def test_basic_entry_creation(self):
        """Test basic cache entry creation."""
        entry = CacheEntry("test_value")

        assert entry.value == "test_value"
        assert entry.ttl_seconds is None
        assert isinstance(entry.created_at, float)
        assert entry.created_at <= time.time()

    def test_entry_with_ttl(self):
        """Test cache entry with TTL."""
        entry = CacheEntry("test_value", ttl_seconds=10.0)

        assert entry.value == "test_value"
        assert entry.ttl_seconds == 10.0
        assert isinstance(entry.created_at, float)

    def test_entry_no_expiration_when_no_ttl(self):
        """Test that entries without TTL never expire."""
        entry = CacheEntry("eternal_value")

        # Should never expire regardless of time
        assert not entry.is_expired()

        # Even with mocked future time
        with patch("time.time", return_value=time.time() + 1000000):
            assert not entry.is_expired()

    def test_entry_not_expired_within_ttl(self):
        """Test that entries are not expired within TTL period."""
        entry = CacheEntry("fresh_value", ttl_seconds=10.0)

        # Should not be expired immediately
        assert not entry.is_expired()

        # Should not be expired just before TTL
        with patch("time.time", return_value=entry.created_at + 9.9):
            assert not entry.is_expired()

    def test_entry_expired_after_ttl(self):
        """Test that entries expire after TTL period."""
        entry = CacheEntry("stale_value", ttl_seconds=1.0)

        # Should be expired after TTL
        with patch("time.time", return_value=entry.created_at + 1.1):
            assert entry.is_expired()

        # Should be expired well after TTL
        with patch("time.time", return_value=entry.created_at + 100):
            assert entry.is_expired()

    def test_entry_expired_exactly_at_ttl(self):
        """Test expiration behavior exactly at TTL boundary."""
        entry = CacheEntry("boundary_value", ttl_seconds=5.0)

        # Should NOT be expired exactly at TTL boundary (> not >=)
        with patch("time.time", return_value=entry.created_at + 5.0):
            assert not entry.is_expired()

        # Should be expired just after TTL boundary
        with patch("time.time", return_value=entry.created_at + 5.0001):
            assert entry.is_expired()

    def test_zero_ttl_immediate_expiration(self):
        """Test that zero TTL means immediate expiration."""
        entry = CacheEntry("immediate_expire", ttl_seconds=0.0)

        # Should be expired immediately
        assert entry.is_expired()

    def test_negative_ttl_immediate_expiration(self):
        """Test that negative TTL means immediate expiration."""
        entry = CacheEntry("negative_expire", ttl_seconds=-1.0)

        # Should be expired immediately
        assert entry.is_expired()

    def test_fractional_ttl(self):
        """Test entries with fractional TTL values."""
        entry = CacheEntry("fractional_value", ttl_seconds=0.5)

        # Should not be expired before fractional TTL
        with patch("time.time", return_value=entry.created_at + 0.3):
            assert not entry.is_expired()

        # Should be expired after fractional TTL
        with patch("time.time", return_value=entry.created_at + 0.7):
            assert entry.is_expired()

    def test_different_value_types(self):
        """Test cache entries with different value types."""
        # Test different value types
        test_cases = [
            "string_value",
            42,
            3.14,
            [1, 2, 3],
            {"key": "value"},
            None,
            True,
        ]

        for test_value in test_cases:
            entry = CacheEntry(test_value, ttl_seconds=10.0)
            assert entry.value == test_value
            assert not entry.is_expired()


class TestCacheConfig:
    """Test CacheConfig functionality."""

    def test_default_config(self):
        """Test default configuration values."""
        config = CacheConfig()

        # Local cache defaults
        assert config.local_max_size == 1000
        assert config.local_default_ttl_seconds == 300.0

        # Remote cache defaults
        assert config.remote_enabled is False
        assert config.remote_default_ttl_seconds == 900.0

        # Redis connection defaults
        assert config.redis_host == "localhost"
        assert config.redis_port == 6379
        assert config.redis_db == 0
        assert config.redis_password is None

        # Redis SSL defaults
        assert config.redis_ssl_enabled is False
        assert config.redis_ssl_cert_reqs == "required"
        assert config.redis_ssl_ca_certs is None
        assert config.redis_ssl_certfile is None
        assert config.redis_ssl_keyfile is None

    def test_custom_config(self):
        """Test custom configuration values."""
        config = CacheConfig(
            local_max_size=2000,
            local_default_ttl_seconds=600.0,
            remote_enabled=True,
            remote_default_ttl_seconds=1800.0,
            redis_host="redis.example.com",
            redis_port=6380,
            redis_db=1,
            redis_password="test-password",  # noqa: S106
        )

        assert config.local_max_size == 2000
        assert config.local_default_ttl_seconds == 600.0
        assert config.remote_enabled is True
        assert config.remote_default_ttl_seconds == 1800.0
        assert config.redis_host == "redis.example.com"
        assert config.redis_port == 6380
        assert config.redis_db == 1
        assert config.redis_password == "test-password"  # noqa: S105

    def test_validation_local_max_size_positive(self):
        """Test that local_max_size must be positive."""
        # Valid positive values
        CacheConfig(local_max_size=1)
        CacheConfig(local_max_size=10000)

        # Invalid zero value
        with pytest.raises(ValueError, match="local_max_size must be positive"):
            CacheConfig(local_max_size=0)

        # Invalid negative value
        with pytest.raises(ValueError, match="local_max_size must be positive"):
            CacheConfig(local_max_size=-1)

    def test_validation_redis_port_range(self):
        """Test that redis_port must be in valid range."""
        # Valid port values
        CacheConfig(redis_port=1)
        CacheConfig(redis_port=6379)
        CacheConfig(redis_port=65535)

        # Invalid zero port
        with pytest.raises(ValueError, match="redis_port must be between 1 and 65535"):
            CacheConfig(redis_port=0)

        # Invalid negative port
        with pytest.raises(ValueError, match="redis_port must be between 1 and 65535"):
            CacheConfig(redis_port=-1)

        # Invalid port too high
        with pytest.raises(ValueError, match="redis_port must be between 1 and 65535"):
            CacheConfig(redis_port=65536)

    def test_frozen_dataclass(self):
        """Test that CacheConfig is frozen (immutable)."""
        config = CacheConfig()

        # Should not be able to modify after creation
        with pytest.raises(Exception):  # noqa: B017  # noqa: B017
            config.local_max_size = 2000


class TestCacheExceptions:
    """Test cache exception classes."""

    def test_cache_unavailable_error_inheritance(self):
        """Test CacheUnavailableError inherits from CacheError."""
        error = CacheUnavailableError("Cache is down")

        assert isinstance(error, CacheError)
        assert isinstance(error, Exception)
        assert str(error) == "Cache is down"

    def test_cache_timeout_error_inheritance(self):
        """Test CacheTimeoutError inherits from CacheError."""
        error = CacheTimeoutError("Operation timed out")

        assert isinstance(error, CacheError)
        assert isinstance(error, Exception)
        assert str(error) == "Operation timed out"

    def test_exception_catching_hierarchy(self):
        """Test that cache exceptions can be caught by parent classes."""

        def raise_cache_unavailable():
            raise CacheUnavailableError("Service unavailable")

        def raise_cache_timeout():
            raise CacheTimeoutError("Request timeout")

        # Should be catchable by CacheError
        with pytest.raises(CacheError):
            raise_cache_unavailable()

        with pytest.raises(CacheError):
            raise_cache_timeout()

        # Should be catchable by Exception
        with pytest.raises(Exception):  # noqa: B017
            raise_cache_unavailable()

        with pytest.raises(Exception):  # noqa: B017
            raise_cache_timeout()

    def test_exception_distinguishable(self):
        """Test that different cache exceptions can be distinguished."""

        def raise_unavailable():
            raise CacheUnavailableError("Redis down")

        def raise_timeout():
            raise CacheTimeoutError("Redis timeout")

        # Should be able to catch specifically
        with pytest.raises(CacheUnavailableError):
            raise_unavailable()

        with pytest.raises(CacheTimeoutError):
            raise_timeout()


class TestCacheFactory:
    """Test CacheFactory functionality."""

    def test_create_local_cache(self):
        """Test creating standalone local cache."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
        }

        cache = CacheFactory.create_local_cache(config)

        assert isinstance(cache, DefaultLocalCache)
        assert cache._config.local_max_size == 100
        assert cache._config.local_default_ttl_seconds == 60.0

        # Test basic operations
        cache.set("test_key", 42)
        assert cache.get("test_key") == 42

    def test_create_remote_cache_missing_config(self):
        """Test creating remote cache with missing configuration."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
            # Missing redis_host, redis_port, etc.
        }

        # Should return None when required config is missing
        cache = CacheFactory.create_remote_cache(config)
        assert cache is None

    def test_create_hybrid_cache_missing_redis(self):
        """Test that hybrid cache requires Redis configuration."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
            "redis_ttl_seconds": 300,
            # Missing redis_host, redis_port, etc.
        }

        # Should raise RuntimeError when Redis is required but not available
        try:
            CacheFactory.create_hybrid_cache(config)
            raise AssertionError("Expected RuntimeError")
        except RuntimeError as e:
            assert "Failed to create remote cache" in str(e)

    def test_create_hybrid_cache_redis_disabled(self):
        """Test that hybrid cache fails when Redis is explicitly disabled."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
            "redis_ttl_seconds": 300,
        }

        # Should raise ValueError when Redis is disabled for hybrid cache
        try:
            CacheFactory.create_hybrid_cache(config, enable_redis=False)
            raise AssertionError("Expected ValueError")
        except ValueError as e:
            assert "Hybrid cache requires Redis to be enabled" in str(e)


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
