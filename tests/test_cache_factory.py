"""
Tests for CacheFactory functionality.
"""

from kafka_smart_producer.caching import CacheFactory, DefaultLocalCache


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
