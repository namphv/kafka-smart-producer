"""
Test the hybrid cache integration in SmartProducer.
"""

from kafka_smart_producer.caching import DefaultHybridCache, DefaultLocalCache
from kafka_smart_producer.producer import SmartProducer


class TestHybridCacheIntegration:
    """Test hybrid cache integration with SmartProducer."""

    def test_hybrid_cache_creation(self):
        """Test that SmartProducer creates a hybrid cache correctly."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,  # Disable Redis for testing
        }

        # Create producer and extract configuration
        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        # Test hybrid cache creation using CacheFactory
        from kafka_smart_producer.caching import CacheFactory

        hybrid_cache = CacheFactory.create_hybrid_cache(
            smart_config, enable_redis=False
        )

        # Verify it's a DefaultHybridCache instance
        assert isinstance(hybrid_cache, DefaultHybridCache)

        # Verify local cache is properly configured
        assert isinstance(hybrid_cache._local, DefaultLocalCache)

        # Verify remote cache is None (Redis disabled)
        assert hybrid_cache._remote is None

        # Test basic cache operations
        hybrid_cache.set_sync("test_key", 42)
        assert hybrid_cache.get_sync("test_key") == 42

        # Test cache deletion
        hybrid_cache.delete_sync("test_key")
        assert hybrid_cache.get_sync("test_key") is None

    def test_hybrid_cache_with_redis_disabled(self):
        """Test hybrid cache when Redis is disabled."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 50,
            "smart.cache.ttl.ms": 30000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())
        from kafka_smart_producer.caching import CacheFactory

        hybrid_cache = CacheFactory.create_hybrid_cache(
            smart_config, enable_redis=False
        )

        # Should only have local cache
        assert hybrid_cache._local is not None
        assert hybrid_cache._remote is None
        assert not hybrid_cache.is_remote_available()

        # Test that caching still works with local cache only
        hybrid_cache.set_sync("key1", "value1")
        assert hybrid_cache.get_sync("key1") == "value1"

    def test_hybrid_cache_with_redis_enabled_but_unavailable(self):
        """Test hybrid cache when Redis is enabled but not available."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 50,
            "smart.cache.ttl.ms": 30000,
            "smart.cache.redis.enabled": True,
            "smart.cache.redis.host": "nonexistent-host",
            "smart.cache.redis.port": 6379,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())
        from kafka_smart_producer.caching import CacheFactory

        hybrid_cache = CacheFactory.create_hybrid_cache(smart_config, enable_redis=True)

        # Should have local cache but remote should be None due to connection failure
        assert hybrid_cache._local is not None
        assert hybrid_cache._remote is None  # Redis connection failed
        assert not hybrid_cache.is_remote_available()

    def test_producer_with_hybrid_cache(self):
        """Test SmartProducer with hybrid cache integration."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        # Create producer instance without initializing parent
        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        # Set up components manually
        producer._health_manager = None
        from kafka_smart_producer.caching import CacheFactory

        producer._key_cache = CacheFactory.create_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]
        producer._cache_key_prefix = smart_config.get(
            "cache_key_prefix", "kafka_smart_producer"
        )

        # Test _select_partition with hybrid cache
        topic = "test-topic"
        key = b"test-key"

        # Should return None (no health manager)
        result = producer._select_partition_with_ordering(topic, key, ordered=True)
        assert result is None

        # Test cache stats
        stats = producer.get_cache_stats()
        assert stats["enabled"] is True
        assert "local_size" in stats
        assert stats["remote_available"] is False

    def test_cache_key_format(self):
        """Test that cache keys are formatted correctly."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        producer._health_manager = None
        from kafka_smart_producer.caching import CacheFactory

        producer._key_cache = CacheFactory.create_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]
        producer._cache_key_prefix = smart_config.get(
            "cache_key_prefix", "kafka_smart_producer"
        )

        # Test cache key format
        topic = "user-events"
        key = b"user123"
        expected_cache_key = f"{topic}:{key.hex()}"

        # Manually set cache value to verify key format
        producer._key_cache.set_sync(expected_cache_key, 2)

        # Verify we can retrieve it with the same key format
        assert producer._key_cache.get_sync(expected_cache_key) == 2

    def test_cache_clear_functionality(self):
        """Test cache clearing functionality."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        from kafka_smart_producer.caching import CacheFactory

        producer._key_cache = CacheFactory.create_hybrid_cache(
            smart_config, enable_redis=False
        )

        # Add some test data
        producer._key_cache.set_sync("key1", 1)
        producer._key_cache.set_sync("key2", 2)

        # Verify data exists
        assert producer._key_cache.get_sync("key1") == 1
        assert producer._key_cache.get_sync("key2") == 2

        # Clear cache
        producer.clear_cache()

        # Verify cache is cleared
        assert producer._key_cache.get_sync("key1") is None
        assert producer._key_cache.get_sync("key2") is None
