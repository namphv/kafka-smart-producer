"""
Integration test for SmartProducer with hybrid cache.
"""

from kafka_smart_producer.caching import DefaultHybridCache
from kafka_smart_producer.producer import SmartProducer


class TestSmartProducerHybridCache:
    """Test SmartProducer with hybrid cache implementation."""

    def test_producer_initialization_with_hybrid_cache(self):
        """Test SmartProducer initializes with hybrid cache correctly."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        # Create producer without connecting to Kafka
        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        # Initialize components
        producer._health_manager = None
        producer._key_cache = producer._create_secure_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]

        # Verify hybrid cache is created
        assert isinstance(producer._key_cache, DefaultHybridCache)
        assert producer._key_cache._local is not None
        assert producer._key_cache._remote is None  # Redis disabled

        # Test partition selection works
        topic = "test-topic"
        key = b"test-key"

        # Should return None (no health manager)
        result = producer._select_partition_with_ordering(topic, key, ordered=False)
        assert result is None

    def test_cache_stats_with_hybrid_cache(self):
        """Test cache stats work with hybrid cache."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 50,
            "smart.cache.ttl.ms": 30000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        producer._health_manager = None
        producer._key_cache = producer._create_secure_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]

        # Test cache stats
        stats = producer.get_cache_stats()

        assert stats["enabled"] is True
        assert "local_cache" in stats
        assert "remote_cache" in stats
        assert stats["remote_cache"]["enabled"] is False
        assert stats["cache_ttl_ms"] == 30000

    def test_cache_operations_with_hybrid_cache(self):
        """Test cache operations with hybrid cache."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        producer._health_manager = None
        producer._key_cache = producer._create_secure_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]

        # Test cache operations
        cache_key = (
            "kafka_smart_producer:test-topic:74657374"  # Updated format with prefix
        )

        # Set value
        producer._key_cache.set_sync(cache_key, 2)

        # Get value
        result = producer._key_cache.get_sync(cache_key)
        assert result == 2

        # Clear cache
        producer.clear_cache()

        # Verify cleared
        result = producer._key_cache.get_sync(cache_key)
        assert result is None

    def test_select_partition_with_hybrid_cache(self):
        """Test _select_partition method with hybrid cache."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        producer._health_manager = None
        producer._key_cache = producer._create_secure_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]

        # Test partition selection
        topic = "test-topic"
        key = b"test-key"

        # With no health manager, should return None for both ordered and unordered
        result = producer._select_partition_with_ordering(topic, key, ordered=False)
        assert result is None

        result = producer._select_partition_with_ordering(topic, key, ordered=True)
        assert result is None

        # Test ordered messages with cache
        expected_cache_key = f"kafka_smart_producer:{topic}:{key.hex()}"

        # Manually set cache value
        producer._key_cache.set_sync(expected_cache_key, 1)

        # Unordered messages should NOT use cache (simplified logic)
        result = producer._select_partition_with_ordering(topic, key, ordered=False)
        assert result is None

        # Ordered messages should use cache
        result = producer._select_partition_with_ordering(topic, key, ordered=True)
        assert result == 1

    def test_memory_bounds_with_hybrid_cache(self):
        """Test that hybrid cache respects memory bounds."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 10,  # Small cache
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.enabled": False,
        }

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        producer._health_manager = None
        producer._key_cache = producer._create_secure_hybrid_cache(
            smart_config, enable_redis=False
        )
        producer._cache_ttl_ms = smart_config["cache_ttl_ms"]
        producer._health_check_enabled = smart_config["health_check_enabled"]
        producer._smart_enabled = smart_config["enabled"]

        # Fill cache beyond max size
        for i in range(20):
            cache_key = f"topic:{i:04x}"
            producer._key_cache.set_sync(cache_key, i)

        # Check local cache size is bounded
        local_size = producer._key_cache._local.size()
        assert local_size <= 10  # Should not exceed max size

        # Verify oldest entries were evicted (LRU behavior)
        # The first few entries should be evicted
        first_key = "topic:0000"
        assert producer._key_cache.get_sync(first_key) is None

        # More recent entries should still be there
        recent_key = "topic:0013"  # 19 in hex
        assert producer._key_cache.get_sync(recent_key) == 19
