"""
Tests for the expanded CacheFactory with 3 cache types.
"""

from kafka_smart_producer.caching import (
    CacheFactory,
    DefaultHybridCache,
    DefaultLocalCache,
)


class TestCacheFactory:
    """Test CacheFactory with all three cache types."""

    def test_create_local_cache(self):
        """Test creating standalone local cache."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
            "ordered_message_ttl": 3600.0,
        }

        cache = CacheFactory.create_local_cache(config)

        assert isinstance(cache, DefaultLocalCache)
        assert cache._config.local_max_size == 100
        assert cache._config.local_default_ttl_seconds == 60.0
        assert cache._config.ordered_message_ttl == 3600.0
        assert cache._config.stats_collection_enabled is True

        # Test basic operations
        cache.set("test_key", 42)
        assert cache.get("test_key") == 42
        cache.delete("test_key")
        assert cache.get("test_key") is None

    def test_create_remote_cache_without_redis(self):
        """Test creating remote cache when Redis is not available."""
        config = {
            "redis_host": "nonexistent-host",
            "redis_port": 6379,
            "redis_db": 0,
            "redis_password": None,
            "redis_ssl_enabled": False,
        }

        cache = CacheFactory.create_remote_cache(config)

        # Should return None when Redis connection fails
        assert cache is None

    def test_create_remote_cache_missing_config(self):
        """Test creating remote cache with missing configuration."""
        config = {
            # Missing required redis_host
            "redis_port": 6379,
            "redis_db": 0,
        }

        cache = CacheFactory.create_remote_cache(config)

        # Should return None when required config is missing
        assert cache is None

    def test_create_hybrid_cache_local_only(self):
        """Test creating hybrid cache with local cache only."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
            "redis_ttl_seconds": 300,
            "ordered_message_ttl": 3600.0,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_db": 0,
        }

        cache = CacheFactory.create_hybrid_cache(config, enable_redis=False)

        assert isinstance(cache, DefaultHybridCache)
        assert cache._local is not None
        assert cache._remote is None  # Redis not enabled
        assert not cache.is_remote_available()

        # Test basic operations
        cache.set_sync("test_key", 42)
        assert cache.get_sync("test_key") == 42

    def test_create_hybrid_cache_with_redis_unavailable(self):
        """Test creating hybrid cache when Redis is unavailable."""
        config = {
            "cache_max_size": 100,
            "cache_ttl_ms": 60000,
            "redis_ttl_seconds": 300,
            "ordered_message_ttl": 3600.0,
            "redis_host": "nonexistent-host",
            "redis_port": 6379,
            "redis_db": 0,
        }

        cache = CacheFactory.create_hybrid_cache(config, enable_redis=True)

        assert isinstance(cache, DefaultHybridCache)
        assert cache._local is not None
        assert cache._remote is None  # Redis connection failed
        assert not cache.is_remote_available()

    def test_factory_methods_use_each_other(self):
        """Test that hybrid cache uses local/remote factory methods."""
        config = {
            "cache_max_size": 50,
            "cache_ttl_ms": 30000,
            "redis_ttl_seconds": 300,
            "ordered_message_ttl": 3600.0,
            "redis_host": "localhost",
            "redis_port": 6379,
            "redis_db": 0,
        }

        # Create individual caches
        local_cache = CacheFactory.create_local_cache(config)
        remote_cache = CacheFactory.create_remote_cache(config)
        hybrid_cache = CacheFactory.create_hybrid_cache(config, enable_redis=False)

        # Verify they're the expected types
        assert isinstance(local_cache, DefaultLocalCache)
        assert remote_cache is None  # Redis not available
        assert isinstance(hybrid_cache, DefaultHybridCache)

        # Verify hybrid cache has local component
        assert hybrid_cache._local is not None
        assert isinstance(hybrid_cache._local, DefaultLocalCache)


class TestSmartProducerCacheTypes:
    """Test SmartProducer with different cache types."""

    def test_local_cache_type_config(self):
        """Test SmartProducer with local cache type."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.type": "local",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
        }

        # Create producer without initializing parent
        from kafka_smart_producer.producer import SmartProducer

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        # Verify cache type is configured
        assert smart_config["cache_type"] == "local"

    def test_remote_cache_type_config(self):
        """Test SmartProducer with remote cache type."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.type": "remote",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
            "smart.cache.redis.host": "localhost",
            "smart.cache.redis.port": 6379,
        }

        # Create producer without initializing parent
        from kafka_smart_producer.producer import SmartProducer

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        # Verify cache type is configured
        assert smart_config["cache_type"] == "remote"
        assert smart_config["redis_host"] == "localhost"
        assert smart_config["redis_port"] == 6379

    def test_hybrid_cache_type_config_default(self):
        """Test SmartProducer defaults to hybrid cache type."""
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.cache.max.size": 100,
            "smart.cache.ttl.ms": 60000,
        }

        # Create producer without initializing parent
        from kafka_smart_producer.producer import SmartProducer

        producer = SmartProducer.__new__(SmartProducer)
        smart_config = producer._extract_smart_config(config.copy())

        # Verify default cache type is hybrid
        assert smart_config["cache_type"] == "hybrid"


class TestHealthManagerCacheFactory:
    """Test HealthManager uses CacheFactory."""

    def test_health_manager_uses_cache_factory(self):
        """Test that HealthManager uses CacheFactory for default cache."""
        from kafka_smart_producer.caching import DefaultLocalCache
        from kafka_smart_producer.health import DefaultHealthManager

        # Mock dependencies
        class MockLagCollector:
            def get_lag_data_sync(self, topic):
                return {0: 10, 1: 5}

            def is_healthy(self):
                return True

        class MockHealthCalculator:
            def calculate_scores(self, lag_data, metadata=None):
                return {pid: 1.0 - (lag / 100.0) for pid, lag in lag_data.items()}

        lag_collector = MockLagCollector()
        health_calculator = MockHealthCalculator()

        # Create health manager without providing cache
        health_manager = DefaultHealthManager(lag_collector, health_calculator)

        # Verify it has a cache created via factory
        assert health_manager._cache is not None
        assert isinstance(health_manager._cache, DefaultLocalCache)

    def test_health_manager_accepts_provided_cache(self):
        """Test that HealthManager accepts pre-configured cache."""
        from kafka_smart_producer.health import DefaultHealthManager

        # Mock dependencies
        class MockLagCollector:
            def get_lag_data_sync(self, topic):
                return {0: 10, 1: 5}

            def is_healthy(self):
                return True

        class MockHealthCalculator:
            def calculate_scores(self, lag_data, metadata=None):
                return {pid: 1.0 - (lag / 100.0) for pid, lag in lag_data.items()}

        lag_collector = MockLagCollector()
        health_calculator = MockHealthCalculator()

        # Create custom cache
        config = {
            "cache_max_size": 50,
            "cache_ttl_ms": 30000,
            "ordered_message_ttl": 1800.0,
        }
        custom_cache = CacheFactory.create_local_cache(config)

        # Create health manager with provided cache
        health_manager = DefaultHealthManager(
            lag_collector, health_calculator, cache=custom_cache
        )

        # Verify it uses the provided cache
        assert health_manager._cache is custom_cache
