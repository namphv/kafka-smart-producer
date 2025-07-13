"""
Tests for SmartProducerConfig facade pattern and unified configuration.

This module tests the SmartProducerConfig class that implements the facade pattern
for managing CacheConfig and HealthManagerConfig internally.
"""

import pytest

from kafka_smart_producer.caching import CacheConfig
from kafka_smart_producer.health_config import HealthManagerConfig
from kafka_smart_producer.producer_config import SmartProducerConfig


class TestSmartProducerConfig:
    """Test SmartProducerConfig facade pattern and validation."""

    def test_basic_initialization(self):
        """Test basic SmartProducerConfig initialization."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"}, topics=["test-topic"]
        )

        assert config.kafka_config == {"bootstrap.servers": "localhost:9092"}
        assert config.topics == ["test-topic"]
        assert config.smart_enabled is True
        assert config.key_stickiness is True
        assert config.health_manager is None
        assert config.cache is None

        # Internal configs should be created
        assert isinstance(config.cache_config, CacheConfig)
        assert config.health_config is None

    def test_with_health_manager_dict(self):
        """Test initialization with health manager configuration as dict."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["orders"],
            health_manager={
                "consumer_group": "order-consumers",
                "refresh_interval": 30.0,
            },
        )

        assert config.health_manager == {
            "consumer_group": "order-consumers",
            "refresh_interval": 30.0,
        }
        assert isinstance(config.health_config, HealthManagerConfig)
        assert config.health_config.consumer_group == "order-consumers"
        assert config.health_config.refresh_interval == 30.0

    def test_with_cache_dict(self):
        """Test initialization with cache configuration as dict."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["events"],
            cache={
                "local_max_size": 2000,
                "local_ttl_seconds": 600.0,
                "remote_enabled": True,
                "redis_host": "redis.example.com",
                "redis_port": 6380,
            },
        )

        assert config.cache == {
            "local_max_size": 2000,
            "local_ttl_seconds": 600.0,
            "remote_enabled": True,
            "redis_host": "redis.example.com",
            "redis_port": 6380,
        }
        assert isinstance(config.cache_config, CacheConfig)
        assert config.cache_config.local_max_size == 2000
        assert config.cache_config.local_default_ttl_seconds == 600.0
        assert config.cache_config.remote_enabled is True
        assert config.cache_config.redis_host == "redis.example.com"
        assert config.cache_config.redis_port == 6380

    def test_from_dict_legacy_compatibility(self):
        """Test from_dict method for legacy dict-based configuration."""
        legacy_config = {
            "bootstrap.servers": "localhost:9092",
            "topics": ["legacy-topic"],
            "health_manager": {"consumer_group": "legacy-consumers"},
            "cache": {"local_max_size": 500},
            "smart_enabled": False,
            "key_stickiness": False,
        }

        config = SmartProducerConfig.from_dict(legacy_config)

        assert config.kafka_config == {"bootstrap.servers": "localhost:9092"}
        assert config.topics == ["legacy-topic"]
        assert config.smart_enabled is False
        assert config.key_stickiness is False
        assert config.health_manager == {"consumer_group": "legacy-consumers"}
        assert config.cache == {"local_max_size": 500}

        # Internal configs should be created properly
        assert config.cache_config.local_max_size == 500
        assert config.health_config.consumer_group == "legacy-consumers"

    def test_get_clean_kafka_config(self):
        """Test that smart producer fields are filtered out of Kafka config."""
        config = SmartProducerConfig(
            kafka_config={
                "bootstrap.servers": "localhost:9092",
                "security.protocol": "SASL_SSL",
                "client.id": "test-producer",
                # These should be filtered out
                "topics": ["should-be-filtered"],
                "health_manager": {"should": "be-filtered"},
                "cache": {"should": "be-filtered"},
                "smart_enabled": True,
                "key_stickiness": True,
            },
            topics=["actual-topics"],
        )

        clean_config = config.get_clean_kafka_config()

        # Should only contain actual Kafka config
        expected = {
            "bootstrap.servers": "localhost:9092",
            "security.protocol": "SASL_SSL",
            "client.id": "test-producer",
        }
        assert clean_config == expected

    def test_cache_config_remote_enabled_property(self):
        """Test cache config remote_enabled property."""
        # Local cache only (default)
        config1 = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"}, topics=["test"]
        )
        assert config1.cache_config.remote_enabled is False

        # Remote cache enabled
        config2 = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test"],
            cache={"remote_enabled": True},
        )
        assert config2.cache_config.remote_enabled is True

        # Explicit local mode even with remote config
        config3 = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test"],
            cache={"remote_enabled": False},
        )
        assert config3.cache_config.remote_enabled is False

    def test_invalid_kafka_config(self):
        """Test that kafka_config dict validation passes through."""
        # SmartProducerConfig doesn't validate kafka config contents
        config = SmartProducerConfig(
            kafka_config={"client.id": "test"},  # Missing bootstrap.servers is ok
            topics=["test"],
        )
        assert config.kafka_config == {"client.id": "test"}

    def test_empty_topics(self):
        """Test validation of topics list."""
        with pytest.raises(ValueError, match="topics must be a non-empty list"):
            SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"}, topics=[]
            )

    def test_health_manager_config_passes_through(self):
        """Test that health manager config is passed through without validation."""
        # SmartProducerConfig doesn't validate the content, just stores it
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test"],
            health_manager={"consumer_group": 123},  # Will be passed through
        )
        assert config.health_manager == {"consumer_group": 123}

    def test_cache_config_validation_error(self):
        """Test that invalid cache config raises error during CacheConfig creation."""
        # SmartProducerConfig validates by creating CacheConfig
        with pytest.raises(TypeError):
            SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["test"],
                cache={"local_max_size": "invalid"},  # Will fail CacheConfig validation
            )

    def test_config_immutability(self):
        """Test that internal configs are immutable (frozen dataclasses)."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test"],
            cache={"local_max_size": 1000},
        )

        # Should not be able to modify frozen cache config
        with pytest.raises(AttributeError):
            config.cache_config.local_max_size = 2000

    def test_health_config_creation_with_defaults(self):
        """Test health config creation with default values."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test"],
            health_manager={"consumer_group": "test-group"},
        )

        health_config = config.health_config
        assert health_config.consumer_group == "test-group"
        assert health_config.refresh_interval == 5.0  # Default
        assert health_config.max_lag_for_health == 1000  # Default

    def test_cache_config_creation_with_defaults(self):
        """Test cache config creation with default values."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"}, topics=["test"]
        )

        cache_config = config.cache_config
        assert cache_config.local_max_size == 1000  # Default
        assert cache_config.local_default_ttl_seconds == 300.0  # Default
        assert cache_config.remote_enabled is False  # Default
        assert cache_config.redis_host == "localhost"  # Default
        assert cache_config.redis_port == 6379  # Default

    def test_complex_configuration_scenario(self):
        """Test complex configuration with all options."""
        config = SmartProducerConfig(
            kafka_config={
                "bootstrap.servers": "kafka1:9092,kafka2:9092",
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "user",
                "sasl.password": "pass",
                "client.id": "smart-producer",
            },
            topics=["orders", "payments", "notifications"],
            health_manager={
                "consumer_group": "payment-processors",
                "refresh_interval": 45.0,
                "max_lag_for_health": 500,
            },
            cache={
                "local_max_size": 5000,
                "local_ttl_seconds": 900.0,
                "remote_enabled": True,
                "redis_host": "cache.prod.example.com",
                "redis_port": 6380,
                "redis_db": 2,
                "redis_password": "cache-password",
            },
            smart_enabled=True,
            key_stickiness=True,
        )

        # Validate all configuration is properly set
        assert len(config.topics) == 3
        assert config.smart_enabled is True
        assert config.key_stickiness is True

        # Validate health config
        assert config.health_config.consumer_group == "payment-processors"
        assert config.health_config.refresh_interval == 45.0
        assert config.health_config.max_lag_for_health == 500

        # Validate cache config
        assert config.cache_config.local_max_size == 5000
        assert config.cache_config.remote_enabled is True
        assert config.cache_config.redis_host == "cache.prod.example.com"
        assert config.cache_config.redis_port == 6380

        # Validate clean kafka config excludes smart producer fields
        clean_config = config.get_clean_kafka_config()
        assert "topics" not in clean_config
        assert "health_manager" not in clean_config
        assert "cache" not in clean_config
        assert "smart_enabled" not in clean_config
        assert "key_stickiness" not in clean_config
        assert clean_config["bootstrap.servers"] == "kafka1:9092,kafka2:9092"

    def test_repr_string(self):
        """Test string representation of SmartProducerConfig."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test-topic"],
            smart_enabled=False,
        )

        repr_str = repr(config)
        assert "SmartProducerConfig" in repr_str
        assert "test-topic" in repr_str
        assert "smart_enabled=False" in repr_str
        assert "cache_config=" in repr_str
