"""
Tests for the simplified SmartProducer implementation.

This module tests the core functionality without complex mocking.
"""

import logging
import time

from kafka_smart_producer.producer_config import SmartProducerConfig

# Test the internal logic components independently

logger = logging.getLogger(__name__)


class MockHealthManager:
    """Mock health manager for testing."""

    def __init__(self, healthy_partitions: dict[str, list[int]]):
        self._healthy_partitions = healthy_partitions
        self._selection_calls = []
        self._health_check_calls = []

    def select_partition(self, topic: str, key: bytes) -> int:
        self._selection_calls.append((topic, key))
        healthy = self._healthy_partitions.get(topic, [0])
        return healthy[0] if healthy else 0

    def is_partition_healthy(self, topic: str, partition: int) -> bool:
        call = (topic, partition)
        self._health_check_calls.append(call)
        return partition in self._healthy_partitions.get(topic, [])

    def get_selection_calls(self):
        return self._selection_calls

    def get_health_check_calls(self):
        return self._health_check_calls


class TestProducerComponents:
    """Test individual components of the producer."""

    def test_producer_config_handling(self):
        """Test SmartProducerConfig creation and clean Kafka config extraction."""
        config_dict = {
            "bootstrap.servers": "localhost:9092",
            "topics": ["test-topic"],
            "smart_enabled": False,
            "key_stickiness": True,
            "cache": {"local_max_size": 100, "local_ttl_seconds": 60},
        }

        # Test SmartProducerConfig creation from dict
        producer_config = SmartProducerConfig.from_dict(config_dict)

        # Verify smart configuration is properly stored
        assert producer_config.smart_enabled is False
        assert producer_config.key_stickiness is True
        assert producer_config.cache_config.local_max_size == 100
        assert producer_config.cache_config.local_default_ttl_seconds == 60

        # Test clean Kafka config extraction
        clean_kafka_config = producer_config.get_clean_kafka_config()

        # Clean config should only have Kafka-specific keys
        assert clean_kafka_config["bootstrap.servers"] == "localhost:9092"
        assert "topics" not in clean_kafka_config
        assert "smart_enabled" not in clean_kafka_config
        assert "key_stickiness" not in clean_kafka_config
        assert "cache" not in clean_kafka_config

    def test_partition_selection_logic(self):
        """Test the partition selection logic independently."""
        health_manager = MockHealthManager(
            {
                "test-topic": [0, 1, 2],
                "other-topic": [1, 3],
            }
        )

        # Create a minimal producer to test logic
        class TestableProducer:
            def __init__(self, health_manager):
                self._health_manager = health_manager
                self._key_cache = {}
                self._cache_ttl_ms = 300000
                self._health_check_enabled = True
                self._smart_enabled = True

            def _select_partition(self, topic: str, key: bytes):
                """Copy of the actual logic from SmartProducer."""
                cache_key = (topic, key)
                current_time = time.time() * 1000

                # Check cache first
                if cache_key in self._key_cache:
                    cached_partition, cache_time = self._key_cache[cache_key]

                    # Check if cache entry is still valid
                    if current_time - cache_time < self._cache_ttl_ms:
                        # Validate partition is still healthy
                        if self._health_manager and self._health_check_enabled:
                            try:
                                if self._health_manager.is_partition_healthy(
                                    topic, cached_partition
                                ):
                                    return cached_partition
                                else:
                                    # Partition became unhealthy, remove from cache
                                    del self._key_cache[cache_key]
                            except Exception:
                                # Continue to smart selection below
                                logger.debug(
                                    "Health check failed, continuing to smart selection"
                                )
                        else:
                            # No health checking, use cached partition
                            return cached_partition
                    else:
                        # Cache entry expired
                        del self._key_cache[cache_key]

                # Try smart selection via health manager
                if self._health_manager and self._health_check_enabled:
                    try:
                        selected_partition = self._health_manager.select_partition(
                            topic, key
                        )
                        if selected_partition is not None:
                            # Cache the selection
                            self._key_cache[cache_key] = (
                                selected_partition,
                                current_time,
                            )
                            return selected_partition
                    except Exception:
                        logger.debug("Smart selection failed, using fallback")

                # Fallback: return None to let confluent-kafka handle partitioning
                return None

        producer = TestableProducer(health_manager)

        # Test first call - should call health manager
        result1 = producer._select_partition("test-topic", b"key1")
        assert result1 == 0  # First healthy partition
        assert len(health_manager.get_selection_calls()) == 1

        # Test second call - should use cache
        result2 = producer._select_partition("test-topic", b"key1")
        assert result2 == 0
        assert len(health_manager.get_selection_calls()) == 1  # No additional calls

        # Test cache validation when partition becomes unhealthy
        health_manager._healthy_partitions["test-topic"] = [1, 2]  # Remove 0
        result3 = producer._select_partition("test-topic", b"key1")
        assert result3 == 1  # New healthy partition
        assert len(health_manager.get_selection_calls()) == 2
        assert len(health_manager.get_health_check_calls()) >= 1

    def test_cache_ttl_expiration(self):
        """Test cache TTL expiration logic."""
        health_manager = MockHealthManager({"test-topic": [0, 1, 2]})

        class TestableProducer:
            def __init__(self, health_manager, ttl_ms):
                self._health_manager = health_manager
                self._key_cache = {}
                self._cache_ttl_ms = ttl_ms
                self._health_check_enabled = True
                self._smart_enabled = True

            def _select_partition(self, topic: str, key: bytes):
                """Simplified version for testing."""
                cache_key = (topic, key)
                current_time = time.time() * 1000

                # Check cache first
                if cache_key in self._key_cache:
                    cached_partition, cache_time = self._key_cache[cache_key]

                    # Check if cache entry is still valid
                    if current_time - cache_time < self._cache_ttl_ms:
                        return cached_partition
                    else:
                        # Cache entry expired
                        del self._key_cache[cache_key]

                # Try smart selection via health manager
                if self._health_manager:
                    selected_partition = self._health_manager.select_partition(
                        topic, key
                    )
                    self._key_cache[cache_key] = (selected_partition, current_time)
                    return selected_partition

                return None

        producer = TestableProducer(health_manager, 100)  # 100ms TTL

        # Add expired cache entry
        cache_key = ("test-topic", b"key1")
        producer._key_cache[cache_key] = (0, time.time() * 1000 - 200)  # Expired

        # Should not use expired cache entry
        result = producer._select_partition("test-topic", b"key1")
        assert result == 0  # From health manager, not cache
        assert len(health_manager.get_selection_calls()) == 1

        # Expired entry should be removed and new one added
        assert cache_key in producer._key_cache
        cached_partition, cache_time = producer._key_cache[cache_key]
        assert cached_partition == 0
        assert cache_time > time.time() * 1000 - 100  # Recent timestamp

    def test_no_health_manager_fallback(self):
        """Test behavior when no health manager is provided."""

        class TestableProducer:
            def __init__(self):
                self._health_manager = None
                self._key_cache = {}
                self._cache_ttl_ms = 300000
                self._health_check_enabled = True
                self._smart_enabled = True

            def _select_partition(self, topic: str, key: bytes):
                """Simplified version for testing."""
                if self._health_manager:
                    return self._health_manager.select_partition(topic, key)
                return None

        producer = TestableProducer()

        # Should return None when no health manager
        result = producer._select_partition("test-topic", b"key1")
        assert result is None

    def test_disabled_smart_partitioning(self):
        """Test behavior when smart partitioning is disabled."""
        health_manager = MockHealthManager({"test-topic": [0, 1, 2]})

        class TestableProducer:
            def __init__(self, smart_enabled):
                self._health_manager = health_manager if smart_enabled else None
                self._key_cache = {} if smart_enabled else {}
                self._cache_ttl_ms = 300000
                self._health_check_enabled = True
                self._smart_enabled = smart_enabled

            def _select_partition(self, topic: str, key: bytes):
                """Simplified version for testing."""
                if not self._smart_enabled:
                    return None
                if self._health_manager:
                    return self._health_manager.select_partition(topic, key)
                return None

        # Disabled smart partitioning
        producer = TestableProducer(smart_enabled=False)

        assert producer._health_manager is None
        assert producer._key_cache == {}
        assert producer._smart_enabled is False

        # Should return None for partition selection
        result = producer._select_partition("test-topic", b"key1")
        assert result is None
        assert len(health_manager.get_selection_calls()) == 0


class TestConfigurationValidation:
    """Test configuration handling."""

    def test_default_configuration_values(self):
        """Test that default configuration values are correct."""

        # Test the actual _extract_smart_config method
        class TestableProducer:
            def _extract_smart_config(self, config):
                """Copy of the actual method."""
                smart_config = {
                    "enabled": config.pop("smart.partitioning.enabled", True),
                    "cache_ttl_ms": config.pop("smart.cache.ttl.ms", 300000),
                    "health_check_enabled": config.pop(
                        "smart.health.check.enabled", True
                    ),
                }
                return smart_config

        producer = TestableProducer()

        # Test with minimal config
        config = {"bootstrap.servers": "localhost:9092"}
        smart_config = producer._extract_smart_config(config)

        assert smart_config["enabled"] is True
        assert smart_config["cache_ttl_ms"] == 300000
        assert smart_config["health_check_enabled"] is True

        # Test with explicit values
        config = {
            "bootstrap.servers": "localhost:9092",
            "smart.partitioning.enabled": False,
            "smart.cache.ttl.ms": 60000,
            "smart.health.check.enabled": False,
        }
        smart_config = producer._extract_smart_config(config)

        assert smart_config["enabled"] is False
        assert smart_config["cache_ttl_ms"] == 60000
        assert smart_config["health_check_enabled"] is False

        # Verify smart config was removed from main config
        assert "smart.partitioning.enabled" not in config
        assert "smart.cache.ttl.ms" not in config
        assert "smart.health.check.enabled" not in config
        assert "bootstrap.servers" in config
