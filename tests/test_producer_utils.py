"""
Tests for producer utilities and factory functions.

This module tests the utility functions for creating caches and health managers
from configuration, including error handling and edge cases.
"""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.producer_config import ProducerConfig
from kafka_smart_producer.producer_utils import (
    BasePartitionSelector,
    create_cache_from_config,
    create_health_manager_from_config,
)


class TestCreateCacheFromConfig:
    """Test cache creation utility function."""

    @pytest.fixture
    def base_config(self):
        """Base config for testing."""
        return ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"}, topics=["test-topic"]
        )

    def test_create_local_cache(self, base_config):
        """Test creation of local cache."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test-topic"],
            cache={"local_max_size": 500, "local_ttl_seconds": 120.0},
        )

        with patch("kafka_smart_producer.producer_utils.CacheFactory") as mock_factory:
            mock_cache = Mock()
            mock_factory.create_local_cache.return_value = mock_cache

            result = create_cache_from_config(config)

            assert result == mock_cache
            mock_factory.create_local_cache.assert_called_once_with(
                {
                    "cache_max_size": 500,
                    "cache_ttl_ms": 120000,  # Converted to milliseconds
                }
            )

    def test_create_hybrid_cache_success(self):
        """Test successful creation of hybrid cache when remote is enabled."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test-topic"],
            cache={
                "remote_enabled": True,
                "redis_host": "redis.example.com",
                "redis_port": 6380,
            },
        )

        with patch("kafka_smart_producer.producer_utils.CacheFactory") as mock_factory:
            mock_cache = Mock()
            mock_factory.create_hybrid_cache.return_value = mock_cache

            result = create_cache_from_config(config)

            assert result == mock_cache
            mock_factory.create_hybrid_cache.assert_called_once()

    def test_create_hybrid_cache_failure(self):
        """Test hybrid cache creation failure."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test-topic"],
            cache={"remote_enabled": True},
        )

        with patch("kafka_smart_producer.producer_utils.CacheFactory") as mock_factory:
            mock_factory.create_hybrid_cache.return_value = None

            with pytest.raises(RuntimeError, match="Failed to create hybrid cache"):
                create_cache_from_config(config)


class TestCreateHealthManagerFromConfig:
    """Test health manager creation utility function."""

    def test_create_sync_health_manager(self):
        """Test creation of sync health manager."""
        config = ProducerConfig(
            kafka_config={
                "bootstrap.servers": "kafka1:9092,kafka2:9092",
                "security.protocol": "PLAINTEXT",
            },
            topics=["orders", "payments"],
            health_manager={
                "consumer_group": "order-processors",
                "refresh_interval": 30.0,
            },
        )

        with patch(
            "kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor"
        ) as mock_sync_hm:
            mock_health_manager = Mock()
            mock_sync_hm.embedded.return_value = mock_health_manager

            with patch(
                "kafka_smart_producer.lag_collector.KafkaAdminLagCollector"
            ) as mock_collector:
                mock_lag_collector = Mock()
                mock_collector.return_value = mock_lag_collector

                result = create_health_manager_from_config(config, manager_type="sync")

                assert result == mock_health_manager

                # Should create lag collector with proper config
                # Note: security.protocol is passed as kwargs with dots preserved
                expected_call = mock_collector.call_args
                assert (
                    expected_call[1]["bootstrap_servers"] == "kafka1:9092,kafka2:9092"
                )
                assert expected_call[1]["consumer_group"] == "order-processors"
                assert expected_call[1]["security.protocol"] == "PLAINTEXT"

                # Should create sync health manager
                mock_sync_hm.embedded.assert_called_once_with(
                    mock_lag_collector, topics=["orders", "payments"]
                )

    def test_create_async_health_manager(self):
        """Test creation of async health manager."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["events"],
            health_manager={"consumer_group": "event-processors"},
        )

        with patch(
            "kafka_smart_producer.async_partition_health_monitor.AsyncPartitionHealthMonitor"
        ) as mock_async_hm:
            mock_health_manager = Mock()
            mock_async_hm.embedded.return_value = mock_health_manager

            with patch(
                "kafka_smart_producer.lag_collector.KafkaAdminLagCollector"
            ) as mock_collector:
                mock_lag_collector = Mock()
                mock_collector.return_value = mock_lag_collector

                result = create_health_manager_from_config(config, manager_type="async")

                assert result == mock_health_manager
                mock_async_hm.embedded.assert_called_once_with(
                    mock_lag_collector, topics=["events"]
                )

    def test_no_health_config(self):
        """Test when no health manager config is provided."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"}, topics=["test-topic"]
        )

        result = create_health_manager_from_config(config)
        assert result is None

    def test_health_manager_creation_failure(self):
        """Test health manager creation failure."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test-topic"],
            health_manager={"consumer_group": "test-group"},
        )

        with patch(
            "kafka_smart_producer.lag_collector.KafkaAdminLagCollector"
        ) as mock_collector:
            mock_collector.side_effect = Exception("Lag collector creation failed")

            with pytest.raises(RuntimeError, match="Health manager creation failed"):
                create_health_manager_from_config(config)

    def test_health_config_dict_conversion(self):
        """Test conversion of dict health config to HealthManagerConfig."""
        config = ProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["test-topic"],
            health_manager={
                "consumer_group": "test-group",
                "refresh_interval": 45.0,
                "max_lag_for_health": 2000,
            },
        )

        with patch(
            "kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor"
        ):
            with patch(
                "kafka_smart_producer.lag_collector.KafkaAdminLagCollector"
            ) as mock_collector:
                mock_lag_collector = Mock()
                mock_collector.return_value = mock_lag_collector

                create_health_manager_from_config(config, manager_type="sync")

                # Should create lag collector with converted config
                mock_collector.assert_called_once_with(
                    bootstrap_servers="localhost:9092", consumer_group="test-group"
                )


class TestBasePartitionSelectorEdgeCases:
    """Test edge cases and error scenarios in BasePartitionSelector."""

    def test_no_cache_no_health_manager(self):
        """Test selector with no cache and no health manager."""
        selector = BasePartitionSelector(
            health_manager=None, cache=None, use_key_stickiness=True
        )

        result = selector.select_partition("test-topic", b"any-key")
        assert result is None

    def test_health_manager_get_partitions_exception(self):
        """Test exception handling in health manager get_healthy_partitions."""
        mock_health_manager = Mock()
        mock_health_manager.get_healthy_partitions.side_effect = Exception(
            "Connection failed"
        )

        selector = BasePartitionSelector(
            health_manager=mock_health_manager, cache=None, use_key_stickiness=False
        )

        # Should not raise exception
        result = selector.select_partition("test-topic", b"test-key")
        assert result is None

    def test_cache_get_exception_handling(self):
        """Test cache get exception handling with recovery."""
        mock_cache = Mock()
        mock_cache.get.side_effect = Exception("Cache connection failed")
        mock_cache.set.return_value = None  # set should work

        mock_health_manager = Mock()
        mock_health_manager.get_healthy_partitions.return_value = [3]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        result = selector.select_partition("test-topic", b"user-123")

        # Should recover by using health manager
        assert result == 3

        # Should still try to cache the result
        mock_cache.set.assert_called_once()

    def test_cache_set_exception_handling(self):
        """Test cache set exception handling."""
        mock_cache = Mock()
        mock_cache.get.return_value = None  # Cache miss
        mock_cache.set.side_effect = Exception("Cache write failed")

        mock_health_manager = Mock()
        mock_health_manager.get_healthy_partitions.return_value = [1, 2]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        # Should not raise exception despite cache set failure
        result = selector.select_partition("test-topic", b"user-456")
        assert result in [1, 2]

    def test_force_refresh_exception_handling(self):
        """Test force refresh exception handling."""
        mock_health_manager = Mock()
        mock_health_manager.get_healthy_partitions.return_value = []
        mock_health_manager.force_refresh_threadsafe.side_effect = Exception(
            "Refresh failed"
        )

        selector = BasePartitionSelector(
            health_manager=mock_health_manager, cache=None, use_key_stickiness=False
        )

        # Should not raise exception despite refresh failure
        result = selector.select_partition("test-topic", b"test-key")
        assert result is None

    def test_binary_key_with_null_bytes(self):
        """Test handling of binary keys containing null bytes."""
        mock_cache = Mock()
        mock_cache.get.return_value = 2

        selector = BasePartitionSelector(
            health_manager=Mock(), cache=mock_cache, use_key_stickiness=True
        )

        # Key with null bytes and other binary data
        binary_key = b"\x00\x01\x02\xff\xfe"
        result = selector.select_partition("test-topic", binary_key)

        assert result == 2

        # Should handle binary data in cache key
        mock_cache.get.assert_called_once()
        cache_key = mock_cache.get.call_args[0][0]
        assert cache_key.startswith("test-topic:")

    def test_very_large_key(self):
        """Test handling of very large keys."""
        mock_cache = Mock()
        mock_cache.get.return_value = None
        mock_cache.set.return_value = None

        mock_health_manager = Mock()
        mock_health_manager.get_healthy_partitions.return_value = [5]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        # Very large key (10KB)
        large_key = b"x" * 10240
        result = selector.select_partition("test-topic", large_key)

        assert result == 5

        # Should handle large keys without issues
        mock_cache.get.assert_called_once()
        mock_cache.set.assert_called_once()

    def test_topic_name_with_special_characters(self):
        """Test handling of topic names with special characters."""
        mock_cache = Mock()
        mock_cache.get.return_value = 1

        selector = BasePartitionSelector(
            health_manager=Mock(), cache=mock_cache, use_key_stickiness=True
        )

        # Topic with special characters
        special_topic = "topic-with.dots_and-dashes:and:colons"
        result = selector.select_partition(special_topic, b"test-key")

        assert result == 1

        # Should properly construct cache key
        mock_cache.get.assert_called_once()
        cache_key = mock_cache.get.call_args[0][0]
        assert cache_key.startswith(special_topic + ":")

    def test_empty_key_handling(self):
        """Test handling of empty byte keys."""
        mock_cache = Mock()
        mock_health_manager = Mock()
        mock_health_manager.get_healthy_partitions.return_value = [3]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        # Empty key (b"") evaluates to False, so it goes to "no key" branch
        # This means cache is not used - the selector goes directly to health manager
        result = selector.select_partition("test-topic", b"")

        assert result == 3

        # Cache should not be called for empty keys since bool(b"") is False
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()
