"""
Tests for health manager factory methods and producer initialization.

Tests the uncovered factory methods:
- create_health_manager_from_config() for sync and async
- PartitionHealthMonitor.from_config()
- AsyncPartitionHealthMonitor.from_config()
- Producer health manager initialization
"""

import asyncio
import pytest
from unittest.mock import Mock, patch

from kafka_smart_producer.health_config import PartitionHealthMonitorConfig
from kafka_smart_producer.caching import CacheConfig
from kafka_smart_producer.producer_config import SmartProducerConfig
from kafka_smart_producer.health_mode import HealthMode


class TestCreateHealthManagerFromConfig:
    """Test create_health_manager_from_config() from producer_utils."""

    def test_create_sync_health_manager_with_topics(self):
        """Test creating sync health manager with topics from SmartProducerConfig."""
        # Patch where KafkaAdminLagCollector is imported (inside the function)
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class:
            # Mock lag collector
            mock_collector = Mock()
            mock_collector.collect_lag_data = Mock(return_value={0: 100})
            mock_collector_class.return_value = mock_collector

            # Create config with health manager dict (not health_config directly)
            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["topic1", "topic2"],
                health_manager={"consumer_group": "test-group", "health_threshold": 0.7},
            )

            from kafka_smart_producer.producer_utils import create_health_manager_from_config

            manager = create_health_manager_from_config(config, manager_type="sync")

            # Verify manager was created
            assert manager is not None
            assert manager._health_threshold == 0.7
            assert manager._mode == HealthMode.EMBEDDED

            # Verify topics were initialized
            assert "topic1" in manager._health_data
            assert "topic2" in manager._health_data

    @pytest.mark.asyncio
    async def test_create_async_health_manager_with_topics(self):
        """Test creating async health manager with topics from SmartProducerConfig."""
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class:
            # Mock lag collector
            mock_collector = Mock()
            mock_collector.collect_lag_data = Mock(return_value={0: 100})
            mock_collector_class.return_value = mock_collector

            # Create config with health manager dict
            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["async-topic"],
                health_manager={"consumer_group": "test-group", "health_threshold": 0.6},
            )

            from kafka_smart_producer.producer_utils import create_health_manager_from_config

            manager = create_health_manager_from_config(config, manager_type="async")

            # Verify manager was created
            assert manager is not None
            assert manager._health_threshold == 0.6
            assert manager._mode == HealthMode.EMBEDDED

            # Verify topics were initialized
            assert "async-topic" in manager._health_data

            await manager.stop()

    def test_create_health_manager_returns_none_without_config(self):
        """Test that None is returned when health_config is not provided."""
        # Create config without health manager or consumer group
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["topic1"],
            # No health_manager or consumer_group provided
        )

        from kafka_smart_producer.producer_utils import create_health_manager_from_config

        manager = create_health_manager_from_config(config, manager_type="sync")

        assert manager is None

    def test_create_health_manager_error_handling(self):
        """Test error handling when health manager creation fails."""
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class:
            # Make lag collector raise an error
            mock_collector_class.side_effect = Exception("Kafka connection failed")

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["topic1"],
                health_manager={"consumer_group": "test-group"},
            )

            from kafka_smart_producer.producer_utils import create_health_manager_from_config

            # Should raise RuntimeError
            with pytest.raises(RuntimeError, match="Health manager creation failed"):
                create_health_manager_from_config(config, manager_type="sync")


class TestPartitionHealthMonitorFromConfig:
    """Test PartitionHealthMonitor.from_config() factory method."""

    def test_from_config_validation_kafka_config_not_dict(self):
        """Test that from_config validates kafka_config is a dict."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        health_config = PartitionHealthMonitorConfig(consumer_group="test-group")

        # Pass non-dict kafka_config
        with pytest.raises(ValueError, match="Kafka configuration must be a dictionary"):
            PartitionHealthMonitor.from_config(health_config, kafka_config="not-a-dict")

    def test_from_config_validation_health_config_type(self):
        """Test that from_config validates health_config type."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        kafka_config = {"bootstrap.servers": "localhost:9092"}

        # Pass wrong type for health_config
        with pytest.raises(ValueError, match="PartitionHealthMonitorConfig instance"):
            PartitionHealthMonitor.from_config(health_config="wrong-type", kafka_config=kafka_config)

    def test_from_config_embedded_mode(self):
        """Test from_config creates monitor in EMBEDDED mode."""
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class:
            # Mock lag collector
            mock_collector = Mock()
            mock_collector.collect_lag_data = Mock(return_value={0: 100})
            mock_collector_class.return_value = mock_collector

            from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

            # Create health config with sync_options set at creation time
            # timeout_seconds must be less than refresh_interval
            health_config = PartitionHealthMonitorConfig(
                consumer_group="test-group",
                health_threshold=0.8,
                refresh_interval=25.0,  # Higher than default timeout_seconds (20.0)
                max_lag_for_health=600,
                sync_options={"mode": "embedded"},  # Set mode at creation
            )

            kafka_config = {
                "bootstrap.servers": "localhost:9092",
                "security.protocol": "PLAINTEXT",
            }

            monitor = PartitionHealthMonitor.from_config(health_config, kafka_config)

            # Verify monitor was created with correct settings
            assert monitor is not None
            assert monitor._health_threshold == 0.8
            assert monitor._refresh_interval == 25.0
            assert monitor._max_lag_for_health == 600

    def test_from_config_standalone_mode(self):
        """Test from_config in STANDALONE mode."""
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class:

            # Mock lag collector
            mock_collector = Mock()
            mock_collector.collect_lag_data = Mock(return_value={0: 100})
            mock_collector_class.return_value = mock_collector

            from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

            # Create health config with standalone mode
            # Note: Redis publisher creation requires cache attribute which is set internally
            health_config = PartitionHealthMonitorConfig(
                consumer_group="test-group",
                health_threshold=0.5,
                refresh_interval=25.0,  # Higher than default timeout
                sync_options={"mode": "standalone"},  # Set at creation
            )

            kafka_config = {"bootstrap.servers": "localhost:9092"}

            # Should succeed - Redis publisher will be None if cache not configured
            monitor = PartitionHealthMonitor.from_config(health_config, kafka_config)

            assert monitor is not None
            assert monitor._health_threshold == 0.5
            assert monitor._refresh_interval == 25.0


class TestAsyncPartitionHealthMonitorFromConfig:
    """Test AsyncPartitionHealthMonitor.from_config() factory method."""

    @pytest.mark.asyncio
    async def test_from_config_validation_kafka_config_not_dict(self):
        """Test that from_config validates kafka_config is a dict."""
        from kafka_smart_producer.async_partition_health_monitor import AsyncPartitionHealthMonitor

        health_config = PartitionHealthMonitorConfig(consumer_group="test-group")

        # Pass non-dict kafka_config
        with pytest.raises(ValueError, match="Kafka configuration must be a dictionary"):
            AsyncPartitionHealthMonitor.from_config(health_config, kafka_config=123)

    @pytest.mark.asyncio
    async def test_from_config_validation_health_config_type(self):
        """Test that from_config validates health_config type."""
        from kafka_smart_producer.async_partition_health_monitor import AsyncPartitionHealthMonitor

        kafka_config = {"bootstrap.servers": "localhost:9092"}

        # Pass wrong type for health_config
        with pytest.raises(ValueError, match="PartitionHealthMonitorConfig instance"):
            AsyncPartitionHealthMonitor.from_config(health_config={"wrong": "type"}, kafka_config=kafka_config)

    @pytest.mark.asyncio
    async def test_from_config_embedded_mode(self):
        """Test from_config creates async monitor in EMBEDDED mode."""
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class:
            # Mock lag collector
            mock_collector = Mock()
            mock_collector.collect_lag_data = Mock(return_value={0: 100})
            mock_collector_class.return_value = mock_collector

            from kafka_smart_producer.async_partition_health_monitor import AsyncPartitionHealthMonitor

            # Create health config with async_options set at creation
            # timeout_seconds must be less than refresh_interval
            health_config = PartitionHealthMonitorConfig(
                consumer_group="async-group",
                health_threshold=0.75,
                refresh_interval=25.0,  # Higher than default timeout_seconds (20.0)
                max_lag_for_health=700,
                async_options={"mode": "embedded"},  # Set at creation
            )

            kafka_config = {"bootstrap.servers": "localhost:9092"}

            monitor = AsyncPartitionHealthMonitor.from_config(health_config, kafka_config)

            # Verify monitor was created with correct settings
            assert monitor is not None
            assert monitor._health_threshold == 0.75
            assert monitor._refresh_interval == 25.0
            assert monitor._max_lag_for_health == 700

            await monitor.stop()
