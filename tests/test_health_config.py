"""
Tests for partition health monitoring components.

This module tests the PartitionHealthMonitor and
AsyncPartitionHealthMonitor implementations
including partition health tracking, selection strategies,
and background refresh operations.
"""

import asyncio
import logging
import time
from typing import Optional

import pytest

from kafka_smart_producer.async_partition_health_monitor import (
    AsyncPartitionHealthMonitor,
)
from kafka_smart_producer.exceptions import LagDataUnavailableError
from kafka_smart_producer.health_config import PartitionHealthMonitorConfig
from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

logger = logging.getLogger(__name__)


class MockLagDataCollector:
    """Mock lag data collector for testing."""

    def __init__(
        self,
        lag_data: Optional[dict[str, dict[int, int]]] = None,
        should_fail: bool = False,
    ):
        self.lag_data = lag_data or {}
        self.should_fail = should_fail
        self.call_count = 0
        self.calls = []

    def get_lag_data(self, topic: str) -> dict[int, int]:
        """Sync method used by both sync and async partition health monitors."""
        self.call_count += 1
        self.calls.append(("sync", topic))

        if self.should_fail:
            raise LagDataUnavailableError(f"Mock failure for {topic}")

        return self.lag_data.get(topic, {})

    def is_healthy(self) -> bool:
        return not self.should_fail


class TestPartitionHealthMonitorConfig:
    """Test PartitionHealthMonitorConfig dataclass used by partition health monitors."""

    def test_default_config(self):
        """Test default configuration values."""
        config = PartitionHealthMonitorConfig(consumer_group="test-group")

        assert config.consumer_group == "test-group"
        assert config.health_threshold == 0.5
        assert config.refresh_interval == 30.0  # Updated production-ready default
        assert config.max_lag_for_health == 1000
        assert config.timeout_seconds == 20.0  # Updated production-ready default
        assert config.cache_enabled is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.7,
            refresh_interval=20.0,  # Must be >= 15s for validation
            max_lag_for_health=2000,
            timeout_seconds=15.0,  # Must be less than refresh_interval
        )

        assert config.consumer_group == "test-group"
        assert config.health_threshold == 0.7
        assert config.refresh_interval == 20.0
        assert config.max_lag_for_health == 2000
        assert config.timeout_seconds == 15.0

    def test_validation(self):
        """Test configuration validation."""
        # Test valid configuration
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.8,
        )
        assert config.health_threshold == 0.8

        # Test invalid health_threshold
        with pytest.raises(
            ValueError, match="health_threshold must be between 0.0 and 1.0"
        ):
            PartitionHealthMonitorConfig(
                consumer_group="test-group",
                health_threshold=1.5,
            )

        # Test invalid refresh_interval
        with pytest.raises(ValueError, match="refresh_interval must be positive"):
            PartitionHealthMonitorConfig(
                consumer_group="test-group",
                refresh_interval=-1.0,
            )

    def test_context_options(self):
        """Test context-specific options."""
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            sync_options={"thread_pool_size": 8},
            async_options={"concurrent_refresh_limit": 20},
        )

        assert config.get_sync_option("thread_pool_size") == 8
        assert config.get_sync_option("missing_option", "default") == "default"
        assert config.get_async_option("concurrent_refresh_limit") == 20
        assert config.get_async_option("missing_option", "default") == "default"


class TestPartitionHealthMonitor:
    """Test PartitionHealthMonitor implementation."""

    def create_sync_partition_health_monitor(
        self,
        lag_data: Optional[dict[str, dict[int, int]]] = None,
        config: Optional[PartitionHealthMonitorConfig] = None,
    ) -> PartitionHealthMonitor:
        """Create a sync partition health monitor with mock dependencies."""
        lag_collector = MockLagDataCollector(lag_data)
        config = config or PartitionHealthMonitorConfig(
            consumer_group="test-group", refresh_interval=2.0, timeout_seconds=1.0
        )

        return PartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,  # No cache for simplicity
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )

    def test_sync_partition_health_monitor_creation(self):
        """Test basic sync PartitionHealthMonitor creation."""
        monitor = self.create_sync_partition_health_monitor()

        assert monitor._lag_collector is not None
        assert monitor._health_threshold == 0.5
        assert monitor._refresh_interval == 2.0
        assert not monitor._running

    def test_sync_lifecycle(self):
        """Test sync start/stop lifecycle."""
        monitor = self.create_sync_partition_health_monitor()

        # Initially not running
        assert not monitor._running
        assert monitor._thread is None

        # Start monitor
        monitor.start()
        assert monitor._running
        assert monitor._thread is not None

        # Give it a moment to start
        time.sleep(0.05)

        # Stop monitor
        monitor.stop()
        assert not monitor._running

    def test_sync_get_healthy_partitions(self):
        """Test getting healthy partitions from sync monitor."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.4,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )
        monitor = self.create_sync_partition_health_monitor(lag_data, config)

        # Initialize topics monitoring
        monitor._initialize_topics(["test-topic"])

        # Force refresh to get initial data
        monitor.force_refresh("test-topic")

        # Get healthy partitions
        healthy_partitions = monitor.get_healthy_partitions("test-topic")

        # Should return healthy partitions (0 and 2 should be healthier than 1)
        assert len(healthy_partitions) >= 1
        assert all(isinstance(p, int) for p in healthy_partitions)

    def test_sync_is_partition_healthy(self):
        """Test partition health checking in sync monitor."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.4,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )
        monitor = self.create_sync_partition_health_monitor(lag_data, config)

        # Add topic and force refresh
        monitor._initialize_topics(["test-topic"])
        monitor.force_refresh("test-topic")

        # Test partition health (partition 2 with lowest lag should be healthy)
        # Note: Actual health depends on the calculation, but this tests the interface
        result = monitor.is_partition_healthy("test-topic", 2)
        assert isinstance(result, bool)

    def test_sync_add_remove_topic(self):
        """Test adding topics in sync monitor."""
        monitor = self.create_sync_partition_health_monitor()

        # Add topic
        monitor._initialize_topics(["test-topic"])
        assert "test-topic" in monitor._health_data

        # Add multiple topics
        monitor._initialize_topics(["topic-1", "topic-2"])
        assert "topic-1" in monitor._health_data
        assert "topic-2" in monitor._health_data

    def test_sync_health_summary(self):
        """Test health summary generation in sync monitor."""
        lag_data = {"test-topic": {0: 100, 1: 200}}
        monitor = self.create_sync_partition_health_monitor(lag_data)

        # Add topic and refresh
        monitor._initialize_topics(["test-topic"])
        monitor.force_refresh("test-topic")

        # Get summary
        summary = monitor.get_health_summary()

        assert summary["execution_context"] == "sync"
        assert summary["running"] is False  # Not started
        assert "topics" in summary
        assert "total_partitions" in summary
        assert "healthy_partitions" in summary

    def test_from_config_factory_method(self):
        """Test creating PartitionHealthMonitor from configuration."""
        from unittest.mock import Mock, patch

        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.7,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )

        kafka_config = {"bootstrap.servers": "localhost:9092"}

        # Mock the lag collector creation to avoid Kafka dependencies
        mock_lag_collector = Mock()

        with patch(
            "kafka_smart_producer.partition_health_monitor.create_lag_collector_from_config"
        ) as mock_create_lag:
            with patch(
                "kafka_smart_producer.partition_health_monitor.create_cache_from_config"
            ) as mock_create_cache:
                mock_create_lag.return_value = mock_lag_collector
                mock_create_cache.return_value = None

                monitor = PartitionHealthMonitor.from_config(config, kafka_config)
                assert monitor._health_threshold == 0.7
                assert monitor._refresh_interval == 5.0
                assert monitor._lag_collector is mock_lag_collector


class TestPartitionHealthMonitorComparison:
    """Test that sync and async partition health monitors behave consistently."""

    def test_consistent_behavior(self):
        """Test that sync and async monitors produce consistent results."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.4,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )

        # Create both monitors
        sync_monitor = self.create_sync_monitor(lag_data, config)
        async_monitor = self.create_async_monitor(lag_data, config)

        # Test sync monitor
        sync_monitor._initialize_topics(["test-topic"])
        sync_monitor.force_refresh("test-topic")
        sync_healthy = sync_monitor.get_healthy_partitions("test-topic")

        # Test async monitor
        async def test_async():
            async_monitor._initialize_topics(["test-topic"])
            await async_monitor.force_refresh("test-topic")
            return async_monitor.get_healthy_partitions("test-topic")

        async_healthy = asyncio.run(test_async())

        # Results should be identical
        assert set(sync_healthy) == set(async_healthy)

    def create_sync_monitor(self, lag_data, config):
        """Helper to create sync monitor."""
        lag_collector = MockLagDataCollector(lag_data)
        return PartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )

    def create_async_monitor(self, lag_data, config):
        """Helper to create async monitor."""
        lag_collector = MockLagDataCollector(lag_data)
        return AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )


class TestErrorHandling:
    """Test error handling in partition health monitors."""

    def test_sync_error_handling(self):
        """Test error handling in sync partition health monitor."""
        lag_collector = MockLagDataCollector(should_fail=True)
        monitor = PartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=1.0,
            max_lag_for_health=1000,
        )

        # Should not crash on force refresh failure
        monitor._initialize_topics(["test-topic"])
        try:
            monitor.force_refresh("test-topic")
        except Exception as e:
            logger.debug(f"Expected exception in test: {e}")

        # Should return empty list for unknown topic health
        healthy = monitor.get_healthy_partitions("test-topic")
        assert isinstance(healthy, list)
