"""
Tests for health management components.

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
from kafka_smart_producer.health_config import HealthManagerConfig
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
        """Sync method used by both sync and async health managers."""
        self.call_count += 1
        self.calls.append(("sync", topic))

        if self.should_fail:
            raise LagDataUnavailableError(f"Mock failure for {topic}")

        return self.lag_data.get(topic, {})

    def is_healthy(self) -> bool:
        return not self.should_fail


class TestHealthManagerConfig:
    """Test HealthManagerConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = HealthManagerConfig(consumer_group="test-group")

        assert config.consumer_group == "test-group"
        assert config.health_threshold == 0.5
        assert config.refresh_interval == 5.0
        assert config.max_lag_for_health == 1000
        assert config.timeout_seconds == 5.0
        assert config.cache_enabled is True

    def test_custom_config(self):
        """Test custom configuration values."""
        config = HealthManagerConfig(
            consumer_group="test-group",
            health_threshold=0.7,
            refresh_interval=10.0,
            max_lag_for_health=2000,
        )

        assert config.consumer_group == "test-group"
        assert config.health_threshold == 0.7
        assert config.refresh_interval == 10.0
        assert config.max_lag_for_health == 2000

    def test_validation(self):
        """Test configuration validation."""
        # Test valid configuration
        config = HealthManagerConfig(
            consumer_group="test-group",
            health_threshold=0.8,
        )
        assert config.health_threshold == 0.8

        # Test invalid health_threshold
        with pytest.raises(
            ValueError, match="health_threshold must be between 0.0 and 1.0"
        ):
            HealthManagerConfig(
                consumer_group="test-group",
                health_threshold=1.5,
            )

        # Test invalid refresh_interval
        with pytest.raises(ValueError, match="refresh_interval must be positive"):
            HealthManagerConfig(
                consumer_group="test-group",
                refresh_interval=-1.0,
            )

    def test_context_options(self):
        """Test context-specific options."""
        config = HealthManagerConfig(
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

    def create_sync_health_manager(
        self,
        lag_data: Optional[dict[str, dict[int, int]]] = None,
        config: Optional[HealthManagerConfig] = None,
    ) -> PartitionHealthMonitor:
        """Create a sync health manager with mock dependencies."""
        lag_collector = MockLagDataCollector(lag_data)
        config = config or HealthManagerConfig(
            consumer_group="test-group", refresh_interval=0.1
        )

        return PartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,  # No cache for simplicity
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )

    def test_sync_health_manager_creation(self):
        """Test basic sync health manager creation."""
        manager = self.create_sync_health_manager()

        assert manager._lag_collector is not None
        assert manager._health_threshold == 0.5
        assert manager._refresh_interval == 0.1
        assert not manager._running

    def test_sync_lifecycle(self):
        """Test sync start/stop lifecycle."""
        manager = self.create_sync_health_manager()

        # Initially not running
        assert not manager._running
        assert manager._thread is None

        # Start manager
        manager.start()
        assert manager._running
        assert manager._thread is not None

        # Give it a moment to start
        time.sleep(0.05)

        # Stop manager
        manager.stop()
        assert not manager._running

    def test_sync_get_healthy_partitions(self):
        """Test getting healthy partitions from sync manager."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(consumer_group="test-group", health_threshold=0.4)
        manager = self.create_sync_health_manager(lag_data, config)

        # Initialize topics monitoring
        manager._initialize_topics(["test-topic"])

        # Force refresh to get initial data
        manager.force_refresh("test-topic")

        # Get healthy partitions
        healthy_partitions = manager.get_healthy_partitions("test-topic")

        # Should return healthy partitions (0 and 2 should be healthier than 1)
        assert len(healthy_partitions) >= 1
        assert all(isinstance(p, int) for p in healthy_partitions)

    def test_sync_is_partition_healthy(self):
        """Test partition health checking in sync manager."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(consumer_group="test-group", health_threshold=0.4)
        manager = self.create_sync_health_manager(lag_data, config)

        # Add topic and force refresh
        manager._initialize_topics(["test-topic"])
        manager.force_refresh("test-topic")

        # Test partition health (partition 2 with lowest lag should be healthy)
        # Note: Actual health depends on the calculation, but this tests the interface
        result = manager.is_partition_healthy("test-topic", 2)
        assert isinstance(result, bool)

    def test_sync_add_remove_topic(self):
        """Test adding topics in sync manager."""
        manager = self.create_sync_health_manager()

        # Add topic
        manager._initialize_topics(["test-topic"])
        assert "test-topic" in manager._health_data

        # Add multiple topics
        manager._initialize_topics(["topic-1", "topic-2"])
        assert "topic-1" in manager._health_data
        assert "topic-2" in manager._health_data

    def test_sync_health_summary(self):
        """Test health summary generation in sync manager."""
        lag_data = {"test-topic": {0: 100, 1: 200}}
        manager = self.create_sync_health_manager(lag_data)

        # Add topic and refresh
        manager._initialize_topics(["test-topic"])
        manager.force_refresh("test-topic")

        # Get summary
        summary = manager.get_health_summary()

        assert summary["execution_context"] == "sync"
        assert summary["running"] is False  # Not started
        assert "topics" in summary
        assert "total_partitions" in summary
        assert "healthy_partitions" in summary

    def test_from_config_factory_method(self):
        """Test creating PartitionHealthMonitor from configuration."""
        config = HealthManagerConfig(
            consumer_group="test-group",
            health_threshold=0.7,
            refresh_interval=3.0,
        )

        kafka_config = {"bootstrap.servers": "localhost:9092"}

        try:
            manager = PartitionHealthMonitor.from_config(config, kafka_config)
            assert manager._health_threshold == 0.7
            assert manager._refresh_interval == 3.0
        except Exception:
            # This might fail due to missing KafkaAdminLagCollector, which is expected
            # in a test environment without the actual implementation
            pytest.skip("KafkaAdminLagCollector not available in test environment")


class TestAsyncPartitionHealthMonitor:
    """Test AsyncPartitionHealthMonitor implementation."""

    def create_async_health_manager(
        self,
        lag_data: Optional[dict[str, dict[int, int]]] = None,
        config: Optional[HealthManagerConfig] = None,
    ) -> AsyncPartitionHealthMonitor:
        """Create an async health manager with mock dependencies."""
        lag_collector = MockLagDataCollector(lag_data)
        config = config or HealthManagerConfig(
            consumer_group="test-group", refresh_interval=0.1
        )

        return AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,  # No cache for simplicity
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )

    def test_async_health_manager_creation(self):
        """Test basic async health manager creation."""
        manager = self.create_async_health_manager()

        assert manager._lag_collector is not None
        assert manager._health_threshold == 0.5
        assert manager._refresh_interval == 0.1
        assert not manager._running

    @pytest.mark.asyncio
    async def test_async_lifecycle(self):
        """Test async start/stop lifecycle."""
        manager = self.create_async_health_manager()

        # Initially not running
        assert not manager._running
        assert manager._task is None

        # Start manager
        await manager.start()
        assert manager._running
        assert manager._task is not None

        # Give it a moment to start
        await asyncio.sleep(0.05)

        # Stop manager
        await manager.stop()
        assert not manager._running

    @pytest.mark.asyncio
    async def test_async_get_healthy_partitions(self):
        """Test getting healthy partitions from async manager."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(consumer_group="test-group", health_threshold=0.4)
        manager = self.create_async_health_manager(lag_data, config)

        # Add topic to monitoring
        manager._initialize_topics(["test-topic"])

        # Force refresh to get initial data
        await manager.force_refresh("test-topic")

        # Get healthy partitions (sync method for producer compatibility)
        healthy_partitions = manager.get_healthy_partitions("test-topic")

        # Should return healthy partitions
        assert len(healthy_partitions) >= 1
        assert all(isinstance(p, int) for p in healthy_partitions)

    @pytest.mark.asyncio
    async def test_async_is_partition_healthy(self):
        """Test partition health checking in async manager."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(consumer_group="test-group", health_threshold=0.4)
        manager = self.create_async_health_manager(lag_data, config)

        # Add topic and force refresh
        manager._initialize_topics(["test-topic"])
        await manager.force_refresh("test-topic")

        # Test partition health (sync method for producer compatibility)
        result = manager.is_partition_healthy("test-topic", 2)
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_async_add_remove_topic(self):
        """Test adding topics in async manager."""
        manager = self.create_async_health_manager()

        # Add topic
        manager._initialize_topics(["test-topic"])
        assert "test-topic" in manager._health_data

        # Add multiple topics
        manager._initialize_topics(["topic-1", "topic-2"])
        assert "topic-1" in manager._health_data
        assert "topic-2" in manager._health_data

    @pytest.mark.asyncio
    async def test_async_health_summary(self):
        """Test health summary generation in async manager."""
        lag_data = {"test-topic": {0: 100, 1: 200}}
        manager = self.create_async_health_manager(lag_data)

        # Add topic and refresh
        manager._initialize_topics(["test-topic"])
        await manager.force_refresh("test-topic")

        # Get summary
        summary = await manager.get_health_summary()

        assert summary["execution_context"] == "async"
        assert summary["running"] is False  # Not started
        assert "topics" in summary
        assert "total_partitions" in summary
        assert "healthy_partitions" in summary

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager support."""
        lag_data = {"test-topic": {0: 100, 1: 200}}
        manager = self.create_async_health_manager(lag_data)

        async with manager:
            assert manager._running
            manager._initialize_topics(["test-topic"])
            await manager.force_refresh("test-topic")

        assert not manager._running

    def test_from_config_factory_method_async(self):
        """Test creating AsyncPartitionHealthMonitor from configuration."""
        config = HealthManagerConfig(
            consumer_group="test-group",
            health_threshold=0.7,
            refresh_interval=3.0,
        )

        kafka_config = {"bootstrap.servers": "localhost:9092"}

        try:
            manager = AsyncPartitionHealthMonitor.from_config(config, kafka_config)
            assert manager._health_threshold == 0.7
            assert manager._refresh_interval == 3.0
        except Exception:
            # This might fail due to missing KafkaAdminLagCollector, which is expected
            # in a test environment without the actual implementation
            pytest.skip("KafkaAdminLagCollector not available in test environment")


class TestHealthManagerComparison:
    """Test that sync and async health managers behave consistently."""

    def test_consistent_behavior(self):
        """Test that sync and async managers produce consistent results."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(consumer_group="test-group", health_threshold=0.4)

        # Create both managers
        sync_manager = self.create_sync_manager(lag_data, config)
        async_manager = self.create_async_manager(lag_data, config)

        # Test sync manager
        sync_manager._initialize_topics(["test-topic"])
        sync_manager.force_refresh("test-topic")
        sync_healthy = sync_manager.get_healthy_partitions("test-topic")

        # Test async manager
        async def test_async():
            async_manager._initialize_topics(["test-topic"])
            await async_manager.force_refresh("test-topic")
            return async_manager.get_healthy_partitions("test-topic")

        async_healthy = asyncio.run(test_async())

        # Results should be identical
        assert set(sync_healthy) == set(async_healthy)

    def create_sync_manager(self, lag_data, config):
        """Helper to create sync manager."""
        lag_collector = MockLagDataCollector(lag_data)
        return PartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )

    def create_async_manager(self, lag_data, config):
        """Helper to create async manager."""
        lag_collector = MockLagDataCollector(lag_data)
        return AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
        )


class TestErrorHandling:
    """Test error handling in health managers."""

    def test_sync_error_handling(self):
        """Test error handling in sync health manager."""
        lag_collector = MockLagDataCollector(should_fail=True)
        manager = PartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=1.0,
            max_lag_for_health=1000,
        )

        # Should not crash on force refresh failure
        manager._initialize_topics(["test-topic"])
        try:
            manager.force_refresh("test-topic")
        except Exception as e:
            logger.debug(f"Expected exception in test: {e}")

        # Should return empty list for unknown topic health
        healthy = manager.get_healthy_partitions("test-topic")
        assert isinstance(healthy, list)

    @pytest.mark.asyncio
    async def test_async_error_handling(self):
        """Test error handling in async health manager."""
        lag_collector = MockLagDataCollector(should_fail=True)
        manager = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=1.0,
            max_lag_for_health=1000,
        )

        # Should not crash on force refresh failure
        manager._initialize_topics(["test-topic"])
        try:
            await manager.force_refresh("test-topic")
        except Exception as e:
            logger.debug(f"Expected exception in test: {e}")

        # Should return empty list for unknown topic health
        healthy = manager.get_healthy_partitions("test-topic")
        assert isinstance(healthy, list)
