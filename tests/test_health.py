"""
Tests for health management components.

This module tests the HealthManager implementation including partition health
tracking, selection strategies, and background refresh operations.
"""

import asyncio
import threading
import time
from typing import Any, Dict, Optional

import pytest

from kafka_smart_producer.caching import CacheConfig, DefaultLocalCache
from kafka_smart_producer.exceptions import (
    HealthCalculationError,
    LagDataUnavailableError,
    PartitionSelectionError,
)
from kafka_smart_producer.health import (
    DefaultHealthManager,
    HealthManagerConfig,
    NoHealthyPartitionsError,
    PartitionHealth,
    TopicHealth,
)


class MockLagDataCollector:
    """Mock lag data collector for testing."""

    def __init__(
        self,
        lag_data: Optional[Dict[str, Dict[int, int]]] = None,
        should_fail: bool = False,
    ):
        self.lag_data = lag_data or {}
        self.should_fail = should_fail
        self.call_count = 0
        self.calls = []

    async def get_lag_data(self, topic: str) -> Dict[int, int]:
        self.call_count += 1
        self.calls.append(("async", topic))

        if self.should_fail:
            raise LagDataUnavailableError(f"Mock failure for {topic}")

        return self.lag_data.get(topic, {})

    def get_lag_data_sync(self, topic: str) -> Dict[int, int]:
        self.call_count += 1
        self.calls.append(("sync", topic))

        if self.should_fail:
            raise LagDataUnavailableError(f"Mock failure for {topic}")

        return self.lag_data.get(topic, {})

    def is_healthy(self) -> bool:
        return not self.should_fail


class MockHotPartitionCalculator:
    """Mock health calculator for testing."""

    def __init__(
        self,
        scores: Optional[Dict[str, Dict[int, float]]] = None,
        should_fail: bool = False,
    ):
        self.scores = scores or {}
        self.should_fail = should_fail
        self.call_count = 0
        self.calls = []

    def calculate_scores(
        self, lag_data: Dict[int, int], metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[int, float]:
        self.call_count += 1
        self.calls.append(lag_data)

        if self.should_fail:
            raise HealthCalculationError("Mock calculation failure")

        # Simple inverse scoring: lower lag = higher score
        if not lag_data:
            return {}

        max_lag = max(lag_data.values()) if lag_data.values() else 1
        return {
            partition_id: max(0.0, 1.0 - (lag / max_lag))
            for partition_id, lag in lag_data.items()
        }

    def get_threshold_config(self) -> Dict[str, Any]:
        return {"mock": True}


class TestPartitionHealthDataclasses:
    """Test PartitionHealth and TopicHealth dataclasses."""

    def test_partition_health_creation(self):
        """Test PartitionHealth dataclass creation."""
        health = PartitionHealth(
            partition_id=0,
            health_score=0.8,
            lag_count=100,
            last_updated=time.time(),
            is_healthy=True,
        )

        assert health.partition_id == 0
        assert health.health_score == 0.8
        assert health.lag_count == 100
        assert health.is_healthy is True

    def test_topic_health_creation(self):
        """Test TopicHealth dataclass creation."""
        partitions = {
            0: PartitionHealth(0, 0.8, 100, time.time(), True),
            1: PartitionHealth(1, 0.3, 500, time.time(), False),
        }

        health = TopicHealth(
            topic="test-topic",
            partitions=partitions,
            last_refresh=time.time(),
            healthy_partitions=[0],
            total_partitions=2,
        )

        assert health.topic == "test-topic"
        assert len(health.partitions) == 2
        assert health.healthy_partitions == [0]
        assert health.total_partitions == 2


class TestHealthManagerConfig:
    """Test HealthManagerConfig dataclass."""

    def test_default_config(self):
        """Test default configuration values."""
        config = HealthManagerConfig()

        assert config.refresh_interval_seconds == 5.0
        assert config.health_threshold == 0.5
        assert config.cache_ttl_seconds == 60.0
        assert config.default_partition_count == 3
        assert config.max_refresh_failures == 5

    def test_custom_config(self):
        """Test custom configuration values."""
        config = HealthManagerConfig(
            refresh_interval_seconds=10.0,
            health_threshold=0.7,
        )

        assert config.refresh_interval_seconds == 10.0
        assert config.health_threshold == 0.7


class TestDefaultHealthManager:
    """Test DefaultHealthManager implementation."""

    def create_health_manager(
        self,
        lag_data: Optional[Dict[str, Dict[int, int]]] = None,
        config: Optional[HealthManagerConfig] = None,
    ) -> DefaultHealthManager:
        """Create a health manager with mock dependencies."""
        lag_collector = MockLagDataCollector(lag_data)
        health_calculator = MockHotPartitionCalculator()
        cache = DefaultLocalCache(
            CacheConfig(local_max_size=100, local_default_ttl_seconds=60.0)
        )
        config = config or HealthManagerConfig(refresh_interval_seconds=0.1)

        return DefaultHealthManager(lag_collector, health_calculator, cache, config)

    def test_health_manager_creation(self):
        """Test basic health manager creation."""
        manager = self.create_health_manager()

        assert manager._lag_collector is not None
        assert manager._health_calculator is not None
        assert manager._cache is not None
        assert manager._config is not None
        assert not manager._running

    @pytest.mark.asyncio
    async def test_async_lifecycle(self):
        """Test async start/stop lifecycle."""
        manager = self.create_health_manager()

        # Initially not running
        assert not manager._running
        assert manager._background_refresh is None

        # Start manager
        await manager.start()
        assert manager._running
        assert manager._is_async_context
        assert manager._background_refresh is not None

        # Stop manager
        await manager.stop()
        assert not manager._running
        assert manager._background_refresh is None

    def test_sync_lifecycle(self):
        """Test sync start/stop lifecycle."""
        manager = self.create_health_manager()

        # Run in sync context (no event loop)
        import asyncio

        async def run_test():
            await manager.start()
            assert manager._running
            assert not manager._is_async_context  # Should detect sync context
            assert manager._background_refresh is not None

            await manager.stop()
            assert not manager._running

        # Run outside event loop to simulate sync context
        def sync_test():
            # Start without event loop
            try:
                asyncio.get_running_loop()
                pytest.skip("Already in async context")
            except RuntimeError:
                pass

            # Simulate sync start
            manager._running = True
            manager._is_async_context = False
            from kafka_smart_producer.threading import create_sync_background_refresh

            manager._background_refresh = create_sync_background_refresh(
                manager._refresh_all_topics, 0.1
            )
            manager._background_refresh.start()

            time.sleep(0.05)  # Let it run briefly

            manager._background_refresh.stop()
            manager._running = False

        sync_test()

    def test_topic_health_refresh(self):
        """Test topic health data refresh."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        manager = self.create_health_manager(lag_data)

        # Force refresh
        manager.force_refresh("test-topic")

        # Check health data
        health = manager.get_topic_health("test-topic")
        assert health is not None
        assert health.topic == "test-topic"
        assert len(health.partitions) == 3
        assert health.total_partitions == 3

        # Check partition health scores
        # Partition 2 should have highest score (lowest lag)
        # Partition 1 should have lowest score (highest lag)
        assert health.partitions[2].health_score > health.partitions[0].health_score
        assert health.partitions[0].health_score > health.partitions[1].health_score

    def test_get_healthy_partitions_consistency(self):
        """Test that get_healthy_partitions returns consistent results."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}  # 0 and 2 should be healthy
        config = HealthManagerConfig(health_threshold=0.4)
        manager = self.create_health_manager(lag_data, config)

        # Refresh health data
        manager.force_refresh("test-topic")

        # Get healthy partitions multiple times
        selections = []
        for _ in range(10):
            healthy_partitions = manager.get_healthy_partitions("test-topic")
            selections.append(set(healthy_partitions))

        # Should consistently return the same healthy partitions
        expected_healthy = {0, 2}  # Only healthy partitions
        for selection in selections:
            assert selection == expected_healthy

    def test_get_healthy_partitions_basic(self):
        """Test getting healthy partitions for a topic."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(health_threshold=0.4)
        manager = self.create_health_manager(lag_data, config)

        # Refresh health data
        manager.force_refresh("test-topic")

        # Get healthy partitions
        healthy_partitions = manager.get_healthy_partitions("test-topic")

        # Should return healthy partitions based on health threshold
        expected_healthy = [0, 2]  # Partitions with health score >= 0.4
        assert set(healthy_partitions) == set(expected_healthy)

    def test_get_healthy_partitions_with_available_filter(self):
        """Test getting healthy partitions with available partitions filter."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50, 3: 75}}
        config = HealthManagerConfig(health_threshold=0.4)
        manager = self.create_health_manager(lag_data, config)

        # Refresh health data
        manager.force_refresh("test-topic")

        # Get healthy partitions with available filter
        available_partitions = [1, 2, 3]
        healthy_partitions = manager.get_healthy_partitions(
            "test-topic", available_partitions
        )

        # Should return intersection of healthy and available partitions
        expected_healthy = [2, 3]  # Healthy partitions that are also available
        assert set(healthy_partitions) == set(expected_healthy)

    def test_get_healthy_partitions_fallback(self):
        """Test getting healthy partitions when no health data available."""
        manager = self.create_health_manager()

        # Get healthy partitions for unknown topic
        healthy_partitions = manager.get_healthy_partitions("unknown-topic")
        assert healthy_partitions == [0, 1, 2]  # Default partition count

    def test_get_healthy_partitions_no_healthy(self):
        """Test getting healthy partitions when no partitions are healthy."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(health_threshold=0.95)  # Very high threshold
        manager = self.create_health_manager(lag_data, config)

        # Refresh health data
        manager.force_refresh("test-topic")

        # Get healthy partitions - should fall back to all partitions
        healthy_partitions = manager.get_healthy_partitions("test-topic")
        assert healthy_partitions == [0, 1, 2]  # Default partition count as fallback

    def test_get_healthy_partitions_with_available_fallback(self):
        """Test getting healthy partitions with available partitions as fallback."""
        manager = self.create_health_manager()

        available_partitions = [1, 3, 5]

        # Get healthy partitions for unknown topic with available constraint
        healthy_partitions = manager.get_healthy_partitions(
            "unknown-topic", available_partitions
        )

        # Should return available partitions as fallback
        assert healthy_partitions == available_partitions

    def test_is_partition_healthy(self):
        """Test partition health checking."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = HealthManagerConfig(health_threshold=0.4)
        manager = self.create_health_manager(lag_data, config)

        # Before refresh - should assume healthy
        assert manager.is_partition_healthy("test-topic", 0)
        assert manager.is_partition_healthy("test-topic", 1)

        # After refresh
        manager.force_refresh("test-topic")

        # Check actual health status
        assert manager.is_partition_healthy("test-topic", 0)  # Medium lag
        assert not manager.is_partition_healthy("test-topic", 1)  # High lag
        assert manager.is_partition_healthy("test-topic", 2)  # Low lag

    def test_health_summary(self):
        """Test health summary generation."""
        lag_data = {"topic1": {0: 100, 1: 500}, "topic2": {0: 50, 1: 200, 2: 75}}
        manager = self.create_health_manager(lag_data)

        # Refresh both topics
        manager.force_refresh("topic1")
        manager.force_refresh("topic2")

        # Get summary
        summary = manager.get_health_summary()

        assert summary["topics"] == 2
        assert summary["total_partitions"] == 5
        assert "healthy_partitions" in summary
        assert "last_refresh" in summary
        assert "config" in summary
        assert "topics_detail" in summary

        # Check topic details
        assert "topic1" in summary["topics_detail"]
        assert "topic2" in summary["topics_detail"]

    def test_error_handling_lag_collector_failure(self):
        """Test error handling when lag collector fails."""
        lag_collector = MockLagDataCollector(should_fail=True)
        health_calculator = MockHotPartitionCalculator()
        cache = DefaultLocalCache(CacheConfig())
        config = HealthManagerConfig()

        manager = DefaultHealthManager(lag_collector, health_calculator, cache, config)

        # Force refresh should not crash
        manager.force_refresh("test-topic")

        # Should still return fallback partitions
        partitions = manager.get_healthy_partitions("test-topic")
        assert partitions == [0, 1, 2]  # Default partition count

    def test_error_handling_calculator_failure(self):
        """Test error handling when health calculator fails."""
        lag_collector = MockLagDataCollector({"test-topic": {0: 100, 1: 200}})
        health_calculator = MockHotPartitionCalculator(should_fail=True)
        cache = DefaultLocalCache(CacheConfig())
        config = HealthManagerConfig()

        manager = DefaultHealthManager(lag_collector, health_calculator, cache, config)

        # Force refresh should not crash
        manager.force_refresh("test-topic")

        # Should still return fallback partitions
        partitions = manager.get_healthy_partitions("test-topic")
        assert partitions == [0, 1, 2]  # Default partition count

    @pytest.mark.asyncio
    async def test_background_refresh_async(self):
        """Test background refresh in async context."""
        lag_data = {"test-topic": {0: 100, 1: 500}}
        config = HealthManagerConfig(refresh_interval_seconds=0.05)
        manager = self.create_health_manager(lag_data, config)

        # Add topic to monitoring by forcing an initial refresh
        manager.force_refresh("test-topic")

        # Start background refresh
        await manager.start()

        # Wait for background refresh
        await asyncio.sleep(0.15)

        # Check that health data was refreshed
        health = manager.get_topic_health("test-topic")
        assert health is not None
        assert health.topic == "test-topic"

        await manager.stop()

    def test_background_refresh_sync(self):
        """Test background refresh in sync context."""
        lag_data = {"test-topic": {0: 100, 1: 500}}
        config = HealthManagerConfig(refresh_interval_seconds=0.05)
        manager = self.create_health_manager(lag_data, config)

        # Add topic to monitoring by forcing an initial refresh
        manager.force_refresh("test-topic")

        # Simulate sync context
        manager._is_async_context = False
        manager._running = True

        from kafka_smart_producer.threading import create_sync_background_refresh

        manager._background_refresh = create_sync_background_refresh(
            manager._refresh_all_topics, 0.05
        )
        manager._background_refresh.start()

        # Wait for background refresh
        time.sleep(0.15)

        # Check that health data was refreshed
        health = manager.get_topic_health("test-topic")
        assert health is not None
        assert health.topic == "test-topic"

        # Stop background refresh
        manager._background_refresh.stop()
        manager._running = False

    def test_concurrent_access(self):
        """Test concurrent access to health manager."""
        lag_data = {"test-topic": {0: 100, 1: 200, 2: 50}}
        manager = self.create_health_manager(lag_data)

        # Refresh initial data
        manager.force_refresh("test-topic")

        results = []
        errors = []

        def worker():
            try:
                for _ in range(50):
                    partitions = manager.get_healthy_partitions("test-topic")
                    partition = partitions[0] if partitions else 0
                    results.append(partition)

                    if len(results) % 10 == 0:
                        manager.force_refresh("test-topic")
            except Exception as e:
                errors.append(e)

        # Run multiple threads
        threads = [threading.Thread(target=worker) for _ in range(5)]
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        assert len(results) == 250  # 5 threads * 50 selections each
        assert all(isinstance(r, int) for r in results)

    def test_max_refresh_failures(self):
        """Test handling of max refresh failures."""
        lag_collector = MockLagDataCollector(should_fail=True)
        health_calculator = MockHotPartitionCalculator()
        cache = DefaultLocalCache(CacheConfig())
        config = HealthManagerConfig(max_refresh_failures=2)

        manager = DefaultHealthManager(lag_collector, health_calculator, cache, config)

        # Add topic to monitoring
        manager._topic_metadata["test-topic"] = TopicHealth(
            topic="test-topic",
            partitions={},
            last_refresh=time.time(),
            healthy_partitions=[],
            total_partitions=0,
        )

        # Trigger multiple refresh failures
        for _ in range(3):
            manager._refresh_all_topics()

        # Topic should be removed after max failures
        assert "test-topic" not in manager._topic_metadata


class TestExceptionClasses:
    """Test custom exception classes."""

    def test_no_healthy_partitions_error(self):
        """Test NoHealthyPartitionsError exception."""
        error = NoHealthyPartitionsError("No healthy partitions available")
        assert isinstance(error, PartitionSelectionError)
        assert str(error) == "No healthy partitions available"

    def test_partition_selection_error_empty_partitions(self):
        """Test PartitionSelectionError with empty partitions."""
        manager = DefaultHealthManager(
            MockLagDataCollector(),
            MockHotPartitionCalculator(),
            DefaultLocalCache(CacheConfig()),
            HealthManagerConfig(),
        )

        # Should handle empty partitions gracefully
        partitions = manager.get_healthy_partitions(
            "test-topic", available_partitions=[]
        )
        assert partitions == []  # Empty available partitions should return empty


class TestPartitionSelectionEdgeCases:
    """Test edge cases in partition selection."""

    def test_empty_lag_data(self):
        """Test handling of empty lag data."""
        manager = DefaultHealthManager(
            MockLagDataCollector({"test-topic": {}}),
            MockHotPartitionCalculator(),
            DefaultLocalCache(CacheConfig()),
            HealthManagerConfig(),
        )

        manager.force_refresh("test-topic")

        # Should fallback gracefully
        partitions = manager.get_healthy_partitions("test-topic")
        assert partitions == [0, 1, 2]  # Default partition count

    def test_zero_weights_weighted_random(self):
        """Test weighted random with zero weights."""
        manager = DefaultHealthManager(
            MockLagDataCollector(),
            MockHotPartitionCalculator(),
            DefaultLocalCache(CacheConfig()),
            HealthManagerConfig(),
        )

        # Test getting healthy partitions when no health data
        partitions = manager.get_healthy_partitions("test-topic")
        assert partitions == [0, 1, 2]  # Default partition count

    def test_single_partition_topic(self):
        """Test handling of single partition topics."""
        lag_data = {"single-partition": {0: 100}}
        config = HealthManagerConfig(
            health_threshold=0.0,  # Ensure partition 0 is healthy
        )
        manager = DefaultHealthManager(
            MockLagDataCollector(lag_data),
            MockHotPartitionCalculator(),
            DefaultLocalCache(CacheConfig()),
            config,
        )

        manager.force_refresh("single-partition")

        # Verify the topic health was created correctly
        health = manager.get_topic_health("single-partition")
        assert health is not None
        assert 0 in health.partitions
        assert health.partitions[0].is_healthy
        assert health.healthy_partitions == [0]

        # Should always return the only partition
        partitions = manager.get_healthy_partitions("single-partition")
        assert partitions == [0]
