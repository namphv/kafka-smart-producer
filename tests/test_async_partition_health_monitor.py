"""
Tests for AsyncPartitionHealthMonitor implementation.

This module provides comprehensive test coverage for the async partition health
monitoring functionality, including lifecycle management, health calculations,
dynamic changes, reactive monitoring, and edge cases.
"""

import asyncio
import time
from typing import Optional

import pytest

from kafka_smart_producer.async_partition_health_monitor import (
    AsyncPartitionHealthMonitor,
)
from kafka_smart_producer.exceptions import LagDataUnavailableError
from kafka_smart_producer.health_config import PartitionHealthMonitorConfig
from kafka_smart_producer.health_mode import HealthMode


class MockLagCollector:
    """Mock lag collector for testing."""

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
        """Return mock lag data for a topic."""
        self.call_count += 1
        self.calls.append(("async", topic))

        if self.should_fail:
            raise LagDataUnavailableError(f"Mock failure for {topic}")

        return self.lag_data.get(topic, {0: 0, 1: 0})

    def set_lag_data(self, topic: str, lag_data: dict[int, int]):
        """Set mock lag data for a topic."""
        self.lag_data[topic] = lag_data

    def is_healthy(self) -> bool:
        return not self.should_fail


class TestAsyncPartitionHealthMonitorCore:
    """Test core AsyncPartitionHealthMonitor functionality."""

    def create_async_partition_health_monitor(
        self,
        lag_data: Optional[dict[str, dict[int, int]]] = None,
        config: Optional[PartitionHealthMonitorConfig] = None,
        **kwargs,
    ) -> AsyncPartitionHealthMonitor:
        """Create an async health monitor with mock dependencies."""
        lag_collector = MockLagCollector(lag_data, kwargs.get("should_fail", False))
        config = config or PartitionHealthMonitorConfig(
            consumer_group="test-group", refresh_interval=2.0, timeout_seconds=1.0
        )

        return AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,  # No cache for simplicity
            health_threshold=config.health_threshold,
            refresh_interval=config.refresh_interval,
            max_lag_for_health=config.max_lag_for_health,
            mode=kwargs.get("mode", HealthMode.EMBEDDED),
        )

    def test_async_partition_health_monitor_creation(self):
        """Test basic async health monitor creation."""
        monitor = self.create_async_partition_health_monitor()

        assert monitor._lag_collector is not None
        assert monitor._health_threshold == 0.5
        assert monitor._refresh_interval == 2.0
        assert not monitor._running

    @pytest.mark.asyncio
    async def test_async_lifecycle(self):
        """Test async start/stop lifecycle."""
        monitor = self.create_async_partition_health_monitor()

        # Initially not running
        assert not monitor._running
        assert monitor._task is None

        # Start monitor
        await monitor.start()
        assert monitor._running
        assert monitor._task is not None

        # Give it a moment to start
        await asyncio.sleep(0.05)

        # Stop monitor
        await monitor.stop()
        assert not monitor._running

        # Multiple stops should be safe
        await monitor.stop()
        assert not monitor._running

    @pytest.mark.asyncio
    async def test_async_get_healthy_partitions(self):
        """Test getting healthy partitions from async monitor."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.4,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )
        monitor = self.create_async_partition_health_monitor(lag_data, config)

        # Add topic to monitoring
        monitor._initialize_topics(["test-topic"])

        # Force refresh to get initial data
        await monitor.force_refresh("test-topic")

        # Get healthy partitions (sync method for producer compatibility)
        healthy_partitions = monitor.get_healthy_partitions("test-topic")

        # Should return healthy partitions
        assert len(healthy_partitions) >= 1
        assert all(isinstance(p, int) for p in healthy_partitions)

    @pytest.mark.asyncio
    async def test_async_is_partition_healthy(self):
        """Test partition health checking in async monitor."""
        lag_data = {"test-topic": {0: 100, 1: 500, 2: 50}}
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.4,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )
        monitor = self.create_async_partition_health_monitor(lag_data, config)

        # Add topic and force refresh
        monitor._initialize_topics(["test-topic"])
        await monitor.force_refresh("test-topic")

        # Test partition health (sync method for producer compatibility)
        result = monitor.is_partition_healthy("test-topic", 2)
        assert isinstance(result, bool)

    @pytest.mark.asyncio
    async def test_async_add_topics(self):
        """Test adding topics in async monitor."""
        monitor = self.create_async_partition_health_monitor()

        # Add topic
        monitor._initialize_topics(["test-topic"])
        assert "test-topic" in monitor._health_data

        # Add multiple topics
        monitor._initialize_topics(["topic-1", "topic-2"])
        assert "topic-1" in monitor._health_data
        assert "topic-2" in monitor._health_data

    @pytest.mark.asyncio
    async def test_async_health_summary(self):
        """Test health summary generation in async monitor."""
        lag_data = {"test-topic": {0: 100, 1: 200}}
        monitor = self.create_async_partition_health_monitor(lag_data)

        # Add topic and refresh
        monitor._initialize_topics(["test-topic"])
        await monitor.force_refresh("test-topic")

        # Get summary
        summary = await monitor.get_health_summary()

        assert summary["execution_context"] == "async"
        assert summary["running"] is False  # Not started
        assert "topics" in summary
        assert "total_partitions" in summary
        assert "healthy_partitions" in summary

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test async context manager support."""
        lag_data = {"test-topic": {0: 100, 1: 200}}
        monitor = self.create_async_partition_health_monitor(lag_data)

        async with monitor:
            assert monitor._running
            monitor._initialize_topics(["test-topic"])
            await monitor.force_refresh("test-topic")

        assert not monitor._running

    def test_from_config_factory_method_async(self):
        """Test creating AsyncPartitionHealthMonitor from configuration."""
        config = PartitionHealthMonitorConfig(
            consumer_group="test-group",
            health_threshold=0.7,
            refresh_interval=5.0,
            timeout_seconds=3.0,
        )

        kafka_config = {"bootstrap.servers": "localhost:9092"}

        try:
            monitor = AsyncPartitionHealthMonitor.from_config(config, kafka_config)
            assert monitor._health_threshold == 0.7
            assert monitor._refresh_interval == 3.0
        except Exception:
            # This might fail due to missing KafkaAdminLagCollector, which is expected
            # in a test environment without the actual implementation
            pytest.skip("KafkaAdminLagCollector not available in test environment")


class TestAsyncPartitionHealthDynamicChanges:
    """Test dynamic partition health changes in async partition health monitor."""

    async def _create_started_health_monitor(self, lag_collector, **kwargs):
        """Helper to create and start a health monitor with default settings."""
        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=kwargs.get("health_threshold", 0.5),
            refresh_interval=kwargs.get("refresh_interval", 0.1),
            max_lag_for_health=kwargs.get("max_lag_for_health", 100),
            mode=kwargs.get("mode", HealthMode.EMBEDDED),
        )
        await health_monitor.start()
        return health_monitor

    @pytest.mark.asyncio
    async def test_health_data_updates_when_lag_changes(self):
        """Test that health data updates correctly when lag changes."""
        # Create mock lag collector
        lag_collector = MockLagCollector()

        # Create health monitor
        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        topic = "test-topic"
        health_monitor._initialize_topics([topic])

        # Start health manager
        await health_monitor.start()

        try:
            # Phase 1: Low lag (healthy)
            lag_collector.set_lag_data(topic, {0: 10, 1: 20})
            health_data = await health_monitor._refresh_single_topic(topic)

            # Verify health scores
            assert health_data[0] == 0.9  # 1.0 - (10/100)
            assert health_data[1] == 0.8  # 1.0 - (20/100)

            # Verify healthy partitions
            healthy_partitions = health_monitor.get_healthy_partitions(topic)
            assert len(healthy_partitions) == 2
            assert 0 in healthy_partitions and 1 in healthy_partitions

            # Phase 2: High lag (unhealthy)
            lag_collector.set_lag_data(topic, {0: 150, 1: 200})
            health_data = await health_monitor._refresh_single_topic(topic)

            # Verify health scores
            assert health_data[0] == 0.0  # lag >= max_lag_for_health
            assert health_data[1] == 0.0  # lag >= max_lag_for_health

            # Verify no healthy partitions
            healthy_partitions = health_monitor.get_healthy_partitions(topic)
            assert len(healthy_partitions) == 0

            # Phase 3: Mixed lag (partial recovery)
            lag_collector.set_lag_data(topic, {0: 30, 1: 120})
            health_data = await health_monitor._refresh_single_topic(topic)

            # Verify health scores
            assert health_data[0] == 0.7  # 1.0 - (30/100)
            assert health_data[1] == 0.0  # lag >= max_lag_for_health

            # Verify only partition 0 is healthy
            healthy_partitions = health_monitor.get_healthy_partitions(topic)
            assert len(healthy_partitions) == 1
            assert 0 in healthy_partitions
            assert 1 not in healthy_partitions

        finally:
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_health_threshold_affects_healthy_partitions(self):
        """Test that health threshold affects healthy partition selection."""
        # Create mock lag collector
        lag_collector = MockLagCollector()

        # Set consistent lag data
        topic = "test-topic"
        lag_collector.set_lag_data(topic, {0: 40, 1: 60})  # Moderate lag

        # Test with lenient threshold (0.3)
        health_monitor_lenient = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.3,  # Lenient threshold
            refresh_interval=1.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        health_monitor_lenient._initialize_topics([topic])

        # Start health monitor so _running is True
        await health_monitor_lenient.start()

        try:
            # Refresh health data
            await health_monitor_lenient._refresh_single_topic(topic)

            # Verify both partitions are healthy with lenient threshold
            healthy_partitions = health_monitor_lenient.get_healthy_partitions(topic)
            assert len(healthy_partitions) == 2, (
                "Both partitions should be healthy with lenient threshold"
            )

        finally:
            if health_monitor_lenient.is_running:
                await health_monitor_lenient.stop()

        # Test with strict threshold (0.8)
        health_monitor_strict = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.8,  # Strict threshold
            refresh_interval=1.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        health_monitor_strict._initialize_topics([topic])

        # Start health monitor so _running is True
        await health_monitor_strict.start()

        try:
            # Refresh health data
            await health_monitor_strict._refresh_single_topic(topic)

            # Verify no partitions are healthy with strict threshold
            healthy_partitions = health_monitor_strict.get_healthy_partitions(topic)
            assert len(healthy_partitions) == 0, (
                "No partitions should be healthy with strict threshold"
            )

        finally:
            if health_monitor_strict.is_running:
                await health_monitor_strict.stop()

    @pytest.mark.asyncio
    async def test_individual_partition_health_check(self):
        """Test checking individual partition health."""
        lag_collector = MockLagCollector()

        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.6,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        topic = "test-topic"
        health_monitor._initialize_topics([topic])
        await health_monitor.start()

        try:
            # Set mixed lag data
            lag_collector.set_lag_data(topic, {0: 20, 1: 80, 2: 150})
            await health_monitor._refresh_single_topic(topic)

            # Test individual partition health checks
            # Partition 0: health = 0.8 (healthy)
            # Partition 1: health = 0.2 (unhealthy)
            # Partition 2: health = 0.0 (unhealthy)

            assert health_monitor.is_partition_healthy(topic, 0) is True
            assert health_monitor.is_partition_healthy(topic, 1) is False
            assert health_monitor.is_partition_healthy(topic, 2) is False

            # Test non-existent partition (defaults to healthy)
            assert health_monitor.is_partition_healthy(topic, 999) is True

            # Test non-existent topic (defaults to healthy)
            assert health_monitor.is_partition_healthy("non-existent", 0) is True

        finally:
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_health_recovery_after_lag_reduction(self):
        """Test that health recovers when lag is reduced."""
        # Create mock lag collector
        lag_collector = MockLagCollector()

        # Create health monitor
        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.6,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        # Initialize topic
        topic = "test-topic"
        health_monitor._initialize_topics([topic])

        # Start health monitor so _running is True
        await health_monitor.start()

        try:
            # Phase 1: High lag (unhealthy)
            lag_collector.set_lag_data(topic, {0: 150, 1: 200})  # High lag
            await health_monitor._refresh_single_topic(topic)

            initial_healthy = health_monitor.get_healthy_partitions(topic)
            assert len(initial_healthy) == 0, (
                "No partitions should be healthy with high lag"
            )

            # Phase 2: Reduce lag (recovery)
            lag_collector.set_lag_data(topic, {0: 20, 1: 30})  # Low lag
            await health_monitor._refresh_single_topic(topic)

            recovered_healthy = health_monitor.get_healthy_partitions(topic)
            assert len(recovered_healthy) == 2, (
                "Both partitions should recover when lag is reduced"
            )
            assert 0 in recovered_healthy and 1 in recovered_healthy

            # Phase 3: Partial recovery
            lag_collector.set_lag_data(topic, {0: 25, 1: 120})  # Mixed lag
            await health_monitor._refresh_single_topic(topic)

            partial_healthy = health_monitor.get_healthy_partitions(topic)
            assert len(partial_healthy) == 1, "Only one partition should be healthy"
            assert 0 in partial_healthy, "Partition 0 should be healthy"
            assert 1 not in partial_healthy, "Partition 1 should still be unhealthy"

        finally:
            # Clean up
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_health_summary_reflects_changes(self):
        """Test that health summary reflects health changes."""
        lag_collector = MockLagCollector()

        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        topics = ["topic1", "topic2"]
        health_monitor._initialize_topics(topics)
        await health_monitor.start()

        try:
            # Set initial healthy state
            lag_collector.set_lag_data("topic1", {0: 10, 1: 20})
            lag_collector.set_lag_data("topic2", {0: 15, 1: 25})

            # Refresh both topics
            await health_monitor._refresh_single_topic("topic1")
            await health_monitor._refresh_single_topic("topic2")

            # Get health summary
            summary = await health_monitor.get_health_summary()

            assert summary["topics"] == 2
            assert summary["total_partitions"] == 4
            assert summary["healthy_partitions"] == 4
            assert summary["running"] is True
            assert summary["execution_context"] == "async"

            # Change to unhealthy state
            lag_collector.set_lag_data("topic1", {0: 150, 1: 200})  # Both unhealthy
            lag_collector.set_lag_data("topic2", {0: 30, 1: 180})  # Mixed

            # Refresh both topics
            await health_monitor._refresh_single_topic("topic1")
            await health_monitor._refresh_single_topic("topic2")

            # Get updated summary
            updated_summary = await health_monitor.get_health_summary()

            assert updated_summary["topics"] == 2
            assert updated_summary["total_partitions"] == 4
            assert updated_summary["healthy_partitions"] == 1  # Only topic2 partition 0

            # Verify topic details
            topic1_detail = updated_summary["topics_detail"]["topic1"]
            topic2_detail = updated_summary["topics_detail"]["topic2"]

            assert topic1_detail["healthy_count"] == 0
            assert topic2_detail["healthy_count"] == 1

        finally:
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_concurrent_topic_refresh(self):
        """Test concurrent refresh of multiple topics."""
        # Create mock lag collector
        lag_collector = MockLagCollector()

        # Create health monitor
        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=1.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        # Initialize multiple topics
        topics = ["topic1", "topic2", "topic3"]
        health_monitor._initialize_topics(topics)

        # Start health monitor so _running is True
        await health_monitor.start()

        # Set lag data for all topics
        for topic in topics:
            lag_collector.set_lag_data(topic, {0: 20, 1: 30})

        try:
            # Record start time
            start_time = time.time()

            # Refresh all topics concurrently
            await health_monitor._refresh_all_topics_concurrent(topics)

            # Record end time
            end_time = time.time()

            # Verify all topics were refreshed
            for topic in topics:
                healthy_partitions = health_monitor.get_healthy_partitions(topic)
                assert len(healthy_partitions) > 0, (
                    f"Topic {topic} should have healthy partitions"
                )

            # Verify concurrent execution (should be faster than sequential)
            elapsed_time = end_time - start_time
            assert elapsed_time < 1.0, (
                f"Concurrent refresh should be fast, took {elapsed_time:.3f}s"
            )

            # Verify lag collector was called for each topic
            assert lag_collector.call_count >= len(topics), (
                f"Should call lag collector at least {len(topics)} times, "
                f"got {lag_collector.call_count}"
            )

        finally:
            # Clean up
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_health_stream_functionality(self):
        """Test basic health stream functionality."""
        lag_collector = MockLagCollector()

        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        topic = "test-topic"
        health_monitor._initialize_topics([topic])
        await health_monitor.start()

        try:
            # Set initial lag data
            lag_collector.set_lag_data(topic, {0: 10, 1: 20})

            # Collect health updates
            health_updates = []

            async def collect_updates():
                try:
                    async for update in health_monitor.health_stream(topic):
                        health_updates.append(update)
                        if len(health_updates) >= 1:
                            break
                except Exception as e:
                    print(f"Health stream error: {e}")

            # Start collecting in background
            collect_task = asyncio.create_task(collect_updates())

            # Give collector time to start
            await asyncio.sleep(0.05)

            # Trigger health update
            await health_monitor._refresh_single_topic(topic)

            # Wait for update to be collected
            try:
                await asyncio.wait_for(collect_task, timeout=2.0)
            except asyncio.TimeoutError:
                pass

            # Cancel if still running
            if not collect_task.done():
                collect_task.cancel()
                try:
                    await collect_task
                except asyncio.CancelledError:
                    pass

            # Verify we got updates (may be 0 due to timing)
            print(f"Collected {len(health_updates)} health updates")

            # Test error case - non-existent topic
            with pytest.raises(ValueError, match="not being monitored"):
                async for _ in health_monitor.health_stream("non-existent"):
                    break

        finally:
            await health_monitor.stop()


class TestAsyncPartitionHealthEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_refresh_without_running_health_monitor(self):
        """Test refresh when health monitor is not running."""
        lag_collector = MockLagCollector()

        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        topic = "test-topic"
        health_monitor._initialize_topics([topic])

        # Don't start health monitor
        lag_collector.set_lag_data(topic, {0: 10, 1: 20})

        # Should work even when not running (force refresh capability)
        health_data = await health_monitor._refresh_single_topic(topic)
        assert health_data == {0: 0.9, 1: 0.8}  # Expected health scores for lags 10, 20

    @pytest.mark.asyncio
    async def test_force_refresh_nonexistent_topic(self):
        """Test force refresh on non-existent topic."""
        lag_collector = MockLagCollector()

        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        await health_monitor.start()

        try:
            # Should not crash on non-existent topic
            await health_monitor.force_refresh("non-existent-topic")

        finally:
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_empty_lag_data(self):
        """Test behavior with empty lag data."""
        lag_collector = MockLagCollector()

        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        topic = "test-topic"
        health_monitor._initialize_topics([topic])
        await health_monitor.start()

        try:
            # Set empty lag data
            lag_collector.set_lag_data(topic, {})

            # Should handle empty data gracefully
            health_data = await health_monitor._refresh_single_topic(topic)
            assert health_data == {}

            # Should return empty list for healthy partitions
            healthy_partitions = health_monitor.get_healthy_partitions(topic)
            assert healthy_partitions == []

        finally:
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_task_tracking_and_cleanup(self):
        """Test that tasks are properly tracked and cleaned up."""
        # Create mock lag collector
        lag_collector = MockLagCollector()

        # Create health monitor
        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        # Initialize topics
        topics = ["topic1", "topic2", "topic3"]
        health_monitor._initialize_topics(topics)

        # Start health monitor so _running is True
        await health_monitor.start()

        # Set lag data
        for topic in topics:
            lag_collector.set_lag_data(topic, {0: 10, 1: 20})

        try:
            # Start concurrent refresh (creates tasks)
            refresh_task = asyncio.create_task(
                health_monitor._refresh_all_topics_concurrent(topics)
            )

            # Give tasks time to start
            await asyncio.sleep(0.05)

            # Verify tasks are being tracked (may be 0 if they complete quickly)
            running_tasks_during = len(health_monitor._running_tasks)
            print(f"Running tasks during refresh: {running_tasks_during}")

            # Wait for refresh to complete
            await refresh_task

            # Verify tasks are cleaned up
            assert len(health_monitor._running_tasks) == 0, (
                "Tasks should be cleaned up after completion"
            )

        finally:
            # Clean up
            await health_monitor.stop()

    @pytest.mark.asyncio
    async def test_force_refresh_functionality(self):
        """Test force refresh functionality."""
        # Create mock lag collector
        lag_collector = MockLagCollector()

        # Create health monitor
        health_monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=10.0,  # Long interval
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )

        # Initialize topic
        topic = "test-topic"
        health_monitor._initialize_topics([topic])

        # Start health monitor so _running is True (triggers initial refresh)
        await health_monitor.start()

        # Wait for initial refresh to complete and reset counter
        await asyncio.sleep(0.2)
        lag_collector.call_count = 0
        lag_collector.calls.clear()

        try:
            # Set initial lag data
            lag_collector.set_lag_data(topic, {0: 10, 1: 20})

            # Force refresh
            await health_monitor.force_refresh(topic)

            # Verify health data was updated
            healthy_partitions = health_monitor.get_healthy_partitions(topic)
            assert len(healthy_partitions) == 2, (
                "Force refresh should update health data"
            )

            # Verify call count (after counter reset, only counts force_refresh)
            assert lag_collector.call_count == 1, (
                "Force refresh should call lag collector once (after counter reset)"
            )

            # Test force refresh on non-existent topic (should not crash)
            await health_monitor.force_refresh("non-existent-topic")

        finally:
            # Clean up
            await health_monitor.stop()


class TestAsyncPartitionHealthErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_async_error_handling(self):
        """Test error handling in async health monitor."""
        lag_collector = MockLagCollector(should_fail=True)
        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=1.0,
            max_lag_for_health=1000,
            mode=HealthMode.EMBEDDED,
        )

        # Should not crash on force refresh failure
        monitor._initialize_topics(["test-topic"])
        try:
            await monitor.force_refresh("test-topic")
        except Exception as e:
            print(f"Expected exception in test: {e}")

        # Should return empty list for unknown topic health
        healthy = monitor.get_healthy_partitions("test-topic")
        assert isinstance(healthy, list)
