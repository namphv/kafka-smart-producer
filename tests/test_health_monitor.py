"""Tests for PartitionHealthMonitor (sync and async modes)."""

import asyncio
import time

import pytest

from kafka_smart_producer.health.monitor import PartitionHealthMonitor


class MockCollector:
    """Mock collector returning configurable lag data."""

    def __init__(self, lag_data=None):
        self._lag_data = lag_data or {}
        self.call_count = 0

    def get_lag_data(self, topic):
        self.call_count += 1
        if topic in self._lag_data:
            return self._lag_data[topic]
        return {}

    def is_healthy(self):
        return True

    def set_lag(self, topic, data):
        self._lag_data[topic] = data


class FailingCollector:
    def get_lag_data(self, topic):
        raise RuntimeError("collector down")

    def is_healthy(self):
        return False


class TestPartitionHealthMonitorCore:
    def test_initialize_topics(self):
        monitor = PartitionHealthMonitor(MockCollector())
        monitor.initialize_topics(["t1", "t2"])
        # No health data yet, but topics registered
        assert monitor.get_healthy_partitions("t1") == []

    def test_get_healthy_partitions_default_healthy(self):
        monitor = PartitionHealthMonitor(MockCollector())
        # Unknown partition defaults to healthy
        assert monitor.is_partition_healthy("t", 0) is True

    def test_get_healthy_partitions_after_refresh(self):
        collector = MockCollector({"t1": {0: 0, 1: 500, 2: 1000}})
        monitor = PartitionHealthMonitor(collector, health_threshold=0.5, max_lag=1000)
        monitor.initialize_topics(["t1"])
        monitor.force_refresh("t1")

        healthy = monitor.get_healthy_partitions("t1")
        assert 0 in healthy  # lag=0, score=1.0
        assert 1 in healthy  # lag=500, score=0.5
        assert 2 not in healthy  # lag=1000, score=0.0

    def test_force_refresh_unregistered_topic_is_noop(self):
        monitor = PartitionHealthMonitor(MockCollector())
        monitor.force_refresh("unregistered")  # should not raise

    def test_get_health_scores(self):
        collector = MockCollector({"t1": {0: 0, 1: 1000}})
        monitor = PartitionHealthMonitor(collector, max_lag=1000)
        monitor.initialize_topics(["t1"])
        monitor.force_refresh("t1")

        scores = monitor.get_health_scores("t1")
        assert scores[0] == 1.0
        assert scores[1] == 0.0

    def test_empty_lag_data_returns_no_healthy(self):
        collector = MockCollector({"t1": {}})
        monitor = PartitionHealthMonitor(collector)
        monitor.initialize_topics(["t1"])
        monitor.force_refresh("t1")
        assert monitor.get_healthy_partitions("t1") == []

    def test_collector_failure_preserves_old_data(self):
        collector = MockCollector({"t1": {0: 0}})
        monitor = PartitionHealthMonitor(collector, max_lag=1000)
        monitor.initialize_topics(["t1"])
        monitor.force_refresh("t1")
        assert monitor.get_healthy_partitions("t1") == [0]

        # Now collector fails
        collector._lag_data["t1"] = None  # will cause error
        # We need to make it actually fail

        def fail(topic):
            raise RuntimeError("down")

        collector.get_lag_data = fail
        monitor.force_refresh("t1")
        # Old data should be preserved (force_refresh swallows on None return)
        # Actually force_refresh calls _collect_and_score which returns None on error
        # and force_refresh only updates if scores is not None
        assert monitor.get_healthy_partitions("t1") == [0]

    def test_get_summary(self):
        collector = MockCollector({"t1": {0: 0, 1: 500, 2: 1000}})
        monitor = PartitionHealthMonitor(collector, health_threshold=0.5, max_lag=1000)
        monitor.initialize_topics(["t1"])
        monitor.force_refresh("t1")

        summary = monitor.get_summary()
        assert summary["topics"] == 1
        assert summary["total_partitions"] == 3
        assert summary["healthy_partitions"] == 2
        assert summary["running"] is False

    def test_custom_scorer(self):
        class AlwaysHealthyScorer:
            def calculate_scores(self, lag_data):
                return dict.fromkeys(lag_data, 1.0)

        collector = MockCollector({"t1": {0: 99999}})
        monitor = PartitionHealthMonitor(collector, scorer=AlwaysHealthyScorer())
        monitor.initialize_topics(["t1"])
        monitor.force_refresh("t1")
        assert monitor.get_healthy_partitions("t1") == [0]


class TestPartitionHealthMonitorSync:
    def test_sync_lifecycle(self):
        collector = MockCollector({"t1": {0: 0, 1: 0}})
        monitor = PartitionHealthMonitor(collector, refresh_interval=0.1, max_lag=1000)
        monitor.initialize_topics(["t1"])

        monitor.start_sync()
        assert monitor.is_running

        # Wait for at least one refresh
        time.sleep(0.3)
        assert collector.call_count >= 1
        assert len(monitor.get_healthy_partitions("t1")) == 2

        monitor.stop_sync()
        assert not monitor.is_running

    def test_sync_double_start_is_noop(self):
        monitor = PartitionHealthMonitor(MockCollector(), refresh_interval=0.1)
        monitor.start_sync()
        monitor.start_sync()  # should not raise
        monitor.stop_sync()

    def test_sync_stop_without_start(self):
        monitor = PartitionHealthMonitor(MockCollector())
        monitor.stop_sync()  # should not raise

    def test_sync_context_manager(self):
        collector = MockCollector({"t1": {0: 0}})
        monitor = PartitionHealthMonitor(collector, refresh_interval=0.1, max_lag=1000)
        monitor.initialize_topics(["t1"])

        with monitor:
            assert monitor.is_running
            time.sleep(0.2)

        assert not monitor.is_running

    def test_sync_survives_collector_errors(self):
        monitor = PartitionHealthMonitor(FailingCollector(), refresh_interval=0.1)
        monitor.initialize_topics(["t1"])

        monitor.start_sync()
        time.sleep(0.3)
        # Should still be running despite errors
        assert monitor.is_running
        monitor.stop_sync()

    def test_sync_health_updates_dynamically(self):
        collector = MockCollector({"t1": {0: 0, 1: 0}})
        monitor = PartitionHealthMonitor(
            collector, refresh_interval=0.1, max_lag=1000, health_threshold=0.5
        )
        monitor.initialize_topics(["t1"])

        monitor.start_sync()
        time.sleep(0.2)
        assert len(monitor.get_healthy_partitions("t1")) == 2

        # Make partition 1 unhealthy
        collector.set_lag("t1", {0: 0, 1: 2000})
        time.sleep(0.3)
        healthy = monitor.get_healthy_partitions("t1")
        assert 0 in healthy
        assert 1 not in healthy

        monitor.stop_sync()


class TestPartitionHealthMonitorAsync:
    @pytest.fixture
    def collector(self):
        return MockCollector({"t1": {0: 0, 1: 0}})

    async def test_async_lifecycle(self, collector):
        monitor = PartitionHealthMonitor(collector, refresh_interval=0.1, max_lag=1000)
        monitor.initialize_topics(["t1"])

        await monitor.start_async()
        assert monitor.is_running

        await asyncio.sleep(0.3)
        assert collector.call_count >= 1
        assert len(monitor.get_healthy_partitions("t1")) == 2

        await monitor.stop_async()
        assert not monitor.is_running

    async def test_async_double_start_is_noop(self):
        monitor = PartitionHealthMonitor(MockCollector(), refresh_interval=0.1)
        await monitor.start_async()
        await monitor.start_async()  # noop
        await monitor.stop_async()

    async def test_async_stop_without_start(self):
        monitor = PartitionHealthMonitor(MockCollector())
        await monitor.stop_async()  # should not raise

    async def test_async_context_manager(self, collector):
        monitor = PartitionHealthMonitor(collector, refresh_interval=0.1, max_lag=1000)
        monitor.initialize_topics(["t1"])

        async with monitor:
            assert monitor.is_running
            await asyncio.sleep(0.2)

        assert not monitor.is_running

    async def test_async_concurrent_topic_refresh(self):
        collector = MockCollector(
            {
                "t1": {0: 0},
                "t2": {0: 0, 1: 0},
                "t3": {0: 0, 1: 0, 2: 0},
            }
        )
        monitor = PartitionHealthMonitor(collector, refresh_interval=0.1, max_lag=1000)
        monitor.initialize_topics(["t1", "t2", "t3"])

        await monitor.start_async()
        await asyncio.sleep(0.3)

        assert len(monitor.get_healthy_partitions("t1")) == 1
        assert len(monitor.get_healthy_partitions("t2")) == 2
        assert len(monitor.get_healthy_partitions("t3")) == 3

        await monitor.stop_async()

    async def test_async_survives_collector_errors(self):
        monitor = PartitionHealthMonitor(FailingCollector(), refresh_interval=0.1)
        monitor.initialize_topics(["t1"])

        await monitor.start_async()
        await asyncio.sleep(0.3)
        assert monitor.is_running
        await monitor.stop_async()

    async def test_async_dynamic_health_updates(self, collector):
        monitor = PartitionHealthMonitor(
            collector, refresh_interval=0.1, max_lag=1000, health_threshold=0.5
        )
        monitor.initialize_topics(["t1"])

        await monitor.start_async()
        await asyncio.sleep(0.2)
        assert len(monitor.get_healthy_partitions("t1")) == 2

        collector.set_lag("t1", {0: 0, 1: 2000})
        await asyncio.sleep(0.3)
        healthy = monitor.get_healthy_partitions("t1")
        assert 0 in healthy
        assert 1 not in healthy

        await monitor.stop_async()
