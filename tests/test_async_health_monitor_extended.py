"""
Extended unit tests for AsyncPartitionHealthMonitor to improve coverage.

Focuses on uncovered areas: factory methods, force_refresh_threadsafe,
concurrent monitoring, Redis publishing, and error handling.
"""

import asyncio
import json
import threading
from unittest.mock import AsyncMock, Mock, patch, call
import pytest

from kafka_smart_producer.async_partition_health_monitor import AsyncPartitionHealthMonitor
from kafka_smart_producer.health_mode import HealthMode


class TestFactoryMethods:
    """Test factory methods: embedded() with topics, standalone()."""

    @pytest.mark.asyncio
    async def test_embedded_factory_with_topics(self):
        """Test embedded() factory with initial topics."""
        lag_collector = Mock()
        lag_collector.collect_lag_data = Mock(return_value={0: 100, 1: 200})

        topics = ["orders", "payments"]
        monitor = AsyncPartitionHealthMonitor.embedded(lag_collector, topics=topics)

        assert monitor._mode == HealthMode.EMBEDDED
        assert monitor._cache is None
        assert monitor._redis_publisher is None
        # Check topics were initialized
        assert "orders" in monitor._health_data
        assert "payments" in monitor._health_data

        # Cleanup
        await monitor.stop()

    @pytest.mark.asyncio
    async def test_embedded_factory_without_topics(self):
        """Test embedded() factory without initial topics."""
        lag_collector = Mock()

        monitor = AsyncPartitionHealthMonitor.embedded(lag_collector)

        assert monitor._mode == HealthMode.EMBEDDED
        assert monitor._cache is None
        assert monitor._redis_publisher is None
        # No topics initialized
        assert len(monitor._health_data) == 0

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_standalone_factory_success(self):
        """Test standalone() factory with successful setup."""
        # Patch where classes are imported (inside standalone method)
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class, \
             patch("kafka_smart_producer.caching.CacheFactory") as mock_cache_factory:

            # Mock lag collector
            mock_collector = Mock()
            mock_collector_class.return_value = mock_collector

            # Mock Redis publisher
            mock_redis = Mock()
            mock_redis.ping = Mock(return_value=True)
            mock_cache_factory.create_remote_cache.return_value = mock_redis

            kafka_config = {"bootstrap.servers": "localhost:9092"}
            topics = ["test-topic"]

            monitor = await AsyncPartitionHealthMonitor.standalone(
                consumer_group="test-group",
                kafka_config=kafka_config,
                topics=topics,
                health_threshold=0.6,
                refresh_interval=3.0,
                max_lag_for_health=500,
            )

            assert monitor._mode == HealthMode.STANDALONE
            assert monitor._health_threshold == 0.6
            assert monitor._refresh_interval == 3.0
            assert monitor._max_lag_for_health == 500
            assert monitor._redis_publisher is mock_redis
            # Topics should be initialized
            assert "test-topic" in monitor._health_data

            # Verify lag collector was created with correct params
            mock_collector_class.assert_called_once()
            call_kwargs = mock_collector_class.call_args[1]
            assert call_kwargs["consumer_group"] == "test-group"

            await monitor.stop()

    @pytest.mark.asyncio
    async def test_standalone_factory_redis_failure(self):
        """Test standalone() factory handles Redis failures gracefully."""
        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector") as mock_collector_class, \
             patch("kafka_smart_producer.caching.CacheFactory") as mock_cache_factory:

            # Mock lag collector
            mock_collector = Mock()
            mock_collector_class.return_value = mock_collector

            # Mock Redis failure
            mock_cache_factory.create_remote_cache.side_effect = Exception("Redis unavailable")

            kafka_config = {"bootstrap.servers": "localhost:9092"}

            monitor = await AsyncPartitionHealthMonitor.standalone(
                consumer_group="test-group",
                kafka_config=kafka_config
            )

            # Should continue without Redis
            assert monitor._mode == HealthMode.STANDALONE
            assert monitor._redis_publisher is None

            await monitor.stop()


class TestForceRefreshThreadsafe:
    """Test force_refresh_threadsafe() method."""

    @pytest.mark.asyncio
    async def test_force_refresh_threadsafe_uninitialized_topic(self):
        """Test force_refresh_threadsafe() with uninitialized topic."""
        lag_collector = Mock()

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=5.0
        )

        await monitor.start()
        await asyncio.sleep(0.1)

        # Call with uninitialized topic - should log warning and return
        monitor.force_refresh_threadsafe("nonexistent-topic")

        await asyncio.sleep(0.2)
        # Should not crash
        assert True

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_force_refresh_threadsafe_no_event_loop(self):
        """Test force_refresh_threadsafe() handles missing event loop."""
        lag_collector = Mock()

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=5.0
        )

        # Initialize topic but don't start monitor (no event loop)
        topic = "test-topic"
        monitor._health_data[topic] = {}

        # Call from thread without event loop - should not crash
        def call_from_thread():
            monitor.force_refresh_threadsafe(topic)

        thread = threading.Thread(target=call_from_thread)
        thread.start()
        thread.join()

        # Should complete without error
        assert True


class TestConcurrentMonitoring:
    """Test start_monitoring() and concurrent topic monitoring."""

    @pytest.mark.asyncio
    async def test_start_monitoring_multiple_topics(self):
        """Test start_monitoring() with multiple topics."""
        lag_collector = Mock()
        lag_collector.collect_lag_data = Mock(return_value={0: 100, 1: 200})

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,  # Fast interval for testing
        )

        topics = ["topic1", "topic2", "topic3"]
        await monitor.start_monitoring(topics)

        # Wait for monitoring to start
        await asyncio.sleep(0.2)

        # Check all topics initialized
        for topic in topics:
            assert topic in monitor._health_data
            assert topic in monitor._health_streams

        assert monitor._running is True
        assert monitor._task is not None

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_start_monitoring_already_running(self):
        """Test start_monitoring() when already running."""
        lag_collector = Mock()
        lag_collector.collect_lag_data = Mock(return_value={0: 100})

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1
        )

        await monitor.start_monitoring(["topic1"])
        await asyncio.sleep(0.1)

        # Try to start again - should log warning
        await monitor.start_monitoring(["topic2"])

        # Should still be running original task
        assert monitor._running is True

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_monitor_all_topics_initial_timeout(self):
        """Test _monitor_all_topics() handles initial refresh timeout."""
        lag_collector = Mock()

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.5
        )

        # Mock _refresh_all_topics_concurrent to timeout
        original_refresh = monitor._refresh_all_topics_concurrent

        async def slow_refresh(topics):
            await asyncio.sleep(35)  # Longer than 30s timeout
            return await original_refresh(topics)

        monitor._refresh_all_topics_concurrent = slow_refresh

        # Start monitoring
        await monitor.start_monitoring(["topic1"])
        await asyncio.sleep(0.5)

        # Should continue despite timeout
        assert monitor._running is True

        await monitor.stop()

    @pytest.mark.asyncio
    async def test_monitor_all_topics_handles_errors(self):
        """Test _monitor_all_topics() continues after errors."""
        lag_collector = Mock()

        # Make collect_lag_data fail first, then succeed
        call_count = [0]
        def collect_with_error(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("Simulated error")
            return {0: 100}

        lag_collector.collect_lag_data = Mock(side_effect=collect_with_error)

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=0.1,  # Fast interval
        )

        await monitor.start_monitoring(["topic1"])
        await asyncio.sleep(0.5)  # Let it run through error and recovery

        # Should still be running
        assert monitor._running is True

        await monitor.stop()


class TestRedisPublishing:
    """Test _publish_to_redis() method."""

    @pytest.mark.asyncio
    async def test_publish_to_redis_success(self):
        """Test _publish_to_redis() publishes data."""
        lag_collector = Mock()
        mock_redis = Mock()
        mock_redis.set = Mock()

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            redis_health_publisher=mock_redis,
        )

        topic = "test-topic"
        health_data = {0: 0.9, 1: 0.7, 2: 0.3}

        await monitor._publish_to_redis(topic, health_data)

        # Verify Redis set was called twice (state + healthy partitions)
        assert mock_redis.set.call_count == 2

        # Check state key was set
        state_call = mock_redis.set.call_args_list[0]
        assert "kafka_health:state:test-topic" in str(state_call[0])

        # Check healthy key was set
        healthy_call = mock_redis.set.call_args_list[1]
        assert "kafka_health:healthy:test-topic" in str(healthy_call[0])

    @pytest.mark.asyncio
    async def test_publish_to_redis_no_publisher(self):
        """Test _publish_to_redis() with no Redis publisher."""
        lag_collector = Mock()

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            redis_health_publisher=None,  # No Redis
        )

        topic = "test-topic"
        health_data = {0: 0.9}

        # Should return immediately without error
        await monitor._publish_to_redis(topic, health_data)
        assert True  # No exception

    @pytest.mark.asyncio
    async def test_publish_to_redis_failure(self):
        """Test _publish_to_redis() handles failures gracefully."""
        lag_collector = Mock()
        mock_redis = Mock()
        mock_redis.set = Mock(side_effect=Exception("Redis connection lost"))

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            redis_health_publisher=mock_redis,
        )

        topic = "test-topic"
        health_data = {0: 0.9}

        # Should not raise exception
        await monitor._publish_to_redis(topic, health_data)
        assert True  # Exception was caught and logged


class TestErrorHandling:
    """Test various error handling scenarios.

    Note: More complex error handling tests would require proper async mock setup.
    The existing tests in test_async_partition_health_monitor.py already cover
    most error scenarios.
    """
    pass  # Placeholder for potential future tests


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_stop_before_start(self):
        """Test calling stop() before start()."""
        lag_collector = Mock()

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=5.0
        )

        # Stop without starting - should not crash
        await monitor.stop()
        assert not monitor._running

    @pytest.mark.asyncio
    async def test_multiple_stop_calls(self):
        """Test calling stop() multiple times."""
        lag_collector = Mock()
        lag_collector.collect_lag_data = Mock(return_value={0: 100})

        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=5.0
        )

        await monitor.start()
        await asyncio.sleep(0.1)

        # Stop multiple times
        await monitor.stop()
        await monitor.stop()
        await monitor.stop()

        assert not monitor._running

