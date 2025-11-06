"""
Tests for PartitionHealthMonitor (sync) functionality and lifecycle management.

This module tests the sync PartitionHealthMonitor class including:
- Lifecycle management (start/stop)
- Health data collection and caching
- Redis publishing for standalone mode
- Integration with SmartProducer
- Different health modes (embedded, standalone, redis_consumer)
"""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.health_mode import HealthMode
from kafka_smart_producer.producer_config import SmartProducerConfig
from kafka_smart_producer.sync_producer import SmartProducer


class TestPartitionHealthMonitorLifecycle:
    """Test PartitionHealthMonitor lifecycle in SmartProducer."""

    @pytest.fixture
    def health_enabled_config(self):
        """Configuration with health manager enabled."""
        return SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": True,
                "consumer_group": "test-group",
                "health_manager": {
                    "consumer_group": "test-group",
                    "health_threshold": 0.5,
                    "refresh_interval": 10.0,
                    "timeout_seconds": 5.0,
                },
            }
        )

    @pytest.fixture
    def no_health_config(self):
        """Configuration without health manager."""
        return SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": False,
            }
        )

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    @patch("kafka_smart_producer.producer_utils.create_health_manager_from_config")
    def test_partition_health_monitor_created_and_started(
        self, mock_create_health_manager, mock_confluent_producer, health_enabled_config
    ):
        """Test new behavior: self-created PartitionHealthMonitor is auto-started."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Mock PartitionHealthMonitor instance
        mock_partition_health_monitor = Mock()
        mock_partition_health_monitor.is_running = False
        mock_create_health_manager.return_value = mock_partition_health_monitor

        # Create producer
        producer = SmartProducer(health_enabled_config)

        # Verify PartitionHealthMonitor was created
        mock_create_health_manager.assert_called_once()
        assert producer.health_manager is mock_partition_health_monitor

        # Verify start() WAS called (new correct behavior)
        mock_partition_health_monitor.start.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_no_partition_health_monitor_when_disabled(
        self, mock_confluent_producer, no_health_config
    ):
        """Test that no PartitionHealthMonitor is created when smart_enabled=False."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Create producer
        producer = SmartProducer(no_health_config)

        # Should have no PartitionHealthMonitor
        assert producer.health_manager is None
        assert not producer.smart_enabled

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_explicit_health_manager_used(
        self, mock_confluent_producer, health_enabled_config
    ):
        """Test that explicitly provided health manager is used."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Create explicit health manager
        mock_health_manager = Mock()
        mock_health_manager.is_running = True

        # Create producer with explicit health manager
        producer = SmartProducer(
            health_enabled_config, health_manager=mock_health_manager
        )

        # Should use the provided health manager
        assert producer.health_manager is mock_health_manager

        # Should not interfere with existing health manager
        mock_health_manager.start.assert_not_called()
        mock_health_manager.stop.assert_not_called()

    def test_health_manager_lifecycle_integration(self, health_enabled_config):
        """Integration test: verify health manager works when started manually."""
        # This test will show the current broken state
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_confluent_producer.return_value = Mock()

            # Create producer (health manager should be created but not started)
            producer = SmartProducer(health_enabled_config)

            if producer.health_manager:
                # Manually start health manager (what should happen automatically)
                producer.health_manager.start()

                # Verify it's running
                assert producer.health_manager.is_running

                # Test getting healthy partitions (should work now)
                healthy_partitions = producer.health_manager.get_healthy_partitions(
                    "test-topic"
                )
                assert isinstance(healthy_partitions, list)

                # Manually stop
                producer.health_manager.stop()
                assert not producer.health_manager.is_running

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    @patch("kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor")
    def test_producer_cleanup_without_lifecycle_management(
        self, mock_health_manager_class, mock_confluent_producer, health_enabled_config
    ):
        """Test what happens when producer is destroyed without proper cleanup."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Mock health manager instance
        mock_health_manager = Mock()
        mock_health_manager.is_running = False
        mock_health_manager_class.return_value = mock_health_manager

        # Create producer
        producer = SmartProducer(health_enabled_config)

        # Simulate health manager was started manually
        mock_health_manager.is_running = True

        # Delete producer without proper cleanup
        del producer

        # Health manager should still be running (this is the problem!)
        # No cleanup happened because there's no lifecycle management
        mock_health_manager.stop.assert_not_called()


class TestHealthManagerLifecycleFixes:
    """Test the fixes we need to implement."""

    @pytest.fixture
    def health_enabled_config(self):
        """Configuration with health manager enabled."""
        return SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": True,
                "consumer_group": "test-group",
                "health_manager": {
                    "consumer_group": "test-group",
                    "health_threshold": 0.5,
                    "refresh_interval": 10.0,
                    "timeout_seconds": 5.0,
                },
            }
        )

    def test_should_auto_start_self_created_health_manager(self, health_enabled_config):
        """Test that self-created health manager should be auto-started."""
        # This test defines the behavior we want to implement
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_confluent_producer.return_value = Mock()

            with patch(
                "kafka_smart_producer.producer_utils.create_health_manager_from_config"
            ) as mock_create_health_manager:
                mock_health_manager = Mock()
                mock_health_manager.is_running = False
                mock_create_health_manager.return_value = mock_health_manager

                # Create producer
                SmartProducer(health_enabled_config)

                # SHOULD auto-start the health manager
                mock_health_manager.start.assert_called_once()

    def test_should_not_auto_start_explicit_health_manager(self, health_enabled_config):
        """Test that explicitly provided health manager should NOT be auto-started."""
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_confluent_producer.return_value = Mock()

            # Create explicit health manager
            mock_health_manager = Mock()
            mock_health_manager.is_running = False

            # Create producer with explicit health manager
            SmartProducer(health_enabled_config, health_manager=mock_health_manager)

            # Should NOT auto-start explicitly provided health manager
            mock_health_manager.start.assert_not_called()

    def test_should_auto_stop_self_created_health_manager_on_close(
        self, health_enabled_config
    ):
        """Test that close() should stop self-created health manager."""
        # This test defines the close() behavior we want to implement
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_producer_instance = Mock()
            mock_confluent_producer.return_value = mock_producer_instance

            with patch(
                "kafka_smart_producer.producer_utils.create_health_manager_from_config"
            ) as mock_create_health_manager:
                mock_health_manager = Mock()
                mock_health_manager.is_running = True
                mock_create_health_manager.return_value = mock_health_manager

                # Create producer
                producer = SmartProducer(health_enabled_config)

                # Add close method and test it
                if hasattr(producer, "close"):
                    producer.close()

                    # Should flush and stop health manager
                    mock_producer_instance.flush.assert_called_once()
                    mock_health_manager.stop.assert_called_once()

    def test_minimal_config_auto_starts_health_monitoring(self):
        """Test that minimal config automatically starts background health monitoring.

        This test verifies that with just kafka config, topics, and consumer_group,
        the producer will:
        1. Auto-create and start a PartitionHealthMonitor
        2. Use it for intelligent partition selection
        3. Return healthy partitions from background monitoring
        4. Clean up properly on close()
        """
        # Minimal config - just kafka, topics, and consumer_group at top level
        minimal_config = SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["orders", "payments"],
                "consumer_group": "order-processors",  # Top-level consumer group
            }
        )

        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent:
            mock_confluent.return_value = Mock()

            with patch(
                "kafka_smart_producer.producer_utils.create_health_manager_from_config"
            ) as mock_create_health_manager:
                # Mock health manager that will return healthy partitions
                mock_health_manager = Mock()
                mock_health_manager.is_running = False
                mock_health_manager.get_healthy_partitions.return_value = [0, 1, 2]
                mock_create_health_manager.return_value = mock_health_manager

                # Create producer with minimal config
                producer = SmartProducer(minimal_config)

                # Health manager should be auto-started
                mock_health_manager.start.assert_called_once()

                # Simulate that health manager is now running after start
                mock_health_manager.is_running = True

                # Producer should be smart-enabled
                assert producer.smart_enabled is True
                assert producer.health_manager is mock_health_manager

                # Should be able to get healthy partitions from background monitoring
                healthy_partitions = producer.health_manager.get_healthy_partitions(
                    "orders"
                )
                assert healthy_partitions == [0, 1, 2]

                # Produce a message - should use smart partitioning
                producer.produce(
                    topic="orders", value=b"test-order", key=b"customer-123"
                )

                # Should have called the underlying producer with partition selection
                mock_confluent.return_value.produce.assert_called_once()
                call_kwargs = mock_confluent.return_value.produce.call_args[1]
                assert "partition" in call_kwargs  # Smart partitioning applied
                assert call_kwargs["partition"] in [0, 1, 2]  # Used healthy partition

                # Cleanup should stop the health manager
                producer.close()
                mock_health_manager.stop.assert_called_once()


class TestPartitionHealthMonitorCore:
    """Test core PartitionHealthMonitor functionality."""

    @pytest.fixture
    def mock_lag_collector(self):
        """Mock lag collector for testing."""
        mock_collector = Mock()
        mock_collector.get_lag_data.return_value = {0: 100, 1: 0, 2: 50, 3: 200}
        return mock_collector

    @pytest.fixture
    def mock_redis_publisher(self):
        """Mock Redis publisher for testing standalone mode."""
        return Mock()

    def test_partition_health_monitor_embedded_mode(self, mock_lag_collector):
        """Test PartitionHealthMonitor in embedded mode."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        monitor = PartitionHealthMonitor(
            lag_collector=mock_lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=5.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
            redis_health_publisher=None,
        )

        # Test mode property
        assert monitor._mode == HealthMode.EMBEDDED
        assert not monitor.is_running

        # Test topics initialization
        monitor._initialize_topics(["test-topic"])
        assert "test-topic" in monitor._health_data

    def test_partition_health_monitor_standalone_mode(
        self, mock_lag_collector, mock_redis_publisher
    ):
        """Test PartitionHealthMonitor in standalone mode with Redis publishing."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        monitor = PartitionHealthMonitor(
            lag_collector=mock_lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=5.0,
            max_lag_for_health=100,
            mode=HealthMode.STANDALONE,
            redis_health_publisher=mock_redis_publisher,
        )

        # Test mode property
        assert monitor._mode == HealthMode.STANDALONE
        assert monitor._redis_publisher is mock_redis_publisher

    def test_health_score_calculation(self, mock_lag_collector):
        """Test health score calculation from lag data."""
        from kafka_smart_producer.health_utils import calculate_health_scores

        # Test health score calculation using health_utils
        lag_data = {0: 0, 1: 50, 2: 100, 3: 200}
        health_scores = calculate_health_scores(lag_data, max_lag_for_health=100)

        assert health_scores[0] == 1.0  # No lag = perfect health
        assert health_scores[1] == 0.5  # Half max lag = 0.5 health
        assert health_scores[2] == 0.0  # Max lag = 0 health
        assert health_scores[3] == 0.0  # Over max lag = 0 health

    def test_get_healthy_partitions(self, mock_lag_collector):
        """Test getting healthy partitions list."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        monitor = PartitionHealthMonitor(
            lag_collector=mock_lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=5.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
            redis_health_publisher=None,
        )

        # Initialize with mock health data
        monitor._initialize_topics(["test-topic"])
        monitor._health_data["test-topic"] = {0: 1.0, 1: 0.8, 2: 0.3, 3: 0.0}

        # Test getting healthy partitions (>= 0.5 threshold)
        healthy = monitor.get_healthy_partitions("test-topic")
        assert sorted(healthy) == [0, 1]  # Only partitions with >= 0.5 health

    def test_is_partition_healthy(self, mock_lag_collector):
        """Test checking if specific partition is healthy."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        monitor = PartitionHealthMonitor(
            lag_collector=mock_lag_collector,
            cache=None,
            health_threshold=0.5,
            refresh_interval=5.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
            redis_health_publisher=None,
        )

        # Initialize with mock health data
        monitor._initialize_topics(["test-topic"])
        monitor._health_data["test-topic"] = {0: 1.0, 1: 0.8, 2: 0.3, 3: 0.0}

        # Test individual partition health checks
        assert monitor.is_partition_healthy("test-topic", 0) is True  # 1.0 >= 0.5
        assert monitor.is_partition_healthy("test-topic", 1) is True  # 0.8 >= 0.5
        assert monitor.is_partition_healthy("test-topic", 2) is False  # 0.3 < 0.5
        assert monitor.is_partition_healthy("test-topic", 3) is False  # 0.0 < 0.5

        # Test unknown partition (should default to healthy)
        assert monitor.is_partition_healthy("test-topic", 99) is True

    def test_health_summary(self, mock_lag_collector):
        """Test getting health summary for monitoring."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        monitor = PartitionHealthMonitor(
            lag_collector=mock_lag_collector,
            cache=None,
            health_threshold=0.6,
            refresh_interval=10.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
            redis_health_publisher=None,
        )

        # Initialize with mock health data
        monitor._initialize_topics(["topic1", "topic2"])
        monitor._health_data["topic1"] = {0: 1.0, 1: 0.7, 2: 0.5}  # 2 healthy (>=0.6)
        monitor._health_data["topic2"] = {0: 0.8, 1: 0.4}  # 1 healthy (>=0.6)

        summary = monitor.get_health_summary()

        assert summary["execution_context"] == "sync"
        assert summary["topics"] == 2
        assert summary["total_partitions"] == 5
        assert summary["healthy_partitions"] == 3  # 2 from topic1 + 1 from topic2
        assert summary["health_threshold"] == 0.6
        assert summary["refresh_interval"] == 10.0
        assert "topics_detail" in summary


class TestPartitionHealthMonitorModes:
    """Test different PartitionHealthMonitor modes and configurations."""

    def test_redis_consumer_mode_config(self):
        """Test Redis consumer mode configuration in SmartProducer."""
        config = SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "health_mode": "redis_consumer",
                "health_manager": {
                    "consumer_group": "test-group",
                    "health_threshold": 0.7,
                },
                "cache": {
                    "remote_enabled": True,
                    "redis_host": "localhost",
                    "redis_port": 6379,
                },
            }
        )

        assert config.health_mode == "redis_consumer"
        assert config.health_config.health_threshold == 0.7
        assert config.cache_config.remote_enabled is True

    def test_standalone_monitor_factory_method(self):
        """Test PartitionHealthMonitor.standalone() factory method."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        with patch("kafka_smart_producer.lag_collector.KafkaAdminLagCollector"):
            with patch("kafka_smart_producer.cache_factory.CacheFactory.create_remote_cache"):
                monitor = PartitionHealthMonitor.standalone(
                    consumer_group="test-group",
                    kafka_config={"bootstrap.servers": "localhost:9092"},
                    topics=["topic1", "topic2"],
                    health_threshold=0.5,
                    refresh_interval=5.0,
                    max_lag_for_health=50,
                )

                assert monitor._mode == HealthMode.STANDALONE
                assert monitor._health_threshold == 0.5
                assert monitor._refresh_interval == 5.0
                assert monitor._max_lag_for_health == 50
                assert monitor._redis_publisher is not None


class TestRedisHealthConsumer:
    """Test Redis health consumer functionality."""

    def test_redis_health_consumer_initialization(self):
        """Test RedisHealthConsumer initialization."""
        from kafka_smart_producer.caching import DefaultRemoteCache
        from kafka_smart_producer.redis_health_consumer import RedisHealthConsumer

        # Mock Redis cache
        mock_redis_cache = Mock(spec=DefaultRemoteCache)

        consumer = RedisHealthConsumer(
            redis_cache=mock_redis_cache, health_threshold=0.6
        )

        assert consumer._redis_cache is mock_redis_cache
        assert consumer._health_threshold == 0.6
        assert consumer._mode == HealthMode.REDIS_CONSUMER
        assert consumer.is_running is True  # Always running (stateless)

    def test_redis_health_consumer_get_healthy_partitions(self):
        """Test RedisHealthConsumer getting healthy partitions."""
        from kafka_smart_producer.redis_health_consumer import RedisHealthConsumer

        # Mock Redis cache with health data
        mock_redis_cache = Mock()
        mock_redis_cache.get_health_data.return_value = {0: 0.9, 1: 0.7, 2: 0.4, 3: 0.1}

        consumer = RedisHealthConsumer(
            redis_cache=mock_redis_cache, health_threshold=0.5
        )

        healthy_partitions = consumer.get_healthy_partitions("test-topic")

        # Should return partitions with health >= 0.5
        assert sorted(healthy_partitions) == [0, 1]
        mock_redis_cache.get_health_data.assert_called_once_with("test-topic")

    def test_redis_health_consumer_no_data(self):
        """Test RedisHealthConsumer when no health data is available."""
        from kafka_smart_producer.redis_health_consumer import RedisHealthConsumer

        # Mock Redis cache with no data
        mock_redis_cache = Mock()
        mock_redis_cache.get_health_data.return_value = None

        consumer = RedisHealthConsumer(
            redis_cache=mock_redis_cache, health_threshold=0.5
        )

        healthy_partitions = consumer.get_healthy_partitions("test-topic")

        # Should return empty list when no data
        assert healthy_partitions == []

    def test_redis_health_consumer_exception_handling(self):
        """Test RedisHealthConsumer handles Redis exceptions gracefully."""
        from kafka_smart_producer.redis_health_consumer import RedisHealthConsumer

        # Mock Redis cache that raises exception
        mock_redis_cache = Mock()
        mock_redis_cache.get_health_data.side_effect = Exception(
            "Redis connection failed"
        )

        consumer = RedisHealthConsumer(
            redis_cache=mock_redis_cache, health_threshold=0.5
        )

        # Should handle exception gracefully and return empty list
        healthy_partitions = consumer.get_healthy_partitions("test-topic")
        assert healthy_partitions == []

    def test_hybrid_redis_health_consumer(self):
        """Test HybridRedisHealthConsumer wrapper."""
        from kafka_smart_producer.caching import DefaultHybridCache
        from kafka_smart_producer.redis_health_consumer import HybridRedisHealthConsumer

        # Mock hybrid cache with remote component
        mock_remote_cache = Mock()
        mock_hybrid_cache = Mock(spec=DefaultHybridCache)
        mock_hybrid_cache._remote = mock_remote_cache

        hybrid_consumer = HybridRedisHealthConsumer(
            hybrid_cache=mock_hybrid_cache, health_threshold=0.7
        )

        assert hybrid_consumer.is_running is True

        # Test delegation to underlying Redis health consumer
        mock_remote_cache.get_health_data.return_value = {0: 0.8, 1: 0.6, 2: 0.5}

        healthy = hybrid_consumer.get_healthy_partitions("test-topic")
        # Should return partitions >= 0.7 threshold
        assert sorted(healthy) == [0]
