"""
Focused unit tests for SmartProducer (sync) functionality.

This module tests the core sync producer logic including smart partition selection,
health manager integration, and graceful degradation scenarios.
"""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.producer_config import SmartProducerConfig
from kafka_smart_producer.sync_producer import SmartProducer


class TestSmartProducerCore:
    """Test core SmartProducer functionality."""

    @pytest.fixture
    def basic_config(self):
        """Basic producer configuration."""
        return SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "health_manager": {
                    "consumer_group": "test-consumers",
                    "refresh_interval": 30.0,  # Use valid refresh interval
                },
                "smart_enabled": True,
                "key_stickiness": True,
            }
        )

    @pytest.fixture
    def mock_health_manager(self):
        """Mock health manager with configurable behavior."""
        health_manager = Mock()
        health_manager.is_running = True
        health_manager.get_healthy_partitions.return_value = [0, 1, 2]
        return health_manager

    @pytest.fixture
    def mock_producer(self):
        """Mock confluent-kafka producer."""
        producer = Mock()
        producer.produce = Mock()
        producer.poll = Mock()
        producer.flush = Mock()
        return producer

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_smart_producer_initialization(self, mock_confluent_producer, basic_config):
        """Test SmartProducer initialization with proper config."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_health_manager = Mock()
            mock_health_manager.is_running = False
            mock_health_manager.start = Mock()
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Should create confluent producer
            mock_confluent_producer.assert_called_once()

            # Should create and start health manager
            mock_create_health.assert_called_once()
            mock_health_manager.start.assert_called_once()

            # Should be properly configured
            assert producer.smart_enabled is True
            assert producer.topics == ["test-topic"]
            assert producer._partition_selector is not None

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_smart_partition_selection_with_key(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test smart partition selection with key stickiness."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Produce message with key
            producer.produce(topic="test-topic", value=b"test-message", key=b"user-123")

            # Should call underlying producer with selected partition
            mock_producer_instance.produce.assert_called_once()
            call_kwargs = mock_producer_instance.produce.call_args[1]

            assert call_kwargs["topic"] == "test-topic"
            assert call_kwargs["value"] == b"test-message"
            assert call_kwargs["key"] == b"user-123"
            assert "partition" in call_kwargs
            assert call_kwargs["partition"] in [0, 1, 2]

            # Should poll after produce
            mock_producer_instance.poll.assert_called_with(0)

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_explicit_partition_override(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test that explicit partition overrides smart selection."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Produce with explicit partition
            producer.produce(topic="test-topic", value=b"test-message", partition=5)

            # Should use explicit partition, not smart selection
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert call_kwargs["partition"] == 5

            # Health manager should not be called for partition selection
            mock_health_manager.get_healthy_partitions.assert_not_called()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_health_manager_failure_graceful_degradation(
        self, mock_confluent_producer, basic_config
    ):
        """Test graceful degradation when health manager fails."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_health_manager = Mock()
            mock_health_manager.is_running = True
            mock_health_manager.get_healthy_partitions.side_effect = Exception(
                "Health check failed"
            )
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Should not crash when health manager fails
            producer.produce(topic="test-topic", key=b"any-key", value=b"message")

            # Should fallback to default partitioning (no partition specified)
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert "partition" not in call_kwargs

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_smart_disabled_fallback(self, mock_confluent_producer):
        """Test producer with smart partitioning disabled."""
        config = SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": False,
            }
        )

        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(config)

        # Should not have partition selector
        assert producer.smart_enabled is False
        assert producer._partition_selector is None

        # Produce message
        producer.produce(topic="test-topic", key=b"any-key", value=b"message")

        # Should not add partition parameter
        call_kwargs = mock_producer_instance.produce.call_args[1]
        assert "partition" not in call_kwargs

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_key_stickiness_with_cache(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test key stickiness using cache for consistent partition selection."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # First message with key - should select and cache partition
            producer.produce(topic="test-topic", key=b"sticky-key", value=b"message1")
            first_call = mock_producer_instance.produce.call_args[1]
            first_partition = first_call["partition"]

            # Second message with same key - should use cached partition
            producer.produce(topic="test-topic", key=b"sticky-key", value=b"message2")
            second_call = mock_producer_instance.produce.call_args[1]
            second_partition = second_call["partition"]

            # Should use same partition for same key
            assert first_partition == second_partition

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_no_key_stickiness(self, mock_confluent_producer, mock_health_manager):
        """Test producer with key stickiness disabled."""
        config = SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "health_manager": {
                    "consumer_group": "test-consumers",
                    "refresh_interval": 30.0,
                },
                "smart_enabled": True,
                "key_stickiness": False,
            }
        )

        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(config)

            # Should not have cache for key stickiness
            assert producer._partition_selector._cache is None

            # Produce message
            producer.produce(topic="test-topic", key=b"any-key", value=b"message")

            # Should still select healthy partition
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert call_kwargs["partition"] in [0, 1, 2]

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_producer_close_lifecycle(self, mock_confluent_producer, basic_config):
        """Test proper resource cleanup on producer close."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_health_manager = Mock()
            mock_health_manager.is_running = True
            mock_health_manager.stop = Mock()
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Close producer
            producer.close()

            # Should flush messages
            mock_producer_instance.flush.assert_called_once()

            # Should stop health manager (since we created it)
            mock_health_manager.stop.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_external_health_manager_not_stopped(
        self, mock_confluent_producer, basic_config
    ):
        """Test that externally provided health manager is not stopped on close."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        mock_health_manager = Mock()
        mock_health_manager.is_running = True
        mock_health_manager.stop = Mock()

        # Provide external health manager
        producer = SmartProducer(basic_config, health_manager=mock_health_manager)

        # Close producer
        producer.close()

        # Should flush messages
        mock_producer_instance.flush.assert_called_once()

        # Should NOT stop external health manager
        mock_health_manager.stop.assert_not_called()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_flush_with_timeout(self, mock_confluent_producer, basic_config):
        """Test flush method with timeout."""
        mock_producer_instance = Mock()
        mock_producer_instance.flush.return_value = 5  # 5 messages remaining
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ):
            producer = SmartProducer(basic_config)

            # Flush with timeout
            result = producer.flush(timeout=10.0)

            # Should call underlying flush with timeout
            mock_producer_instance.flush.assert_called_with(10.0)
            assert result == 5

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_flush_without_timeout(self, mock_confluent_producer, basic_config):
        """Test flush method without timeout."""
        mock_producer_instance = Mock()
        mock_producer_instance.flush.return_value = 0  # All messages sent
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ):
            producer = SmartProducer(basic_config)

            # Flush without timeout
            result = producer.flush()

            # Should call underlying flush without timeout
            mock_producer_instance.flush.assert_called_with()
            assert result == 0

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_produce_with_all_parameters(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test produce method with all possible parameters."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Mock callback
            mock_callback = Mock()

            # Produce with all parameters
            producer.produce(
                topic="test-topic",
                value=b"test-value",
                key=b"test-key",
                partition=3,  # Explicit partition
                on_delivery=mock_callback,
                timestamp=1234567890,
                headers={"header1": b"value1"},
            )

            # Should pass all parameters to underlying producer
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert call_kwargs["topic"] == "test-topic"
            assert call_kwargs["value"] == b"test-value"
            assert call_kwargs["key"] == b"test-key"
            assert call_kwargs["partition"] == 3
            assert call_kwargs["on_delivery"] == mock_callback
            assert call_kwargs["timestamp"] == 1234567890
            assert call_kwargs["headers"] == {"header1": b"value1"}

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_produce_with_none_values(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test produce method filters out None values."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Produce with None values
            producer.produce(
                topic="test-topic",
                value=None,
                key=None,
                partition=None,  # Should use smart selection
                on_delivery=None,
                timestamp=None,
                headers=None,
            )

            # Should only pass non-None parameters
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert call_kwargs["topic"] == "test-topic"
            assert "partition" in call_kwargs  # Smart selection should add partition
            assert call_kwargs["partition"] in [0, 1, 2]

            # None values should be filtered out
            assert "value" not in call_kwargs
            assert "key" not in call_kwargs
            assert "on_delivery" not in call_kwargs
            assert "timestamp" not in call_kwargs
            assert "headers" not in call_kwargs

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_invalid_config_type(self, mock_confluent_producer):
        """Test that invalid config type raises ValueError."""
        with pytest.raises(
            ValueError, match="config must be SmartProducerConfig instance"
        ):
            SmartProducer({"invalid": "config"})

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_create_health_manager_none_when_no_config(self, mock_confluent_producer):
        """Test that no health manager is created when health_config is None."""
        config = SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": True,
                # No health_manager config
            }
        )

        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(config)

        # Should not have health manager
        assert producer.health_manager is None
        assert producer.smart_enabled is False  # Disabled when no health manager

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_health_manager_creation_error_handling(
        self, mock_confluent_producer, basic_config
    ):
        """Test graceful handling when health manager creation fails."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            # Simulate health manager creation failure
            mock_create_health.return_value = None

            producer = SmartProducer(basic_config)

            # Should handle gracefully
            assert producer.health_manager is None
            assert producer.smart_enabled is False  # Disabled when no health manager
            assert producer._partition_selector is None

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_properties_access(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test property accessors."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Test properties
            assert producer.topics == ["test-topic"]
            assert producer.health_manager == mock_health_manager
            assert producer.smart_enabled is True

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_properties_access_disabled_smart(self, mock_confluent_producer):
        """Test properties when smart partitioning is disabled."""
        config = SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic1", "test-topic2"],
                "smart_enabled": False,
            }
        )

        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(config)

        # Test properties
        assert producer.topics == ["test-topic1", "test-topic2"]
        assert producer.health_manager is None
        assert producer.smart_enabled is False

        # Test that topics property returns a copy (immutable)
        topics_copy = producer.topics
        topics_copy.append("modified")
        assert producer.topics == ["test-topic1", "test-topic2"]  # Original unchanged

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_proxy_method_delegation(self, mock_confluent_producer, basic_config):
        """Test that unknown methods are delegated to underlying producer."""
        mock_producer_instance = Mock()
        mock_producer_instance.list_topics = Mock(return_value="topic_metadata")
        mock_producer_instance.init_transactions = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ):
            producer = SmartProducer(basic_config)

            # Test method delegation
            result = producer.list_topics()
            assert result == "topic_metadata"
            mock_producer_instance.list_topics.assert_called_once()

            # Test method with no return value
            producer.init_transactions()
            mock_producer_instance.init_transactions.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_string_representation(
        self, mock_confluent_producer, basic_config, mock_health_manager
    ):
        """Test __repr__ method."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_producer.SmartProducer._create_health_manager"
        ) as mock_create_health:
            mock_create_health.return_value = mock_health_manager

            producer = SmartProducer(basic_config)
            repr_str = repr(producer)

            assert "SmartProducer" in repr_str
            assert (
                "topics=['test-topic']" in repr_str
                or 'topics=["test-topic"]' in repr_str
            )
            assert "smart_enabled=True" in repr_str
            assert "cache_enabled" in repr_str
