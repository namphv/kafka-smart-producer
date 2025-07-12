"""
Tests for SmartProducer and AsyncSmartProducer integration scenarios.

This module tests the complete integration of producers with caching,
health management, and partition selection logic.
"""

import asyncio
from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.async_producer import AsyncSmartProducer
from kafka_smart_producer.producer_config import ProducerConfig
from kafka_smart_producer.sync_producer import SmartProducer


class TestSmartProducerIntegration:
    """Test SmartProducer with real configuration and mocked dependencies."""

    @pytest.fixture
    def basic_config(self):
        """Basic producer configuration."""
        return ProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "health_manager": {"consumer_group": "test-consumers"},
                "cache": {"local_max_size": 100},
                "smart_enabled": True,
                "key_stickiness": True,
            }
        )

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_initialization_with_config(
        self, mock_confluent_producer, basic_config
    ):
        """Test sync producer initialization with ProducerConfig."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_health_manager.SyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager_class.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Should create confluent producer with clean config
            mock_confluent_producer.assert_called_once()
            call_args = mock_confluent_producer.call_args[0][0]
            assert "bootstrap.servers" in call_args
            assert "topics" not in call_args  # Should be filtered out

            # Should be properly configured
            assert producer.smart_enabled is True
            assert producer.topics == ["test-topic"]
            assert producer._partition_selector is not None

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    def test_async_producer_initialization_with_config(
        self, mock_confluent_producer, basic_config
    ):
        """Test async producer initialization with ProducerConfig."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.async_health_manager.AsyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager_class.return_value = mock_health_manager

            producer = AsyncSmartProducer(basic_config)

            # Should create confluent producer with clean config
            mock_confluent_producer.assert_called_once()
            call_args = mock_confluent_producer.call_args[0][0]
            assert "bootstrap.servers" in call_args
            assert "topics" not in call_args  # Should be filtered out

            # Should be properly configured
            assert producer.smart_enabled is True
            assert producer.topics == ["test-topic"]
            assert producer._partition_selector is not None

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_smart_partition_selection(
        self, mock_confluent_producer, basic_config
    ):
        """Test smart partition selection in sync producer."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_health_manager.SyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [0, 2, 4]
            mock_health_manager_class.return_value = mock_health_manager

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
            assert call_kwargs["partition"] in [0, 2, 4]

            # Should flush after produce
            mock_producer_instance.flush.assert_called_once()

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_async_producer_smart_partition_selection(
        self, mock_confluent_producer, basic_config
    ):
        """Test smart partition selection in async producer."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        # Mock successful delivery
        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                kwargs["on_delivery"](None, Mock())  # Successful delivery

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 0

        with patch(
            "kafka_smart_producer.async_health_manager.AsyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [1, 3, 5]
            mock_health_manager_class.return_value = mock_health_manager

            producer = AsyncSmartProducer(basic_config)

            # Produce message with key
            await producer.produce(
                topic="test-topic", value=b"test-message", key=b"user-456"
            )

            # Should call underlying producer with selected partition
            mock_producer_instance.produce.assert_called_once()
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert call_kwargs["topic"] == "test-topic"
            assert call_kwargs["value"] == b"test-message"
            assert call_kwargs["key"] == b"user-456"
            assert "partition" in call_kwargs
            assert call_kwargs["partition"] in [1, 3, 5]

            # Should poll after produce
            mock_producer_instance.poll.assert_called_once_with(0)

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_cache_key_stickiness(
        self, mock_confluent_producer, basic_config
    ):
        """Test cache-based key stickiness in sync producer."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_health_manager.SyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [0, 1, 2]
            mock_health_manager_class.return_value = mock_health_manager

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

            # Should have called produce twice and flush twice
            assert mock_producer_instance.produce.call_count == 2
            assert mock_producer_instance.flush.call_count == 2

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_disabled_smart_partitioning(self, mock_confluent_producer):
        """Test sync producer with smart partitioning disabled."""
        config = ProducerConfig.from_dict(
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

        # Should still flush
        mock_producer_instance.flush.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_explicit_partition_override(
        self, mock_confluent_producer, basic_config
    ):
        """Test that explicit partition overrides smart selection."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_health_manager.SyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [0, 1, 2]
            mock_health_manager_class.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Produce with explicit partition
            producer.produce(
                topic="test-topic", key=b"any-key", value=b"message", partition=5
            )

            # Should use explicit partition, not smart selection
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert call_kwargs["partition"] == 5

            # Health manager should not be called for partition selection
            mock_health_manager.get_healthy_partitions.assert_not_called()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_health_manager_failure_graceful_degradation(
        self, mock_confluent_producer, basic_config
    ):
        """Test graceful degradation when health manager fails."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_health_manager.SyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.side_effect = Exception(
                "Health check failed"
            )
            mock_health_manager_class.return_value = mock_health_manager

            producer = SmartProducer(basic_config)

            # Should not crash when health manager fails
            producer.produce(topic="test-topic", key=b"any-key", value=b"message")

            # Should fallback to default partitioning (no partition specified)
            call_kwargs = mock_producer_instance.produce.call_args[1]
            assert "partition" not in call_kwargs

            # Should still flush
            mock_producer_instance.flush.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_producer_no_key_stickiness(self, mock_confluent_producer):
        """Test sync producer with key stickiness disabled."""
        config = ProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "health_manager": {"consumer_group": "test-consumers"},
                "smart_enabled": True,
                "key_stickiness": False,
            }
        )

        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        with patch(
            "kafka_smart_producer.sync_health_manager.SyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [0, 1, 2]
            mock_health_manager_class.return_value = mock_health_manager

            producer = SmartProducer(config)

            # Produce multiple messages with same key
            producer.produce(topic="test-topic", key=b"same-key", value=b"message1")
            first_partition = mock_producer_instance.produce.call_args[1].get(
                "partition"
            )

            producer.produce(topic="test-topic", key=b"same-key", value=b"message2")
            second_partition = mock_producer_instance.produce.call_args[1].get(
                "partition"
            )

            # Both should use healthy partitions but may be different (no caching)
            assert first_partition in [0, 1, 2]
            assert second_partition in [0, 1, 2]
            # No guarantee they're the same since caching is disabled

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_async_producer_delivery_failure(
        self, mock_confluent_producer, basic_config
    ):
        """Test async producer handling of delivery failures."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        # Mock delivery failure
        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                kwargs["on_delivery"]("Delivery failed", None)

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 0

        with patch(
            "kafka_smart_producer.async_health_manager.AsyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [0]
            mock_health_manager_class.return_value = mock_health_manager

            producer = AsyncSmartProducer(basic_config)

            # Should raise exception on delivery failure
            with pytest.raises(Exception, match="Message delivery failed"):
                await producer.produce(topic="test-topic", value=b"test-message")

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_async_producer_concurrent_produces(
        self, mock_confluent_producer, basic_config
    ):
        """Test async producer handling concurrent produce calls."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        # Mock successful delivery
        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                kwargs["on_delivery"](None, Mock())

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 0

        with patch(
            "kafka_smart_producer.async_health_manager.AsyncHealthManager"
        ) as mock_health_manager_class:
            mock_health_manager = Mock()
            mock_health_manager.get_healthy_partitions.return_value = [0, 1, 2]
            mock_health_manager_class.return_value = mock_health_manager

            producer = AsyncSmartProducer(basic_config)

            # Execute multiple concurrent produces
            tasks = []
            for i in range(5):
                task = producer.produce(
                    topic="test-topic",
                    key=f"key-{i}".encode(),
                    value=f"message-{i}".encode(),
                )
                tasks.append(task)

            # All should complete successfully
            await asyncio.gather(*tasks)

            # Should have called produce 5 times
            assert mock_producer_instance.produce.call_count == 5
            assert mock_producer_instance.poll.call_count == 5
