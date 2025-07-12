"""
Tests for the AsyncSmartProducer implementation.

This module tests the async functionality including proper async/await patterns,
delivery callbacks, and integration with the health manager.
"""

import asyncio
from typing import Dict, List
from unittest.mock import MagicMock, Mock, patch

import pytest

from kafka_smart_producer.async_producer import AsyncSmartProducer
from kafka_smart_producer.producer_config import ProducerConfig


class MockHealthManager:
    """Mock health manager for testing."""

    def __init__(self, healthy_partitions: Dict[str, List[int]]):
        self._healthy_partitions = healthy_partitions
        self._selection_calls = []
        self._health_check_calls = []

    def select_partition(self, topic: str, key: bytes) -> int:
        self._selection_calls.append((topic, key))
        healthy = self._healthy_partitions.get(topic, [0])
        return healthy[0] if healthy else 0

    def is_partition_healthy(self, topic: str, partition: int) -> bool:
        call = (topic, partition)
        self._health_check_calls.append(call)
        return partition in self._healthy_partitions.get(topic, [])

    def get_selection_calls(self):
        return self._selection_calls

    def get_health_check_calls(self):
        return self._health_check_calls


@pytest.fixture
def basic_config():
    """Basic Kafka configuration for testing."""
    return ProducerConfig.from_dict(
        {
            "bootstrap.servers": "localhost:9092",
            "client.id": "test-async-producer",
            "topics": ["logs"],
        }
    )


@pytest.fixture
def health_manager():
    """Mock health manager with test partitions."""
    return MockHealthManager(
        {
            "test-topic": [0, 1, 2],
            "other-topic": [1, 3],
        }
    )


@pytest.fixture
def mock_confluent_producer():
    """Mock the confluent-kafka Producer for testing."""
    with patch("kafka_smart_producer.async_producer.ConfluentProducer") as mock_class:
        mock_instance = MagicMock()
        mock_class.return_value = mock_instance

        # Mock the poll method to return 0 (no events)
        mock_instance.poll.return_value = 0

        # Mock the flush method
        mock_instance.flush.return_value = 0

        # Mock the close method
        mock_instance.close.return_value = None

        # Mock the list_topics method
        mock_metadata = MagicMock()
        mock_metadata.topics = {"test-topic": MagicMock()}
        mock_instance.list_topics.return_value = mock_metadata

        yield mock_class, mock_instance


class TestAsyncSmartProducerInitialization:
    """Test AsyncSmartProducer initialization."""

    def test_basic_initialization(self, mock_confluent_producer, basic_config):
        """Test basic initialization of AsyncSmartProducer."""
        mock_class, _ = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config)

        # Should create confluent-kafka producer
        mock_class.assert_called_once()

        # Should not be closed initially
        assert not producer.closed

        # Should have executor
        assert producer._executor is not None

    def test_initialization_with_health_manager(
        self, mock_confluent_producer, basic_config, health_manager
    ):
        """Test initialization with health manager."""
        mock_class, mock_instance = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config, health_manager, max_workers=8)

        # Should create confluent-kafka producer
        mock_class.assert_called_once()

        # Should have the health manager
        assert producer._health_manager is health_manager

        # Should use custom max_workers
        assert producer._executor._max_workers == 8


class TestAsyncProduceMethod:
    """Test async produce method."""

    @pytest.mark.asyncio
    async def test_successful_produce(self, mock_confluent_producer, basic_config):
        """Test successful message production."""
        mock_class, mock_instance = mock_confluent_producer

        # Mock successful produce call
        def mock_produce(**kwargs):
            # Simulate successful delivery
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback(
                None,
                MagicMock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 123),
            )

        mock_instance.produce.side_effect = mock_produce

        producer = AsyncSmartProducer(basic_config)

        # Should complete without error
        await producer.produce("test-topic", value=b"test-value", key=b"test-key")

        # Should have called sync producer
        mock_instance.produce.assert_called_once()
        mock_instance.poll.assert_called_once_with(0)

    @pytest.mark.asyncio
    async def test_produce_with_delivery_callback(
        self, mock_confluent_producer, basic_config
    ):
        """Test produce with user-provided delivery callback."""
        mock_class, mock_instance = mock_confluent_producer

        user_callback = Mock()

        def mock_produce(**kwargs):
            # Simulate successful delivery
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            mock_msg = MagicMock(
                topic=lambda: topic, partition=lambda: 0, offset=lambda: 123
            )
            callback(None, mock_msg)

        mock_instance.produce.side_effect = mock_produce

        producer = AsyncSmartProducer(basic_config)

        await producer.produce(
            "test-topic",
            value=b"test-value",
            key=b"test-key",
            on_delivery=user_callback,
        )

        # User callback should have been called
        user_callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_produce_delivery_failure(
        self, mock_confluent_producer, basic_config
    ):
        """Test produce with delivery failure."""
        mock_class, mock_instance = mock_confluent_producer

        def mock_produce(**kwargs):
            # Simulate delivery failure
            callback = kwargs["on_delivery"]
            callback(Exception("Delivery failed"), None)

        mock_instance.produce.side_effect = mock_produce

        producer = AsyncSmartProducer(basic_config)

        # Should raise exception on delivery failure
        with pytest.raises(Exception, match="Message delivery failed"):
            await producer.produce("test-topic", value=b"test-value", key=b"test-key")

    @pytest.mark.asyncio
    async def test_produce_when_closed(self, mock_confluent_producer, basic_config):
        """Test produce when producer is closed."""
        mock_class, mock_instance = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config)
        await producer.close()

        # Should raise RuntimeError when closed
        with pytest.raises(RuntimeError, match="AsyncSmartProducer is closed"):
            await producer.produce("test-topic", value=b"test-value")

    @pytest.mark.asyncio
    async def test_concurrent_produce(self, mock_confluent_producer, basic_config):
        """Test concurrent message production."""
        mock_class, mock_instance = mock_confluent_producer

        def mock_produce(**kwargs):
            # Simulate successful delivery
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback(
                None,
                MagicMock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 123),
            )

        mock_instance.produce.side_effect = mock_produce

        producer = AsyncSmartProducer(basic_config)

        # Produce multiple messages concurrently
        tasks = [
            producer.produce(
                "test-topic", value=f"value{i}".encode(), key=f"key{i}".encode()
            )
            for i in range(10)
        ]

        # Should complete all tasks successfully
        await asyncio.gather(*tasks)

        # Should have called produce for each message
        assert mock_instance.produce.call_count == 10
        assert mock_instance.poll.call_count == 10


class TestAsyncFlushMethod:
    """Test async flush method."""

    @pytest.mark.asyncio
    async def test_successful_flush(self, mock_confluent_producer, basic_config):
        """Test successful flush operation."""
        mock_class, mock_instance = mock_confluent_producer

        mock_instance.flush.return_value = 0  # No messages remaining

        producer = AsyncSmartProducer(basic_config)

        remaining = await producer.flush(timeout=10.0)

        assert remaining == 0
        mock_instance.flush.assert_called_once_with(10.0)

    @pytest.mark.asyncio
    async def test_flush_when_closed(self, mock_confluent_producer, basic_config):
        """Test flush when producer is closed."""
        mock_class, mock_instance = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config)
        await producer.close()

        # Should return 0 when closed
        remaining = await producer.flush()
        assert remaining == 0

        # Should have called sync producer flush once during close()
        # No additional calls after close
        mock_instance.flush.assert_called_once()


class TestAsyncPollMethod:
    """Test async poll method."""

    @pytest.mark.asyncio
    async def test_successful_poll(self, mock_confluent_producer, basic_config):
        """Test successful poll operation."""
        mock_class, mock_instance = mock_confluent_producer

        mock_instance.poll.return_value = 5  # 5 events processed

        producer = AsyncSmartProducer(basic_config)

        events = await producer.poll(timeout=1.0)

        assert events == 5
        mock_instance.poll.assert_called_with(1.0)

    @pytest.mark.asyncio
    async def test_poll_when_closed(self, mock_confluent_producer, basic_config):
        """Test poll when producer is closed."""
        mock_class, mock_instance = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config)
        await producer.close()

        # Should return 0 when closed
        events = await producer.poll()
        assert events == 0


class TestAsyncCloseMethod:
    """Test async close method."""

    @pytest.mark.asyncio
    async def test_successful_close(self, mock_confluent_producer, basic_config):
        """Test successful close operation."""
        mock_class, mock_instance = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config)

        await producer.close()

        # Should be marked as closed
        assert producer.closed

        # Should have called sync producer methods
        mock_instance.flush.assert_called_once()
        mock_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_idempotent(self, mock_confluent_producer, basic_config):
        """Test that close is idempotent."""
        mock_class, mock_instance = mock_confluent_producer

        producer = AsyncSmartProducer(basic_config)

        # Close multiple times
        await producer.close()
        await producer.close()
        await producer.close()

        # Should only call sync producer close once
        mock_instance.close.assert_called_once()


class TestAsyncContextManager:
    """Test async context manager functionality."""

    @pytest.mark.asyncio
    async def test_async_context_manager(self, mock_confluent_producer, basic_config):
        """Test async context manager usage."""
        mock_class, mock_instance = mock_confluent_producer

        def mock_produce(**kwargs):
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback(
                None,
                MagicMock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 123),
            )

        mock_instance.produce.side_effect = mock_produce

        # Use as async context manager
        async with AsyncSmartProducer(basic_config) as producer:
            await producer.produce("test-topic", value=b"test-value", key=b"test-key")

            # Should not be closed inside context
            assert not producer.closed

        # Should be closed after context exit
        assert producer.closed
        mock_instance.close.assert_called_once()


class TestErrorHandling:
    """Test error handling scenarios."""

    @pytest.mark.asyncio
    async def test_produce_executor_error(self, mock_confluent_producer, basic_config):
        """Test error handling in produce method."""
        mock_class, mock_instance = mock_confluent_producer

        # Mock produce to raise exception
        mock_instance.produce.side_effect = Exception("Produce failed")

        producer = AsyncSmartProducer(basic_config)

        # Should propagate exception
        with pytest.raises(Exception, match="Produce failed"):
            await producer.produce("test-topic", value=b"test-value")

    @pytest.mark.asyncio
    async def test_user_callback_error(self, mock_confluent_producer, basic_config):
        """Test error handling when user callback raises exception."""
        mock_class, mock_instance = mock_confluent_producer

        def mock_produce(**kwargs):
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback(
                None,
                MagicMock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 123),
            )

        mock_instance.produce.side_effect = mock_produce

        def failing_callback(err, msg):
            raise ValueError("Callback failed")

        producer = AsyncSmartProducer(basic_config)

        # Should propagate callback exception
        with pytest.raises(ValueError, match="Callback failed"):
            await producer.produce(
                "test-topic", value=b"test-value", on_delivery=failing_callback
            )


class TestRealWorldUsage:
    """Test real-world usage patterns."""

    @pytest.mark.asyncio
    async def test_batch_processing(self, mock_confluent_producer, basic_config):
        """Test batch processing pattern."""
        mock_class, mock_instance = mock_confluent_producer

        def mock_produce(**kwargs):
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback(
                None,
                MagicMock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 123),
            )

        mock_instance.produce.side_effect = mock_produce

        producer = AsyncSmartProducer(basic_config)

        # Simulate processing a batch of events
        events = [{"user_id": f"user{i}", "event": f"event{i}"} for i in range(20)]

        tasks = []
        for event in events:
            task = producer.produce(
                "events", key=event["user_id"].encode(), value=str(event).encode()
            )
            tasks.append(task)

        # Process all events concurrently
        await asyncio.gather(*tasks)

        # Should have processed all events
        assert mock_instance.produce.call_count == 20

        # Cleanup
        await producer.close()

    @pytest.mark.asyncio
    async def test_producer_lifecycle(self, mock_confluent_producer, basic_config):
        """Test complete producer lifecycle."""
        mock_class, mock_instance = mock_confluent_producer

        def mock_produce(**kwargs):
            callback = kwargs["on_delivery"]
            topic = kwargs["topic"]
            callback(
                None,
                MagicMock(topic=lambda: topic, partition=lambda: 0, offset=lambda: 123),
            )

        mock_instance.produce.side_effect = mock_produce

        # Create producer
        producer = AsyncSmartProducer(basic_config)

        try:
            # Produce some messages
            await producer.produce("events", value=b"event1", key=b"user1")
            await producer.produce("events", value=b"event2", key=b"user2")

            # Poll for events
            await producer.poll(timeout=0.1)

            # Flush messages
            remaining = await producer.flush(timeout=5.0)
            assert remaining == 0

        finally:
            # Close producer
            await producer.close()
            assert producer.closed
