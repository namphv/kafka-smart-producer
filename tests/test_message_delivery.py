"""
Tests for message delivery guarantees in sync and async producers.

This module tests the critical message delivery behavior differences
between sync (flush) and async (poll + delivery callback) producers.
"""

import asyncio
from unittest.mock import Mock, call, patch

import pytest

from kafka_smart_producer.async_producer import AsyncSmartProducer
from kafka_smart_producer.sync_producer import SmartProducer


class TestSyncProducerMessageDelivery:
    """Test sync producer message delivery guarantees."""

    @pytest.fixture
    def basic_config(self):
        """Basic configuration without health manager."""
        return {
            "bootstrap.servers": "localhost:9092",
            "topics": ["test-topic"],
            "smart_enabled": False,  # Disable smart features for simpler testing
        }

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_produce_calls_flush_immediately(
        self, mock_confluent_producer, basic_config
    ):
        """Test that sync producer calls flush() immediately after produce()."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(basic_config)

        # Produce a message
        producer.produce(topic="test-topic", value=b"test-message")

        # Should call produce then flush immediately
        expected_calls = [
            call.produce(topic="test-topic", value=b"test-message"),
            call.flush(),
        ]
        assert mock_producer_instance.mock_calls == expected_calls

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_produce_with_all_parameters_then_flush(
        self, mock_confluent_producer, basic_config
    ):
        """Test that flush is called even with all parameters."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(basic_config)

        # Callback function
        def delivery_callback(err, msg):
            pass

        # Produce with all parameters
        producer.produce(
            topic="test-topic",
            value=b"test-message",
            key=b"test-key",
            partition=2,
            on_delivery=delivery_callback,
            timestamp=1234567890,
            headers={"header1": b"value1"},
        )

        # Should call produce with all params then flush
        mock_producer_instance.produce.assert_called_once_with(
            topic="test-topic",
            value=b"test-message",
            key=b"test-key",
            partition=2,
            on_delivery=delivery_callback,
            timestamp=1234567890,
            headers={"header1": b"value1"},
        )
        mock_producer_instance.flush.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_multiple_produces_flush_each_time(
        self, mock_confluent_producer, basic_config
    ):
        """Test that each produce call results in a flush."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(basic_config)

        # Produce multiple messages
        producer.produce(topic="test-topic", value=b"message1")
        producer.produce(topic="test-topic", value=b"message2")
        producer.produce(topic="test-topic", value=b"message3")

        # Should call flush after each produce
        assert mock_producer_instance.produce.call_count == 3
        assert mock_producer_instance.flush.call_count == 3

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_produce_exception_does_not_call_flush(
        self, mock_confluent_producer, basic_config
    ):
        """Test that flush is not called if produce raises exception."""
        mock_producer_instance = Mock()
        mock_producer_instance.produce.side_effect = Exception("Produce failed")
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(basic_config)

        # Produce should raise exception
        with pytest.raises(Exception, match="Produce failed"):
            producer.produce(topic="test-topic", value=b"test-message")

        # Should not call flush when produce fails
        mock_producer_instance.produce.assert_called_once()
        mock_producer_instance.flush.assert_not_called()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_flush_exception_is_propagated(self, mock_confluent_producer, basic_config):
        """Test that flush exceptions are propagated to caller."""
        mock_producer_instance = Mock()
        mock_producer_instance.flush.side_effect = Exception("Flush failed")
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(basic_config)

        # Should propagate flush exception
        with pytest.raises(Exception, match="Flush failed"):
            producer.produce(topic="test-topic", value=b"test-message")

        # Both produce and flush should be called
        mock_producer_instance.produce.assert_called_once()
        mock_producer_instance.flush.assert_called_once()


class TestAsyncProducerMessageDelivery:
    """Test async producer message delivery guarantees."""

    @pytest.fixture
    def basic_config(self):
        """Basic configuration without health manager."""
        return {
            "bootstrap.servers": "localhost:9092",
            "topics": ["test-topic"],
            "smart_enabled": False,
        }

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_produce_calls_poll_and_waits_for_delivery(
        self, mock_confluent_producer, basic_config
    ):
        """Test that async producer calls poll(0) and waits for delivery."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        # Mock successful delivery
        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                # Simulate immediate successful delivery
                kwargs["on_delivery"](None, Mock())

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(basic_config)

        # Produce a message
        await producer.produce(topic="test-topic", value=b"test-message")

        # Should call produce then poll
        mock_producer_instance.produce.assert_called_once()
        mock_producer_instance.poll.assert_called_once_with(0)

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_delivery_callback_success(
        self, mock_confluent_producer, basic_config
    ):
        """Test successful delivery callback handling."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        # Track delivery callback calls
        delivery_callback_called = False
        delivery_result = None

        def mock_produce(**kwargs):
            nonlocal delivery_callback_called, delivery_result
            if "on_delivery" in kwargs:
                delivery_callback_called = True
                # Simulate successful delivery
                mock_msg = Mock()
                kwargs["on_delivery"](None, mock_msg)
                delivery_result = mock_msg

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(basic_config)

        # Should complete successfully
        await producer.produce(topic="test-topic", value=b"test-message")

        assert delivery_callback_called
        assert delivery_result is not None

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_delivery_callback_failure(
        self, mock_confluent_producer, basic_config
    ):
        """Test delivery callback failure handling."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                # Simulate delivery failure
                kwargs["on_delivery"]("Delivery failed", None)

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(basic_config)

        # Should raise exception on delivery failure
        with pytest.raises(Exception, match="Message delivery failed"):
            await producer.produce(topic="test-topic", value=b"test-message")

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_user_delivery_callback_is_called(
        self, mock_confluent_producer, basic_config
    ):
        """Test that user-provided delivery callback is called before internal
        handling."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        user_callback_called = False
        user_callback_args = None

        def user_callback(err, msg):
            nonlocal user_callback_called, user_callback_args
            user_callback_called = True
            user_callback_args = (err, msg)

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                # Simulate successful delivery with user callback
                mock_msg = Mock()
                kwargs["on_delivery"](None, mock_msg)

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(basic_config)

        # Produce with user callback
        await producer.produce(
            topic="test-topic", value=b"test-message", on_delivery=user_callback
        )

        # User callback should be called
        assert user_callback_called
        assert user_callback_args[0] is None  # No error
        assert user_callback_args[1] is not None  # Message object

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_user_callback_exception_propagated(
        self, mock_confluent_producer, basic_config
    ):
        """Test that user callback exceptions are properly propagated."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        def failing_user_callback(err, msg):
            raise ValueError("User callback failed")

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                # Simulate successful delivery but user callback will fail
                mock_msg = Mock()
                kwargs["on_delivery"](None, mock_msg)

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(basic_config)

        # Should propagate user callback exception
        with pytest.raises(ValueError, match="User callback failed"):
            await producer.produce(
                topic="test-topic",
                value=b"test-message",
                on_delivery=failing_user_callback,
            )

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_concurrent_produces_each_get_delivery_confirmation(
        self, mock_confluent_producer, basic_config
    ):
        """Test that concurrent produces each get their own delivery confirmation."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        # Track all delivery callbacks
        delivery_callbacks = []

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                delivery_callbacks.append(kwargs["on_delivery"])
                # Don't call callback immediately - simulate async delivery

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(basic_config)

        # Start multiple concurrent produces
        tasks = []
        for i in range(3):
            task = asyncio.create_task(
                producer.produce(topic="test-topic", value=f"message-{i}".encode())
            )
            tasks.append(task)

        # Let them start
        await asyncio.sleep(0.01)

        # Should have 3 delivery callbacks waiting
        assert len(delivery_callbacks) == 3

        # Complete deliveries
        for callback in delivery_callbacks:
            callback(None, Mock())  # Successful delivery

        # All tasks should complete
        await asyncio.gather(*tasks)

        # Should have called produce and poll 3 times
        assert mock_producer_instance.produce.call_count == 3
        assert mock_producer_instance.poll.call_count == 3

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_executor_produce_failure(
        self, mock_confluent_producer, basic_config
    ):
        """Test handling of executor failure during produce."""
        mock_producer_instance = Mock()
        mock_producer_instance.produce.side_effect = Exception(
            "Executor produce failed"
        )
        mock_confluent_producer.return_value = mock_producer_instance

        producer = AsyncSmartProducer(basic_config)

        # Should propagate executor exception
        with pytest.raises(Exception, match="Executor produce failed"):
            await producer.produce(topic="test-topic", value=b"test-message")

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_poll_exception_handling(self, mock_confluent_producer, basic_config):
        """Test handling of poll() exceptions."""
        mock_producer_instance = Mock()
        mock_producer_instance.poll.side_effect = Exception("Poll failed")

        def mock_produce(**kwargs):
            pass  # Don't call delivery callback

        mock_producer_instance.produce.side_effect = mock_produce
        mock_confluent_producer.return_value = mock_producer_instance

        producer = AsyncSmartProducer(basic_config)

        # Should propagate poll exception
        with pytest.raises(Exception, match="Poll failed"):
            await producer.produce(topic="test-topic", value=b"test-message")


class TestDeliveryBehaviorComparison:
    """Test comparison between sync and async delivery behavior."""

    @pytest.fixture
    def config(self):
        return {
            "bootstrap.servers": "localhost:9092",
            "topics": ["test-topic"],
            "smart_enabled": False,
        }

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_sync_guarantees_immediate_delivery(self, mock_confluent_producer, config):
        """Test that sync producer guarantees immediate delivery via flush."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        producer = SmartProducer(config)

        # Produce message
        producer.produce(topic="test-topic", value=b"test-message")

        # flush() should be called which guarantees delivery
        mock_producer_instance.flush.assert_called_once()

        # This means message is guaranteed to be sent before function returns

    @patch("kafka_smart_producer.async_producer.ConfluentProducer")
    async def test_async_waits_for_confirmation(self, mock_confluent_producer, config):
        """Test that async producer waits for delivery confirmation."""
        mock_producer_instance = Mock()
        mock_confluent_producer.return_value = mock_producer_instance

        message_delivered = False

        def mock_produce(**kwargs):
            nonlocal message_delivered
            if "on_delivery" in kwargs:
                # Simulate immediate delivery completion (runs in executor)
                kwargs["on_delivery"](None, Mock())
                message_delivered = True

        mock_producer_instance.produce.side_effect = mock_produce
        mock_producer_instance.poll.return_value = 1

        producer = AsyncSmartProducer(config)

        # Should wait for delivery confirmation
        await producer.produce(topic="test-topic", value=b"test-message")

        # Function should not return until delivery is confirmed
        assert message_delivered
