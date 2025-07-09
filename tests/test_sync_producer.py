"""
Tests for the SyncSmartProducer implementation.

This module tests the synchronous smart producer including message delivery,
error handling, partition selection, and integration with the underlying
Kafka producer.
"""

import threading
import time
from typing import Dict, List, Optional
from unittest.mock import Mock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from kafka_smart_producer.exceptions import PartitionSelectionError
from kafka_smart_producer.producer import (
    MetadataRefreshError,
    PartitioningStrategy,
    ProducerConfig,
    ProduceResult,
    ProducerNotReadyError,
    SyncSmartProducer,
)


class MockHealthManager:
    """Mock health manager for testing."""

    def __init__(self, healthy_partitions: Dict[str, List[int]]):
        self._healthy_partitions = healthy_partitions
        self._selection_calls = []
        self._health_check_calls = []

    def select_partition(
        self,
        topic: str,
        key: Optional[bytes] = None,
        available_partitions: Optional[List[int]] = None,
    ) -> int:
        self._selection_calls.append((topic, key, available_partitions))
        healthy = self._healthy_partitions.get(topic, [0])
        if available_partitions:
            healthy = [p for p in healthy if p in available_partitions]
        return healthy[0] if healthy else 0

    def is_partition_healthy(self, topic: str, partition: int) -> bool:
        call = (topic, partition)
        self._health_check_calls.append(call)
        return partition in self._healthy_partitions.get(topic, [])

    def get_selection_calls(self) -> List:
        return self._selection_calls.copy()

    def get_health_check_calls(self) -> List:
        return self._health_check_calls.copy()


class MockConfluentProducer:
    """Mock confluent-kafka Producer for testing."""

    def __init__(self, config: Dict):
        self.config = config
        self.produced_messages = []
        self.delivery_callbacks = []
        self.closed = False
        self.flush_timeout = None
        self.remaining_messages = 0
        self.should_fail_produce = False
        self.should_fail_flush = False
        self.topic_metadata = {}

    def produce(
        self,
        topic,
        value=None,
        key=None,
        partition=None,
        timestamp=None,
        headers=None,
        callback=None,
    ):
        """Mock produce method."""
        if self.closed:
            raise RuntimeError("Producer is closed")

        if self.should_fail_produce:
            raise Exception("Produce failed")

        # Store the message
        msg_data = {
            "topic": topic,
            "value": value,
            "key": key,
            "partition": partition,
            "timestamp": timestamp,
            "headers": headers,
        }
        self.produced_messages.append(msg_data)

        # Store callback for later invocation
        if callback:
            self.delivery_callbacks.append((callback, msg_data))

    def poll(self, timeout=0):
        """Mock poll method."""
        # Simulate delivery callbacks
        for callback, msg_data in self.delivery_callbacks:
            mock_msg = Mock()
            mock_msg.offset.return_value = len(self.produced_messages)
            # Use original timestamp if provided, otherwise use current time
            original_timestamp = msg_data.get("timestamp", int(time.time() * 1000))
            mock_msg.timestamp.return_value = (1, original_timestamp)
            callback(None, mock_msg)  # Success
        self.delivery_callbacks.clear()
        return 0

    def flush(self, timeout=None):
        """Mock flush method."""
        if self.should_fail_flush:
            raise Exception("Flush failed")
        self.flush_timeout = timeout

        # Trigger all pending callbacks on flush
        for callback, msg_data in self.delivery_callbacks:
            mock_msg = Mock()
            mock_msg.offset.return_value = len(self.produced_messages)
            # Use original timestamp if provided, otherwise use current time
            original_timestamp = msg_data.get("timestamp", int(time.time() * 1000))
            mock_msg.timestamp.return_value = (1, original_timestamp)
            callback(None, mock_msg)  # Success
        self.delivery_callbacks.clear()

        return self.remaining_messages

    def list_topics(self, topic=None, timeout=None):
        """Mock list_topics method."""
        mock_metadata = Mock()
        mock_metadata.topics = {}

        if topic and topic in self.topic_metadata:
            mock_topic = Mock()
            mock_topic.partitions = {
                i: Mock() for i in range(self.topic_metadata[topic])
            }
            mock_metadata.topics[topic] = mock_topic

        return mock_metadata

    def set_topic_partitions(self, topic: str, partition_count: int):
        """Helper to set topic partition count."""
        self.topic_metadata[topic] = partition_count


class TestSyncSmartProducer:
    """Test SyncSmartProducer functionality."""

    def create_producer(
        self,
        config: Optional[ProducerConfig] = None,
        healthy_partitions: Optional[Dict[str, List[int]]] = None,
        kafka_config: Optional[Dict] = None,
    ) -> tuple[SyncSmartProducer, MockHealthManager, MockConfluentProducer]:
        """Create a test producer with mocks."""
        config = config or ProducerConfig()
        healthy_partitions = healthy_partitions or {"test-topic": [0, 1, 2]}
        kafka_config = kafka_config or {"bootstrap.servers": "localhost:9092"}

        health_manager = MockHealthManager(healthy_partitions)
        mock_producer = MockConfluentProducer(kafka_config)

        with patch(
            "kafka_smart_producer.producer.ConfluentProducer",
            return_value=mock_producer,
        ):
            producer = SyncSmartProducer(config, health_manager, kafka_config)

        return producer, health_manager, mock_producer

    def test_producer_initialization(self):
        """Test producer initialization."""
        producer, health_manager, mock_producer = self.create_producer()

        assert not producer.closed
        assert producer._producer is mock_producer

    def test_basic_message_production(self):
        """Test basic message production with smart partition selection."""
        producer, health_manager, mock_producer = self.create_producer()

        result = producer.produce(
            topic="test-topic", value=b"test-value", key=b"test-key"
        )

        # Check result
        assert isinstance(result, ProduceResult)
        assert result.metadata.topic == "test-topic"
        assert result.metadata.key == b"test-key"
        assert result.metadata.partition in [0, 1, 2]  # Smart selection

        # Check that message was produced
        assert len(mock_producer.produced_messages) == 1
        produced_msg = mock_producer.produced_messages[0]
        assert produced_msg["topic"] == "test-topic"
        assert produced_msg["value"] == b"test-value"
        assert produced_msg["key"] == b"test-key"

        # Check that smart partition selection was used
        assert len(health_manager.get_selection_calls()) == 1

    def test_explicit_partition_bypasses_smart_selection(self):
        """Test that explicit partition bypasses smart selection."""
        producer, health_manager, mock_producer = self.create_producer()

        result = producer.produce(topic="test-topic", value=b"test-value", partition=1)

        assert result.metadata.partition == 1
        assert len(health_manager.get_selection_calls()) == 0  # No smart selection

        produced_msg = mock_producer.produced_messages[0]
        assert produced_msg["partition"] == 1

    def test_delivery_callback_success(self):
        """Test delivery callback on successful delivery."""
        producer, health_manager, mock_producer = self.create_producer()

        callback_results = []

        def on_delivery(result: ProduceResult):
            callback_results.append(result)

        producer.produce(
            topic="test-topic", value=b"test-value", on_delivery=on_delivery
        )

        # Trigger delivery callback
        producer._producer.poll(0)

        # Check callback was called
        assert len(callback_results) == 1
        callback_result = callback_results[0]
        assert callback_result.success
        assert callback_result.metadata.offset is not None
        assert callback_result.latency_ms is not None

    def test_delivery_callback_failure(self):
        """Test delivery callback on delivery failure."""
        producer, health_manager, mock_producer = self.create_producer()

        callback_results = []

        def on_delivery(result: ProduceResult):
            callback_results.append(result)

        # Override the mock flush method to simulate failure
        def mock_flush_with_error(timeout=None):
            for callback, _msg_data in mock_producer.delivery_callbacks:
                error = KafkaError(1, "Test error")
                callback(error, None)
            mock_producer.delivery_callbacks.clear()
            return 0

        mock_producer.flush = mock_flush_with_error

        producer.produce(
            topic="test-topic", value=b"test-value", on_delivery=on_delivery
        )

        # Check callback was called with error
        assert len(callback_results) == 1
        callback_result = callback_results[0]
        assert not callback_result.success
        assert isinstance(callback_result.error, KafkaException)

    def test_produce_with_headers_and_timestamp(self):
        """Test producing with headers and timestamp."""
        producer, health_manager, mock_producer = self.create_producer()

        headers = {"header1": b"value1", "header2": b"value2"}
        timestamp = int(time.time() * 1000)

        result = producer.produce(
            topic="test-topic",
            value=b"test-value",
            key=b"test-key",
            headers=headers,
            timestamp=timestamp,
        )

        assert result.metadata.timestamp == timestamp

        produced_msg = mock_producer.produced_messages[0]
        assert produced_msg["headers"] == headers
        assert produced_msg["timestamp"] == timestamp

    def test_produce_failure_handling(self):
        """Test handling of produce failures."""
        producer, health_manager, mock_producer = self.create_producer()

        # Make producer fail
        mock_producer.should_fail_produce = True

        result = producer.produce(topic="test-topic", value=b"test-value")

        assert not result.success
        assert result.error is not None
        assert result.latency_ms is not None

    def test_partition_selection_failure(self):
        """Test handling of partition selection failures."""
        config = ProducerConfig(force_fallback_on_error=False)
        producer, health_manager, mock_producer = self.create_producer(config)

        # Make health manager fail
        def failing_select(*args, **kwargs):
            raise Exception("Health manager failed")

        health_manager.select_partition = failing_select

        with pytest.raises(PartitionSelectionError):
            producer.produce(topic="test-topic", value=b"test-value")

    def test_produce_on_closed_producer(self):
        """Test producing on a closed producer."""
        producer, health_manager, mock_producer = self.create_producer()

        producer.close()

        with pytest.raises(ProducerNotReadyError):
            producer.produce(topic="test-topic", value=b"test-value")

    def test_flush_functionality(self):
        """Test flush functionality."""
        producer, health_manager, mock_producer = self.create_producer()

        # Produce some messages
        producer.produce(topic="test-topic", value=b"msg1")
        producer.produce(topic="test-topic", value=b"msg2")

        # Test flush with timeout
        producer.flush(timeout=5.0)

        assert mock_producer.flush_timeout == 5.0

    def test_flush_with_timeout_warning(self):
        """Test flush with timeout warning."""
        producer, health_manager, mock_producer = self.create_producer()

        # Set remaining messages to simulate timeout
        mock_producer.remaining_messages = 5

        with patch("kafka_smart_producer.producer.logger") as mock_logger:
            producer.flush(timeout=1.0)
            mock_logger.warning.assert_called_once()

    def test_flush_failure_handling(self):
        """Test flush failure handling."""
        producer, health_manager, mock_producer = self.create_producer()

        mock_producer.should_fail_flush = True

        with pytest.raises(Exception, match="Flush failed"):
            producer.flush()

    def test_flush_on_closed_producer(self):
        """Test flush on closed producer."""
        producer, health_manager, mock_producer = self.create_producer()

        producer.close()

        with patch("kafka_smart_producer.producer.logger") as mock_logger:
            producer.flush()
            mock_logger.warning.assert_called_once()

    def test_close_functionality(self):
        """Test producer close functionality."""
        producer, health_manager, mock_producer = self.create_producer()

        # Produce a message
        producer.produce(topic="test-topic", value=b"test-value")

        # Close producer
        producer.close()

        assert producer.closed
        assert producer._producer is None
        assert mock_producer.flush_timeout == 10.0  # Default flush timeout

    def test_close_idempotent(self):
        """Test that close is idempotent."""
        producer, health_manager, mock_producer = self.create_producer()

        producer.close()
        producer.close()  # Should not raise error

        assert producer.closed

    def test_close_with_flush_failure(self):
        """Test close with flush failure."""
        producer, health_manager, mock_producer = self.create_producer()

        mock_producer.should_fail_flush = True

        with pytest.raises(Exception, match="Flush failed"):
            producer.close()

        # Producer should still be marked as closed
        assert producer.closed

    def test_metadata_refresh_success(self):
        """Test successful metadata refresh."""
        producer, health_manager, mock_producer = self.create_producer()

        # Set up mock metadata
        mock_producer.set_topic_partitions("test-topic", 5)

        # Trigger metadata refresh
        producer._refresh_topic_metadata("test-topic")

        # Check that partition count was updated
        assert producer._partition_counts["test-topic"] == 5

    def test_metadata_refresh_topic_not_found(self):
        """Test metadata refresh when topic not found."""
        producer, health_manager, mock_producer = self.create_producer()

        with patch("kafka_smart_producer.producer.logger") as mock_logger:
            producer._refresh_topic_metadata("nonexistent-topic")
            mock_logger.warning.assert_called_once()

    def test_metadata_refresh_failure(self):
        """Test metadata refresh failure."""
        producer, health_manager, mock_producer = self.create_producer()

        # Make list_topics fail
        def failing_list_topics(*args, **kwargs):
            raise Exception("Metadata fetch failed")

        mock_producer.list_topics = failing_list_topics

        with pytest.raises(MetadataRefreshError):
            producer._refresh_topic_metadata("test-topic")

    def test_metadata_refresh_on_closed_producer(self):
        """Test metadata refresh on closed producer."""
        producer, health_manager, mock_producer = self.create_producer()

        producer.close()

        # Should not raise error, just return
        producer._refresh_topic_metadata("test-topic")

    def test_concurrent_produce_operations(self):
        """Test concurrent produce operations."""
        producer, health_manager, mock_producer = self.create_producer()

        num_threads = 10
        messages_per_thread = 20
        results = []
        errors = []

        def worker(thread_id: int):
            try:
                for i in range(messages_per_thread):
                    result = producer.produce(
                        topic="test-topic",
                        value=f"thread_{thread_id}_msg_{i}".encode(),
                        key=f"key_{thread_id}_{i}".encode(),
                    )
                    results.append(result)
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Concurrent produce errors: {errors}"
        assert len(results) == num_threads * messages_per_thread
        assert len(mock_producer.produced_messages) == num_threads * messages_per_thread

        # Verify all results have valid metadata
        for result in results:
            assert isinstance(result, ProduceResult)
            assert result.metadata.topic == "test-topic"
            assert result.metadata.partition in [0, 1, 2]

    def test_integration_with_base_class_caching(self):
        """Test integration with base class key caching."""
        config = ProducerConfig(enable_key_caching=True)
        producer, health_manager, mock_producer = self.create_producer(config)

        key = b"test-key"

        # First produce should trigger partition selection
        result1 = producer.produce(topic="test-topic", key=key, value=b"msg1")

        # Second produce should use cached partition
        result2 = producer.produce(topic="test-topic", key=key, value=b"msg2")

        # Should use same partition
        assert result1.metadata.partition == result2.metadata.partition

        # Should only have one health manager call (caching worked)
        assert len(health_manager.get_selection_calls()) == 1

    def test_integration_with_base_class_metrics(self):
        """Test integration with base class metrics."""
        config = ProducerConfig(enable_metrics=True)
        producer, health_manager, mock_producer = self.create_producer(config)

        # Produce several messages
        for i in range(5):
            producer.produce(topic="test-topic", value=f"msg{i}".encode())

        # Check metrics
        metrics = producer.get_metrics()
        assert metrics["messages_produced"] == 5
        assert metrics["smart_selection_rate"] == 1.0
        assert metrics["avg_partition_selection_time_ms"] > 0

    def test_integration_with_base_class_fallback(self):
        """Test integration with base class fallback strategies."""
        config = ProducerConfig(
            enable_smart_partitioning=False,
            fallback_strategy=PartitioningStrategy.ROUND_ROBIN,
        )
        producer, health_manager, mock_producer = self.create_producer(config)

        # Set up metadata for the topic
        producer._partition_counts["test-topic"] = 3

        # Produce messages and check round-robin behavior
        partitions = []
        for i in range(6):
            result = producer.produce(topic="test-topic", value=f"msg{i}".encode())
            partitions.append(result.metadata.partition)

        # Should see all partitions in round-robin
        assert set(partitions) == {0, 1, 2}

        # Should not use smart partitioning
        assert len(health_manager.get_selection_calls()) == 0
