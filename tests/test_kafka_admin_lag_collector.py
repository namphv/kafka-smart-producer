"""
Comprehensive tests for KafkaAdminLagCollector implementation.

This module provides complete test coverage for the KafkaAdminLagCollector class,
including core functionality, error handling, health checks, and edge cases.
"""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.exceptions import LagDataUnavailableError
from kafka_smart_producer.lag_collector import KafkaAdminLagCollector


class TestKafkaAdminLagCollectorCore:
    """Test core KafkaAdminLagCollector functionality."""

    @pytest.fixture
    def mock_admin_client(self):
        """Mock AdminClient for testing."""
        admin_client = Mock()
        return admin_client

    @pytest.fixture
    def mock_consumer(self):
        """Mock Consumer for testing."""
        consumer = Mock()
        return consumer

    @pytest.fixture
    def lag_collector(self, mock_admin_client):
        """Create lag collector with mocked dependencies."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            return collector

    def test_initialization(self, mock_admin_client):
        """Test KafkaAdminLagCollector initialization."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092,localhost:9093",
                consumer_group="my-consumer-group",
                timeout_seconds=10.0,
                security_protocol="SASL_SSL",
            )

            assert collector._bootstrap_servers == "localhost:9092,localhost:9093"
            assert collector._consumer_group == "my-consumer-group"
            assert collector._timeout_seconds == 10.0
            assert collector._topic_partitions_cache == {}

            # Verify AdminClient was created with correct config
            mock_admin_class.assert_called_once()
            call_kwargs = mock_admin_class.call_args[0][0]
            assert call_kwargs["bootstrap.servers"] == "localhost:9092,localhost:9093"
            assert call_kwargs["security_protocol"] == "SASL_SSL"

    def test_repr(self, lag_collector):
        """Test string representation."""
        repr_str = repr(lag_collector)
        assert "KafkaAdminLagCollector" in repr_str
        assert "localhost:9092" in repr_str
        assert "test-group" in repr_str
        assert "5.0s" in repr_str

    def test_get_lag_data_full_workflow(self, lag_collector):
        """Test complete get_lag_data workflow."""
        # Mock all internal methods
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "_get_committed_offsets") as mock_committed,
            patch.object(lag_collector, "_get_high_water_marks") as mock_watermarks,
        ):
            # Setup mocks
            mock_partitions.return_value = [0, 1, 2]
            mock_committed.return_value = {0: 100, 1: 200, 2: 50}
            mock_watermarks.return_value = {0: 120, 1: 180, 2: 75}

            # Call get_lag_data
            result = lag_collector.get_lag_data("test-topic")

            # Verify result
            expected_lag = {0: 20, 1: 0, 2: 25}  # max(0, watermark - committed)
            assert result == expected_lag

            # Verify method calls
            mock_partitions.assert_called_once_with("test-topic")
            mock_committed.assert_called_once_with("test-topic", [0, 1, 2])
            mock_watermarks.assert_called_once_with("test-topic", [0, 1, 2])

    def test_get_lag_data_negative_lag_handling(self, lag_collector):
        """Test that negative lag values are handled correctly."""
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "_get_committed_offsets") as mock_committed,
            patch.object(lag_collector, "_get_high_water_marks") as mock_watermarks,
        ):
            # Setup scenario where committed > watermark (handle gracefully)
            mock_partitions.return_value = [0, 1]
            mock_committed.return_value = {0: 100, 1: 200}
            mock_watermarks.return_value = {
                0: 120,
                1: 150,
            }  # 1 has committed > watermark

            result = lag_collector.get_lag_data("test-topic")

            # Lag should never be negative
            expected_lag = {0: 20, 1: 0}  # max(0, 150-200) = 0
            assert result == expected_lag

    def test_get_lag_data_error_handling(self, lag_collector):
        """Test error handling in get_lag_data."""
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "clear_topic_cache") as mock_clear_cache,
        ):
            # Mock an exception during partition retrieval
            mock_partitions.side_effect = Exception("Connection failed")

            # Should raise LagDataUnavailableError
            with pytest.raises(LagDataUnavailableError) as exc_info:
                lag_collector.get_lag_data("test-topic")

            # Verify error details
            assert "Failed to collect lag data" in str(exc_info.value)
            assert "test-topic" in str(exc_info.value)
            assert "test-group" in str(exc_info.value)
            assert exc_info.value.context["topic"] == "test-topic"
            assert exc_info.value.context["consumer_group"] == "test-group"

            # Verify cache was cleared
            mock_clear_cache.assert_called_once_with("test-topic")


class TestKafkaAdminLagCollectorTopicPartitions:
    """Test topic partition discovery functionality."""

    @pytest.fixture
    def lag_collector(self):
        """Create lag collector with mocked AdminClient."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            # Store reference to mock for test access
            collector._mock_admin_client = mock_admin_client
            return collector

    def test_get_topic_partitions_describe_topics_success(self, lag_collector):
        """Test successful partition discovery via describe_topics."""
        # Mock successful describe_topics response
        mock_topic_collection = Mock()
        mock_partition_info = [Mock(id=0), Mock(id=1), Mock(id=2)]
        mock_topic_desc = Mock(partitions=mock_partition_info)
        mock_future = Mock()
        mock_future.result.return_value = mock_topic_desc

        with patch(
            "kafka_smart_producer.lag_collector._TopicCollection"
        ) as mock_topic_collection_class:
            mock_topic_collection_class.return_value = mock_topic_collection
            lag_collector._admin_client.describe_topics.return_value = {
                "test-topic": mock_future
            }

            result = lag_collector._get_topic_partitions("test-topic")

            assert result == [0, 1, 2]
            lag_collector._admin_client.describe_topics.assert_called_once_with(
                mock_topic_collection, request_timeout=5.0
            )

    def test_get_topic_partitions_list_topics_fallback(self, lag_collector):
        """Test fallback to list_topics when describe_topics fails."""
        # Mock describe_topics to fail
        with patch("kafka_smart_producer.lag_collector._TopicCollection"):
            lag_collector._admin_client.describe_topics.side_effect = Exception(
                "Describe failed"
            )

            # Mock successful list_topics response
            mock_topic_metadata = Mock()
            mock_topic_metadata.error = None
            mock_topic_metadata.partitions = {
                0: Mock(),
                1: Mock(),
                2: Mock(),
                3: Mock(),
            }

            mock_cluster_metadata = Mock()
            mock_cluster_metadata.topics = {"test-topic": mock_topic_metadata}
            lag_collector._admin_client.list_topics.return_value = mock_cluster_metadata

            result = lag_collector._get_topic_partitions("test-topic")

            assert sorted(result) == [0, 1, 2, 3]
            lag_collector._admin_client.list_topics.assert_called_once_with(
                topic="test-topic", timeout=5.0
            )

    def test_get_topic_partitions_topic_error(self, lag_collector):
        """Test handling of topic-level errors."""
        # Mock describe_topics to fail
        with patch("kafka_smart_producer.lag_collector._TopicCollection"):
            lag_collector._admin_client.describe_topics.side_effect = Exception(
                "Describe failed"
            )

            # Mock list_topics with topic error
            mock_topic_error = Exception("Topic not found")
            mock_topic_metadata = Mock()
            mock_topic_metadata.error = mock_topic_error

            mock_cluster_metadata = Mock()
            mock_cluster_metadata.topics = {"test-topic": mock_topic_metadata}
            lag_collector._admin_client.list_topics.return_value = mock_cluster_metadata

            with pytest.raises(
                LagDataUnavailableError, match="Topic 'test-topic' has error"
            ):
                lag_collector._get_topic_partitions("test-topic")

    def test_get_topic_partitions_topic_not_found(self, lag_collector):
        """Test handling when topic is not found."""
        # Mock describe_topics to fail
        with patch("kafka_smart_producer.lag_collector._TopicCollection"):
            lag_collector._admin_client.describe_topics.side_effect = Exception(
                "Describe failed"
            )

            # Mock list_topics with empty topics
            mock_cluster_metadata = Mock()
            mock_cluster_metadata.topics = {}  # Topic not in cluster
            lag_collector._admin_client.list_topics.return_value = mock_cluster_metadata

            with pytest.raises(
                LagDataUnavailableError, match="Topic 'test-topic' not found"
            ):
                lag_collector._get_topic_partitions("test-topic")

    def test_get_topic_partitions_complete_failure(self, lag_collector):
        """Test handling when both describe_topics and list_topics fail."""
        # Mock both methods to fail
        with patch("kafka_smart_producer.lag_collector._TopicCollection"):
            lag_collector._admin_client.describe_topics.side_effect = Exception(
                "Describe failed"
            )
            lag_collector._admin_client.list_topics.side_effect = Exception(
                "List failed"
            )

            with pytest.raises(
                LagDataUnavailableError, match="Failed to get partition metadata"
            ):
                lag_collector._get_topic_partitions("test-topic")


class TestKafkaAdminLagCollectorCommittedOffsets:
    """Test committed offset retrieval functionality."""

    @pytest.fixture
    def lag_collector(self):
        """Create lag collector with mocked AdminClient."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            collector._mock_admin_client = mock_admin_client
            return collector

    def test_get_committed_offsets_success(self, lag_collector):
        """Test successful committed offset retrieval."""
        # Mock successful response
        mock_topic_partition_1 = Mock()
        mock_topic_partition_1.topic = "test-topic"
        mock_topic_partition_1.partition = 0
        mock_topic_partition_1.offset = 100

        mock_topic_partition_2 = Mock()
        mock_topic_partition_2.topic = "test-topic"
        mock_topic_partition_2.partition = 1
        mock_topic_partition_2.offset = 200

        mock_result = Mock()
        mock_result.topic_partitions = [mock_topic_partition_1, mock_topic_partition_2]

        mock_future = Mock()
        mock_future.result.return_value = mock_result

        with patch("kafka_smart_producer.lag_collector._ConsumerGroupTopicPartitions"):
            lag_collector._admin_client.list_consumer_group_offsets.return_value = {
                "test-group": mock_future
            }

            result = lag_collector._get_committed_offsets("test-topic", [0, 1])

            assert result == {0: 100, 1: 200}

            # Verify AdminClient call
            lag_collector._admin_client.list_consumer_group_offsets.assert_called_once()

    def test_get_committed_offsets_no_committed_offset(self, lag_collector):
        """Test handling of partitions with no committed offsets (offset = -1001)."""
        # Mock response with no committed offset
        mock_topic_partition = Mock()
        mock_topic_partition.topic = "test-topic"
        mock_topic_partition.partition = 0
        mock_topic_partition.offset = -1001  # No committed offset

        mock_result = Mock()
        mock_result.topic_partitions = [mock_topic_partition]

        mock_future = Mock()
        mock_future.result.return_value = mock_result

        with patch("kafka_smart_producer.lag_collector._ConsumerGroupTopicPartitions"):
            lag_collector._admin_client.list_consumer_group_offsets.return_value = {
                "test-group": mock_future
            }

            result = lag_collector._get_committed_offsets("test-topic", [0])

            # Should default to offset 0 when no offset is committed
            assert result == {0: 0}

    def test_get_committed_offsets_mixed_topics(self, lag_collector):
        """Test filtering of topic partitions for correct topic."""
        # Mock response with mixed topics
        mock_tp_correct = Mock()
        mock_tp_correct.topic = "test-topic"
        mock_tp_correct.partition = 0
        mock_tp_correct.offset = 100

        mock_tp_other = Mock()
        mock_tp_other.topic = "other-topic"
        mock_tp_other.partition = 0
        mock_tp_other.offset = 200

        mock_result = Mock()
        mock_result.topic_partitions = [mock_tp_correct, mock_tp_other]

        mock_future = Mock()
        mock_future.result.return_value = mock_result

        with patch("kafka_smart_producer.lag_collector._ConsumerGroupTopicPartitions"):
            lag_collector._admin_client.list_consumer_group_offsets.return_value = {
                "test-group": mock_future
            }

            result = lag_collector._get_committed_offsets("test-topic", [0])

            # Should only return offsets for the requested topic
            assert result == {0: 100}

    def test_get_committed_offsets_error_handling(self, lag_collector):
        """Test error handling in committed offset retrieval."""
        with patch("kafka_smart_producer.lag_collector._ConsumerGroupTopicPartitions"):
            lag_collector._admin_client.list_consumer_group_offsets.side_effect = (
                Exception("API failed")
            )

            with pytest.raises(
                LagDataUnavailableError, match="Failed to get committed offsets"
            ):
                lag_collector._get_committed_offsets("test-topic", [0, 1])


class TestKafkaAdminLagCollectorHighWaterMarks:
    """Test high water mark retrieval functionality."""

    @pytest.fixture
    def lag_collector(self):
        """Create lag collector with mocked AdminClient."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            return collector

    def test_get_high_water_marks_success(self, lag_collector):
        """Test successful high water mark retrieval."""
        # Mock consumer at the method call level
        with patch(
            "kafka_smart_producer.lag_collector.Consumer"
        ) as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer_class.return_value = mock_consumer

            # Mock watermark responses
            def get_watermark_side_effect(tp, timeout):
                if tp.partition == 0:
                    return (10, 120)  # (low, high)
                elif tp.partition == 1:
                    return (5, 250)
                elif tp.partition == 2:
                    return (0, 75)

            mock_consumer.get_watermark_offsets.side_effect = get_watermark_side_effect

            result = lag_collector._get_high_water_marks("test-topic", [0, 1, 2])

            assert result == {0: 120, 1: 250, 2: 75}

            # Verify consumer was closed
            mock_consumer.close.assert_called_once()

    def test_get_high_water_marks_consumer_error(self, lag_collector):
        """Test error handling in high water mark retrieval."""
        # Mock consumer at the method call level
        with patch(
            "kafka_smart_producer.lag_collector.Consumer"
        ) as mock_consumer_class:
            mock_consumer = Mock()
            mock_consumer.get_watermark_offsets.side_effect = Exception(
                "Consumer failed"
            )
            mock_consumer_class.return_value = mock_consumer

            with pytest.raises(
                LagDataUnavailableError, match="Failed to get high water marks"
            ):
                lag_collector._get_high_water_marks("test-topic", [0])

            # Verify consumer was still closed despite error
            mock_consumer.close.assert_called_once()

    def test_get_high_water_marks_consumer_creation_error(self, lag_collector):
        """Test error when consumer creation fails."""
        # Mock consumer creation to fail
        with patch(
            "kafka_smart_producer.lag_collector.Consumer"
        ) as mock_consumer_class:
            mock_consumer_class.side_effect = Exception("Consumer creation failed")

            with pytest.raises(
                LagDataUnavailableError, match="Failed to get high water marks"
            ):
                lag_collector._get_high_water_marks("test-topic", [0])


class TestKafkaAdminLagCollectorHealthCheck:
    """Test health check functionality."""

    @pytest.fixture
    def lag_collector(self):
        """Create lag collector with mocked AdminClient."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            collector._mock_admin_client = mock_admin_client
            return collector

    def test_is_healthy_success(self, lag_collector):
        """Test successful health check."""
        # Mock successful cluster description
        mock_future = Mock()
        mock_future.result.return_value = Mock()  # Successful cluster metadata
        lag_collector._admin_client.describe_cluster.return_value = mock_future

        assert lag_collector.is_healthy() is True

        # Verify describe_cluster was called with shorter timeout
        lag_collector._admin_client.describe_cluster.assert_called_once_with(
            request_timeout=2.0
        )

    def test_is_healthy_failure(self, lag_collector):
        """Test health check failure."""
        # Mock failed cluster description
        lag_collector._admin_client.describe_cluster.side_effect = Exception(
            "Connection failed"
        )

        assert lag_collector.is_healthy() is False

    def test_is_healthy_timeout_adjustment(self, lag_collector):
        """Test that health check uses minimum of 2.0s and configured timeout."""
        # Test with shorter configured timeout
        lag_collector._timeout_seconds = 1.0

        mock_future = Mock()
        mock_future.result.return_value = Mock()
        lag_collector._admin_client.describe_cluster.return_value = mock_future

        lag_collector.is_healthy()

        # Should use the configured timeout (1.0s) since it's less than 2.0s
        lag_collector._admin_client.describe_cluster.assert_called_once_with(
            request_timeout=1.0
        )


class TestKafkaAdminLagCollectorCacheIntegration:
    """Test cache integration with existing caching tests."""

    @pytest.fixture
    def lag_collector(self):
        """Create lag collector with mocked AdminClient."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            return collector

    def test_cache_methods_integration(self, lag_collector):
        """Test cache methods work correctly with actual collector."""
        # Test initial empty cache
        assert lag_collector._topic_partitions_cache == {}

        # Test clear_topic_cache with specific topic
        lag_collector._topic_partitions_cache["topic1"] = [0, 1]
        lag_collector._topic_partitions_cache["topic2"] = [0, 1, 2]

        lag_collector.clear_topic_cache("topic1")
        assert "topic1" not in lag_collector._topic_partitions_cache
        assert "topic2" in lag_collector._topic_partitions_cache

        # Test clear_topic_cache all topics
        lag_collector.clear_topic_cache()
        assert lag_collector._topic_partitions_cache == {}

    def test_cached_vs_uncached_partition_retrieval(self, lag_collector):
        """Test that cached partition retrieval works as expected."""
        with patch.object(
            lag_collector, "_get_topic_partitions"
        ) as mock_get_partitions:
            mock_get_partitions.return_value = [0, 1, 2, 3]

            # First call - should call _get_topic_partitions and cache result
            result1 = lag_collector._get_topic_partitions_cached("test-topic")
            assert result1 == [0, 1, 2, 3]
            assert lag_collector._topic_partitions_cache["test-topic"] == [0, 1, 2, 3]
            mock_get_partitions.assert_called_once_with("test-topic")

            # Second call - should use cache
            result2 = lag_collector._get_topic_partitions_cached("test-topic")
            assert result2 == [0, 1, 2, 3]
            mock_get_partitions.assert_called_once()  # Still only called once

    def test_error_clears_cache_integration(self, lag_collector):
        """Test that errors in get_lag_data clear the topic cache."""
        # Pre-populate cache
        lag_collector._topic_partitions_cache["test-topic"] = [0, 1]
        lag_collector._topic_partitions_cache["other-topic"] = [0, 1, 2]

        # Mock to trigger error in get_lag_data
        with patch.object(
            lag_collector, "_get_topic_partitions_cached"
        ) as mock_partitions:
            mock_partitions.side_effect = Exception("Test error")

            with pytest.raises(LagDataUnavailableError):
                lag_collector.get_lag_data("test-topic")

            # test-topic cache should be cleared, other-topic should remain
            assert "test-topic" not in lag_collector._topic_partitions_cache
            assert "other-topic" in lag_collector._topic_partitions_cache


class TestKafkaAdminLagCollectorEdgeCases:
    """Test edge cases and unusual scenarios."""

    @pytest.fixture
    def lag_collector(self):
        """Create lag collector with mocked AdminClient."""
        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
                timeout_seconds=5.0,
            )

            return collector

    def test_empty_partitions_list(self, lag_collector):
        """Test behavior with empty partitions list."""
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "_get_committed_offsets") as mock_committed,
            patch.object(lag_collector, "_get_high_water_marks") as mock_watermarks,
        ):
            # Setup empty partitions
            mock_partitions.return_value = []
            mock_committed.return_value = {}
            mock_watermarks.return_value = {}

            result = lag_collector.get_lag_data("empty-topic")

            assert result == {}
            mock_committed.assert_called_once_with("empty-topic", [])
            mock_watermarks.assert_called_once_with("empty-topic", [])

    def test_missing_partition_data(self, lag_collector):
        """Test handling when partition data is inconsistent."""
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "_get_committed_offsets") as mock_committed,
            patch.object(lag_collector, "_get_high_water_marks") as mock_watermarks,
        ):
            # Setup inconsistent data - partition 1 missing from offsets
            mock_partitions.return_value = [0, 1, 2]
            mock_committed.return_value = {0: 100, 2: 200}  # Missing partition 1
            mock_watermarks.return_value = {0: 120, 1: 180, 2: 220}

            result = lag_collector.get_lag_data("test-topic")

            # Should handle missing committed offset gracefully (default to 0)
            expected_lag = {0: 20, 1: 180, 2: 20}
            assert result == expected_lag

    def test_very_large_lag_values(self, lag_collector):
        """Test handling of very large lag values."""
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "_get_committed_offsets") as mock_committed,
            patch.object(lag_collector, "_get_high_water_marks") as mock_watermarks,
        ):
            # Setup large lag scenario
            mock_partitions.return_value = [0, 1]
            mock_committed.return_value = {0: 1000, 1: 5000}
            mock_watermarks.return_value = {
                0: 1000000,
                1: 2000000,
            }  # Very large watermarks

            result = lag_collector.get_lag_data("test-topic")

            expected_lag = {0: 999000, 1: 1995000}
            assert result == expected_lag

    def test_zero_values_handling(self, lag_collector):
        """Test handling of zero values in offsets and watermarks."""
        with (
            patch.object(
                lag_collector, "_get_topic_partitions_cached"
            ) as mock_partitions,
            patch.object(lag_collector, "_get_committed_offsets") as mock_committed,
            patch.object(lag_collector, "_get_high_water_marks") as mock_watermarks,
        ):
            # Setup zero values scenario
            mock_partitions.return_value = [0, 1, 2]
            mock_committed.return_value = {0: 0, 1: 0, 2: 10}
            mock_watermarks.return_value = {0: 0, 1: 100, 2: 0}  # Mixed zeros

            result = lag_collector.get_lag_data("test-topic")

            expected_lag = {0: 0, 1: 100, 2: 0}  # max(0, watermark - committed)
            assert result == expected_lag

    def test_custom_kafka_config_parameters(self):
        """Test that custom Kafka configuration is passed correctly."""
        custom_config = {
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": "user",
            "sasl.password": "pass",
        }

        with patch(
            "kafka_smart_producer.lag_collector.AdminClient"
        ) as mock_admin_class:
            mock_admin_client = Mock()
            mock_admin_class.return_value = mock_admin_client

            collector = KafkaAdminLagCollector(
                bootstrap_servers="secure-kafka:9093",
                consumer_group="secure-group",
                timeout_seconds=10.0,
                **custom_config,
            )

            # Verify AdminClient was created with merged config
            call_kwargs = mock_admin_class.call_args[0][0]
            assert call_kwargs["bootstrap.servers"] == "secure-kafka:9093"
            assert call_kwargs["security.protocol"] == "SASL_SSL"
            assert call_kwargs["sasl.mechanism"] == "PLAIN"
            assert call_kwargs["sasl.username"] == "user"
            assert call_kwargs["sasl.password"] == "pass"

            # Verify consumer config was also set up correctly
            assert collector._consumer_config["security.protocol"] == "SASL_SSL"
            assert (
                collector._consumer_config["group.id"] == "lag_collector_secure-group"
            )
            assert collector._consumer_config["enable.auto.commit"] is False
