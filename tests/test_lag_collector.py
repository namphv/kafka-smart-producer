"""Tests for KafkaAdminLagCollector with mocked Kafka."""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.exceptions import LagDataUnavailableError

# Patch at module level since AdminClient is called in __init__
MODULE = "kafka_smart_producer.health.collectors.kafka_admin"


def _make_collector(mock_admin_cls, mock_consumer_cls):
    """Create collector with mocked dependencies."""
    from kafka_smart_producer.health.collectors.kafka_admin import (
        KafkaAdminLagCollector,
    )

    return KafkaAdminLagCollector(
        bootstrap_servers="localhost:9092",
        consumer_group="test-group",
        timeout=5.0,
    )


def _setup_partitions(mock_admin_cls, topic, partition_ids):
    partition_mocks = {pid: Mock(id=pid) for pid in partition_ids}
    topic_meta = Mock(error=None, partitions=partition_mocks)
    cluster_meta = Mock(topics={topic: topic_meta})
    mock_admin_cls.return_value.list_topics.return_value = cluster_meta


def _setup_committed(mock_admin_cls, topic, offsets, group="test-group"):
    from confluent_kafka import TopicPartition

    tps = [TopicPartition(topic, pid, off) for pid, off in offsets.items()]
    result_mock = Mock()
    result_mock.topic_partitions = tps
    future_mock = Mock()
    future_mock.result.return_value = result_mock
    mock_admin_cls.return_value.list_consumer_group_offsets.return_value = {
        group: future_mock
    }


def _setup_watermarks(mock_consumer_cls, watermarks):
    consumer = Mock()
    consumer.get_watermark_offsets.side_effect = lambda tp, timeout=None: (
        0,
        watermarks.get(tp.partition, 0),
    )
    mock_consumer_cls.return_value = consumer


class TestKafkaAdminLagCollector:
    @pytest.fixture(autouse=True)
    def _patch(self):
        with (
            patch(f"{MODULE}.AdminClient") as admin,
            patch(f"{MODULE}.Consumer") as consumer,
        ):
            self.mock_admin = admin
            self.mock_consumer = consumer
            yield

    def _collector(self):
        return _make_collector(self.mock_admin, self.mock_consumer)

    def test_get_lag_data(self):
        _setup_partitions(self.mock_admin, "t", [0, 1, 2])
        _setup_committed(self.mock_admin, "t", {0: 100, 1: 200, 2: 300})
        _setup_watermarks(self.mock_consumer, {0: 150, 1: 250, 2: 350})

        lag = self._collector().get_lag_data("t")
        assert lag == {0: 50, 1: 50, 2: 50}

    def test_lag_never_negative(self):
        _setup_partitions(self.mock_admin, "t", [0])
        _setup_committed(self.mock_admin, "t", {0: 200})
        _setup_watermarks(self.mock_consumer, {0: 100})

        lag = self._collector().get_lag_data("t")
        assert lag[0] == 0

    def test_uncommitted_offset_treated_as_zero(self):
        _setup_partitions(self.mock_admin, "t", [0])
        _setup_committed(self.mock_admin, "t", {0: -1001})
        _setup_watermarks(self.mock_consumer, {0: 500})

        lag = self._collector().get_lag_data("t")
        assert lag[0] == 500

    def test_partition_cache(self):
        _setup_partitions(self.mock_admin, "t", [0])
        _setup_committed(self.mock_admin, "t", {0: 0})
        _setup_watermarks(self.mock_consumer, {0: 0})

        c = self._collector()
        c.get_lag_data("t")
        c.get_lag_data("t")  # cached
        assert self.mock_admin.return_value.list_topics.call_count == 1

    def test_clear_cache(self):
        _setup_partitions(self.mock_admin, "t", [0])
        _setup_committed(self.mock_admin, "t", {0: 0})
        _setup_watermarks(self.mock_consumer, {0: 0})

        c = self._collector()
        c.get_lag_data("t")
        c.clear_cache("t")
        c.get_lag_data("t")
        assert self.mock_admin.return_value.list_topics.call_count == 2

    def test_topic_not_found(self):
        cluster_meta = Mock(topics={})
        self.mock_admin.return_value.list_topics.return_value = cluster_meta

        with pytest.raises(LagDataUnavailableError, match="not found"):
            self._collector().get_lag_data("missing")

    def test_admin_error(self):
        self.mock_admin.return_value.list_topics.side_effect = Exception("boom")

        with pytest.raises(LagDataUnavailableError):
            self._collector().get_lag_data("t")

    def test_is_healthy(self):
        self.mock_admin.return_value.list_topics.return_value = Mock()
        assert self._collector().is_healthy() is True

    def test_is_unhealthy(self):
        self.mock_admin.return_value.list_topics.side_effect = Exception("down")
        assert self._collector().is_healthy() is False

    def test_repr(self):
        c = self._collector()
        assert "localhost:9092" in repr(c)
        assert "test-group" in repr(c)

    def test_multiple_partitions_mixed_lag(self):
        _setup_partitions(self.mock_admin, "t", [0, 1, 2, 3])
        _setup_committed(self.mock_admin, "t", {0: 100, 1: 0, 2: 500, 3: 999})
        _setup_watermarks(self.mock_consumer, {0: 100, 1: 1000, 2: 500, 3: 1000})

        lag = self._collector().get_lag_data("t")
        assert lag == {0: 0, 1: 1000, 2: 0, 3: 1}
