"""Tests for SmartProducer (sync)."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from kafka_smart_producer.config import SmartProducerConfig


def _config(**overrides):
    defaults = {
        "kafka_config": {"bootstrap.servers": "localhost:9092"},
        "topics": ["test-topic"],
        "consumer_group": "test-group",
    }
    defaults.update(overrides)
    return SmartProducerConfig(**defaults)


class MockMonitor:
    """Minimal monitor mock matching PartitionHealthMonitor interface."""

    def __init__(self, healthy=None):
        self._healthy = healthy or {}
        self._running = False

    def get_healthy_partitions(self, topic):
        return self._healthy.get(topic, [])

    def is_partition_healthy(self, topic, pid):
        return pid in self._healthy.get(topic, [])

    def start_sync(self):
        self._running = True

    def stop_sync(self):
        self._running = False

    @property
    def is_running(self):
        return self._running


class TestSmartProducer:
    @pytest.fixture(autouse=True)
    def _patch_kafka(self):
        with (
            patch("kafka_smart_producer.producer.ConfluentProducer") as mock_producer,
            patch("kafka_smart_producer.producer._build_monitor") as mock_build,
        ):
            self.mock_producer_cls = mock_producer
            self.mock_producer = mock_producer.return_value
            self.mock_build = mock_build
            self.mock_build.return_value = None
            yield

    def test_produce_without_health(self):
        from kafka_smart_producer.producer import SmartProducer

        cfg = _config(smart_enabled=False)
        producer = SmartProducer(cfg)
        producer.produce("test-topic", value=b"data")

        self.mock_producer.produce.assert_called_once()
        call_kwargs = self.mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "test-topic"
        assert call_kwargs["value"] == b"data"

    def test_produce_with_explicit_partition(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        producer = SmartProducer(_config(), monitor=monitor)
        producer.produce("test-topic", value=b"data", partition=5)

        call_kwargs = self.mock_producer.produce.call_args[1]
        assert call_kwargs["partition"] == 5  # explicit, not overridden

    def test_produce_selects_healthy_partition(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [1, 2]})
        producer = SmartProducer(_config(), monitor=monitor)
        producer.produce("test-topic", value=b"data")

        call_kwargs = self.mock_producer.produce.call_args[1]
        assert call_kwargs["partition"] in [1, 2]

    def test_produce_fallback_when_no_healthy(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": []})
        producer = SmartProducer(_config(), monitor=monitor)
        producer.produce("test-topic", value=b"data")

        call_kwargs = self.mock_producer.produce.call_args[1]
        assert "partition" not in call_kwargs  # let Kafka decide

    def test_key_stickiness(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        producer = SmartProducer(_config(key_stickiness=True), monitor=monitor)

        # Produce same key multiple times
        for _ in range(5):
            producer.produce("test-topic", value=b"data", key=b"user-1")

        # All should go to the same partition (cached)
        partitions = [
            self.mock_producer.produce.call_args_list[i][1].get("partition")
            for i in range(5)
        ]
        assert len(set(partitions)) == 1
        assert partitions[0] in [0, 1, 2]

    def test_key_stickiness_adapts_to_unhealthy(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        producer = SmartProducer(_config(key_stickiness=True), monitor=monitor)

        # Establish mapping
        producer.produce("test-topic", value=b"v", key=b"key-1")
        first_partition = self.mock_producer.produce.call_args[1]["partition"]

        # Make that partition unhealthy
        remaining = [p for p in [0, 1, 2] if p != first_partition]
        monitor._healthy["test-topic"] = remaining

        # Next produce should pick a different partition
        producer.produce("test-topic", value=b"v", key=b"key-1")
        second_partition = self.mock_producer.produce.call_args[1]["partition"]
        assert second_partition in remaining

    def test_no_stickiness(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        producer = SmartProducer(_config(key_stickiness=False), monitor=monitor)

        # Should not have a cache
        assert producer._cache is None

    def test_flush(self):
        from kafka_smart_producer.producer import SmartProducer

        producer = SmartProducer(_config(smart_enabled=False))
        producer.flush(timeout=5.0)
        self.mock_producer.flush.assert_called_with(5.0)

    def test_close_stops_owned_monitor(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [0]})
        monitor.start_sync()
        # Simulate owned monitor
        producer = SmartProducer(_config(), monitor=monitor)
        producer._monitor_owned = True
        producer.close()
        assert not monitor.is_running

    def test_close_does_not_stop_external_monitor(self):
        from kafka_smart_producer.producer import SmartProducer

        monitor = MockMonitor({"test-topic": [0]})
        monitor.start_sync()
        producer = SmartProducer(_config(), monitor=monitor)
        # monitor_owned is False since we passed monitor explicitly
        producer.close()
        assert monitor.is_running  # not stopped

    def test_context_manager(self):
        from kafka_smart_producer.producer import SmartProducer

        with SmartProducer(_config(smart_enabled=False)) as p:
            p.produce("test-topic", value=b"data")
        self.mock_producer.flush.assert_called()

    def test_getattr_proxies_to_confluent(self):
        from kafka_smart_producer.producer import SmartProducer

        producer = SmartProducer(_config(smart_enabled=False))
        # poll() should proxy to underlying producer
        producer.poll(0)
        self.mock_producer.poll.assert_called_with(0)
