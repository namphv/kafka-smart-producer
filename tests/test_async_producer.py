"""Tests for AsyncSmartProducer."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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

    async def start_async(self):
        self._running = True

    async def stop_async(self):
        self._running = False

    @property
    def is_running(self):
        return self._running


class TestAsyncSmartProducer:
    @pytest.fixture(autouse=True)
    def _patch_kafka(self):
        with (
            patch(
                "kafka_smart_producer.producer.ConfluentProducer"
            ) as mock_producer_cls,
            patch("kafka_smart_producer.producer._build_monitor") as mock_build,
        ):
            self.mock_producer_cls = mock_producer_cls
            self.mock_producer = mock_producer_cls.return_value
            self.mock_build = mock_build
            self.mock_build.return_value = None

            # Make produce trigger delivery callback immediately
            def _produce_with_delivery(**kwargs):
                cb = kwargs.get("on_delivery")
                if cb:
                    cb(None, MagicMock())  # no error, mock message

            self.mock_producer.produce.side_effect = _produce_with_delivery
            self.mock_producer.poll.return_value = 0
            yield

    @pytest.mark.asyncio
    async def test_produce_without_health(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        cfg = _config(smart_enabled=False)
        async with AsyncSmartProducer(cfg) as producer:
            await producer.produce("test-topic", value=b"data")

        self.mock_producer.produce.assert_called_once()
        call_kwargs = self.mock_producer.produce.call_args[1]
        assert call_kwargs["topic"] == "test-topic"
        assert call_kwargs["value"] == b"data"

    @pytest.mark.asyncio
    async def test_produce_with_explicit_partition(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        async with AsyncSmartProducer(_config(), monitor=monitor) as producer:
            await producer.produce("test-topic", value=b"data", partition=5)

        call_kwargs = self.mock_producer.produce.call_args[1]
        assert call_kwargs["partition"] == 5

    @pytest.mark.asyncio
    async def test_produce_selects_healthy_partition(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": [1, 2]})
        async with AsyncSmartProducer(_config(), monitor=monitor) as producer:
            await producer.produce("test-topic", value=b"data")

        call_kwargs = self.mock_producer.produce.call_args[1]
        assert call_kwargs["partition"] in [1, 2]

    @pytest.mark.asyncio
    async def test_produce_fallback_when_no_healthy(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": []})
        async with AsyncSmartProducer(_config(), monitor=monitor) as producer:
            await producer.produce("test-topic", value=b"data")

        call_kwargs = self.mock_producer.produce.call_args[1]
        assert "partition" not in call_kwargs

    @pytest.mark.asyncio
    async def test_key_stickiness(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        producer = AsyncSmartProducer(_config(key_stickiness=True), monitor=monitor)

        for _ in range(5):
            await producer.produce("test-topic", value=b"data", key=b"user-1")

        partitions = [
            self.mock_producer.produce.call_args_list[i][1].get("partition")
            for i in range(5)
        ]
        assert len(set(partitions)) == 1
        assert partitions[0] in [0, 1, 2]

        await producer.close()

    @pytest.mark.asyncio
    async def test_key_stickiness_adapts_to_unhealthy(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": [0, 1, 2]})
        producer = AsyncSmartProducer(_config(key_stickiness=True), monitor=monitor)

        await producer.produce("test-topic", value=b"v", key=b"key-1")
        first_partition = self.mock_producer.produce.call_args[1]["partition"]

        remaining = [p for p in [0, 1, 2] if p != first_partition]
        monitor._healthy["test-topic"] = remaining

        await producer.produce("test-topic", value=b"v", key=b"key-1")
        second_partition = self.mock_producer.produce.call_args[1]["partition"]
        assert second_partition in remaining

        await producer.close()

    @pytest.mark.asyncio
    async def test_flush(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        self.mock_producer.flush.return_value = 0
        async with AsyncSmartProducer(_config(smart_enabled=False)) as producer:
            result = await producer.flush(timeout=5.0)
        assert result == 0

    @pytest.mark.asyncio
    async def test_close_stops_owned_monitor(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": [0]})
        await monitor.start_async()
        producer = AsyncSmartProducer(_config(), monitor=monitor)
        producer._monitor_owned = True
        await producer.close()
        assert not monitor.is_running

    @pytest.mark.asyncio
    async def test_close_does_not_stop_external_monitor(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        monitor = MockMonitor({"test-topic": [0]})
        await monitor.start_async()
        producer = AsyncSmartProducer(_config(), monitor=monitor)
        await producer.close()
        assert monitor.is_running

    @pytest.mark.asyncio
    async def test_produce_after_close_raises(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        producer = AsyncSmartProducer(_config(smart_enabled=False))
        await producer.close()
        with pytest.raises(RuntimeError, match="closed"):
            await producer.produce("test-topic", value=b"data")

    @pytest.mark.asyncio
    async def test_context_manager(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        async with AsyncSmartProducer(_config(smart_enabled=False)) as p:
            await p.produce("test-topic", value=b"data")
        assert p.closed

    @pytest.mark.asyncio
    async def test_closed_property(self):
        from kafka_smart_producer.producer import AsyncSmartProducer

        producer = AsyncSmartProducer(_config(smart_enabled=False))
        assert not producer.closed
        await producer.close()
        assert producer.closed
