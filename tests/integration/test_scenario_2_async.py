"""
Scenario 2: Async Concurrent Producer

Real integration test: AsyncSmartProducer + asyncio health monitor + local LRU cache.
"""

from __future__ import annotations

import asyncio

import pytest

from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig

from .conftest import (
    KAFKA_BOOTSTRAP,
    consume_messages,
    produce_messages,
    require_kafka,
    unique_group,
)

TOPIC = "test-topic"


@require_kafka
class TestScenario2AsyncProducer:
    """AsyncSmartProducer with real Kafka, asyncio health monitor."""

    @pytest.mark.asyncio
    async def test_basic_produce_and_consume(self):
        """Async producer can send messages that are consumable."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={
                "bootstrap.servers": KAFKA_BOOTSTRAP,
                "client.id": "test-async-basic",
            },
            topics=[TOPIC],
            consumer_group=group,
        )

        async with AsyncSmartProducer(config) as producer:
            for i in range(10):
                await producer.produce(TOPIC, value=f"async-{i}".encode())
            await producer.flush(timeout=10)

        messages = consume_messages(TOPIC, group, count=10)
        assert len(messages) == 10

    @pytest.mark.asyncio
    async def test_health_monitor_async_mode(self):
        """Health monitor should start in async mode and collect data."""
        group = unique_group()
        produce_messages(TOPIC, count=20)
        consume_messages(TOPIC, group, count=10)

        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            refresh_interval=2.0,
        )

        async with AsyncSmartProducer(config) as producer:
            # Trigger lazy start
            await producer.produce(TOPIC, value=b"trigger")
            await producer.flush(timeout=10)

            assert producer.monitor is not None
            # Wait for health refresh
            await asyncio.sleep(3)

            scores = producer.monitor.get_health_scores(TOPIC)
            assert len(scores) > 0
            for _pid, score in scores.items():
                assert 0.0 <= score <= 1.0

    @pytest.mark.asyncio
    async def test_concurrent_produce(self):
        """Multiple concurrent produce calls should all succeed."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
        )

        async with AsyncSmartProducer(config) as producer:
            # Fire 20 concurrent produces
            tasks = [
                producer.produce(TOPIC, value=f"concurrent-{i}".encode())
                for i in range(20)
            ]
            await asyncio.gather(*tasks)
            await producer.flush(timeout=10)

        messages = consume_messages(TOPIC, group, count=20)
        assert len(messages) == 20

    @pytest.mark.asyncio
    async def test_multi_topic_concurrent(self):
        """Produce to multiple topics concurrently."""
        group = unique_group()
        topics = ["test-orders", "test-payments"]
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=topics,
            consumer_group=group,
        )

        async with AsyncSmartProducer(config) as producer:
            tasks = []
            for topic in topics:
                for i in range(5):
                    tasks.append(producer.produce(topic, value=f"multi-{i}".encode()))
            await asyncio.gather(*tasks)
            await producer.flush(timeout=10)

        for topic in topics:
            msgs = consume_messages(topic, group, count=5)
            assert len(msgs) == 5

    @pytest.mark.asyncio
    async def test_produce_after_close_raises(self):
        """Producing after close should raise RuntimeError."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=unique_group(),
        )

        producer = AsyncSmartProducer(config)
        await producer.close()

        with pytest.raises(RuntimeError, match="closed"):
            await producer.produce(TOPIC, value=b"after-close")

    @pytest.mark.asyncio
    async def test_key_stickiness_async(self):
        """Key stickiness should work in async mode."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        async with AsyncSmartProducer(config) as producer:
            await asyncio.sleep(3)  # wait for health data

            for i in range(10):
                await producer.produce(
                    TOPIC, value=f"async-sticky-{i}".encode(), key=b"user-456"
                )
            await producer.flush(timeout=10)

        recv_group = unique_group()
        messages = consume_messages(TOPIC, recv_group, count=10)
        sticky_msgs = [
            m for m in messages if m.value().decode().startswith("async-sticky-")
        ]

        if len(sticky_msgs) > 1:
            partitions = {m.partition() for m in sticky_msgs}
            assert len(partitions) == 1, (
                f"Async key stickiness failed: partitions {partitions}"
            )

    @pytest.mark.asyncio
    async def test_smart_disabled_async(self):
        """With smart_enabled=False, works as plain async producer."""
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            smart_enabled=False,
        )

        async with AsyncSmartProducer(config) as producer:
            assert producer.monitor is None
            await producer.produce(TOPIC, value=b"async-plain")
            await producer.flush(timeout=10)
