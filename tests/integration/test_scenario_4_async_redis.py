"""
Scenario 4: Async + Redis (Hybrid Cache)

Real integration test: AsyncSmartProducer + asyncio health monitor + HybridCache.
"""

from __future__ import annotations

import asyncio

import pytest
import redis

from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig
from kafka_smart_producer.cache.local import LocalLRUCache
from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

from .conftest import (
    KAFKA_BOOTSTRAP,
    REDIS_URL,
    consume_messages,
    require_kafka,
    require_redis,
    unique_group,
)

TOPIC = "test-topic"


@require_kafka
@require_redis
class TestScenario4AsyncRedisHybridCache:
    """AsyncSmartProducer with real Kafka + Redis hybrid cache."""

    @pytest.fixture(autouse=True)
    def _clear_redis(self):
        r = redis.from_url(REDIS_URL)
        for key in r.keys("ksp:*"):
            r.delete(key)
        yield
        for key in r.keys("ksp:*"):
            r.delete(key)

    @pytest.mark.asyncio
    async def test_async_produce_with_hybrid_cache(self):
        """End-to-end async produce with hybrid cache."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        producer = AsyncSmartProducer(config)
        try:
            local = LocalLRUCache(max_size=1000, default_ttl=300.0)
            remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)
            producer._cache = HybridCache(local=local, remote=remote)

            # Trigger monitor start + wait for health data
            await producer.produce(TOPIC, value=b"warmup", key=b"warmup-key")
            await asyncio.sleep(3)

            for i in range(10):
                await producer.produce(
                    TOPIC,
                    value=f"async-hybrid-{i}".encode(),
                    key=b"sticky-async",
                )
            await producer.flush(timeout=10)

            # Verify key is in Redis
            r = redis.from_url(REDIS_URL, decode_responses=True)
            cache_key = f"ksp:test-topic:{b'sticky-async'!r}"
            stored = r.get(cache_key)
            assert stored is not None
        finally:
            await producer.close()

        messages = consume_messages(TOPIC, group, count=11)
        assert len(messages) == 11

    @pytest.mark.asyncio
    async def test_concurrent_produce_with_hybrid_cache(self):
        """Concurrent async produces with hybrid cache should all succeed."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        async with AsyncSmartProducer(config) as producer:
            local = LocalLRUCache(max_size=1000, default_ttl=300.0)
            remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)
            producer._cache = HybridCache(local=local, remote=remote)

            await asyncio.sleep(3)

            tasks = [
                producer.produce(
                    TOPIC,
                    value=f"conc-{i}".encode(),
                    key=f"key-{i % 5}".encode(),
                )
                for i in range(15)
            ]
            await asyncio.gather(*tasks)
            await producer.flush(timeout=10)

        messages = consume_messages(TOPIC, group, count=15)
        assert len(messages) == 15

    @pytest.mark.asyncio
    async def test_key_stickiness_async_with_redis(self):
        """Same key should route to same partition with hybrid cache in async mode."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        async with AsyncSmartProducer(config) as producer:
            local = LocalLRUCache(max_size=1000, default_ttl=300.0)
            remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)
            producer._cache = HybridCache(local=local, remote=remote)

            await asyncio.sleep(3)

            for i in range(10):
                await producer.produce(
                    TOPIC, value=f"sticky-redis-{i}".encode(), key=b"user-789"
                )
            await producer.flush(timeout=10)

        recv_group = unique_group()
        messages = consume_messages(TOPIC, recv_group, count=10)
        sticky_msgs = [
            m for m in messages if m.value().decode().startswith("sticky-redis-")
        ]
        if len(sticky_msgs) > 1:
            partitions = {m.partition() for m in sticky_msgs}
            assert len(partitions) == 1
