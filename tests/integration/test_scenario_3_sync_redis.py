"""
Scenario 3: Sync + Redis (Hybrid Cache)

Real integration test: SmartProducer + threading health monitor + HybridCache (L1+L2).
Tests distributed key stickiness via Redis.
"""

from __future__ import annotations

import time

import pytest
import redis

from kafka_smart_producer import SmartProducer, SmartProducerConfig
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
class TestScenario3SyncRedisHybridCache:
    """SmartProducer with real Kafka + Redis hybrid cache."""

    @pytest.fixture(autouse=True)
    def _clear_redis(self):
        """Clear Redis keys before each test."""
        r = redis.from_url(REDIS_URL)
        for key in r.keys("ksp:*"):
            r.delete(key)
        yield
        for key in r.keys("ksp:*"):
            r.delete(key)

    def test_hybrid_cache_stores_in_redis(self):
        """Key-partition mapping should be stored in both L1 and L2 (Redis)."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        producer = SmartProducer(config)
        try:
            # Replace local cache with hybrid
            local = LocalLRUCache(max_size=1000, default_ttl=300.0)
            remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)
            producer._cache = HybridCache(local=local, remote=remote)

            time.sleep(3)  # wait for health data

            producer.produce(TOPIC, value=b"redis-test", key=b"user-redis-1")
            producer.flush(timeout=10)

            # Verify key is in Redis
            r = redis.from_url(REDIS_URL, decode_responses=True)
            cache_key = f"ksp:test-topic:{b'user-redis-1'!r}"
            stored = r.get(cache_key)
            assert stored is not None, "Key-partition mapping not found in Redis"
        finally:
            producer.close()

    def test_hybrid_cache_read_through(self):
        """L1 miss should read through to L2 (Redis)."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        # Pre-populate Redis with a key-partition mapping
        remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)
        cache_key = f"test-topic:{b'preloaded-key'!r}"
        remote.set(cache_key, 2)  # partition 2

        producer = SmartProducer(config)
        try:
            # Use hybrid cache with fresh L1 (empty)
            local = LocalLRUCache(max_size=1000, default_ttl=300.0)
            producer._cache = HybridCache(local=local, remote=remote)

            time.sleep(3)  # wait for health data

            # The cache should find the value in L2 and populate L1
            result = producer._cache.get(cache_key)
            assert result == 2

            # L1 should now have it
            assert local.get(cache_key) == 2
        finally:
            producer.close()

    def test_key_stickiness_persists_across_instances(self):
        """Key mappings should survive producer restart via Redis."""
        group = unique_group()
        remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)

        # First producer instance: establish key mapping
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        producer1 = SmartProducer(config)
        try:
            local1 = LocalLRUCache(max_size=1000, default_ttl=300.0)
            producer1._cache = HybridCache(local=local1, remote=remote)
            time.sleep(3)

            producer1.produce(TOPIC, value=b"first", key=b"persist-key")
            producer1.flush(timeout=10)
        finally:
            producer1.close()

        # Get the cached partition from Redis
        cache_key = f"test-topic:{b'persist-key'!r}"
        first_partition = remote.get(cache_key)
        assert first_partition is not None

        # Second producer instance: should read from Redis
        producer2 = SmartProducer(config)
        try:
            local2 = LocalLRUCache(max_size=1000, default_ttl=300.0)
            producer2._cache = HybridCache(local=local2, remote=remote)
            time.sleep(3)

            # Should use the cached partition from Redis
            cached = producer2._cache.get(cache_key)
            assert cached == first_partition
        finally:
            producer2.close()

    def test_produce_with_hybrid_cache(self):
        """End-to-end: produce with hybrid cache, verify messages delivered."""
        group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=group,
            key_stickiness=True,
            refresh_interval=2.0,
        )

        producer = SmartProducer(config)
        try:
            local = LocalLRUCache(max_size=1000, default_ttl=300.0)
            remote = RemoteCache(redis_url=REDIS_URL, default_ttl=300.0)
            producer._cache = HybridCache(local=local, remote=remote)

            time.sleep(3)

            for i in range(10):
                producer.produce(
                    TOPIC,
                    value=f"hybrid-{i}".encode(),
                    key=f"key-{i % 3}".encode(),
                )
            producer.flush(timeout=10)

        finally:
            producer.close()

        messages = consume_messages(TOPIC, group, count=10)
        assert len(messages) == 10
