"""
Integration tests for all 6 deployment scenarios.

These tests verify full wiring of each scenario end-to-end using mocks
for external dependencies (Kafka, Redis). They ensure that the public API
composes correctly for each supported deployment pattern.
"""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest

# ── Shared helpers ──


def _kafka_patches():
    """Patch Kafka dependencies for all scenarios."""
    return [
        patch("kafka_smart_producer.producer.ConfluentProducer"),
        patch("kafka_smart_producer.health.collectors.kafka_admin.AdminClient"),
        patch("kafka_smart_producer.health.collectors.kafka_admin.Consumer"),
    ]


def _setup_lag_mocks(admin_mock, consumer_mock, topic, lag_data):
    """Configure admin/consumer mocks to return specific lag data."""
    # AdminClient.list_topics -> partition metadata
    topic_meta = MagicMock()
    topic_meta.error = None
    topic_meta.partitions = {pid: MagicMock() for pid in lag_data}
    metadata = MagicMock()
    metadata.topics = {topic: topic_meta}
    admin_mock.return_value.list_topics.return_value = metadata

    # Consumer group offsets (committed = 100 for all)
    from confluent_kafka import TopicPartition

    committed_tps = []
    for pid in lag_data:
        tp = TopicPartition(topic, pid)
        tp.offset = 100
        committed_tps.append(tp)

    committed_result = MagicMock()
    committed_result.topic_partitions = committed_tps
    group_future = MagicMock()
    group_future.result.return_value = committed_result
    admin_mock.return_value.list_consumer_group_offsets.return_value = {
        "test-group": group_future
    }

    # Consumer.get_watermark_offsets -> (0, 100 + lag)
    def _watermarks(tp, timeout=None):
        return (0, 100 + lag_data.get(tp.partition, 0))

    consumer_instance = MagicMock()
    consumer_instance.get_watermark_offsets.side_effect = _watermarks
    consumer_mock.return_value = consumer_instance


# ── Scenario 1: Simple Sync Producer ──


class TestScenario1SimpleSyncProducer:
    """
    SmartProducer with threading-based health monitor and local LRU cache.
    The basic "batteries included" setup.
    """

    def test_full_flow(self):
        patches = _kafka_patches()
        with patches[0] as prod_mock, patches[1] as admin_mock, patches[2] as cons_mock:
            # Lag: partition 0=0, 1=500, 2=2000 (unhealthy)
            _setup_lag_mocks(admin_mock, cons_mock, "orders", {0: 0, 1: 500, 2: 2000})

            from kafka_smart_producer import SmartProducer, SmartProducerConfig

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["orders"],
                consumer_group="test-group",
                health_threshold=0.5,
                max_lag=1000,
            )

            producer = SmartProducer(config)
            try:
                # Force refresh to load health data
                if producer.monitor:
                    producer.monitor.force_refresh("orders")

                producer.produce("orders", value=b"order-1")

                call_kwargs = prod_mock.return_value.produce.call_args[1]
                assert call_kwargs["topic"] == "orders"
                # Should pick partition 0 or 1 (healthy), not 2
                if "partition" in call_kwargs:
                    assert call_kwargs["partition"] in [0, 1]
            finally:
                if producer.monitor and producer.monitor.is_running:
                    producer.monitor.stop_sync()

    def test_graceful_degradation_on_failure(self):
        patches = _kafka_patches()
        with patches[0] as prod_mock, patches[1] as admin_mock, patches[2]:
            # Make AdminClient fail
            admin_mock.return_value.list_topics.side_effect = Exception(
                "connection failed"
            )

            from kafka_smart_producer import SmartProducer, SmartProducerConfig

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["orders"],
                consumer_group="test-group",
            )

            producer = SmartProducer(config)
            try:
                producer.produce("orders", value=b"order-1")
                # Should still produce (falls back to default partitioning)
                prod_mock.return_value.produce.assert_called_once()
                call_kwargs = prod_mock.return_value.produce.call_args[1]
                assert "partition" not in call_kwargs
            finally:
                if producer.monitor and producer.monitor.is_running:
                    producer.monitor.stop_sync()


# ── Scenario 2: Async Concurrent Producer ──


class TestScenario2AsyncConcurrentProducer:
    """
    AsyncSmartProducer with asyncio-based health monitor.
    Multi-topic concurrent monitoring.
    """

    @pytest.mark.asyncio
    async def test_full_flow(self):
        patches = _kafka_patches()
        with patches[0] as prod_mock, patches[1] as admin_mock, patches[2] as cons_mock:
            _setup_lag_mocks(admin_mock, cons_mock, "events", {0: 0, 1: 100, 2: 800})

            # Make produce trigger delivery callback
            def _produce_with_delivery(**kwargs):
                cb = kwargs.get("on_delivery")
                if cb:
                    cb(None, MagicMock())

            prod_mock.return_value.produce.side_effect = _produce_with_delivery
            prod_mock.return_value.poll.return_value = 0

            from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["events"],
                consumer_group="test-group",
                health_threshold=0.5,
                max_lag=1000,
            )

            async with AsyncSmartProducer(config) as producer:
                if producer.monitor:
                    producer.monitor.force_refresh("events")

                await producer.produce("events", value=b"event-1")

                call_kwargs = prod_mock.return_value.produce.call_args[1]
                assert call_kwargs["topic"] == "events"

    @pytest.mark.asyncio
    async def test_produce_after_close_raises(self):
        patches = _kafka_patches()
        with patches[0], patches[1], patches[2]:
            from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["events"],
                consumer_group="test-group",
                smart_enabled=False,
            )

            producer = AsyncSmartProducer(config)
            await producer.close()

            with pytest.raises(RuntimeError):
                await producer.produce("events", value=b"data")


# ── Scenario 3: Sync + Redis (Hybrid Cache) ──


class TestScenario3SyncRedisHybridCache:
    """
    SmartProducer with hybrid L1+L2 cache for distributed key stickiness.
    """

    def test_hybrid_cache_with_producer(self):
        patches = _kafka_patches()
        with patches[0] as prod_mock, patches[1] as admin_mock, patches[2] as cons_mock:
            _setup_lag_mocks(admin_mock, cons_mock, "orders", {0: 0, 1: 0, 2: 0})

            with patch(
                "kafka_smart_producer.contrib.redis_cache._import_redis"
            ) as redis_import:
                mock_redis_mod = MagicMock()
                mock_redis_client = MagicMock()
                mock_redis_mod.from_url.return_value = mock_redis_client
                redis_import.return_value = mock_redis_mod
                mock_redis_client.get.return_value = None  # L2 cache miss

                from kafka_smart_producer import SmartProducer, SmartProducerConfig
                from kafka_smart_producer.cache.local import LocalLRUCache
                from kafka_smart_producer.contrib.redis_cache import (
                    HybridCache,
                    RemoteCache,
                )

                config = SmartProducerConfig(
                    kafka_config={"bootstrap.servers": "localhost:9092"},
                    topics=["orders"],
                    consumer_group="test-group",
                    key_stickiness=True,
                )

                producer = SmartProducer(config)
                try:
                    # Replace the local cache with a hybrid cache
                    local = LocalLRUCache(max_size=1000, default_ttl=300.0)
                    remote = RemoteCache()
                    producer._cache = HybridCache(local=local, remote=remote)

                    if producer.monitor:
                        producer.monitor.force_refresh("orders")

                    # First produce with key
                    producer.produce("orders", value=b"data", key=b"user-1")
                    first_partition = prod_mock.return_value.produce.call_args[1].get(
                        "partition"
                    )

                    # Second produce with same key - should hit L1 cache
                    producer.produce("orders", value=b"data", key=b"user-1")
                    second_partition = prod_mock.return_value.produce.call_args[1].get(
                        "partition"
                    )

                    assert first_partition == second_partition

                    # Verify L2 was written
                    assert mock_redis_client.setex.called
                finally:
                    if producer.monitor and producer.monitor.is_running:
                        producer.monitor.stop_sync()


# ── Scenario 4: Async + Redis ──


class TestScenario4AsyncRedisHybridCache:
    """
    AsyncSmartProducer with hybrid cache and health streams.
    """

    @pytest.mark.asyncio
    async def test_async_with_hybrid_cache(self):
        patches = _kafka_patches()
        with patches[0] as prod_mock, patches[1] as admin_mock, patches[2] as cons_mock:
            _setup_lag_mocks(admin_mock, cons_mock, "events", {0: 0, 1: 0})

            def _produce_with_delivery(**kwargs):
                cb = kwargs.get("on_delivery")
                if cb:
                    cb(None, MagicMock())

            prod_mock.return_value.produce.side_effect = _produce_with_delivery
            prod_mock.return_value.poll.return_value = 0

            with patch(
                "kafka_smart_producer.contrib.redis_cache._import_redis"
            ) as redis_import:
                mock_redis_mod = MagicMock()
                mock_redis_client = MagicMock()
                mock_redis_mod.from_url.return_value = mock_redis_client
                redis_import.return_value = mock_redis_mod
                mock_redis_client.get.return_value = None

                from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig
                from kafka_smart_producer.cache.local import LocalLRUCache
                from kafka_smart_producer.contrib.redis_cache import (
                    HybridCache,
                    RemoteCache,
                )

                config = SmartProducerConfig(
                    kafka_config={"bootstrap.servers": "localhost:9092"},
                    topics=["events"],
                    consumer_group="test-group",
                    key_stickiness=True,
                )

                producer = AsyncSmartProducer(config)
                try:
                    local = LocalLRUCache(max_size=1000, default_ttl=300.0)
                    remote = RemoteCache()
                    producer._cache = HybridCache(local=local, remote=remote)

                    if producer.monitor:
                        producer.monitor.force_refresh("events")

                    await producer.produce("events", value=b"data", key=b"user-1")
                    first = prod_mock.return_value.produce.call_args[1].get("partition")

                    await producer.produce("events", value=b"data", key=b"user-1")
                    second = prod_mock.return_value.produce.call_args[1].get(
                        "partition"
                    )

                    assert first == second
                finally:
                    await producer.close()


# ── Scenario 5: Standalone Health Monitor ──


class TestScenario5StandaloneMonitor:
    """
    PartitionHealthMonitor running independently, publishing to Redis.
    No producer involved.
    """

    def test_monitor_publishes_to_redis(self):
        with (
            patch(
                "kafka_smart_producer.health.collectors.kafka_admin.AdminClient"
            ) as admin_mock,
            patch(
                "kafka_smart_producer.health.collectors.kafka_admin.Consumer"
            ) as cons_mock,
            patch(
                "kafka_smart_producer.contrib.redis_health_consumer._import_redis"
            ) as redis_import,
        ):
            _setup_lag_mocks(admin_mock, cons_mock, "orders", {0: 0, 1: 500, 2: 2000})

            mock_redis_mod = MagicMock()
            mock_redis_client = MagicMock()
            mock_redis_mod.from_url.return_value = mock_redis_client
            redis_import.return_value = mock_redis_mod

            from kafka_smart_producer import (
                KafkaAdminLagCollector,
                LinearHealthScorer,
                PartitionHealthMonitor,
            )
            from kafka_smart_producer.contrib.redis_health_consumer import (
                RedisHealthPublisher,
            )

            collector = KafkaAdminLagCollector(
                bootstrap_servers="localhost:9092",
                consumer_group="test-group",
            )
            scorer = LinearHealthScorer(max_lag=1000)
            monitor = PartitionHealthMonitor(
                collector=collector,
                scorer=scorer,
                health_threshold=0.5,
            )
            monitor.initialize_topics(["orders"])

            # Force refresh
            monitor.force_refresh("orders")

            # Get scores and publish to Redis
            scores = monitor.get_health_scores("orders")
            assert len(scores) == 3
            assert scores[0] == 1.0  # lag 0
            assert scores[1] == 0.5  # lag 500
            assert scores[2] == 0.0  # lag 2000, clamped

            publisher = RedisHealthPublisher()
            publisher.publish_scores("orders", scores)

            # Verify Redis was written
            assert mock_redis_client.setex.called
            assert mock_redis_client.publish.called


# ── Scenario 6: Redis Health Consumer ──


class TestScenario6RedisHealthConsumer:
    """
    Producer consuming health data from Redis instead of Kafka directly.
    Pairs with Scenario 5.
    """

    def test_producer_with_redis_health(self):
        with (
            patch("kafka_smart_producer.producer.ConfluentProducer") as prod_mock,
            patch(
                "kafka_smart_producer.contrib.redis_health_consumer._import_redis"
            ) as redis_import,
        ):
            mock_redis_mod = MagicMock()
            mock_redis_client = MagicMock()
            mock_redis_mod.from_url.return_value = mock_redis_client
            redis_import.return_value = mock_redis_mod

            # Simulate health data in Redis (from Scenario 5)
            health_data = {
                "scores": {"0": 1.0, "1": 0.5, "2": 0.0},
                "timestamp": time.time(),
            }
            mock_redis_client.get.return_value = json.dumps(health_data)

            from kafka_smart_producer import (
                PartitionHealthMonitor,
                SmartProducer,
                SmartProducerConfig,
            )
            from kafka_smart_producer.contrib.redis_health_consumer import (
                RedisHealthConsumer,
            )

            # Use RedisHealthConsumer as the collector
            redis_collector = RedisHealthConsumer()

            monitor = PartitionHealthMonitor(
                collector=redis_collector,
                health_threshold=0.5,
                max_lag=1000,
            )
            monitor.initialize_topics(["orders"])
            monitor.force_refresh("orders")

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["orders"],
                consumer_group="test-group",
            )

            producer = SmartProducer(config, monitor=monitor)
            producer.produce("orders", value=b"order-1")

            call_kwargs = prod_mock.return_value.produce.call_args[1]
            assert call_kwargs["topic"] == "orders"
            # Partition 0 (score 1.0) and 1 (score 0.5) are healthy
            # Partition 2 (score 0.0) is unhealthy
            if "partition" in call_kwargs:
                assert call_kwargs["partition"] in [0, 1]

    def test_redis_health_staleness_fallback(self):
        with (
            patch("kafka_smart_producer.producer.ConfluentProducer") as prod_mock,
            patch(
                "kafka_smart_producer.contrib.redis_health_consumer._import_redis"
            ) as redis_import,
        ):
            mock_redis_mod = MagicMock()
            mock_redis_client = MagicMock()
            mock_redis_mod.from_url.return_value = mock_redis_client
            redis_import.return_value = mock_redis_mod

            # Stale data (5 min old, max_age=120s)
            health_data = {
                "scores": {"0": 1.0, "1": 0.5},
                "timestamp": time.time() - 300,
            }
            mock_redis_client.get.return_value = json.dumps(health_data)

            from kafka_smart_producer import (
                PartitionHealthMonitor,
                SmartProducer,
                SmartProducerConfig,
            )
            from kafka_smart_producer.contrib.redis_health_consumer import (
                RedisHealthConsumer,
            )

            redis_collector = RedisHealthConsumer(max_age=120.0)
            monitor = PartitionHealthMonitor(
                collector=redis_collector,
                health_threshold=0.5,
            )
            monitor.initialize_topics(["orders"])
            monitor.force_refresh("orders")

            config = SmartProducerConfig(
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["orders"],
                consumer_group="test-group",
            )

            producer = SmartProducer(config, monitor=monitor)
            producer.produce("orders", value=b"order-1")

            call_kwargs = prod_mock.return_value.produce.call_args[1]
            # Stale data -> no healthy partitions -> fallback
            assert "partition" not in call_kwargs
