"""
Scenario 6: Redis Health Consumer

Real integration test: Producer consuming health data from Redis
(published by Scenario 5's standalone monitor) instead of Kafka AdminClient.
"""

from __future__ import annotations

import json
import time

import pytest
import redis as redis_lib

from kafka_smart_producer import (
    KafkaAdminLagCollector,
    LinearHealthScorer,
    PartitionHealthMonitor,
    SmartProducer,
    SmartProducerConfig,
)
from kafka_smart_producer.contrib.redis_health_consumer import (
    RedisHealthConsumer,
    RedisHealthPublisher,
)

from .conftest import (
    KAFKA_BOOTSTRAP,
    REDIS_URL,
    consume_messages,
    produce_messages,
    require_kafka,
    require_redis,
    unique_group,
)

TOPIC = "test-topic"


@require_kafka
@require_redis
class TestScenario6RedisHealthConsumer:
    """Producer using RedisHealthConsumer instead of direct Kafka lag collection."""

    @pytest.fixture(autouse=True)
    def _clear_redis(self):
        r = redis_lib.from_url(REDIS_URL)
        for key in r.keys("ksp:*"):
            r.delete(key)
        yield
        for key in r.keys("ksp:*"):
            r.delete(key)

    def _publish_health_from_kafka(self, group: str) -> dict[int, float]:
        """
        Simulate Scenario 5: collect real health data from Kafka,
        publish it to Redis, and return the scores.
        """
        collector = KafkaAdminLagCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            consumer_group=group,
            timeout=10.0,
        )
        scorer = LinearHealthScorer(max_lag=100)
        monitor = PartitionHealthMonitor(
            collector=collector,
            scorer=scorer,
            health_threshold=0.5,
        )
        monitor.initialize_topics([TOPIC])
        monitor.force_refresh(TOPIC)

        scores = monitor.get_health_scores(TOPIC)
        publisher = RedisHealthPublisher(redis_url=REDIS_URL, ttl=120.0)
        publisher.publish_scores(TOPIC, scores)
        return scores

    def test_consumer_reads_health_from_redis(self):
        """RedisHealthConsumer should read health data published to Redis."""
        group = unique_group()
        produce_messages(TOPIC, count=40)
        consume_messages(TOPIC, group, count=10)

        # Publish health to Redis (simulating Scenario 5)
        published_scores = self._publish_health_from_kafka(group)
        assert len(published_scores) > 0

        # Now use RedisHealthConsumer to read it
        redis_consumer = RedisHealthConsumer(redis_url=REDIS_URL, max_age=120.0)
        health_scores = redis_consumer.get_health_scores(TOPIC)
        assert health_scores is not None
        assert len(health_scores) == len(published_scores)

        for pid, score in health_scores.items():
            assert pid in published_scores
            assert abs(score - published_scores[pid]) < 0.001

    def test_consumer_returns_synthetic_lag(self):
        """RedisHealthConsumer.get_lag_data should return synthetic lag values."""
        group = unique_group()
        produce_messages(TOPIC, count=30)
        consume_messages(TOPIC, group, count=10)

        self._publish_health_from_kafka(group)

        redis_consumer = RedisHealthConsumer(redis_url=REDIS_URL, max_age=120.0)
        lag_data = redis_consumer.get_lag_data(TOPIC)
        assert len(lag_data) > 0

        # Synthetic lag: score 1.0 -> lag 0, score 0.0 -> lag 1000
        for _pid, lag in lag_data.items():
            assert 0 <= lag <= 1000

    def test_producer_with_redis_health_consumer(self):
        """
        End-to-end: standalone monitor publishes to Redis,
        producer reads health from Redis and produces messages.
        """
        group = unique_group()
        produce_messages(TOPIC, count=50)
        consume_messages(TOPIC, group, count=20)

        # Step 1: Publish health to Redis
        published_scores = self._publish_health_from_kafka(group)
        assert len(published_scores) > 0

        # Step 2: Create producer using RedisHealthConsumer
        redis_consumer = RedisHealthConsumer(redis_url=REDIS_URL, max_age=120.0)
        monitor = PartitionHealthMonitor(
            collector=redis_consumer,
            health_threshold=0.5,
            max_lag=1000,
        )
        monitor.initialize_topics([TOPIC])
        monitor.force_refresh(TOPIC)

        recv_group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=recv_group,
        )

        producer = SmartProducer(config, monitor=monitor)
        try:
            # Verify monitor has health data from Redis
            scores = monitor.get_health_scores(TOPIC)
            assert len(scores) > 0

            # Produce messages (may fall back to default partitioning
            # if all partitions are unhealthy due to accumulated lag)
            for i in range(10):
                producer.produce(TOPIC, value=f"redis-health-{i}".encode())
            producer.flush(timeout=10)
        finally:
            producer.close()

        messages = consume_messages(TOPIC, recv_group, count=10)
        assert len(messages) == 10

    def test_stale_redis_data_falls_back(self):
        """When Redis health data is stale, producer should fall back gracefully."""
        unique_group()
        produce_messages(TOPIC, count=10)

        # Publish health data with a very old timestamp
        r = redis_lib.from_url(REDIS_URL, decode_responses=True)
        stale_data = {
            "scores": {"0": 1.0, "1": 0.5, "2": 0.0, "3": 0.8},
            "timestamp": time.time() - 600,  # 10 minutes old
        }
        r.setex(f"ksp:health:{TOPIC}", 120, json.dumps(stale_data))

        redis_consumer = RedisHealthConsumer(redis_url=REDIS_URL, max_age=120.0)
        monitor = PartitionHealthMonitor(
            collector=redis_consumer,
            health_threshold=0.5,
        )
        monitor.initialize_topics([TOPIC])
        monitor.force_refresh(TOPIC)

        # Stale data should be rejected
        scores = monitor.get_health_scores(TOPIC)
        assert len(scores) == 0  # empty because stale

        recv_group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=recv_group,
        )

        producer = SmartProducer(config, monitor=monitor)
        try:
            # Should fall back to default partitioning (no health data)
            producer.produce(TOPIC, value=b"stale-fallback")
            producer.flush(timeout=10)
        finally:
            producer.close()

        messages = consume_messages(TOPIC, recv_group, count=1)
        assert len(messages) == 1

    def test_redis_consumer_is_healthy(self):
        """Health check against real Redis."""
        redis_consumer = RedisHealthConsumer(redis_url=REDIS_URL)
        assert redis_consumer.is_healthy() is True

    def test_full_pipeline_monitor_to_producer(self):
        """
        Full pipeline: real Kafka lag -> standalone monitor -> Redis -> producer.
        This is the canonical Scenario 5+6 integration.
        """
        lag_group = unique_group()
        produce_messages(TOPIC, count=60)
        consume_messages(TOPIC, lag_group, count=20)

        # === Scenario 5: Standalone monitor ===
        collector = KafkaAdminLagCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            consumer_group=lag_group,
            timeout=10.0,
        )
        scorer = LinearHealthScorer(max_lag=100)
        standalone_monitor = PartitionHealthMonitor(
            collector=collector,
            scorer=scorer,
            health_threshold=0.5,
            refresh_interval=2.0,
        )
        standalone_monitor.initialize_topics([TOPIC])
        publisher = RedisHealthPublisher(redis_url=REDIS_URL, ttl=120.0)

        standalone_monitor.start_sync()
        try:
            time.sleep(3)
            scores = standalone_monitor.get_health_scores(TOPIC)
            assert len(scores) > 0
            publisher.publish_scores(TOPIC, scores)
        finally:
            standalone_monitor.stop_sync()

        # === Scenario 6: Producer consumes from Redis ===
        redis_consumer = RedisHealthConsumer(redis_url=REDIS_URL, max_age=120.0)
        producer_monitor = PartitionHealthMonitor(
            collector=redis_consumer,
            health_threshold=0.5,
            max_lag=1000,
        )
        producer_monitor.initialize_topics([TOPIC])
        producer_monitor.force_refresh(TOPIC)

        # Verify producer monitor has the same data
        producer_scores = producer_monitor.get_health_scores(TOPIC)
        assert len(producer_scores) > 0

        recv_group = unique_group()
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP},
            topics=[TOPIC],
            consumer_group=recv_group,
        )

        with SmartProducer(config, monitor=producer_monitor) as producer:
            for i in range(10):
                producer.produce(TOPIC, value=f"pipeline-{i}".encode())
            producer.flush(timeout=10)

        messages = consume_messages(TOPIC, recv_group, count=10)
        assert len(messages) == 10
