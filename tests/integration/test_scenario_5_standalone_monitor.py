"""
Scenario 5: Standalone Health Monitor

Real integration test: PartitionHealthMonitor running independently,
collecting from real Kafka and publishing health scores to Redis.
No producer involved.
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
)
from kafka_smart_producer.contrib.redis_health_consumer import RedisHealthPublisher

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
class TestScenario5StandaloneMonitor:
    """Standalone PartitionHealthMonitor publishing to Redis."""

    @pytest.fixture(autouse=True)
    def _clear_redis(self):
        r = redis_lib.from_url(REDIS_URL)
        for key in r.keys("ksp:*"):
            r.delete(key)
        yield
        for key in r.keys("ksp:*"):
            r.delete(key)

    def test_monitor_collects_real_lag_and_scores(self):
        """Monitor should collect real lag data and produce health scores."""
        group = unique_group()

        # Create lag: produce to topic, consume partially
        produce_messages(TOPIC, count=50)
        consume_messages(TOPIC, group, count=20)

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
            refresh_interval=2.0,
        )
        monitor.initialize_topics([TOPIC])

        try:
            monitor.start_sync()
            time.sleep(3)  # wait for refresh

            scores = monitor.get_health_scores(TOPIC)
            assert len(scores) > 0, "No health scores collected"

            healthy = monitor.get_healthy_partitions(TOPIC)
            # At least some partitions should exist
            assert isinstance(healthy, list)

            summary = monitor.get_summary()
            assert summary["running"] is True
            assert summary["topics"] == 1
            assert summary["total_partitions"] > 0
        finally:
            monitor.stop_sync()

        assert not monitor.is_running

    def test_monitor_publishes_to_redis(self):
        """Monitor collects scores, publisher writes them to Redis."""
        group = unique_group()
        produce_messages(TOPIC, count=30)
        consume_messages(TOPIC, group, count=10)

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

        publisher = RedisHealthPublisher(redis_url=REDIS_URL, ttl=120.0)

        try:
            monitor.start_sync()
            time.sleep(3)

            scores = monitor.get_health_scores(TOPIC)
            assert len(scores) > 0

            # Publish to Redis
            publisher.publish_scores(TOPIC, scores)

            # Verify in Redis
            r = redis_lib.from_url(REDIS_URL, decode_responses=True)
            raw = r.get(f"ksp:health:{TOPIC}")
            assert raw is not None

            data = json.loads(raw)
            assert "scores" in data
            assert "timestamp" in data
            assert len(data["scores"]) > 0

            # Verify scores match
            for pid_str, score in data["scores"].items():
                pid = int(pid_str)
                assert pid in scores
                assert abs(score - scores[pid]) < 0.001
        finally:
            monitor.stop_sync()

    def test_monitor_context_manager(self):
        """Monitor should work as context manager."""
        group = unique_group()
        produce_messages(TOPIC, count=10)

        collector = KafkaAdminLagCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            consumer_group=group,
            timeout=10.0,
        )

        monitor = PartitionHealthMonitor(
            collector=collector,
            health_threshold=0.5,
        )
        monitor.initialize_topics([TOPIC])

        with monitor:
            time.sleep(3)
            assert monitor.is_running
            scores = monitor.get_health_scores(TOPIC)
            assert len(scores) > 0

        assert not monitor.is_running

    def test_publisher_is_healthy(self):
        """Publisher health check against real Redis."""
        publisher = RedisHealthPublisher(redis_url=REDIS_URL)
        assert publisher.is_healthy() is True

    def test_continuous_publish_cycle(self):
        """Simulate continuous monitor -> publish cycle."""
        group = unique_group()
        produce_messages(TOPIC, count=40)
        consume_messages(TOPIC, group, count=10)

        collector = KafkaAdminLagCollector(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            consumer_group=group,
            timeout=10.0,
        )
        monitor = PartitionHealthMonitor(
            collector=collector,
            health_threshold=0.5,
            refresh_interval=2.0,
        )
        monitor.initialize_topics([TOPIC])
        publisher = RedisHealthPublisher(redis_url=REDIS_URL, ttl=120.0)

        try:
            monitor.start_sync()

            # Simulate 3 publish cycles
            for _cycle in range(3):
                time.sleep(2)
                scores = monitor.get_health_scores(TOPIC)
                if scores:
                    publisher.publish_scores(TOPIC, scores)

            # Final check
            r = redis_lib.from_url(REDIS_URL, decode_responses=True)
            raw = r.get(f"ksp:health:{TOPIC}")
            assert raw is not None
            data = json.loads(raw)
            assert data["timestamp"] > 0
        finally:
            monitor.stop_sync()
