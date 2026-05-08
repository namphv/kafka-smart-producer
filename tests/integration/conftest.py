"""Shared fixtures and helpers for real integration tests."""

from __future__ import annotations

import time
import uuid

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP = "localhost:9092"
REDIS_URL = "redis://localhost:6380/0"


def kafka_available() -> bool:
    """Check if Kafka is reachable."""
    try:
        admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        admin.list_topics(timeout=5)
        return True
    except Exception:
        return False


def redis_available() -> bool:
    """Check if Redis is reachable."""
    try:
        import redis

        r = redis.from_url(REDIS_URL)
        return r.ping()
    except Exception:
        return False


require_kafka = pytest.mark.skipif(
    not kafka_available(), reason="Kafka not available at localhost:9092"
)
require_redis = pytest.mark.skipif(
    not redis_available(), reason="Redis not available at localhost:6380"
)


def unique_group() -> str:
    """Generate a unique consumer group to avoid cross-test interference."""
    return f"test-{uuid.uuid4().hex[:8]}"


def unique_topic_suffix() -> str:
    """Generate unique suffix for topic isolation."""
    return uuid.uuid4().hex[:8]


def produce_messages(topic: str, count: int, partition: int | None = None) -> None:
    """Produce N messages to a topic using plain confluent-kafka."""
    p = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    for i in range(count):
        kwargs = {"topic": topic, "value": f"msg-{i}".encode()}
        if partition is not None:
            kwargs["partition"] = partition
        p.produce(**kwargs)
    p.flush(timeout=10)


def consume_messages(topic: str, group: str, count: int, timeout: float = 15.0) -> list:
    """Consume up to `count` messages, creating committed offsets for the group."""
    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )
    c.subscribe([topic])
    messages = []
    deadline = time.time() + timeout
    while len(messages) < count and time.time() < deadline:
        msg = c.poll(1.0)
        if msg and not msg.error():
            messages.append(msg)
    c.close()
    return messages


def get_committed_offsets(topic: str, group: str) -> dict[int, int]:
    """Get committed offsets for a consumer group on a topic."""
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    metadata = admin.list_topics(topic=topic, timeout=10)
    partitions = list(metadata.topics[topic].partitions.keys())

    c = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": group,
        }
    )
    tps = [TopicPartition(topic, p) for p in partitions]
    committed = c.committed(tps, timeout=10)
    c.close()

    return {tp.partition: tp.offset for tp in committed if tp.offset >= 0}
