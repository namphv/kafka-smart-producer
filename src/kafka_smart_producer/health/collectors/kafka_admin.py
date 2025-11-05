"""Kafka AdminClient-based lag collector."""

from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from ...exceptions import LagDataUnavailableError

logger = logging.getLogger(__name__)


class KafkaAdminLagCollector:
    """
    Collects consumer lag data using Kafka AdminClient and Consumer APIs.

    Flow: topic partitions -> committed offsets -> high water marks -> lag
    """

    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        timeout: float = 10.0,
        **kafka_config: Any,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group = consumer_group
        self._timeout = timeout

        self._kafka_config: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            **kafka_config,
        }
        self._admin = AdminClient(self._kafka_config)

        self._consumer_config = {
            **self._kafka_config,
            "group.id": f"lag_collector_{consumer_group}",
            "enable.auto.commit": False,
            "session.timeout.ms": int(timeout * 1000),
        }

        # Cache topic -> partition list
        self._partition_cache: dict[str, list[int]] = {}

    def get_lag_data(self, topic: str) -> dict[int, int]:
        try:
            partitions = self._get_partitions(topic)
            committed = self._get_committed_offsets(topic, partitions)
            watermarks = self._get_high_water_marks(topic, partitions)

            lag_data = {}
            for pid in partitions:
                committed_offset = committed.get(pid, 0)
                high_water = watermarks.get(pid, 0)
                lag_data[pid] = max(0, high_water - committed_offset)

            return lag_data

        except LagDataUnavailableError:
            raise
        except Exception as e:  # pragma: no cover
            self._partition_cache.pop(topic, None)
            raise LagDataUnavailableError(
                f"Failed to collect lag for topic '{topic}'",
                cause=e,
                context={
                    "topic": topic,
                    "consumer_group": self._consumer_group,
                },
            ) from e

    def _get_partitions(self, topic: str) -> list[int]:
        if topic in self._partition_cache:
            return self._partition_cache[topic]

        try:
            metadata = self._admin.list_topics(topic=topic, timeout=self._timeout)
            if topic not in metadata.topics:
                raise LagDataUnavailableError(f"Topic '{topic}' not found in cluster")

            topic_meta = metadata.topics[topic]
            if topic_meta.error is not None:  # pragma: no cover
                raise LagDataUnavailableError(
                    f"Topic '{topic}' has error: {topic_meta.error}",
                    cause=topic_meta.error,
                )

            partitions = list(topic_meta.partitions.keys())
            self._partition_cache[topic] = partitions
            return partitions

        except LagDataUnavailableError:
            raise
        except Exception as e:
            raise LagDataUnavailableError(
                f"Failed to get partitions for '{topic}'", cause=e
            ) from e

    def _get_committed_offsets(
        self, topic: str, partitions: list[int]
    ) -> dict[int, int]:
        try:
            from confluent_kafka.admin import _ConsumerGroupTopicPartitions

            tps = [TopicPartition(topic, pid) for pid in partitions]
            request = _ConsumerGroupTopicPartitions(
                self._consumer_group, topic_partitions=tps
            )
            result = self._admin.list_consumer_group_offsets(
                [request], request_timeout=self._timeout
            )
            committed_result = result[self._consumer_group].result(
                timeout=self._timeout
            )

            offsets = {}
            for tp in committed_result.topic_partitions:
                if tp.topic == topic:
                    offsets[tp.partition] = tp.offset if tp.offset >= 0 else 0
            return offsets

        except Exception as e:  # pragma: no cover
            raise LagDataUnavailableError(
                f"Failed to get committed offsets for '{topic}'", cause=e
            ) from e

    def _get_high_water_marks(
        self, topic: str, partitions: list[int]
    ) -> dict[int, int]:
        try:
            consumer = Consumer(self._consumer_config)
            try:
                watermarks = {}
                for pid in partitions:
                    tp = TopicPartition(topic, pid)
                    _, high = consumer.get_watermark_offsets(tp, timeout=self._timeout)
                    watermarks[pid] = high
                return watermarks
            finally:
                consumer.close()

        except Exception as e:  # pragma: no cover
            raise LagDataUnavailableError(
                f"Failed to get watermarks for '{topic}'", cause=e
            ) from e

    def clear_cache(self, topic: str | None = None) -> None:
        if topic is None:
            self._partition_cache.clear()  # pragma: no cover
        else:  # pragma: no cover
            self._partition_cache.pop(topic, None)

    def is_healthy(self) -> bool:
        try:
            self._admin.list_topics(timeout=min(2.0, self._timeout))
            return True
        except Exception:
            return False

    def __repr__(self) -> str:
        return (
            f"KafkaAdminLagCollector("
            f"servers='{self._bootstrap_servers}', "
            f"group='{self._consumer_group}')"
        )
