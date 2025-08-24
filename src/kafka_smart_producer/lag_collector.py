"""
Kafka lag collector implementation using confluent-kafka AdminClient.

This module provides a concrete implementation of LagDataCollector protocol
using the confluent-kafka-python AdminClient to fetch consumer lag data.
"""

import logging
from typing import Any

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import (
    AdminClient,
    _ConsumerGroupTopicPartitions,
    _TopicCollection,
)

from .exceptions import LagDataUnavailableError

logger = logging.getLogger(__name__)


class KafkaAdminLagCollector:
    """
    Lag collector using Kafka AdminClient and Consumer APIs.

    Collects consumer lag data by:
    1. Getting topic partition metadata from AdminClient
    2. Getting consumer group committed offsets
    3. Getting high water marks for each partition
    4. Calculating lag = high_water_mark - committed_offset
    """

    def __init__(
        self,
        bootstrap_servers: str,
        consumer_group: str,
        timeout_seconds: float = 5.0,
        **kafka_config: Any,
    ) -> None:
        """
        Initialize lag collector with Kafka connection settings.

        Args:
            bootstrap_servers: Kafka broker endpoints (e.g., "localhost:9092")
            consumer_group: Consumer group to monitor for lag
            timeout_seconds: Request timeout in seconds
            **kafka_config: Additional Kafka configuration (security, etc.)
        """
        self._bootstrap_servers = bootstrap_servers
        self._consumer_group = consumer_group
        self._timeout_seconds = timeout_seconds

        # Build base Kafka configuration
        self._kafka_config = {"bootstrap.servers": bootstrap_servers, **kafka_config}

        # Create AdminClient for metadata operations
        self._admin_client = AdminClient(self._kafka_config)

        # Consumer config for offset and watermark operations
        self._consumer_config = {
            **self._kafka_config,
            "group.id": f"lag_collector_{consumer_group}",
            "enable.auto.commit": False,
            "session.timeout.ms": int(timeout_seconds * 1000),
        }

        # Cache for topic partition metadata
        self._topic_partitions_cache: dict[str, list[int]] = {}

        logger.info(
            f"KafkaAdminLagCollector initialized for group '{consumer_group}' "
            f"on {bootstrap_servers}"
        )

    def get_lag_data(self, topic: str) -> dict[int, int]:
        """
        Get current lag for all partitions of a topic.

        Args:
            topic: Kafka topic name

        Returns:
            Dict[partition_id, lag_count] - lag count per partition

        Raises:
            LagDataUnavailableError: When lag data cannot be retrieved
        """
        try:
            # 1. Get topic metadata to find partition count (cached)
            partitions = self._get_topic_partitions_cached(topic)

            # 2. Get committed offsets for consumer group
            committed_offsets = self._get_committed_offsets(topic, partitions)

            # 3. Get high water marks for all partitions
            high_water_marks = self._get_high_water_marks(topic, partitions)

            # 4. Calculate lag per partition
            lag_data = {}
            for partition_id in partitions:
                committed_offset = committed_offsets.get(partition_id, 0)
                high_water_mark = high_water_marks.get(partition_id, 0)

                # Lag = high water mark - committed offset
                # Ensure lag is never negative
                lag = max(0, high_water_mark - committed_offset)
                lag_data[partition_id] = lag

            logger.debug(f"Collected lag data for topic '{topic}': {lag_data}")
            return lag_data

        except Exception as e:
            error_msg = (
                f"Failed to collect lag data for topic '{topic}' "
                f"and group '{self._consumer_group}'"
            )
            logger.error(f"{error_msg}: {e}")

            # Clear cache for this topic in case the error is due to topic changes
            self.clear_topic_cache(topic)

            raise LagDataUnavailableError(
                error_msg,
                cause=e,
                context={
                    "topic": topic,
                    "consumer_group": self._consumer_group,
                    "bootstrap_servers": self._bootstrap_servers,
                },
            ) from e

    def _get_topic_partitions_cached(self, topic: str) -> list[int]:
        """Get list of partition IDs for a topic with caching."""
        if topic in self._topic_partitions_cache:
            return self._topic_partitions_cache[topic]

        # Cache miss - fetch and cache the result
        partitions = self._get_topic_partitions(topic)
        self._topic_partitions_cache[topic] = partitions
        return partitions

    def _get_topic_partitions(self, topic: str) -> list[int]:
        """Get list of partition IDs for a topic."""
        try:
            # Method 1: Use describe_topics (more detailed metadata)
            try:
                topic_collection = _TopicCollection([topic])
                metadata = self._admin_client.describe_topics(
                    topic_collection, request_timeout=self._timeout_seconds
                )
                topic_metadata = metadata[topic].result(timeout=self._timeout_seconds)

                # TopicDescription.partitions is a list of TopicPartitionInfo objects
                partitions = [p.id for p in topic_metadata.partitions]
                logger.debug(
                    f"Topic '{topic}' has partitions (describe_topics): {partitions}"
                )
                return partitions

            except Exception as describe_error:
                logger.debug(
                    f"describe_topics failed for topic '{topic}': {describe_error}"
                )

                # Method 2: Fallback to list_topics (simpler but reliable)
                cluster_metadata = self._admin_client.list_topics(
                    topic=topic, timeout=self._timeout_seconds
                )

                if topic in cluster_metadata.topics:
                    topic_metadata = cluster_metadata.topics[topic]

                    # Check for topic-level errors
                    if topic_metadata.error is not None:
                        raise LagDataUnavailableError(
                            f"Topic '{topic}' has error: {topic_metadata.error}",
                            cause=topic_metadata.error,
                        ) from topic_metadata.error

                    # TopicMetadata.partitions is a dict with partition_id ->
                    # PartitionMetadata
                    partitions = list(topic_metadata.partitions.keys())
                    logger.debug(
                        f"Topic '{topic}' has partitions (list_topics): {partitions}"
                    )
                    return partitions
                else:
                    raise LagDataUnavailableError(
                        f"Topic '{topic}' not found in cluster", cause=describe_error
                    ) from describe_error

        except LagDataUnavailableError:
            # Re-raise our custom exceptions
            raise
        except Exception as e:
            raise LagDataUnavailableError(
                f"Failed to get partition metadata for topic '{topic}'", cause=e
            ) from e

    def _get_committed_offsets(
        self, topic: str, partitions: list[int]
    ) -> dict[int, int]:
        """Get committed offsets for consumer group."""
        try:
            # Create TopicPartition objects
            topic_partitions = [
                TopicPartition(topic, partition_id) for partition_id in partitions
            ]
            logger.debug(
                f"Getting committed offsets for topic '{topic}' "
                f"partitions: {partitions}"
            )

            # Get committed offsets from AdminClient
            consumer_group_request = _ConsumerGroupTopicPartitions(
                self._consumer_group, topic_partitions=topic_partitions
            )

            group_offsets = self._admin_client.list_consumer_group_offsets(
                [consumer_group_request],
                request_timeout=self._timeout_seconds,
            )

            committed_result = group_offsets[self._consumer_group].result(
                timeout=self._timeout_seconds
            )
            logger.debug(f"Committed result type: {type(committed_result)}")
            logger.debug(f"Committed result: {committed_result}")

            # Extract offsets by partition
            committed_offsets = {}
            for tp in committed_result.topic_partitions:
                logger.debug(
                    f"Processing topic partition: {tp}, topic: {tp.topic}, "
                    f"partition: {tp.partition}, offset: {tp.offset}"
                )
                if tp.topic == topic:
                    # Handle case where no offset is committed (offset = -1001)
                    offset = tp.offset if tp.offset >= 0 else 0
                    committed_offsets[tp.partition] = offset

            logger.debug(f"Committed offsets for '{topic}': {committed_offsets}")
            return committed_offsets

        except Exception as e:
            logger.error(f"Error getting committed offsets: {e}")
            logger.error(f"Exception type: {type(e)}")
            raise LagDataUnavailableError(
                f"Failed to get committed offsets for group '{self._consumer_group}' "
                f"and topic '{topic}'",
                cause=e,
            ) from e

    def _get_high_water_marks(
        self, topic: str, partitions: list[int]
    ) -> dict[int, int]:
        """Get high water marks for all partitions."""
        try:
            # Create temporary consumer to get watermarks
            consumer = Consumer(self._consumer_config)

            try:
                high_water_marks = {}

                for partition_id in partitions:
                    tp = TopicPartition(topic, partition_id)

                    # Get watermark offsets [low, high]
                    low, high = consumer.get_watermark_offsets(
                        tp, timeout=self._timeout_seconds
                    )

                    high_water_marks[partition_id] = high

                logger.debug(f"High water marks for '{topic}': {high_water_marks}")
                return high_water_marks

            finally:
                consumer.close()

        except Exception as e:
            raise LagDataUnavailableError(
                f"Failed to get high water marks for topic '{topic}'", cause=e
            ) from e

    def clear_topic_cache(self, topic: str = None) -> None:
        """
        Clear cached topic partition metadata.

        Args:
            topic: Specific topic to clear (None to clear all)
        """
        if topic is None:
            self._topic_partitions_cache.clear()
            logger.debug("Cleared all topic partition cache")
        else:
            self._topic_partitions_cache.pop(topic, None)
            logger.debug(f"Cleared partition cache for topic '{topic}'")

    def is_healthy(self) -> bool:
        """
        Check if AdminClient can connect to Kafka cluster.

        Returns:
            bool: True if cluster is reachable, False otherwise
        """
        try:
            # Simple cluster metadata check with short timeout
            cluster_metadata = self._admin_client.describe_cluster(
                request_timeout=min(2.0, self._timeout_seconds)
            )
            cluster_metadata.result(timeout=self._timeout_seconds)
            return True

        except Exception as e:
            logger.debug(f"Health check failed: {e}")
            return False

    def __repr__(self) -> str:
        return (
            f"KafkaAdminLagCollector("
            f"servers='{self._bootstrap_servers}', "
            f"group='{self._consumer_group}', "
            f"timeout={self._timeout_seconds}s"
            f")"
        )
