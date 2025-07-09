"""
Smart producer base implementation for Kafka Smart Producer.

This module provides the foundation for both synchronous and asynchronous
smart producers with intelligent partition selection and key caching.
"""

import hashlib
import logging
import random
import threading
import time
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Protocol, Set

from cachetools import TTLCache
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import (
    Producer as ConfluentProducer,  # type: ignore[import-untyped]
)

from .exceptions import PartitionSelectionError

if TYPE_CHECKING:
    from .health import HealthManager

logger = logging.getLogger(__name__)


class MessageMetadata:
    """Metadata for produced messages."""

    def __init__(
        self,
        topic: str,
        partition: int,
        offset: Optional[int] = None,
        timestamp: Optional[int] = None,
        key: Optional[bytes] = None,
    ):
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.timestamp = timestamp
        self.key = key
        self.produced_at = time.time()


class ProduceResult:
    """Result of a produce operation."""

    def __init__(
        self,
        metadata: MessageMetadata,
        success: bool,
        error: Optional[Exception] = None,
        latency_ms: Optional[float] = None,
    ):
        self.metadata = metadata
        self.success = success
        self.error = error
        self.latency_ms = latency_ms


class PartitioningStrategy(Enum):
    """Strategies for partition selection."""

    SMART = "smart"  # Use health-aware selection
    KEY_HASH = "key_hash"  # Standard key-based hashing
    ROUND_ROBIN = "round_robin"  # Round-robin across all partitions
    RANDOM = "random"  # Random partition selection


@dataclass
class ProducerConfig:
    """Configuration for SmartProducer behavior."""

    # Smart partitioning settings
    enable_smart_partitioning: bool = True
    fallback_strategy: PartitioningStrategy = PartitioningStrategy.KEY_HASH
    health_check_interval_seconds: float = 5.0

    # Key-to-partition caching
    enable_key_caching: bool = True
    key_cache_ttl_seconds: float = 300.0  # 5 minutes
    key_cache_max_size: int = 10000

    # Performance settings
    max_partition_selection_time_ms: float = 1.0
    enable_metrics: bool = True

    # Fallback behavior
    unhealthy_partition_threshold: float = 0.5
    force_fallback_on_error: bool = True

    # Key affinity settings
    preserve_key_ordering: bool = True
    key_affinity_mode: str = "strict"  # strict, eventual, hybrid

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.key_cache_ttl_seconds <= 0:
            raise ValueError("key_cache_ttl_seconds must be positive")
        if self.key_cache_max_size <= 0:
            raise ValueError("key_cache_max_size must be positive")
        if self.max_partition_selection_time_ms <= 0:
            raise ValueError("max_partition_selection_time_ms must be positive")
        if not (0.0 <= self.unhealthy_partition_threshold <= 1.0):
            raise ValueError(
                "unhealthy_partition_threshold must be between 0.0 and 1.0"
            )
        if self.key_affinity_mode not in ("strict", "eventual", "hybrid"):
            raise ValueError(
                "key_affinity_mode must be 'strict', 'eventual', or 'hybrid'"
            )


class KeyPartitionCache:
    """Cache for key-to-partition mappings using cachetools.TTLCache for O(1) ops."""

    def __init__(self, config: ProducerConfig):
        self._config = config
        # TTLCache provides both maxsize (LRU) and TTL functionality with O(1) ops
        self._cache: TTLCache[bytes, int] = TTLCache(
            maxsize=config.key_cache_max_size, ttl=config.key_cache_ttl_seconds
        )
        self._lock = threading.RLock()

    def get_partition(self, key: bytes) -> Optional[int]:
        """Get cached partition for key if still valid."""
        with self._lock:
            return self._cache.get(key)

    def set_partition(self, key: bytes, partition: int) -> None:
        """Cache key-to-partition mapping."""
        with self._lock:
            self._cache[key] = partition

    def invalidate_key(self, key: bytes) -> None:
        """Remove key from cache (e.g., when partition becomes unhealthy)."""
        with self._lock:
            self._cache.pop(key, None)

    def invalidate_partition(self, partition: int) -> None:
        """Remove all keys mapped to a specific partition."""
        with self._lock:
            keys_to_evict = [k for k, p in self._cache.items() if p == partition]
            for key in keys_to_evict:
                self._cache.pop(key, None)

    def clear(self) -> None:
        """Clear all cached mappings."""
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        """Get current cache size."""
        with self._lock:
            return len(self._cache)


class ProducerMetrics:
    """Metrics collection for producer operations."""

    def __init__(self) -> None:
        self.messages_produced = 0
        self.smart_selections = 0
        self.fallback_selections = 0
        self.key_cache_hits = 0
        self.key_cache_misses = 0
        self.partition_selection_times_ms: List[float] = []
        self.produce_errors = 0
        self.healthy_partitions_used: Set[int] = set()
        self.unhealthy_partitions_avoided: Set[int] = set()
        self.last_reset = time.time()
        self._lock = threading.Lock()

    def record_message_produced(
        self, partition: int, selection_time_ms: float, was_smart: bool
    ) -> None:
        """Record a successful message production."""
        with self._lock:
            self.messages_produced += 1
            if was_smart:
                self.smart_selections += 1
                self.healthy_partitions_used.add(partition)
            else:
                self.fallback_selections += 1
            self.partition_selection_times_ms.append(selection_time_ms)

    def record_cache_hit(self) -> None:
        with self._lock:
            self.key_cache_hits += 1

    def record_cache_miss(self) -> None:
        with self._lock:
            self.key_cache_misses += 1

    def record_error(self) -> None:
        with self._lock:
            self.produce_errors += 1

    def record_partition_avoided(self, partition: int) -> None:
        with self._lock:
            self.unhealthy_partitions_avoided.add(partition)

    def get_summary(self) -> Dict[str, Any]:
        """Get metrics summary."""
        with self._lock:
            avg_selection_time = (
                sum(self.partition_selection_times_ms)
                / len(self.partition_selection_times_ms)
                if self.partition_selection_times_ms
                else 0.0
            )

            cache_hit_rate = (
                self.key_cache_hits / (self.key_cache_hits + self.key_cache_misses)
                if (self.key_cache_hits + self.key_cache_misses) > 0
                else 0.0
            )

            return {
                "messages_produced": self.messages_produced,
                "smart_selection_rate": (
                    self.smart_selections / max(1, self.messages_produced)
                ),
                "avg_partition_selection_time_ms": avg_selection_time,
                "max_partition_selection_time_ms": max(
                    self.partition_selection_times_ms, default=0.0
                ),
                "key_cache_hit_rate": cache_hit_rate,
                "produce_errors": self.produce_errors,
                "unique_healthy_partitions_used": len(self.healthy_partitions_used),
                "unique_unhealthy_partitions_avoided": len(
                    self.unhealthy_partitions_avoided
                ),
                "uptime_seconds": time.time() - self.last_reset,
            }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self.messages_produced = 0
            self.smart_selections = 0
            self.fallback_selections = 0
            self.key_cache_hits = 0
            self.key_cache_misses = 0
            self.partition_selection_times_ms = []
            self.produce_errors = 0
            self.healthy_partitions_used = set()
            self.unhealthy_partitions_avoided = set()
            self.last_reset = time.time()


class SmartProducerBase(Protocol):
    """Base interface for smart producers with health-aware partitioning."""

    @abstractmethod
    def __init__(
        self,
        config: "ProducerConfig",
        health_manager: "HealthManager",
        kafka_config: Dict[str, Any],
    ) -> None:
        """Initialize producer with configuration and health manager."""
        ...

    @abstractmethod
    def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        on_delivery: Optional[Callable[["ProduceResult"], None]] = None,
        timestamp: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> "ProduceResult":
        """Produce a message to Kafka with smart partition selection."""
        ...

    @abstractmethod
    def select_partition(
        self,
        topic: str,
        key: Optional[bytes] = None,
        value: Optional[bytes] = None,
        available_partitions: Optional[List[int]] = None,
    ) -> int:
        """
        Select optimal partition for message delivery.

        Args:
            topic: Target topic name
            key: Message key for ordering/routing
            value: Message value (for size-based decisions)
            available_partitions: Available partitions (from metadata)

        Returns:
            Selected partition ID
        """
        ...

    @abstractmethod
    def get_topic_metadata(self, topic: str) -> Dict[str, Any]:
        """Get topic metadata including partition count."""
        ...

    @abstractmethod
    def get_metrics(self) -> Dict[str, Any]:
        """Get producer performance metrics."""
        ...

    @abstractmethod
    def close(self) -> None:
        """Close producer and cleanup resources."""
        ...

    @abstractmethod
    def flush(self, timeout: Optional[float] = None) -> None:
        """Flush pending messages."""
        ...


class DefaultSmartProducerBase:
    """Default implementation of smart producer base functionality."""

    def __init__(
        self,
        config: ProducerConfig,
        health_manager: "HealthManager",
        kafka_config: Dict[str, Any],
    ) -> None:
        self._config = config
        self._health_manager = health_manager
        self._kafka_config = kafka_config
        self._key_cache = (
            KeyPartitionCache(config) if config.enable_key_caching else None
        )
        self._metrics = ProducerMetrics() if config.enable_metrics else None
        self._partition_counts: Dict[str, int] = {}  # topic -> partition count cache
        self._last_metadata_refresh: Dict[str, float] = {}
        self._round_robin_state: Dict[
            str, int
        ] = {}  # topic -> current round-robin index
        self._rr_lock = threading.Lock()  # For round-robin state

    def select_partition(
        self,
        topic: str,
        key: Optional[bytes] = None,
        value: Optional[bytes] = None,
        available_partitions: Optional[List[int]] = None,
    ) -> int:
        """Select partition using smart or fallback strategy."""
        start_time = time.time()

        # 1. Attempt to get a healthy partition from the key cache
        if key and self._key_cache:
            partition = self._get_from_key_cache(topic, key)
            if partition is not None:
                return self._record_and_return(
                    partition, start_time, True, cache_hit=True
                )

        # 2. Attempt to use the smart selection logic
        if self._config.enable_smart_partitioning:
            partition = self._try_smart_selection(topic, key, available_partitions)
            if partition is not None:
                # Cache key-to-partition mapping if key provided
                if key and self._key_cache:
                    self._key_cache.set_partition(key, partition)
                return self._record_and_return(
                    partition, start_time, True, cache_hit=False
                )

        # 3. Fall back to standard strategy
        partition = self._fallback_partition_selection(topic, key, available_partitions)
        return self._record_and_return(partition, start_time, False, cache_hit=False)

    def _get_from_key_cache(self, topic: str, key: bytes) -> Optional[int]:
        """Check key cache for a healthy partition."""
        if not self._key_cache:
            return None

        cached_partition = self._key_cache.get_partition(key)
        if cached_partition is not None:
            if self._is_partition_healthy(topic, cached_partition):
                return cached_partition
            else:
                # Partition became unhealthy, invalidate cache
                self._key_cache.invalidate_key(key)
                if self._metrics:
                    self._metrics.record_partition_avoided(cached_partition)
        return None

    def _try_smart_selection(
        self,
        topic: str,
        key: Optional[bytes],
        available_partitions: Optional[List[int]],
    ) -> Optional[int]:
        """Attempt smart partition selection, handling errors based on configuration."""
        try:
            return self._health_manager.select_partition(
                topic=topic, key=key, available_partitions=available_partitions
            )
        except Exception as e:
            logger.debug(f"Smart partition selection failed for {topic}: {e}")
            if self._metrics:
                self._metrics.record_error()
            if not self._config.force_fallback_on_error:
                raise
            return None

    def _record_and_return(
        self, partition: int, start_time: float, was_smart: bool, cache_hit: bool
    ) -> int:
        """Record metrics and return the selected partition."""
        selection_time_ms = (time.time() - start_time) * 1000
        self._record_metrics(partition, selection_time_ms, was_smart, cache_hit)
        return partition

    def get_topic_metadata(self, topic: str) -> Dict[str, Any]:
        """Get cached or fresh topic metadata."""
        now = time.time()
        last_refresh = self._last_metadata_refresh.get(topic, 0)

        # Refresh metadata if stale (every 60 seconds)
        if now - last_refresh > 60.0:
            self._refresh_topic_metadata(topic)
            self._last_metadata_refresh[topic] = now

        partition_count = self._partition_counts.get(topic, 1)
        return {
            "partition_count": partition_count,
            "last_refresh": self._last_metadata_refresh.get(topic, 0),
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Get producer metrics summary."""
        if self._metrics:
            return self._metrics.get_summary()
        return {"metrics_disabled": True}

    def _fallback_partition_selection(
        self,
        topic: str,
        key: Optional[bytes],
        available_partitions: Optional[List[int]],
    ) -> int:
        """Fallback partition selection using configured strategy."""
        metadata = self.get_topic_metadata(topic)
        partition_count = metadata["partition_count"]

        if available_partitions is not None:
            partitions = available_partitions
        else:
            partitions = list(range(partition_count))

        if not partitions:
            raise PartitionSelectionError(f"No partitions available for topic {topic}")

        if self._config.fallback_strategy == PartitioningStrategy.KEY_HASH:
            if key:
                hash_value = int(hashlib.sha256(key).hexdigest(), 16)
                return partitions[hash_value % len(partitions)]
            else:
                # No key, fall back to round-robin
                return self._round_robin_partition(topic, partitions)

        elif self._config.fallback_strategy == PartitioningStrategy.ROUND_ROBIN:
            return self._round_robin_partition(topic, partitions)

        elif self._config.fallback_strategy == PartitioningStrategy.RANDOM:
            return random.choice(partitions)

        else:
            # Default to first partition
            return partitions[0]

    def _round_robin_partition(self, topic: str, partitions: List[int]) -> int:
        """Round-robin partition selection with thread safety."""
        with self._rr_lock:
            current = self._round_robin_state.get(topic, 0)
            partition = partitions[current % len(partitions)]
            self._round_robin_state[topic] = current + 1
            return partition

    def _is_partition_healthy(self, topic: str, partition: int) -> bool:
        """Check if partition is currently healthy."""
        try:
            result: bool = self._health_manager.is_partition_healthy(topic, partition)
            return result
        except Exception:
            # Health check failed, assume healthy to avoid blocking
            return True

    def _refresh_topic_metadata(self, topic: str) -> None:
        """Refresh topic metadata (partition count). Override in subclasses."""
        # Default implementation - subclasses should override with actual Kafka calls
        self._partition_counts[topic] = self._partition_counts.get(topic, 3)

    def _record_metrics(
        self, partition: int, selection_time_ms: float, was_smart: bool, cache_hit: bool
    ) -> None:
        """Record metrics for partition selection."""
        if self._metrics:
            self._metrics.record_message_produced(
                partition, selection_time_ms, was_smart
            )
            if cache_hit:
                self._metrics.record_cache_hit()
            elif self._key_cache:  # Only record miss if caching is enabled
                self._metrics.record_cache_miss()


class SyncSmartProducer(DefaultSmartProducerBase):
    """
    Synchronous Kafka Smart Producer with intelligent partition selection.

    This producer extends the DefaultSmartProducerBase with actual Kafka producer
    functionality, providing synchronous message delivery with smart partition
    selection based on consumer health.
    """

    def __init__(
        self,
        config: ProducerConfig,
        health_manager: "HealthManager",
        kafka_config: Dict[str, Any],
    ) -> None:
        """
        Initialize the synchronous smart producer.

        Args:
            config: Producer configuration
            health_manager: Health manager for partition selection
            kafka_config: Kafka producer configuration
        """
        super().__init__(config, health_manager, kafka_config)
        self._producer = ConfluentProducer(kafka_config)
        self._closed = False

    def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        on_delivery: Optional[Callable[[ProduceResult], None]] = None,
        timestamp: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> ProduceResult:
        """
        Produce a message to Kafka with smart partition selection.

        Args:
            topic: Topic name
            value: Message value
            key: Message key
            partition: Explicit partition (bypasses smart selection)
            on_delivery: Delivery callback
            timestamp: Message timestamp
            headers: Message headers

        Returns:
            ProduceResult with metadata and delivery status

        Raises:
            ProducerNotReadyError: If producer is closed
            PartitionSelectionError: If partition selection fails
        """
        if self._closed:
            raise ProducerNotReadyError("Producer is closed")

        start_time = time.time()

        # Use explicit partition or smart selection
        if partition is None:
            try:
                partition = self.select_partition(topic, key, value)
            except Exception as e:
                error_msg = f"Failed to select partition for topic {topic}: {e}"
                logger.error(error_msg)
                raise PartitionSelectionError(error_msg) from e

        # Create message metadata
        metadata = MessageMetadata(
            topic=topic,
            partition=partition,
            timestamp=timestamp,
            key=key,
        )

        # Use thread-safe result container and event for synchronous behavior
        result_container: List[ProduceResult] = []
        delivery_event = threading.Event()

        def delivery_callback(err: Optional[KafkaError], msg: Any) -> None:
            """Internal delivery callback."""
            end_time = time.time()
            latency_ms = (end_time - start_time) * 1000

            produce_result = ProduceResult(
                metadata=metadata,
                success=False,
                latency_ms=latency_ms,
            )

            if err:
                produce_result.error = KafkaException(err)
                logger.error(f"Message delivery failed: {err}")
            else:
                produce_result.success = True
                produce_result.metadata.offset = msg.offset()
                produce_result.metadata.timestamp = msg.timestamp()[1]

            result_container.append(produce_result)

            # Call user callback if provided
            if on_delivery:
                try:
                    on_delivery(produce_result)
                except Exception as e:
                    logger.error(f"User delivery callback failed: {e}")

            delivery_event.set()

        try:
            # Produce message
            self._producer.produce(
                topic=topic,
                value=value,
                key=key,
                partition=partition,
                timestamp=timestamp,
                headers=headers,
                callback=delivery_callback,
            )

            # Flush to ensure message is sent and callback is triggered
            remaining = self._producer.flush(10.0)  # 10 second timeout

            if remaining > 0:
                # Flush timed out
                logger.error(f"Flush timed out: {remaining} messages remain in queue")
                return ProduceResult(
                    metadata=metadata,
                    success=False,
                    error=Exception("Flush timeout"),
                    latency_ms=(time.time() - start_time) * 1000,
                )

            # Wait for delivery callback to complete
            if not delivery_event.wait(timeout=1.0):
                logger.error("Delivery confirmation timed out after flush")
                return ProduceResult(
                    metadata=metadata,
                    success=False,
                    error=Exception("Delivery confirmation timeout"),
                    latency_ms=(time.time() - start_time) * 1000,
                )

            return result_container[0]

        except Exception as e:
            error_msg = f"Failed to produce message to {topic}:{partition}: {e}"
            logger.error(error_msg)
            return ProduceResult(
                metadata=metadata,
                success=False,
                error=e,
                latency_ms=(time.time() - start_time) * 1000,
            )

    def flush(self, timeout: Optional[float] = None) -> None:
        """
        Flush pending messages.

        Args:
            timeout: Timeout in seconds (None for indefinite)
        """
        if self._closed:
            logger.warning("Attempted to flush closed producer")
            return

        try:
            remaining_msgs = self._producer.flush(timeout)
            if remaining_msgs > 0:
                logger.warning(f"Flush timeout: {remaining_msgs} messages remain")
        except Exception as e:
            logger.error(f"Flush failed: {e}")
            raise

    def close(self) -> None:
        """Close the producer and cleanup resources."""
        if self._closed:
            return

        try:
            # Flush pending messages
            self.flush(timeout=10.0)

            # Close underlying producer
            self._producer = None
            self._closed = True

            logger.info("Producer closed successfully")

        except Exception as e:
            logger.error(f"Error closing producer: {e}")
            self._closed = True
            raise

    def _refresh_topic_metadata(self, topic: str) -> None:
        """Refresh topic metadata from Kafka."""
        if self._closed:
            return

        try:
            # Get topic metadata from Kafka
            metadata = self._producer.list_topics(topic, timeout=5.0)

            if topic in metadata.topics:
                topic_metadata = metadata.topics[topic]
                partition_count = len(topic_metadata.partitions)
                self._partition_counts[topic] = partition_count
                logger.debug(
                    f"Refreshed metadata for {topic}: {partition_count} partitions"
                )
            else:
                logger.warning(f"Topic {topic} not found in metadata")

        except Exception as e:
            logger.error(f"Failed to refresh metadata for {topic}: {e}")
            raise MetadataRefreshError(f"Failed to refresh metadata for {topic}") from e

    @property
    def closed(self) -> bool:
        """Check if producer is closed."""
        return self._closed


# Exception classes specific to producer operations
class ProducerNotReadyError(Exception):
    """Raised when producer is not ready for operations."""


class MetadataRefreshError(Exception):
    """Raised when topic metadata refresh fails."""
