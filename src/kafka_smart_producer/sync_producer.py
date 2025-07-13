"""
Synchronous Smart Producer implementation.

This module provides a synchronous Kafka producer with intelligent partition
selection based on consumer health monitoring.
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Optional

from confluent_kafka import Producer as ConfluentProducer

from .producer_config import SmartProducerConfig
from .producer_utils import BasePartitionSelector

if TYPE_CHECKING:
    from .partition_health_monitor import PartitionHealthMonitor

logger = logging.getLogger(__name__)


class SmartProducer:
    """
    Synchronous Kafka Smart Producer with intelligent partition selection.

    This producer wraps confluent-kafka Producer and adds smart partition
    selection based on consumer health while maintaining a clean, simple API.
    """

    def __init__(
        self,
        config: SmartProducerConfig,
        health_manager: Optional["PartitionHealthMonitor"] = None,
    ) -> None:
        """
        Initialize the Smart Producer.

        Args:
            config: Producer configuration
            health_manager: Optional explicit health manager (overrides config)
        """
        if not isinstance(config, SmartProducerConfig):
            raise ValueError("config must be SmartProducerConfig instance")

        self._config = config

        # Create underlying confluent-kafka producer
        kafka_config = self._config.get_clean_kafka_config()
        self._producer = ConfluentProducer(kafka_config)

        # Create or use health manager
        if health_manager is not None:
            self._health_manager = health_manager
            self._health_manager_created = (
                False  # User provided, don't manage lifecycle
            )
            logger.info("Using explicitly provided PartitionHealthMonitor")
        else:
            self._health_manager = self._create_health_manager()
            self._health_manager_created = True  # We created it, we manage lifecycle
            # Auto-start self-created health manager
            if self._health_manager and not self._health_manager.is_running:
                self._health_manager.start()
                logger.info("Auto-started PartitionHealthMonitor")

        # Initialize cache and partition selector
        self._smart_enabled = (
            self._config.smart_enabled and self._health_manager is not None
        )

        if self._smart_enabled:
            cache = self._create_cache() if self._config.key_stickiness else None
            self._partition_selector: Optional[BasePartitionSelector] = (
                BasePartitionSelector(
                    health_manager=self._health_manager,
                    cache=cache,
                    use_key_stickiness=self._config.key_stickiness,
                )
            )
        else:
            self._partition_selector = None

        health_status = "enabled" if self._health_manager else "disabled"
        logger.info(
            f"SmartProducer initialized - topics: {self._config.topics}, "
            f"smart partitioning: {self._smart_enabled}, "
            f"health manager: {health_status}, "
            f"key stickiness: {self._config.key_stickiness}"
        )

    def _create_health_manager(self) -> Optional["PartitionHealthMonitor"]:
        """Create PartitionHealthMonitor from config if health_manager is configured."""
        if not self._config.health_config:
            return None

        from .partition_health_monitor import PartitionHealthMonitor

        return PartitionHealthMonitor.from_config(
            health_config=self._config.health_config,
            kafka_config=self._config.get_clean_kafka_config(),
        )

    def _create_cache(self):
        """Create cache from config."""
        from .producer_utils import create_cache_from_config

        return create_cache_from_config(self._config)

    def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        on_delivery: Optional[Callable[..., None]] = None,
        timestamp: Optional[int] = None,
        headers: Optional[dict[str, bytes]] = None,
    ) -> None:
        """
        Produce a message with intelligent partition selection.

        This method has the same signature as confluent-kafka Producer.produce()
        with smart partition selection when no explicit partition is provided.

        Args:
            topic: Topic name
            value: Message value
            key: Message key
            partition: Explicit partition (None for automatic selection)
            on_delivery: Delivery callback
            timestamp: Message timestamp
            headers: Message headers
        """
        # Apply smart partition selection if enabled and no explicit partition
        if partition is None and self._smart_enabled and self._partition_selector:
            selected_partition = self._partition_selector.select_partition(topic, key)
            if selected_partition is not None:
                partition = selected_partition

        # Call underlying producer - only pass non-None parameters
        produce_kwargs = {"topic": topic}

        if value is not None:
            produce_kwargs["value"] = value
        if key is not None:
            produce_kwargs["key"] = key
        if partition is not None:
            produce_kwargs["partition"] = partition
        if on_delivery is not None:
            produce_kwargs["on_delivery"] = on_delivery
        if timestamp is not None:
            produce_kwargs["timestamp"] = timestamp
        if headers is not None:
            produce_kwargs["headers"] = headers

        self._producer.produce(**produce_kwargs)

        # Poll to trigger delivery callbacks and handle events
        # Users can call flush() manually for guaranteed delivery
        self._producer.poll(0)

    def flush(self, timeout: Optional[float] = None) -> int:
        """
        Manually flush all queued messages.

        Useful for ensuring all messages are delivered before shutdown.

        Args:
            timeout: Maximum time to wait for delivery (None = wait indefinitely)

        Returns:
            Number of messages still in queue after timeout
        """
        if timeout is not None:
            return self._producer.flush(timeout)
        else:
            return self._producer.flush()

    def close(self) -> None:
        """
        Close the producer and clean up resources.

        This method:
        1. Flushes any remaining messages
        2. Stops the health manager if we created it
        3. Closes the underlying Kafka producer

        Call this before destroying the producer instance.
        """
        try:
            # Flush any remaining messages
            self._producer.flush()

            # Stop health manager only if we created it (not explicitly provided)
            if (
                hasattr(self, "_health_manager")
                and self._health_manager
                and hasattr(self, "_health_manager_created")
                and self._health_manager_created
                and self._health_manager.is_running
            ):
                self._health_manager.stop()
                logger.info("Stopped PartitionHealthMonitor")

            logger.info("SmartProducer closed successfully")

        except Exception as e:
            logger.error(f"Error during SmartProducer cleanup: {e}")
            # Don't raise - cleanup should be best effort

    @property
    def topics(self) -> list[str]:
        """Get the topics this producer is configured for."""
        return self._config.topics.copy()

    @property
    def health_manager(self) -> Optional["PartitionHealthMonitor"]:
        """Get the health manager instance."""
        return self._health_manager

    @property
    def smart_enabled(self) -> bool:
        """Check if smart partitioning is enabled."""
        return self._smart_enabled

    # Proxy all other methods to underlying producer
    def __getattr__(self, name: str) -> Any:
        """Proxy all other methods to the underlying ConfluentProducer."""
        return getattr(self._producer, name)

    def __repr__(self) -> str:
        return (
            f"SmartProducer("
            f"topics={self._config.topics}, "
            f"smart_enabled={self._smart_enabled}, "
            f"cache_enabled={self._config.cache_config.remote_enabled}"
            f")"
        )
