"""
Asynchronous Smart Producer implementation.

This module provides an asynchronous Kafka producer with intelligent partition
selection based on consumer health monitoring.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

from confluent_kafka import Producer as ConfluentProducer

from .producer_config import ProducerConfig
from .producer_utils import BasePartitionSelector

if TYPE_CHECKING:
    from .async_partition_health_monitor import AsyncPartitionHealthMonitor

logger = logging.getLogger(__name__)


class AsyncSmartProducer:
    """
    Asynchronous Kafka Smart Producer with intelligent partition selection.

    This producer provides a fully asynchronous API while maintaining the same
    intelligent partition selection capabilities as SmartProducer.
    """

    def __init__(
        self,
        config: ProducerConfig,
        health_manager: Optional["AsyncPartitionHealthMonitor"] = None,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        Initialize the Async Smart Producer.

        Args:
            config: Producer configuration
            health_manager: Optional explicit health manager (overrides config)
            max_workers: Maximum workers for the thread pool executor
        """
        if not isinstance(config, ProducerConfig):
            raise ValueError("config must be ProducerConfig instance")

        self._config = config

        # Create underlying confluent-kafka producer
        kafka_config = self._config.get_clean_kafka_config()
        self._producer = ConfluentProducer(kafka_config)

        # Create or use health manager
        if health_manager is not None:
            self._health_manager = health_manager
            logger.info("Using explicitly provided AsyncPartitionHealthMonitor")
        else:
            self._health_manager = self._create_health_manager()

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

        # Extract async-specific config
        if max_workers is None:
            max_workers = 4  # Default value

        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="async-smart-producer"
        )
        self._closed = False

        health_status = "enabled" if self._health_manager else "disabled"
        logger.info(
            f"AsyncSmartProducer initialized - topics: {self._config.topics}, "
            f"smart partitioning: {self._smart_enabled}, \
                health manager: {health_status}, "
            f"key stickiness: {self._config.key_stickiness}, workers: {max_workers}"
        )

    def _create_health_manager(self) -> Optional["AsyncPartitionHealthMonitor"]:
        """Create AsyncPartitionHealthMonitor if health_manager is configured."""
        if not self._config.health_config:
            return None

        from .async_partition_health_monitor import AsyncPartitionHealthMonitor

        return AsyncPartitionHealthMonitor(
            config=self._config.health_config,
            kafka_config=self._config.get_clean_kafka_config(),
        )

    def _create_cache(self):
        """Create cache from config."""
        from .producer_utils import create_cache_from_config

        return create_cache_from_config(self._config)

    async def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        on_delivery: Optional[Callable[..., None]] = None,
        timestamp: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> None:
        """
        Produce a message asynchronously with intelligent partition selection.

        This method has the same signature as confluent-kafka Producer.produce()
        but is fully asynchronous and won't block the event loop.

        Args:
            topic: Topic name
            value: Message value
            key: Message key
            partition: Explicit partition (None for automatic selection)
            on_delivery: Delivery callback
            timestamp: Message timestamp
            headers: Message headers

        Raises:
            RuntimeError: If producer is closed
            Exception: If message production fails
        """
        if self._closed:
            raise RuntimeError("AsyncSmartProducer is closed")

        loop = asyncio.get_event_loop()

        # Create async future for delivery notification
        delivery_future = loop.create_future()

        def async_delivery_callback(err: Any, msg: Any) -> None:
            """Internal delivery callback that bridges sync callback to async future."""

            def complete_future() -> None:
                try:
                    # Call user callback first if provided
                    if on_delivery:
                        try:
                            on_delivery(err, msg)
                        except Exception as callback_error:
                            if not delivery_future.done():
                                delivery_future.set_exception(callback_error)
                            return

                    # Complete the future
                    if not delivery_future.done():
                        if err:
                            delivery_future.set_exception(
                                Exception(f"Message delivery failed: {err}")
                            )
                        else:
                            delivery_future.set_result(msg)

                except Exception as e:
                    if not delivery_future.done():
                        delivery_future.set_exception(e)

            # Schedule future completion on the event loop thread
            loop.call_soon_threadsafe(complete_future)

        # Apply smart partition selection if enabled and no explicit partition
        if partition is None and self._smart_enabled and self._partition_selector:
            selected_partition = self._partition_selector.select_partition(topic, key)
            if selected_partition is not None:
                partition = selected_partition

        # Run produce in executor to avoid blocking event loop
        try:
            # Call underlying producer - only pass non-None parameters
            produce_kwargs = {"topic": topic}

            if value is not None:
                produce_kwargs["value"] = value
            if key is not None:
                produce_kwargs["key"] = key
            if partition is not None:
                produce_kwargs["partition"] = partition
            if async_delivery_callback is not None:
                produce_kwargs["on_delivery"] = async_delivery_callback
            if timestamp is not None:
                produce_kwargs["timestamp"] = timestamp
            if headers is not None:
                produce_kwargs["headers"] = headers

            await loop.run_in_executor(
                self._executor, lambda: self._producer.produce(**produce_kwargs)
            )

            # Poll for delivery reports in background
            await loop.run_in_executor(self._executor, self._producer.poll, 0)

            # Wait for delivery confirmation
            await delivery_future

        except Exception as e:
            logger.error(f"Failed to produce message to {topic}: {e}")
            raise

    async def flush(self, timeout: Optional[float] = None) -> int:
        """
        Flush pending messages asynchronously.

        Args:
            timeout: Maximum time to wait for flush completion

        Returns:
            Number of messages still in queue after flush
        """
        if self._closed:
            return 0

        loop = asyncio.get_event_loop()

        try:
            remaining = await loop.run_in_executor(
                self._executor, self._producer.flush, timeout
            )
            return int(remaining or 0)

        except Exception as e:
            logger.error(f"Failed to flush messages: {e}")
            raise

    async def poll(self, timeout: float = 0) -> int:
        """
        Poll for delivery reports asynchronously.

        Args:
            timeout: Maximum time to wait for events

        Returns:
            Number of events processed
        """
        if self._closed:
            return 0

        loop = asyncio.get_event_loop()

        try:
            return await loop.run_in_executor(
                self._executor, self._producer.poll, timeout
            )

        except Exception as e:
            logger.error(f"Failed to poll for events: {e}")
            raise

    async def close(self) -> None:
        """
        Close the async producer and cleanup resources.

        This method will flush any remaining messages and shutdown the
        thread pool executor.
        """
        if self._closed:
            return

        try:
            # Flush remaining messages before marking as closed
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self._executor, self._producer.flush)

            # Close producer - no need for executor since close() is fast
            self._producer.close()

            logger.info("AsyncSmartProducer closed successfully")

        except Exception as e:
            logger.error(f"Error during AsyncSmartProducer close: {e}")
            raise

        finally:
            # Mark as closed and shutdown executor
            self._closed = True
            self._executor.shutdown(wait=True)

    @property
    def topics(self) -> List[str]:
        """Get the topics this producer is configured for."""
        return self._config.topics.copy()

    @property
    def health_manager(self) -> Optional["AsyncPartitionHealthMonitor"]:
        """Get the health manager instance."""
        return self._health_manager

    @property
    def smart_enabled(self) -> bool:
        """Check if smart partitioning is enabled."""
        return self._smart_enabled

    @property
    def closed(self) -> bool:
        """Check if the producer is closed."""
        return self._closed

    async def __aenter__(self) -> "AsyncSmartProducer":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()

    def __repr__(self) -> str:
        return (
            f"AsyncSmartProducer("
            f"topics={self._config.topics}, "
            f"smart_enabled={self._smart_enabled}, "
            f"cache_enabled={self._config.cache_config.remote_enabled}, "
            f"closed={self._closed}"
            f")"
        )
