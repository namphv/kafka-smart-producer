"""
Simplified Kafka Smart Producer implementation.

This module provides both synchronous and asynchronous lightweight drop-in
replacements for confluent-kafka-python Producer that add intelligent partition
selection with minimal overhead.
"""

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional

from confluent_kafka import Producer as ConfluentProducer

from .caching import (
    CacheConfig,
    DefaultHybridCache,
    DefaultLocalCache,
    DefaultRemoteCache,
)

if TYPE_CHECKING:
    from .health import HealthManager

logger = logging.getLogger(__name__)


class SmartProducer(ConfluentProducer):  # type: ignore[misc]
    """
    Drop-in replacement for confluent-kafka Producer with intelligent partitioning.

    This producer extends the standard confluent-kafka Producer by adding smart
    partition selection based on consumer health while preserving the exact same API.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        health_manager: Optional["HealthManager"] = None,
        enable_redis: bool = False,  # Simplified parameter
    ) -> None:
        """
        Initialize the Smart Producer.

        Args:
            config: Standard Kafka producer configuration dict with optional config:
                - 'smart.partitioning.enabled': bool (default: True)
                - 'smart.cache.ttl.ms': int (default: 300000, 5 minutes)
                - 'smart.cache.max.size': int (default: 1000, max cache entries)
                - 'smart.health.check.enabled': bool (default: True)
            health_manager: Optional health manager for partition health queries
        """
        # Extract smart producer specific config
        smart_config = self._extract_smart_config(config)

        # Initialize parent with remaining config
        super().__init__(config)

        # Initialize smart producer components
        self._health_manager = health_manager if smart_config["enabled"] else None
        self._key_cache = (
            self._create_secure_hybrid_cache(smart_config, enable_redis)
            if smart_config["enabled"]
            else None
        )
        self._cache_ttl_ms = smart_config["cache_ttl_ms"]
        self._health_check_enabled = smart_config["health_check_enabled"]
        self._smart_enabled = smart_config["enabled"]

        logger.info(
            f"SmartProducer initialized - smart partitioning: \
                {smart_config['enabled']}, "
            f"Redis enabled: {enable_redis}, "
            f"cache TTL: {smart_config['cache_ttl_ms']}ms"
        )

    def _extract_smart_config(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Extract smart producer config with security enhancements."""
        smart_config = {
            # Core settings
            "enabled": config.pop("smart.partitioning.enabled", True),
            "cache_ttl_ms": config.pop("smart.cache.ttl.ms", 300000),
            "cache_max_size": config.pop("smart.cache.max.size", 1000),
            "health_check_enabled": config.pop("smart.health.check.enabled", True),
            # Redis settings
            "redis_host": config.pop("smart.cache.redis.host", "localhost"),
            "redis_port": config.pop("smart.cache.redis.port", 6379),
            "redis_db": config.pop("smart.cache.redis.db", 0),
            "redis_password": config.pop("smart.cache.redis.password", None),
            "redis_ttl_seconds": config.pop("smart.cache.redis.ttl.seconds", 300),
            # NEW: Message-type TTLs
            "ordered_message_ttl": config.pop("smart.cache.ordered.ttl.seconds", 3600),
            # NEW: Redis SSL/TLS security
            "redis_ssl_enabled": config.pop("smart.cache.redis.ssl.enabled", False),
            "redis_ssl_cert_reqs": config.pop(
                "smart.cache.redis.ssl.cert_reqs", "required"
            ),
            "redis_ssl_ca_certs": config.pop("smart.cache.redis.ssl.ca_certs", None),
            "redis_ssl_certfile": config.pop("smart.cache.redis.ssl.certfile", None),
            "redis_ssl_keyfile": config.pop("smart.cache.redis.ssl.keyfile", None),
        }
        return smart_config

    def _create_secure_hybrid_cache(
        self, smart_config: Dict[str, Any], enable_redis: bool
    ) -> DefaultHybridCache:
        """Create secure hybrid cache with proper SSL/TLS support."""
        cache_config = CacheConfig(
            local_max_size=smart_config["cache_max_size"],
            local_default_ttl_seconds=smart_config["cache_ttl_ms"] / 1000.0,
            remote_default_ttl_seconds=smart_config["redis_ttl_seconds"],
            ordered_message_ttl=smart_config["ordered_message_ttl"],
            stats_collection_enabled=True,
        )

        # Create local cache (always enabled)
        local_cache = DefaultLocalCache(cache_config)

        # Create remote cache (Redis) if enabled
        remote_cache = None
        if enable_redis:
            try:
                redis_config = {
                    "host": smart_config["redis_host"],
                    "port": smart_config["redis_port"],
                    "db": smart_config["redis_db"],
                    "password": smart_config.get("redis_password"),
                    "ssl_enabled": smart_config.get("redis_ssl_enabled", False),
                    "ssl_cert_reqs": smart_config.get(
                        "redis_ssl_cert_reqs", "required"
                    ),
                    "ssl_ca_certs": smart_config.get("redis_ssl_ca_certs"),
                    "ssl_certfile": smart_config.get("redis_ssl_certfile"),
                    "ssl_keyfile": smart_config.get("redis_ssl_keyfile"),
                }

                remote_cache_instance = DefaultRemoteCache(redis_config)

                if remote_cache_instance._redis is not None:
                    remote_cache = remote_cache_instance
                    ssl_status = (
                        "with SSL"
                        if smart_config.get("redis_ssl_enabled")
                        else "without SSL"
                    )
                    logger.info(
                        f"Redis remote cache enabled: "
                        f"{smart_config['redis_host']}:{smart_config['redis_port']} "
                        f"({ssl_status})"
                    )
                else:
                    logger.warning(
                        "Redis connection failed, falling back to local cache only"
                    )

            except Exception as e:
                logger.warning(f"Failed to initialize Redis remote cache: {e}")

        return DefaultHybridCache(local_cache, remote_cache, cache_config)

    def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = -1,
        ordered: bool = False,  # NEW parameter for message ordering
        on_delivery: Optional[Callable[..., None]] = None,
        timestamp: Optional[int] = None,
        headers: Optional[Dict[str, bytes]] = None,
    ) -> None:
        """
        Produce a message with intelligent partition selection.

        This method has the exact same signature and behavior as confluent-kafka
        Producer.produce(), with the addition of smart partition selection when no
        explicit partition is provided.

        Args:
            topic: Topic name
            value: Message value
            key: Message key
            partition: Explicit partition (-1 for automatic selection)
            on_delivery: Delivery callback
            timestamp: Message timestamp
            headers: Message headers
        """
        # Only apply smart partition selection if:
        # 1. No explicit partition provided (partition == -1)
        # 2. Key is provided (needed for smart selection)
        # 3. Smart partitioning is enabled
        if partition == -1 and key is not None and self._smart_enabled:
            try:
                selected_partition = self._select_partition_with_ordering(
                    topic, key, ordered
                )
                if selected_partition is not None:
                    partition = selected_partition
            except Exception as e:
                # Log error but continue with default partitioning
                logger.warning(
                    f"Smart partition selection failed for topic {topic}: {e}"
                )

        # Ensure partition is valid for confluent-kafka
        if partition == -1:
            partition = None  # Let confluent-kafka handle default partitioning

        # Call parent produce method with same signature
        super().produce(
            topic=topic,
            value=value,
            key=key,
            partition=partition,
            on_delivery=on_delivery,
            timestamp=timestamp,
            headers=headers,
        )

    def _select_partition_with_ordering(
        self, topic: str, key: bytes, ordered: bool
    ) -> Optional[int]:
        """Select partition with simplified logic for ordered vs unordered messages."""

        # For unordered messages: Just get a healthy partition, no caching
        if not ordered:
            return self._select_via_health_manager(topic)

        # For ordered messages: Use caching for key stickiness
        if not self._key_cache:
            return self._select_via_health_manager(topic)

        # Create cache key with app-specific prefix
        cache_key = f"kafka_smart_producer:{topic}:{key.hex()}"

        # Check cache first for ordered messages
        cached_partition = self._key_cache.get_sync(cache_key)

        if cached_partition is not None:
            # Validate partition is still healthy
            if self._health_manager and self._health_check_enabled:
                try:
                    if self._health_manager.is_partition_healthy(
                        topic, cached_partition
                    ):
                        return int(cached_partition)
                    else:
                        # Partition unhealthy, invalidate topic cache
                        topic_pattern = f"kafka_smart_producer:{topic}:*"
                        self._key_cache.clear_pattern(topic_pattern)
                except Exception as e:
                    logger.debug(
                        f"Health check failed for partition {cached_partition}: {e}"
                    )
            else:
                return int(cached_partition)

        # Get fresh partition from health manager
        selected_partition = self._select_via_health_manager(topic)

        if selected_partition is not None:
            # Cache only for ordered messages (key stickiness)
            self._key_cache.set_with_ordered_ttl(cache_key, selected_partition)
            return selected_partition

        return None

    def _select_via_health_manager(self, topic: str) -> Optional[int]:
        """Select partition via health manager."""
        if self._health_manager and self._health_check_enabled:
            try:
                return self._health_manager.select_partition(topic)
            except Exception as e:
                logger.debug(f"Health manager selection failed for topic {topic}: {e}")
        return None

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get basic cache statistics for monitoring.

        Returns:
            Dictionary with cache size and entries
        """
        if not self._key_cache:
            return {"enabled": False}

        # Hybrid cache system with stats
        if hasattr(self._key_cache, "get_combined_stats"):
            combined_stats = self._key_cache.get_combined_stats()

            return {
                "enabled": True,
                "cache_ttl_ms": self._cache_ttl_ms,
                "local_cache": {
                    "size": combined_stats["local"].hits_local
                    + combined_stats["local"].misses,
                    "hit_ratio": combined_stats["local"].get_hit_ratio(),
                    "avg_latency_ms": combined_stats["local"].get_avg_latency_ms(),
                },
                "remote_cache": (
                    {
                        "enabled": self._key_cache.is_remote_available(),
                        "stats": combined_stats.get("remote"),
                    }
                    if "remote" in combined_stats
                    else {"enabled": False}
                ),
            }
        else:
            # Fallback for caches without detailed stats
            return {
                "enabled": True,
                "cache_ttl_ms": self._cache_ttl_ms,
                "simple_stats": "Cache stats not available",
            }

    def clear_cache(self) -> None:
        """Clear the key-to-partition cache safely."""
        if self._key_cache is not None:
            self._key_cache.clear()
            logger.debug("Partition cache cleared safely")


class AsyncSmartProducer:
    """
    Async-native Kafka Smart Producer with intelligent partition selection.

    This producer provides a fully asynchronous API while maintaining the same
    intelligent partition selection capabilities as SmartProducer. It wraps the
    synchronous SmartProducer with proper async patterns to avoid blocking the
    event loop.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        health_manager: Optional["HealthManager"] = None,
        max_workers: int = 4,
    ) -> None:
        """
        Initialize the Async Smart Producer.

        Args:
            config: Standard Kafka producer config dict with optional smart config
            health_manager: Optional health manager for partition health queries
            max_workers: Maximum workers for the thread pool executor
        """
        self._sync_producer = SmartProducer(config, health_manager)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="async-smart-producer"
        )
        self._closed = False

        logger.info(f"AsyncSmartProducer initialized with {max_workers} workers")

    async def produce(
        self,
        topic: str,
        value: Optional[bytes] = None,
        key: Optional[bytes] = None,
        partition: Optional[int] = -1,
        ordered: bool = False,  # NEW parameter for message ordering
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
            partition: Explicit partition (-1 for automatic selection)
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
            """
            Internal delivery callback that bridges sync callback to async future.
            This runs in the confluent-kafka background thread.
            """

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

        # Run produce in executor to avoid blocking event loop
        try:
            await loop.run_in_executor(
                self._executor,
                lambda: self._sync_producer.produce(
                    topic=topic,
                    value=value,
                    key=key,
                    partition=partition,
                    ordered=ordered,
                    on_delivery=async_delivery_callback,
                    timestamp=timestamp,
                    headers=headers,
                ),
            )

            # Poll for delivery reports in background
            await loop.run_in_executor(self._executor, self._sync_producer.poll, 0)

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
                self._executor, self._sync_producer.flush, timeout
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
                self._executor, self._sync_producer.poll, timeout
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
            await loop.run_in_executor(self._executor, self._sync_producer.flush)

            # Close sync producer
            await loop.run_in_executor(self._executor, self._sync_producer.close)

            logger.info("AsyncSmartProducer closed successfully")

        except Exception as e:
            logger.error(f"Error during AsyncSmartProducer close: {e}")
            raise

        finally:
            # Mark as closed and shutdown executor
            self._closed = True
            self._executor.shutdown(wait=True)

    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get basic cache statistics for monitoring.

        This is a synchronous method since it only reads cached data.

        Returns:
            Dictionary with cache size and entries
        """
        return self._sync_producer.get_cache_stats()

    def clear_cache(self) -> None:
        """
        Clear the key-to-partition cache.

        This is a synchronous method since it's a simple memory operation.
        """
        self._sync_producer.clear_cache()

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
