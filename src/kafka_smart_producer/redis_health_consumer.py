"""
Redis-only health consumer for Kafka Smart Producer.

This module provides a stateless health manager that queries Redis directly
on every produce() call without any background processing or local caching.
Perfect for producer instances that need real-time health data published by
independent health monitoring services.
"""

import logging
from typing import TYPE_CHECKING

from .health_mode import HealthMode

if TYPE_CHECKING:
    from .caching import DefaultHybridCache, DefaultRemoteCache

logger = logging.getLogger(__name__)


class RedisHealthConsumer:
    """
    Pure Redis health consumer for producer integration.

    This health manager acts as a stateless consumer of pre-calculated health data
    from Redis. It queries Redis directly on every get_healthy_partitions() call
    without any background threads, local caching, or state management.

    Architecture:
    - No background threads or processes
    - No local caching (always fresh from Redis)
    - Direct Redis query on every produce() call
    - Stateless operation (no start/stop lifecycle)

    Use Case: Producer instances that need real-time health data published by
    independent health monitoring services (Scenario 5 standalone monitors).
    Trade-off: Higher produce() latency (~1-2ms) for guaranteed fresh health data.
    """

    def __init__(
        self,
        redis_cache: "DefaultRemoteCache",
        health_threshold: float = 0.5,
    ):
        """
        Initialize Redis health consumer.

        Args:
            redis_cache: Redis cache instance for health data retrieval
            health_threshold: Minimum health score to consider partition healthy
        """
        self._redis_cache = redis_cache
        self._health_threshold = health_threshold
        self._mode = HealthMode.REDIS_CONSUMER

        logger.info(
            f"RedisHealthConsumer initialized (stateless mode) with threshold: "
            f"{health_threshold}"
        )

    @property
    def is_running(self) -> bool:
        """Health consumer is always 'running' - it's stateless."""
        return True

    def start(self) -> None:
        """No-op: Redis consumer is stateless and doesn't need to be started."""
        logger.debug(
            "RedisHealthConsumer.start() called (no-op for stateless consumer)"
        )

    def stop(self) -> None:
        """No-op: Redis consumer is stateless and doesn't need to be stopped."""
        logger.debug("RedisHealthConsumer.stop() called (no-op for stateless consumer)")

    def get_healthy_partitions(self, topic: str) -> list[int]:
        """
        Get list of healthy partitions for a topic by querying Redis directly.

        This method is called on EVERY produce() operation and queries Redis
        directly for the freshest health data. No caching is performed.

        Args:
            topic: Kafka topic name

        Returns:
            List of healthy partition IDs based on current Redis data
        """
        try:
            # Direct Redis query - no caching
            health_data = self._redis_cache.get_health_data(topic)

            if health_data:
                # Calculate healthy partitions based on threshold
                healthy_partitions = [
                    partition_id
                    for partition_id, health_score in health_data.items()
                    if health_score >= self._health_threshold
                ]

                logger.debug(
                    f"Redis query for {topic}: "
                    f"{len(healthy_partitions)}/{len(health_data)} healthy partitions"
                )

                return healthy_partitions
            else:
                logger.debug(f"No health data found in Redis for topic {topic}")
                return []

        except Exception as e:
            logger.warning(f"Redis query failed for {topic}: {e}")
            return []  # Fallback to default partitioning

    def force_refresh(self, topic: str) -> None:
        """
        No-op for stateless consumer (data is always fresh).

        Since we query Redis directly on every call, there's no cached data
        to refresh. This method exists for compatibility with other health managers.
        """
        logger.debug(
            f"RedisHealthConsumer.force_refresh({topic}) - no-op for stateless consumer"
        )

    def force_refresh_threadsafe(self, topic: str) -> None:
        """Thread-safe alias for force_refresh (compatibility)."""
        self.force_refresh(topic)


class HybridRedisHealthConsumer:
    """
    Stateless Redis health consumer that works with HybridCache instances.

    This is a wrapper around RedisHealthConsumer that extracts the Redis
    component from a HybridCache for direct health data queries.
    """

    def __init__(
        self,
        hybrid_cache: "DefaultHybridCache",
        health_threshold: float = 0.5,
    ):
        """
        Initialize hybrid Redis health consumer.

        Args:
            hybrid_cache: Hybrid cache instance (must have Redis remote cache)
            health_threshold: Minimum health score to consider partition healthy
        """
        if not hasattr(hybrid_cache, "_remote"):
            raise ValueError("HybridCache must have a remote cache component")

        # Extract Redis cache from hybrid cache
        redis_cache = hybrid_cache._remote

        # Create underlying stateless Redis health consumer
        self._redis_health_consumer = RedisHealthConsumer(
            redis_cache=redis_cache,
            health_threshold=health_threshold,
        )

    @property
    def is_running(self) -> bool:
        """Health consumer is always 'running' - it's stateless."""
        return self._redis_health_consumer.is_running

    def start(self) -> None:
        """No-op: Redis consumer is stateless."""
        self._redis_health_consumer.start()

    def stop(self) -> None:
        """No-op: Redis consumer is stateless."""
        self._redis_health_consumer.stop()

    def get_healthy_partitions(self, topic: str) -> list[int]:
        """Query Redis directly for healthy partitions."""
        return self._redis_health_consumer.get_healthy_partitions(topic)

    def force_refresh(self, topic: str) -> None:
        """No-op for stateless consumer."""
        self._redis_health_consumer.force_refresh(topic)

    def force_refresh_threadsafe(self, topic: str) -> None:
        """Thread-safe alias for force_refresh."""
        self._redis_health_consumer.force_refresh_threadsafe(topic)
