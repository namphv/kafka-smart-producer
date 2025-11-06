"""
Factory functions for creating health monitor instances.

This module provides factory functions for creating both sync and async
health monitors with different configurations (embedded, standalone, from_config).
"""

import logging
import os
from typing import TYPE_CHECKING, Any, Optional

from .health_mode import HealthMode
from .health_utils import (
    create_cache_from_config,
    create_lag_collector_from_config,
)

if TYPE_CHECKING:
    from .async_partition_health_monitor import AsyncPartitionHealthMonitor
    from .health_config import PartitionHealthMonitorConfig
    from .partition_health_monitor import PartitionHealthMonitor
    from .protocols import LagDataCollector

logger = logging.getLogger(__name__)


# Sync Monitor Factories


def create_sync_embedded_monitor(
    lag_collector: "LagDataCollector", topics: Optional[list[str]] = None
) -> "PartitionHealthMonitor":
    """
    Create PartitionHealthMonitor for embedded mode (producer integration).

    This factory method creates a lightweight health manager optimized for
    integration with SmartProducer. No Redis publishing, minimal features.

    Args:
        lag_collector: Lag data collector instance
        topics: Optional list of topics to monitor initially

    Returns:
        PartitionHealthMonitor configured for embedded mode

    Example:
        lag_collector = KafkaAdminLagCollector(...)
        health_manager = create_sync_embedded_monitor(
            lag_collector, ["orders", "payments"]
        )
    """
    from .partition_health_monitor import PartitionHealthMonitor

    manager = PartitionHealthMonitor(
        lag_collector=lag_collector,
        cache=None,  # No cache for embedded mode
        health_threshold=0.5,
        refresh_interval=5.0,
        max_lag_for_health=1000,
        mode=HealthMode.EMBEDDED,
        redis_health_publisher=None,  # No Redis for embedded mode
    )

    # Initialize with topics if provided
    if topics:
        manager.initialize_topics(topics)

    return manager


def create_sync_standalone_monitor(
    consumer_group: str,
    kafka_config: dict[str, Any],
    topics: Optional[list[str]] = None,
    health_threshold: float = 0.5,
    refresh_interval: float = 5.0,
    max_lag_for_health: int = 1000,
) -> "PartitionHealthMonitor":
    """
    Create PartitionHealthMonitor for standalone mode (monitoring service).

    This factory method creates a full-featured health manager for running
    as an independent monitoring service with Redis publishing.

    Args:
        consumer_group: Kafka consumer group to monitor
        kafka_config: Kafka configuration (bootstrap.servers, security, etc.)
        topics: Optional list of topics to monitor initially
        health_threshold: Minimum health score for healthy partitions
        refresh_interval: Seconds between health refreshes
        max_lag_for_health: Maximum lag for 0.0 health score

    Returns:
        PartitionHealthMonitor configured for standalone mode

    Example:
        health_manager = create_sync_standalone_monitor(
            consumer_group="my-consumers",
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["orders", "payments"]
        )
    """
    from .cache_factory import CacheFactory
    from .lag_collector import KafkaAdminLagCollector
    from .partition_health_monitor import PartitionHealthMonitor

    # Create lag collector
    lag_collector = KafkaAdminLagCollector(
        bootstrap_servers=kafka_config.get("bootstrap.servers", "localhost:9092"),
        consumer_group=consumer_group,
        **{k: v for k, v in kafka_config.items() if k != "bootstrap.servers"},
    )

    # Create Redis publisher for standalone mode
    redis_publisher = None
    try:
        # Try to create Redis publisher with default config
        redis_config = {
            "redis_host": os.getenv("REDIS_HOST", "localhost"),
            "redis_port": int(os.getenv("REDIS_PORT", "6379")),
            "redis_db": int(os.getenv("REDIS_DB", "0")),
        }
        redis_publisher = CacheFactory.create_remote_cache(redis_config)
        if redis_publisher:
            logger.info("Redis health publisher enabled for standalone mode")
    except Exception as e:
        logger.warning(
            f"Failed to create Redis publisher: {e}. Continuing without Redis."
        )

    # Create health manager
    manager = PartitionHealthMonitor(
        lag_collector=lag_collector,
        cache=None,  # No additional cache needed
        health_threshold=health_threshold,
        refresh_interval=refresh_interval,
        max_lag_for_health=max_lag_for_health,
        mode=HealthMode.STANDALONE,
        redis_health_publisher=redis_publisher,
    )

    # Initialize with topics if provided
    if topics:
        manager.initialize_topics(topics)

    return manager


def create_sync_monitor_from_config(
    health_config: "PartitionHealthMonitorConfig", kafka_config: dict[str, Any]
) -> "PartitionHealthMonitor":
    """
    Factory method to create PartitionHealthMonitor from unified configuration.

    Args:
        health_config: Health manager configuration
        kafka_config: Producer's Kafka configuration (kafka authentication config)

    Returns:
        Configured PartitionHealthMonitor instance

    Raises:
        ValueError: If configuration is invalid
        RuntimeError: If component creation fails
    """
    from .cache_factory import CacheFactory
    from .health_config import PartitionHealthMonitorConfig
    from .partition_health_monitor import PartitionHealthMonitor

    if not isinstance(kafka_config, dict):
        raise ValueError("Kafka configuration must be a dictionary")

    if not isinstance(health_config, PartitionHealthMonitorConfig):
        raise ValueError(
            "Health configuration must be a PartitionHealthMonitorConfig instance"
        )

    # Extract common settings (already validated by PartitionHealthMonitorConfig)
    health_threshold = health_config.health_threshold
    refresh_interval = health_config.refresh_interval
    max_lag_for_health = health_config.max_lag_for_health

    # Create components using helper functions
    lag_collector = create_lag_collector_from_config(health_config, kafka_config)
    cache = create_cache_from_config(health_config)

    # Determine operation mode and Redis publisher
    mode_str = health_config.get_sync_option("mode", "standalone")
    mode = (
        HealthMode.from_string(mode_str) if isinstance(mode_str, str) else mode_str
    )
    redis_publisher = None

    if mode == HealthMode.STANDALONE:
        # Create Redis publisher for standalone mode
        # Use cache config for Redis connection if available
        if hasattr(health_config, "cache") and health_config.cache:
            redis_publisher = CacheFactory.create_remote_cache(
                health_config.cache.__dict__
            )
            if redis_publisher:
                logger.info("Redis health publisher enabled for standalone mode")
            else:
                logger.warning(
                    "Failed to create Redis publisher, continuing without Redis"
                )

    return PartitionHealthMonitor(
        lag_collector=lag_collector,
        cache=cache,
        health_threshold=health_threshold,
        refresh_interval=refresh_interval,
        max_lag_for_health=max_lag_for_health,
        mode=mode,
        redis_health_publisher=redis_publisher,
    )


# Async Monitor Factories


async def create_async_embedded_monitor(
    lag_collector: "LagDataCollector", topics: Optional[list[str]] = None
) -> "AsyncPartitionHealthMonitor":
    """
    Create AsyncPartitionHealthMonitor for embedded mode (producer integration).

    This factory method creates a lightweight health manager optimized for
    integration with AsyncSmartProducer. No Redis publishing, minimal features.

    Args:
        lag_collector: Lag data collector instance
        topics: Optional list of topics to monitor initially

    Returns:
        AsyncPartitionHealthMonitor configured for embedded mode

    Example:
        lag_collector = KafkaAdminLagCollector(...)
        health_manager = await create_async_embedded_monitor(
            lag_collector, ["orders", "payments"]
        )
    """
    from .async_partition_health_monitor import AsyncPartitionHealthMonitor

    manager = AsyncPartitionHealthMonitor(
        lag_collector=lag_collector,
        cache=None,  # No cache for embedded mode
        health_threshold=0.5,
        refresh_interval=5.0,
        max_lag_for_health=1000,
        mode=HealthMode.EMBEDDED,
        redis_health_publisher=None,  # No Redis for embedded mode
    )

    # Initialize with topics if provided
    if topics:
        manager.initialize_topics(topics)

    return manager


async def create_async_standalone_monitor(
    consumer_group: str,
    kafka_config: dict[str, Any],
    topics: Optional[list[str]] = None,
    health_threshold: float = 0.5,
    refresh_interval: float = 5.0,
    max_lag_for_health: int = 1000,
) -> "AsyncPartitionHealthMonitor":
    """
    Create AsyncPartitionHealthMonitor for standalone mode (monitoring service).

    This factory method creates a full-featured health manager for running
    as an independent monitoring service with Redis publishing and health streams.

    Args:
        consumer_group: Kafka consumer group to monitor
        kafka_config: Kafka configuration (bootstrap.servers, security, etc.)
        topics: Optional list of topics to monitor initially
        health_threshold: Minimum health score for healthy partitions
        refresh_interval: Seconds between health refreshes
        max_lag_for_health: Maximum lag for 0.0 health score

    Returns:
        AsyncPartitionHealthMonitor configured for standalone mode

    Example:
        health_manager = await create_async_standalone_monitor(
            consumer_group="my-consumers",
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["orders", "payments"]
        )
    """
    from .async_partition_health_monitor import AsyncPartitionHealthMonitor
    from .cache_factory import CacheFactory
    from .lag_collector import KafkaAdminLagCollector

    # Create lag collector
    lag_collector = KafkaAdminLagCollector(
        bootstrap_servers=kafka_config.get("bootstrap.servers", "localhost:9092"),
        consumer_group=consumer_group,
        **{k: v for k, v in kafka_config.items() if k != "bootstrap.servers"},
    )

    # Create Redis publisher for standalone mode
    redis_publisher = None
    try:
        # Try to create Redis publisher with default config
        redis_config = {
            "redis_host": os.getenv("REDIS_HOST", "localhost"),
            "redis_port": int(os.getenv("REDIS_PORT", "6379")),
            "redis_db": int(os.getenv("REDIS_DB", "0")),
        }
        redis_publisher = CacheFactory.create_remote_cache(redis_config)
        if redis_publisher:
            logger.info("Redis health publisher enabled for standalone mode")
    except Exception as e:
        logger.warning(
            f"Failed to create Redis publisher: {e}. Continuing without Redis."
        )

    # Create health manager
    manager = AsyncPartitionHealthMonitor(
        lag_collector=lag_collector,
        cache=None,  # No additional cache needed
        health_threshold=health_threshold,
        refresh_interval=refresh_interval,
        max_lag_for_health=max_lag_for_health,
        mode=HealthMode.STANDALONE,
        redis_health_publisher=redis_publisher,
    )

    # Initialize with topics if provided
    if topics:
        manager.initialize_topics(topics)

    return manager


def create_async_monitor_from_config(
    health_config: "PartitionHealthMonitorConfig", kafka_config: dict[str, Any]
) -> "AsyncPartitionHealthMonitor":
    """
    Factory method to create AsyncPartitionHealthMonitor from unified configuration.

    Args:
        health_config: Health manager configuration
        kafka_config: Producer's Kafka configuration (bootstrap.servers,
                      security, etc.)

    Returns:
        Configured AsyncPartitionHealthMonitor instance

    Raises:
        ValueError: If configuration is invalid
        RuntimeError: If component creation fails
    """
    from .async_partition_health_monitor import AsyncPartitionHealthMonitor
    from .cache_factory import CacheFactory
    from .health_config import PartitionHealthMonitorConfig

    if not isinstance(kafka_config, dict):
        raise ValueError("Kafka configuration must be a dictionary")

    if not isinstance(health_config, PartitionHealthMonitorConfig):
        raise ValueError(
            "Health configuration must be a PartitionHealthMonitorConfig instance"
        )

    # Extract common settings (already validated by PartitionHealthMonitorConfig)
    health_threshold = health_config.health_threshold
    refresh_interval = health_config.refresh_interval
    max_lag_for_health = health_config.max_lag_for_health

    # Create components using helper functions
    lag_collector = create_lag_collector_from_config(health_config, kafka_config)
    cache = create_cache_from_config(health_config)

    # Determine operation mode and Redis publisher
    mode_str = health_config.get_async_option("mode", "standalone")
    mode = (
        HealthMode.from_string(mode_str) if isinstance(mode_str, str) else mode_str
    )
    redis_publisher = None

    if mode == HealthMode.STANDALONE:
        # Create Redis publisher for standalone mode
        # Use cache config for Redis connection if available
        if hasattr(health_config, "cache") and health_config.cache:
            redis_publisher = CacheFactory.create_remote_cache(
                health_config.cache.__dict__
            )
            if redis_publisher:
                logger.info("Redis health publisher enabled for standalone mode")
            else:
                logger.warning(
                    "Failed to create Redis publisher, continuing without Redis"
                )

    # Log async capability detection
    is_async_native = hasattr(lag_collector, "get_lag_data_async")
    logger.info(
        f"AsyncPartitionHealthMonitor mode: {mode}, lag collector: "
        f"{'async-native' if is_async_native else 'executor-wrapped'}"
    )

    return AsyncPartitionHealthMonitor(
        lag_collector=lag_collector,
        cache=cache,
        health_threshold=health_threshold,
        refresh_interval=refresh_interval,
        max_lag_for_health=max_lag_for_health,
        mode=mode,
        redis_health_publisher=redis_publisher,
    )
