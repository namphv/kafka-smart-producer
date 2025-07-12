"""
Shared utilities for Smart Producers.

This module contains common functionality used by both sync and async producers.
"""

import logging
import random
from typing import TYPE_CHECKING, Optional, Union

from .caching import CacheFactory

if TYPE_CHECKING:
    from .async_health_manager import AsyncHealthManager
    from .caching import DefaultHybridCache, DefaultLocalCache, DefaultRemoteCache
    from .producer_config import ProducerConfig
    from .sync_health_manager import SyncHealthManager

# Type aliases
HealthManagerType = Union["SyncHealthManager", "AsyncHealthManager"]
CacheType = Union["DefaultLocalCache", "DefaultRemoteCache", "DefaultHybridCache"]

logger = logging.getLogger(__name__)


def create_cache_from_config(config: "ProducerConfig") -> Optional[CacheType]:
    """
    Create cache instance based on ProducerConfig.

    Args:
        config: Producer configuration

    Returns:
        Cache instance or None

    Raises:
        RuntimeError: If cache creation fails
        ValueError: If cache mode is invalid
    """
    cache_config = config.cache_config

    # Determine cache mode based on config
    if cache_config.remote_enabled:
        cache_mode = "hybrid"
    else:
        cache_mode = "local"

    if cache_mode == "local":
        logger.debug("Creating local cache")
        return CacheFactory.create_local_cache(
            {
                "cache_max_size": cache_config.local_max_size,
                "cache_ttl_ms": int(cache_config.local_default_ttl_seconds * 1000),
            }
        )

    elif cache_mode == "remote":
        logger.debug("Creating remote cache")
        legacy_config = {
            "redis_host": cache_config.redis_host,
            "redis_port": cache_config.redis_port,
            "redis_db": cache_config.redis_db,
            "redis_password": cache_config.redis_password,
            "redis_ssl_enabled": cache_config.redis_ssl_enabled,
            "redis_ttl_seconds": int(cache_config.remote_default_ttl_seconds),
        }
        remote_cache = CacheFactory.create_remote_cache(legacy_config)
        if remote_cache is None:
            raise RuntimeError(
                f"Failed to create remote cache. Check Redis configuration: "
                f"host={cache_config.redis_host}, "
                f"port={cache_config.redis_port}"
            )
        return remote_cache

    elif cache_mode == "hybrid":
        logger.debug("Creating hybrid cache")
        legacy_config = {
            "cache_max_size": cache_config.local_max_size,
            "cache_ttl_ms": int(cache_config.local_default_ttl_seconds * 1000),
            "redis_host": cache_config.redis_host,
            "redis_port": cache_config.redis_port,
            "redis_db": cache_config.redis_db,
            "redis_password": cache_config.redis_password,
            "redis_ssl_enabled": cache_config.redis_ssl_enabled,
            "redis_ttl_seconds": int(cache_config.remote_default_ttl_seconds),
        }
        hybrid_cache = CacheFactory.create_hybrid_cache(
            legacy_config, enable_redis=True
        )
        if hybrid_cache is None:
            raise RuntimeError(
                "Failed to create hybrid cache. Check Redis configuration."
            )
        return hybrid_cache

    else:
        raise ValueError(
            f"Invalid cache mode: {cache_mode}. Use 'local', 'remote', or 'hybrid'"
        )


def create_health_manager_from_config(
    config: "ProducerConfig", manager_type: str = "sync"
) -> Optional[HealthManagerType]:
    """
    Create health manager from ProducerConfig.

    Args:
        config: Producer configuration
        manager_type: "sync" or "async"

    Returns:
        Health manager instance or None

    Raises:
        RuntimeError: If health manager creation fails
    """
    health_config = config.health_config
    if not health_config:
        return None

    try:
        from .lag_collector import KafkaAdminLagCollector

        # health_config is already a HealthManagerConfig
        health_manager_config = health_config

        # Create lag collector
        kafka_config = config.get_clean_kafka_config()
        lag_collector = KafkaAdminLagCollector(
            bootstrap_servers=kafka_config.get("bootstrap.servers", "localhost:9092"),
            consumer_group=health_manager_config.consumer_group,
            **{k: v for k, v in kafka_config.items() if k != "bootstrap.servers"},
        )

        # Create appropriate health manager
        if manager_type == "sync":
            from .sync_health_manager import SyncHealthManager

            health_manager = SyncHealthManager.embedded(
                lag_collector, topics=config.topics
            )
            logger.info(
                f"Created SyncHealthManager for embedded mode with "
                f"topics: {config.topics}"
            )
            return health_manager
        else:
            from .async_health_manager import AsyncHealthManager

            health_manager = AsyncHealthManager.embedded(
                lag_collector, topics=config.topics
            )
            logger.info(
                f"Created AsyncHealthManager for embedded mode with "
                f"topics: {config.topics}"
            )
            return health_manager

    except Exception as e:
        logger.error(f"Failed to create {manager_type} health manager from config: {e}")
        raise RuntimeError(f"Health manager creation failed: {e}") from e


class BasePartitionSelector:
    """
    Base class for partition selection logic.

    This class encapsulates the smart partition selection functionality
    that is shared between sync and async producers.
    """

    def __init__(
        self,
        health_manager: Optional[HealthManagerType],
        cache: Optional[CacheType],
        use_key_stickiness: bool = True,
    ):
        """
        Initialize partition selector.

        Args:
            health_manager: Health manager for partition health queries
            cache: Cache for key stickiness
            use_key_stickiness: Whether to use key stickiness
        """
        self._health_manager = health_manager
        self._cache = cache
        self._use_key_stickiness = use_key_stickiness

    def select_partition(self, topic: str, key: Optional[bytes]) -> Optional[int]:
        """
        Select partition for a message using improved logic flow:

        1. Get selected_partition if smart_enabled (from healthy partitions)
        2. Apply key stickiness logic:
           - If key + key_stickiness: check cache first, fallback to selected_partition
           - If key + no key_stickiness: use selected_partition directly
           - If no key: use selected_partition

        Args:
            topic: Kafka topic name
            key: Message key

        Returns:
            Selected partition ID or None for default partitioning
        """
        try:
            # Step 1: Get selected_partition if smart partitioning enabled
            selected_partition = self._get_selected_partition(topic)

            # Step 2: Apply partition selection logic
            if key and self._use_key_stickiness:
                # Check cache first for key stickiness
                cached_partition = self._get_cached_partition(topic, key)
                if cached_partition is not None:
                    return cached_partition
                else:
                    # Cache miss - use selected_partition if available and cache it
                    if selected_partition is not None:
                        self._cache_partition(topic, key, selected_partition)
                        return selected_partition
                    # If no selected_partition, let Kafka handle default partitioning
                    return None

            elif key and not self._use_key_stickiness:
                # Key exists but no stickiness - just use selected_partition
                return selected_partition

            else:
                # No key - use selected_partition if available
                return selected_partition

        except Exception as e:
            logger.warning(f"Smart partition selection failed for topic {topic}: {e}")
            return None

    def _get_selected_partition(self, topic: str) -> Optional[int]:
        """Get a healthy partition if smart partitioning is enabled."""
        if not self._health_manager:
            return None

        try:
            healthy_partitions = self._health_manager.get_healthy_partitions(topic)
            if healthy_partitions:
                return random.choice(healthy_partitions)

            # If no healthy partitions yet, trigger immediate refresh
            # (in background - don't block produce call)
            try:
                if hasattr(self._health_manager, "force_refresh_threadsafe"):
                    self._health_manager.force_refresh_threadsafe(topic)
                else:
                    self._health_manager.force_refresh(topic)
            except Exception as e:
                logger.debug(f"Health refresh failed for topic {topic}: {e}")
                # Don't let refresh errors block produce

        except Exception as e:
            logger.debug(f"Health manager selection failed for topic {topic}: {e}")

        return None

    def _get_cached_partition(self, topic: str, key: bytes) -> Optional[int]:
        """Get cached partition for a topic-key combination."""
        if not self._cache:
            return None

        cache_key = f"{topic}:{key.decode('utf-8', errors='replace')}"
        try:
            cached_partition = self._cache.get(cache_key)
            if cached_partition is not None:
                return int(cached_partition)
        except Exception as e:
            logger.debug(f"Cache get failed for key {cache_key}: {e}")
        return None

    def _cache_partition(self, topic: str, key: bytes, partition: int) -> None:
        """Cache a partition for a topic-key combination."""
        if not self._cache:
            return

        cache_key = f"{topic}:{key.decode('utf-8', errors='replace')}"
        try:
            self._cache.set(cache_key, partition)
        except Exception as e:
            logger.debug(f"Cache set failed for key {cache_key}: {e}")
