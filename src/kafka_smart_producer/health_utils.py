"""
Shared utility functions for health calculation and data processing.
"""

import logging
from typing import TYPE_CHECKING, Any, Optional, Union

if TYPE_CHECKING:
    from .caching import DefaultHybridCache, DefaultLocalCache, DefaultRemoteCache
    from .health_config import HealthManagerConfig
    from .protocols import LagDataCollector

CacheBackend = Union["DefaultLocalCache", "DefaultRemoteCache", "DefaultHybridCache"]

logger = logging.getLogger(__name__)


def calculate_health_scores(
    lag_data: dict[int, int], max_lag_for_health: int
) -> dict[int, float]:
    """
    Calculate health scores from lag data using linear scaling.

    Args:
        lag_data: Dict mapping partition_id -> lag_count
        max_lag_for_health: Maximum lag for 0.0 health score

    Returns:
        Dict mapping partition_id -> health_score (0.0-1.0)
    """
    health_scores = {}

    for partition_id, lag_count in lag_data.items():
        if lag_count <= 0:
            health_score = 1.0
        elif lag_count >= max_lag_for_health:
            health_score = 0.0
        else:
            # Linear interpolation between 1.0 and 0.0
            health_score = 1.0 - (lag_count / max_lag_for_health)

        health_scores[partition_id] = health_score

    return health_scores


def filter_healthy_partitions(
    health_scores: dict[int, float], health_threshold: float
) -> list[int]:
    """
    Filter partitions that meet the health threshold.

    Args:
        health_scores: Dict mapping partition_id -> health_score
        health_threshold: Minimum health score to consider healthy

    Returns:
        List of healthy partition IDs
    """
    return [
        partition_id
        for partition_id, health_score in health_scores.items()
        if health_score >= health_threshold
    ]


def collect_and_calculate_health(
    lag_collector: "LagDataCollector", topic: str, max_lag_for_health: int
) -> dict[int, float]:
    """
    Collect lag data and calculate health scores for a topic.

    Args:
        lag_collector: LagDataCollector instance
        topic: Kafka topic name
        max_lag_for_health: Maximum lag for health calculation

    Returns:
        Dict mapping partition_id -> health_score (0.0-1.0)

    Raises:
        LagDataUnavailableError: When lag data cannot be collected
        HealthCalculationError: When health calculation fails
    """
    try:
        # Collect lag data using sync interface
        lag_data = lag_collector.get_lag_data(topic)

        if not lag_data:
            logger.warning(f"No lag data available for topic '{topic}'")
            return {}

        # Calculate health scores
        health_scores = calculate_health_scores(lag_data, max_lag_for_health)

        logger.debug(f"Calculated health scores for '{topic}': {health_scores}")
        return health_scores

    except Exception as e:
        from .exceptions import HealthCalculationError

        raise HealthCalculationError(
            f"Failed to calculate health scores for topic '{topic}'",
            cause=e,
            context={"topic": topic},
        ) from e


def create_lag_collector_from_config(
    health_config: "HealthManagerConfig", kafka_config: dict[str, Any]
) -> "LagDataCollector":
    """
    Create lag collector from configuration.

    Args:
        health_config: HealthManagerConfig instance
        kafka_config: Kafka configuration

    Returns:
        Configured LagDataCollector instance
    """
    consumer_group = health_config.consumer_group
    timeout_seconds = health_config.timeout_seconds

    try:
        from .lag_collector import KafkaAdminLagCollector

        bootstrap_servers = kafka_config.get("bootstrap.servers")
        if not bootstrap_servers:
            raise ValueError("bootstrap.servers is required in Kafka configuration")

        # Create lag collector with producer's Kafka settings
        lag_collector = KafkaAdminLagCollector(
            bootstrap_servers=bootstrap_servers,
            consumer_group=consumer_group,
            timeout_seconds=timeout_seconds,
            # Pass through any additional Kafka config (security, etc.)
            **{
                k: v
                for k, v in kafka_config.items()
                if k not in ["bootstrap.servers", "consumer_group"]
            },
        )

        return lag_collector

    except Exception as e:
        raise RuntimeError(f"Failed to create lag collector: {e}") from e


def create_cache_from_config(
    health_config: "HealthManagerConfig",
) -> Optional[CacheBackend]:
    """
    Create cache backend from HealthManagerConfig.

    Args:
        health_config: HealthManagerConfig instance

    Returns:
        Configured cache backend or None if disabled
    """
    if not health_config.cache_enabled:
        return None

    cache_max_size = health_config.cache_max_size
    cache_ttl_seconds = health_config.cache_ttl_seconds

    try:
        from .caching import CacheFactory

        cache_config = {
            "cache_max_size": cache_max_size,
            "cache_ttl_ms": cache_ttl_seconds * 1000,
        }
        return CacheFactory.create_local_cache(cache_config)

    except Exception as e:
        logger.warning(f"Failed to create cache, continuing without cache: {e}")
        return None
