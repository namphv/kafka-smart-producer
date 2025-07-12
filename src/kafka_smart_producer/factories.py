"""
Factory module for creating pluggable components in Kafka Smart Producer.

This module provides registry-based factories for lag collectors and cache backends,
enabling the HealthManager to create components from configuration.
"""

import logging
from typing import Any, Dict, List, Type, Union

from .caching import DefaultHybridCache, DefaultLocalCache, DefaultRemoteCache
from .lag_collector import KafkaAdminLagCollector
from .protocols import LagDataCollector

logger = logging.getLogger(__name__)

# Type alias for cache backend types (using existing cache classes)
CacheBackend = Union[DefaultLocalCache, DefaultRemoteCache, DefaultHybridCache]

# --- Lag Collector Registry ---
_LAG_COLLECTOR_REGISTRY: Dict[str, Type[LagDataCollector]] = {
    "kafka_admin": KafkaAdminLagCollector,
    # Future collectors can be registered here:
    # "prometheus": PrometheusLagCollector,
    # "redis": RedisLagCollector,
}


def register_lag_collector(name: str, collector_class: Type[LagDataCollector]) -> None:
    """
    Register a custom lag collector implementation.

    This allows users to add their own custom lag collectors to the system.

    Args:
        name: Unique name for the collector (used in config 'type' field)
        collector_class: Class implementing LagDataCollector protocol

    Raises:
        ValueError: If name is already registered
    """
    if name in _LAG_COLLECTOR_REGISTRY:
        raise ValueError(f"Lag collector '{name}' is already registered")

    _LAG_COLLECTOR_REGISTRY[name] = collector_class
    logger.info(f"Registered lag collector: {name} -> {collector_class.__name__}")


def create_lag_collector(config: Dict[str, Any]) -> LagDataCollector:
    """
    Factory function to create a LagDataCollector instance from configuration.

    Args:
        config: Configuration dict with 'type' and optional 'settings'
            Example:
            {
                'type': 'kafka_admin',
                'settings': {
                    'bootstrap_servers': 'localhost:9092',
                    'consumer_group': 'my-group',
                    'timeout_seconds': 5.0
                }
            }

    Returns:
        Configured LagDataCollector instance

    Raises:
        ValueError: If type is unknown or configuration is invalid
        KeyError: If required configuration is missing
    """
    if not config or "type" not in config:
        raise ValueError("Lag collector config must include a 'type' field")

    config = config.copy()  # Avoid modifying original
    collector_type = config.pop("type")
    settings = config.get("settings", {})

    try:
        collector_class = _LAG_COLLECTOR_REGISTRY[collector_type]
    except KeyError as e:
        available_types = list(_LAG_COLLECTOR_REGISTRY.keys())
        raise ValueError(
            f"Unknown lag collector type: '{collector_type}'. "
            f"Available types: {available_types}"
        ) from e

    try:
        return collector_class(**settings)
    except Exception as e:
        raise ValueError(
            f"Failed to create lag collector '{collector_type}' \
                with settings {settings}"
        ) from e


# --- Cache Backend Factory & Adapter ---


def get_available_lag_collectors() -> Dict[str, Type[LagDataCollector]]:
    """
    Get all registered lag collector types.

    Returns:
        Dict mapping collector names to their classes
    """
    return _LAG_COLLECTOR_REGISTRY.copy()


def get_available_cache_types() -> List[str]:
    """
    Get all available cache backend types.

    Returns:
        List of available cache type names
    """
    return ["local", "redis", "hybrid"]
