"""
Factory module for creating pluggable components in Kafka Smart Producer.

This module provides registry-based factories for lag collectors and cache backends,
enabling the HealthManager to create components from configuration.
"""

import logging
from typing import Any, Dict, List, Type, Union

from .caching import (
    CacheFactory,
    DefaultHybridCache,
    DefaultLocalCache,
    DefaultRemoteCache,
)
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


def _prepare_legacy_cache_config(new_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Adapter function to convert new nested config to legacy flat config.

    Transforms the clean nested structure to the flat structure expected
    by the existing CacheFactory.

    Args:
        new_config: New nested configuration structure

    Returns:
        Flat configuration dict compatible with CacheFactory
    """
    settings = new_config.get("settings", {})
    legacy_config = {}

    # Map local cache settings
    if "local" in settings:
        local_settings = settings["local"]
        legacy_config["cache_max_size"] = local_settings.get("max_size", 1000)
        legacy_config["cache_ttl_ms"] = local_settings.get("ttl_ms", 300000)

    # Map redis cache settings
    if "redis" in settings:
        redis_settings = settings["redis"]
        legacy_config["redis_host"] = redis_settings.get("host", "localhost")
        legacy_config["redis_port"] = redis_settings.get("port", 6379)
        legacy_config["redis_db"] = redis_settings.get("db", 0)
        legacy_config["redis_password"] = redis_settings.get("password")
        legacy_config["redis_ssl_enabled"] = redis_settings.get("ssl", False)
        legacy_config["redis_ttl_seconds"] = redis_settings.get("ttl_seconds", 900)

        # SSL configuration if enabled
        if redis_settings.get("ssl", False):
            legacy_config["redis_ssl_cert_reqs"] = redis_settings.get(
                "ssl_cert_reqs", "required"
            )
            legacy_config["redis_ssl_ca_certs"] = redis_settings.get("ssl_ca_certs")
            legacy_config["redis_ssl_certfile"] = redis_settings.get("ssl_certfile")
            legacy_config["redis_ssl_keyfile"] = redis_settings.get("ssl_keyfile")

    return legacy_config


def create_cache_backend(config: Dict[str, Any]) -> CacheBackend:
    """
    Factory function to create a cache backend using the existing CacheFactory.

    This acts as an adapter between the new configuration structure and
    the existing CacheFactory implementation.

    Args:
        config: Configuration dict with 'type' and 'settings'
            Example:
            {
                'type': 'hybrid',
                'settings': {
                    'local': {'max_size': 1000, 'ttl_ms': 60000},
                    'redis': {'host': 'localhost', 'port': 6379, 'ttl_seconds': 300}
                }
            }

    Returns:
        Configured cache backend instance

    Raises:
        ValueError: If type is unknown or configuration is invalid
        RuntimeError: If cache creation fails (e.g., Redis unavailable)
    """
    if not config or "type" not in config:
        raise ValueError("Cache config must include a 'type' field")

    config = config.copy()  # Avoid modifying original
    cache_type = config.pop("type")

    # Transform new config to legacy format
    legacy_config = _prepare_legacy_cache_config(config)

    try:
        if cache_type == "local":
            local_cache = CacheFactory.create_local_cache(legacy_config)
            return local_cache

        elif cache_type == "redis":
            remote_cache = CacheFactory.create_remote_cache(legacy_config)
            if remote_cache is None:
                raise RuntimeError(
                    "Failed to create Redis cache - check connection and configuration"
                )
            host = legacy_config.get("redis_host", "localhost")
            port = legacy_config.get("redis_port", 6379)
            logger.info(f"Created Redis cache: {host}:{port}")
            return remote_cache

        elif cache_type == "hybrid":
            # Check if Redis settings are provided for hybrid mode
            settings = config.get("settings", {})
            if "redis" not in settings:
                raise ValueError("Hybrid cache requires 'redis' settings")

            hybrid_cache = CacheFactory.create_hybrid_cache(
                legacy_config, enable_redis=True
            )
            if hybrid_cache is None:
                raise RuntimeError(
                    "Failed to create hybrid cache - check Redis connection"
                )
            logger.info("Created hybrid cache with local + Redis backend")
            return hybrid_cache

        else:
            raise ValueError(
                f"Unknown cache type: '{cache_type}'. "
                f"Available types: ['local', 'redis', 'hybrid']"
            )

    except Exception as e:
        if isinstance(e, (ValueError, RuntimeError)):
            raise
        raise RuntimeError(f"Failed to create {cache_type} cache") from e


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
