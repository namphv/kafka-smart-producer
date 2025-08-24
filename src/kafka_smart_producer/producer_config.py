"""
Unified configuration for Smart Producers.

This module provides a single configuration interface that acts as a facade
for all component configurations (Cache, HealthManager) while maintaining
backward compatibility with dict-based configs.
"""

from dataclasses import dataclass, field
from typing import Any, Optional

from .caching import CacheConfig
from .health_config import HealthManagerConfig


@dataclass
class SmartProducerConfig:
    """
    Unified configuration facade for SmartProducer and AsyncSmartProducer.

    Acts as the main entry point for all configuration. Internally creates
    typed configuration objects for components while maintaining a simple
    user interface with dict-based configuration.

    Example:
        # Simple configuration
        config = SmartProducerConfig(
            kafka_config={'bootstrap.servers': 'localhost:9092'},
            topics=['orders']
        )

        # With health manager and custom cache settings
        config = SmartProducerConfig(
            kafka_config={'bootstrap.servers': 'localhost:9092'},
            topics=['orders'],
            health_manager={'consumer_group': 'my-group'},
            cache={'local_max_size': 2000, 'remote_enabled': True}
        )
    """

    # Required configuration
    kafka_config: dict[str, Any]
    topics: list[str]

    # Optional simplified health monitoring configuration
    consumer_group: Optional[str] = None

    # Optional component configurations (user-friendly dict format)
    health_manager: Optional[dict[str, Any]] = None
    cache: Optional[dict[str, Any]] = None

    # Producer-level settings with defaults
    smart_enabled: bool = True
    key_stickiness: bool = True

    # Internal composed configuration objects (created automatically)
    _cache_config: CacheConfig = field(init=False)
    _health_config: Optional[HealthManagerConfig] = field(init=False, default=None)

    def __post_init__(self) -> None:
        """Create internal configuration objects and validate."""
        # Validate required fields
        if not isinstance(self.kafka_config, dict):
            raise ValueError("kafka_config must be a dictionary")

        if not self.topics or not isinstance(self.topics, list):
            raise ValueError("topics must be a non-empty list")

        if not all(isinstance(topic, str) for topic in self.topics):
            raise ValueError("All topics must be strings")

        # Create cache config (always present)
        self._cache_config = self._create_cache_config()

        # Create health config (priority: explicit health_manager, then consumer_group)
        if self.health_manager:
            self._health_config = self._create_health_config()
        elif self.consumer_group:
            self._health_config = self._create_health_config_from_consumer_group()

    def _create_cache_config(self) -> CacheConfig:
        """Create CacheConfig from user cache dict or defaults."""
        cache_dict = self.cache or {}

        return CacheConfig(
            local_max_size=cache_dict.get("local_max_size", 1000),
            local_default_ttl_seconds=cache_dict.get("local_ttl_seconds", 300.0),
            remote_enabled=cache_dict.get("remote_enabled", False),
            remote_default_ttl_seconds=cache_dict.get("remote_ttl_seconds", 900.0),
            redis_host=cache_dict.get("redis_host", "localhost"),
            redis_port=cache_dict.get("redis_port", 6379),
            redis_db=cache_dict.get("redis_db", 0),
            redis_password=cache_dict.get("redis_password"),
            redis_ssl_enabled=cache_dict.get("redis_ssl_enabled", False),
            redis_ssl_cert_reqs=cache_dict.get("redis_ssl_cert_reqs", "required"),
            redis_ssl_ca_certs=cache_dict.get("redis_ssl_ca_certs"),
            redis_ssl_certfile=cache_dict.get("redis_ssl_certfile"),
            redis_ssl_keyfile=cache_dict.get("redis_ssl_keyfile"),
        )

    def _create_health_config(self) -> HealthManagerConfig:
        """Create HealthManagerConfig from user health_manager dict."""
        if not self.health_manager:
            raise ValueError(
                "health_manager dict is required to create HealthManagerConfig"
            )

        hm_dict = self.health_manager

        # consumer_group is required
        if "consumer_group" not in hm_dict:
            raise ValueError("health_manager must include 'consumer_group'")

        return HealthManagerConfig(
            consumer_group=hm_dict["consumer_group"],
            health_threshold=hm_dict.get("health_threshold", 0.5),
            refresh_interval=hm_dict.get("refresh_interval", 30.0),
            max_lag_for_health=hm_dict.get("max_lag_for_health", 1000),
            timeout_seconds=hm_dict.get("timeout_seconds", 20.0),
            cache_enabled=hm_dict.get("cache_enabled", True),
            cache_max_size=hm_dict.get("cache_max_size", 1000),
            cache_ttl_seconds=hm_dict.get("cache_ttl_seconds", 300),
            sync_options=hm_dict.get("sync_options"),
            async_options=hm_dict.get("async_options"),
        )

    def _create_health_config_from_consumer_group(self) -> HealthManagerConfig:
        """Create HealthManagerConfig from top-level consumer_group with defaults."""
        if not self.consumer_group:
            raise ValueError("consumer_group is required to create health config")

        return HealthManagerConfig(
            consumer_group=self.consumer_group,
            health_threshold=0.5,
            refresh_interval=30.0,
            max_lag_for_health=1000,
            timeout_seconds=20.0,
            cache_enabled=True,
            cache_max_size=1000,
            cache_ttl_seconds=300,
        )

    # Public API - clean access to internal configuration objects
    @property
    def cache_config(self) -> CacheConfig:
        """Get cache configuration object."""
        return self._cache_config

    @property
    def health_config(self) -> Optional[HealthManagerConfig]:
        """Get health manager configuration object (None if not configured)."""
        return self._health_config

    def get_clean_kafka_config(self) -> dict[str, Any]:
        """
        Get clean Kafka configuration without Smart Producer specific keys.

        Returns:
            Clean Kafka configuration dict for confluent-kafka Producer
        """
        # Keys to exclude from Kafka producer config
        exclude_keys = {
            "topics",
            "health_manager",
            "cache",
            "smart_enabled",
            "key_stickiness",
            # Smart Producer specific keys that might leak into kafka_config
            "smart.partitioning.enabled",
            "smart.partitioning.key_stickiness",
            "smart.cache.mode",
            "smart.cache.ttl.seconds",
            "smart.cache.max.size",
            "smart.async.max_workers",
            # Redis config keys
            "redis_host",
            "redis_port",
            "redis_db",
            "redis_password",
            "redis_ssl_enabled",
            "redis_ssl_cert_reqs",
            "redis_ssl_ca_certs",
            "redis_ssl_certfile",
            "redis_ssl_keyfile",
            "redis_ttl_seconds",
        }

        # Filter out Smart Producer specific keys
        return {
            key: value
            for key, value in self.kafka_config.items()
            if key not in exclude_keys
        }

    @classmethod
    def from_dict(cls, config_dict: dict[str, Any]) -> "SmartProducerConfig":
        """
        Create SmartProducerConfig from a dictionary (backward compatibility).

        This enables the old usage pattern:
            config = {'bootstrap.servers': '...', 'topics': [...]}
            producer = SmartProducer(config)

        Args:
            config_dict: Configuration dictionary

        Returns:
            SmartProducerConfig instance
        """
        # Extract topics from dict or kafka_config
        topics = config_dict.get("topics")
        if not topics:
            raise ValueError("'topics' must be specified in config")

        # Build kafka_config by excluding Smart Producer specific keys
        kafka_config = {}
        smart_keys = {
            "topics",
            "consumer_group",
            "health_manager",
            "cache",
            "smart_enabled",
            "key_stickiness",
        }

        for key, value in config_dict.items():
            if key not in smart_keys:
                kafka_config[key] = value

        # Extract optional configurations
        consumer_group = config_dict.get("consumer_group")
        health_manager = config_dict.get("health_manager")
        cache = config_dict.get("cache")
        smart_enabled = config_dict.get("smart_enabled", True)
        key_stickiness = config_dict.get("key_stickiness", True)

        return cls(
            kafka_config=kafka_config,
            topics=topics,
            consumer_group=consumer_group,
            health_manager=health_manager,
            cache=cache,
            smart_enabled=smart_enabled,
            key_stickiness=key_stickiness,
        )

    def to_dict(self) -> dict[str, Any]:
        """
        Convert SmartProducerConfig back to dictionary format.

        Useful for serialization or backward compatibility.

        Returns:
            Dictionary representation
        """
        result = self.kafka_config.copy()
        result.update(
            {
                "topics": self.topics,
                "smart_enabled": self.smart_enabled,
                "key_stickiness": self.key_stickiness,
            }
        )

        if self.consumer_group:
            result["consumer_group"] = self.consumer_group

        if self.health_manager:
            result["health_manager"] = self.health_manager

        if self.cache:
            result["cache"] = self.cache

        return result

    def __repr__(self) -> str:
        """String representation for debugging."""
        return (
            f"SmartProducerConfig("
            f"topics={self.topics}, "
            f"smart_enabled={self.smart_enabled}, "
            f"cache_config={self._cache_config}, "
            f"health_config={'configured' if self._health_config else 'none'}"
            f")"
        )
