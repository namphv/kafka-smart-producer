"""Unified configuration for Smart Producers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SmartProducerConfig:
    """
    Configuration for SmartProducer and AsyncSmartProducer.

    Examples:
        # Minimal (scenario 1/2 - local cache only)
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["orders"],
            consumer_group="my-consumers",
        )

        # With Redis (scenario 3/4 - distributed cache)
        config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": "localhost:9092"},
            topics=["orders"],
            consumer_group="my-consumers",
            redis_url="redis://localhost:6379/0",
        )
    """

    # Required
    kafka_config: dict[str, Any]
    topics: list[str]

    # Health monitoring (None = smart routing disabled)
    consumer_group: str | None = None

    # Health tuning
    health_threshold: float = 0.5
    refresh_interval: float = 30.0
    max_lag: int = 1000
    collector_timeout: float = 10.0

    # Cache settings
    cache_max_size: int = 1000
    cache_ttl: float = 300.0

    # Redis (None = local cache only)
    redis_url: str | None = None
    redis_ttl: float = 900.0

    # Feature flags
    smart_enabled: bool = True
    key_stickiness: bool = True

    # Kafka config keys to strip before passing to confluent-kafka
    _strip_keys: set[str] = field(
        init=False,
        repr=False,
        default_factory=lambda: {
            "topics",
            "consumer_group",
            "smart_enabled",
            "key_stickiness",
        },
    )

    def __post_init__(self) -> None:
        if not isinstance(self.kafka_config, dict):
            raise ValueError("kafka_config must be a dict")
        if not self.topics or not isinstance(self.topics, list):
            raise ValueError("topics must be a non-empty list")
        if not all(isinstance(t, str) for t in self.topics):
            raise ValueError("all topics must be strings")
        if not 0.0 <= self.health_threshold <= 1.0:
            raise ValueError("health_threshold must be between 0.0 and 1.0")
        if self.refresh_interval <= 0:
            raise ValueError("refresh_interval must be positive")
        if self.max_lag <= 0:
            raise ValueError("max_lag must be positive")
        if self.collector_timeout <= 0:
            raise ValueError("collector_timeout must be positive")
        if self.cache_max_size <= 0:
            raise ValueError("cache_max_size must be positive")
        if self.cache_ttl <= 0:
            raise ValueError("cache_ttl must be positive")

    def get_kafka_config(self) -> dict[str, Any]:
        """Get clean Kafka config for confluent-kafka Producer."""
        return {k: v for k, v in self.kafka_config.items() if k not in self._strip_keys}

    @property
    def has_health_monitoring(self) -> bool:
        return self.smart_enabled and self.consumer_group is not None

    @property
    def has_redis(self) -> bool:
        return self.redis_url is not None
