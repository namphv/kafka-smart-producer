"""
Kafka Smart Producer - Intelligent Kafka producer with lag-aware partition selection.
"""

from __future__ import annotations

__version__ = "0.1.0"

# Config
from .config import SmartProducerConfig

# Exceptions
from .exceptions import (
    CacheError,
    ConfigurationError,
    HealthError,
    LagDataUnavailableError,
    PartitionSelectionError,
    SmartProducerError,
)

# Lag collection
from .health.collectors.kafka_admin import KafkaAdminLagCollector

# Health monitoring
from .health.monitor import PartitionHealthMonitor
from .health.protocols import HealthScorer, HealthSignalCollector, PartitionSelector
from .health.scorer import LinearHealthScorer

# Partition selection
from .partition.selector import RandomHealthySelector

# Producers
from .producer import AsyncSmartProducer, SmartProducer

__all__ = [
    # Producers
    "SmartProducer",
    "AsyncSmartProducer",
    # Config
    "SmartProducerConfig",
    # Health
    "PartitionHealthMonitor",
    "KafkaAdminLagCollector",
    "LinearHealthScorer",
    "RandomHealthySelector",
    # Protocols
    "HealthSignalCollector",
    "HealthScorer",
    "PartitionSelector",
    # Exceptions
    "SmartProducerError",
    "ConfigurationError",
    "LagDataUnavailableError",
    "HealthError",
    "CacheError",
    "PartitionSelectionError",
]
