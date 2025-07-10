"""
Kafka Smart Producer - Intelligent Kafka producer with real-time, lag-aware partition
selection.

This library extends confluent-kafka-python with smart partition selection based on
consumer lag monitoring to avoid "hot partitions" and improve throughput.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Caching system
from .caching import (
    CacheConfig,
    CacheEntry,
    CacheFactory,
    CacheTimeoutError,
    CacheUnavailableError,
    DefaultHybridCache,
    DefaultLocalCache,
    DefaultRemoteCache,
    HybridCache,
    LocalCache,
    RemoteCache,
)

# Core protocol interfaces
# Exception classes
from .exceptions import (
    CacheError,
    ConfigurationError,
    HealthCalculationError,
    HealthManagerError,
    LagDataUnavailableError,
    PartitionSelectionError,
    SmartProducerError,
)

# Health management
from .health import (
    DefaultHealthManager,
    HealthManager,
    HealthManagerConfig,
    PartitionHealth,
    TopicHealth,
)

# Main producer classes
from .producer import AsyncSmartProducer, SmartProducer
from .protocols import CacheBackend, HotPartitionCalculator, LagDataCollector

# Threading utilities
from .threading import (
    SimpleBackgroundRefresh,
    create_async_background_task,
    create_sync_background_refresh,
    run_periodic_async,
)

# Default implementations
# from .collectors import KafkaAdminLagCollector
# from .calculators import ThresholdHotPartitionCalculator

__all__ = [
    # Protocols
    "LagDataCollector",
    "HotPartitionCalculator",
    "CacheBackend",
    # Exceptions
    "SmartProducerError",
    "LagDataUnavailableError",
    "HealthCalculationError",
    "HealthManagerError",
    "CacheError",
    "PartitionSelectionError",
    "ConfigurationError",
    # Caching system
    "LocalCache",
    "RemoteCache",
    "HybridCache",
    "DefaultLocalCache",
    "DefaultRemoteCache",
    "DefaultHybridCache",
    "CacheConfig",
    "CacheEntry",
    "CacheFactory",
    "CacheTimeoutError",
    "CacheUnavailableError",
    # Threading utilities
    "SimpleBackgroundRefresh",
    "run_periodic_async",
    "create_async_background_task",
    "create_sync_background_refresh",
    # Health management
    "HealthManager",
    "DefaultHealthManager",
    "HealthManagerConfig",
    "PartitionHealth",
    "TopicHealth",
    # Removed PartitionSelectionStrategy
    # Producer classes
    "SmartProducer",
    "AsyncSmartProducer",
    # Future components
    # "KafkaAdminLagCollector",
    # "ThresholdHotPartitionCalculator",
]
