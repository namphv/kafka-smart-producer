"""
Kafka Smart Producer - Intelligent Kafka producer with real-time, lag-aware partition
selection.

This library extends confluent-kafka-python with smart partition selection based on
consumer lag monitoring to avoid "hot partitions" and improve throughput.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

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
from .protocols import CacheBackend, HotPartitionCalculator, LagDataCollector

# Main producer classes (will be implemented in later tasks)
# from .producers import SyncSmartProducer, AsyncSmartProducer

# Health management
# from .health import HealthManager

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
    # Future components
    # "SyncSmartProducer",
    # "AsyncSmartProducer",
    # "HealthManager",
    # "KafkaAdminLagCollector",
    # "ThresholdHotPartitionCalculator",
]
