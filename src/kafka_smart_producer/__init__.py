"""
Kafka Smart Producer - Intelligent Kafka producer with real-time, lag-aware partition selection.

This library extends confluent-kafka-python with smart partition selection based on
consumer lag monitoring to avoid "hot partitions" and improve throughput.
"""

__version__ = "0.1.0"
__author__ = "Your Name"
__email__ = "your.email@example.com"

# Core protocol interfaces
from .protocols import LagDataCollector, HotPartitionCalculator

# Main producer classes (will be implemented in later tasks)
# from .producers import SyncSmartProducer, AsyncSmartProducer

# Health management
# from .health import HealthManager

# Default implementations
# from .collectors import KafkaAdminLagCollector
# from .calculators import ThresholdHotPartitionCalculator

__all__ = [
    "LagDataCollector",
    "HotPartitionCalculator",
    # "SyncSmartProducer",
    # "AsyncSmartProducer", 
    # "HealthManager",
    # "KafkaAdminLagCollector",
    # "ThresholdHotPartitionCalculator",
]