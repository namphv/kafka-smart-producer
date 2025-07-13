"""
Protocol interfaces for pluggable data collection and health calculation components.

These protocols define the contracts for extensible lag data collection and
health score calculation, enabling custom implementations for different
monitoring systems and business requirements.
"""

from abc import abstractmethod
from typing import Protocol


class LagDataCollector(Protocol):
    """
    Protocol for collecting consumer lag data from various sources.

    Implementations can collect lag data from Kafka AdminClient, Redis cache,
    Prometheus metrics, or any other monitoring system.

    Threading Considerations:
    - All methods are synchronous for simplicity and compatibility
    - Implementations should be thread-safe for concurrent access
    - Can be used with asyncio via run_in_executor when needed
    """

    @abstractmethod
    def get_lag_data(self, topic: str) -> dict[int, int]:
        """
        Collect consumer lag data for all partitions of a topic.

        This method should complete reasonably quickly (< 5s typical).
        For async contexts, use asyncio.run_in_executor.

        Args:
            topic: Kafka topic name

        Returns:
            Dict mapping partition_id -> lag_count
            - partition_id: Non-negative integer
            - lag_count: Non-negative integer

        Raises:
            LagDataUnavailableError: When lag data cannot be retrieved
        """
        ...

    @abstractmethod
    def is_healthy(self) -> bool:
        """
        Check if the data collector is operational.

        Performance requirement: Must complete in < 100ms.

        Returns:
            bool: True if collector can retrieve data, False otherwise
        """
        ...
