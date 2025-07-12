"""
Protocol interfaces for pluggable data collection and health calculation components.

These protocols define the contracts for extensible lag data collection and
health score calculation, enabling custom implementations for different
monitoring systems and business requirements.
"""

from abc import abstractmethod
from typing import Any, Dict, Optional, Protocol


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
    def get_lag_data(self, topic: str) -> Dict[int, int]:
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


class HotPartitionCalculator(Protocol):
    """
    Protocol for calculating partition health scores from lag data.

    DEPRECATED: The new simplified HealthManager includes built-in
    health calculation. This protocol is kept for backward compatibility.

    Implementations define the logic for converting raw lag metrics into
    normalized health scores that guide partition selection.

    Threading Considerations:
    - Should be CPU-bound and thread-safe
    - All methods are synchronous for simplicity
    - Must handle concurrent access safely
    """

    @abstractmethod
    def calculate_scores(
        self, lag_data: Dict[int, int], metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[int, float]:
        """
        Calculate health scores for partitions based on lag data.

        Higher lag should result in lower health scores.
        Must handle edge cases gracefully (empty data, extreme values).

        Args:
            lag_data: Partition lag counts from LagDataCollector
                     - Keys: partition_id (non-negative int)
                     - Values: lag_count (non-negative int)
            metadata: Optional additional context (broker metrics, etc.)
                     - Can include broker health, throughput metrics, etc.

        Returns:
            Dict mapping partition_id -> health_score
            - health_score: Float in range [0.0, 1.0]
            - 1.0 = healthy (low lag)
            - 0.0 = unhealthy (high lag)
            - Must not return NaN or infinite values

        Raises:
            HealthCalculationError: When score calculation fails
        """
        ...

    @abstractmethod
    def get_threshold_config(self) -> Dict[str, Any]:
        """
        Return current threshold configuration for debugging.

        Returns:
            Dict containing current configuration parameters
            Used for diagnostics and health monitoring
        """
        ...


class CacheBackend(Protocol):
    """
    Protocol for cache backend implementations.

    Supports both local (in-memory) and distributed (Redis) caching
    with consistent async/sync interfaces.

    Threading Considerations:
    - Async methods must not block event loop
    - Sync methods for sync contexts
    - Thread-safe for concurrent access
    """

    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """
        Get value from cache asynchronously.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        ...

    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set value in cache asynchronously.

        Args:
            key: Cache key
            value: Value to cache (must be serializable)
            ttl: Time-to-live in seconds, None for no expiration
        """
        ...

    @abstractmethod
    async def delete(self, key: str) -> None:
        """
        Delete key from cache asynchronously.

        Args:
            key: Cache key to delete
        """
        ...

    @abstractmethod
    def get_sync(self, key: str) -> Optional[Any]:
        """
        Synchronous get for sync contexts.

        Args:
            key: Cache key

        Returns:
            Cached value or None if not found/expired
        """
        ...

    @abstractmethod
    def set_sync(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Synchronous set for sync contexts.

        Args:
            key: Cache key
            value: Value to cache
            ttl: Time-to-live in seconds
        """
        ...

    @abstractmethod
    def delete_sync(self, key: str) -> None:
        """
        Synchronous delete for sync contexts.

        Args:
            key: Cache key to delete
        """
        ...
