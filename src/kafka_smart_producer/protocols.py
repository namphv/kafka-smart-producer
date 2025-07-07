"""
Protocol interfaces for pluggable data collection and health calculation components.

These protocols define the contracts for extensible lag data collection and 
health score calculation, enabling custom implementations for different
monitoring systems and business requirements.
"""

from typing import Protocol, Dict, Any, Optional
from abc import abstractmethod


class LagDataCollector(Protocol):
    """Protocol for collecting consumer lag data from various sources."""
    
    @abstractmethod
    async def get_lag_data(self, topic: str) -> Dict[int, int]:
        """
        Collect consumer lag data for all partitions of a topic.
        
        Args:
            topic: Kafka topic name
            
        Returns:
            Dict mapping partition_id -> lag_count
            
        Raises:
            LagDataUnavailableError: When lag data cannot be retrieved
        """
        ...
    
    @abstractmethod
    def get_lag_data_sync(self, topic: str) -> Dict[int, int]:
        """Synchronous version of get_lag_data for sync contexts."""
        ...
    
    @abstractmethod
    def is_healthy(self) -> bool:
        """Check if the data collector is operational."""
        ...


class HotPartitionCalculator(Protocol):
    """Protocol for calculating partition health scores from lag data."""
    
    @abstractmethod
    def calculate_scores(
        self, 
        lag_data: Dict[int, int],
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[int, float]:
        """
        Calculate health scores for partitions based on lag data.
        
        Args:
            lag_data: Dict mapping partition_id -> lag_count
            metadata: Optional additional context for scoring
            
        Returns:
            Dict mapping partition_id -> health_score (0.0 to 1.0)
            where 1.0 = healthy, 0.0 = unhealthy
        """
        ...
    
    @abstractmethod
    def get_config(self) -> Dict[str, Any]:
        """Get current calculator configuration."""
        ...


class CacheBackend(Protocol):
    """Protocol for cache backend implementations."""
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        ...
    
    @abstractmethod
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set value in cache with optional TTL."""
        ...
    
    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete key from cache."""
        ...
    
    @abstractmethod
    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous get for sync contexts."""
        ...
    
    @abstractmethod
    def set_sync(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Synchronous set for sync contexts."""
        ...
    
    @abstractmethod
    def delete_sync(self, key: str) -> None:
        """Synchronous delete for sync contexts."""
        ...