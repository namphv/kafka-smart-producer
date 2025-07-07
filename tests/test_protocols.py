"""
Tests for protocol interfaces.
"""

import pytest
from typing import Dict, Any, Optional

from kafka_smart_producer.protocols import (
    LagDataCollector,
    HotPartitionCalculator,
    CacheBackend,
)


class MockLagDataCollector:
    """Mock implementation of LagDataCollector for testing."""
    
    def __init__(self, lag_data: Dict[int, int]):
        self.lag_data = lag_data
        self.healthy = True
    
    async def get_lag_data(self, topic: str) -> Dict[int, int]:
        return self.lag_data.copy()
    
    def get_lag_data_sync(self, topic: str) -> Dict[int, int]:
        return self.lag_data.copy()
    
    def is_healthy(self) -> bool:
        return self.healthy


class MockHotPartitionCalculator:
    """Mock implementation of HotPartitionCalculator for testing."""
    
    def __init__(self, threshold: int = 1000):
        self.threshold = threshold
    
    def calculate_scores(
        self, 
        lag_data: Dict[int, int], 
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[int, float]:
        return {
            partition: 0.0 if lag > self.threshold else 1.0
            for partition, lag in lag_data.items()
        }
    
    def get_config(self) -> Dict[str, Any]:
        return {"threshold": self.threshold}


class MockCacheBackend:
    """Mock implementation of CacheBackend for testing."""
    
    def __init__(self):
        self.data: Dict[str, Any] = {}
    
    async def get(self, key: str) -> Optional[Any]:
        return self.data.get(key)
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self.data[key] = value
    
    async def delete(self, key: str) -> None:
        self.data.pop(key, None)
    
    def get_sync(self, key: str) -> Optional[Any]:
        return self.data.get(key)
    
    def set_sync(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        self.data[key] = value
    
    def delete_sync(self, key: str) -> None:
        self.data.pop(key, None)


def test_mock_lag_data_collector():
    """Test mock lag data collector implementation."""
    lag_data = {0: 100, 1: 500, 2: 1500}
    collector = MockLagDataCollector(lag_data)
    
    assert collector.is_healthy()
    assert collector.get_lag_data_sync("test-topic") == lag_data


@pytest.mark.asyncio
async def test_mock_lag_data_collector_async():
    """Test mock lag data collector async implementation."""
    lag_data = {0: 100, 1: 500, 2: 1500}
    collector = MockLagDataCollector(lag_data)
    
    result = await collector.get_lag_data("test-topic")
    assert result == lag_data


def test_mock_hot_partition_calculator():
    """Test mock hot partition calculator implementation."""
    calculator = MockHotPartitionCalculator(threshold=1000)
    lag_data = {0: 100, 1: 500, 2: 1500}
    
    scores = calculator.calculate_scores(lag_data)
    
    assert scores[0] == 1.0  # Below threshold
    assert scores[1] == 1.0  # Below threshold
    assert scores[2] == 0.0  # Above threshold
    
    assert calculator.get_config() == {"threshold": 1000}


@pytest.mark.asyncio
async def test_mock_cache_backend():
    """Test mock cache backend implementation."""
    cache = MockCacheBackend()
    
    # Test async operations
    await cache.set("key1", "value1")
    assert await cache.get("key1") == "value1"
    
    await cache.delete("key1")
    assert await cache.get("key1") is None
    
    # Test sync operations
    cache.set_sync("key2", "value2")
    assert cache.get_sync("key2") == "value2"
    
    cache.delete_sync("key2")
    assert cache.get_sync("key2") is None