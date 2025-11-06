"""
Tests to improve coverage for areas identified in coverage analysis.

These tests focus on factory methods and edge cases that were missing coverage.
"""

import asyncio
from unittest.mock import Mock
import pytest

from kafka_smart_producer.async_partition_health_monitor import AsyncPartitionHealthMonitor
from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor
from kafka_smart_producer.health_mode import HealthMode
from kafka_smart_producer.producer_config import SmartProducerConfig


class MockLagCollector:
    """Simple mock lag collector."""
    
    def __init__(self):
        self.lag_data = {}
        
    def get_lag_data(self, topic):
        return self.lag_data.get(topic, {})
        
    def set_lag_data(self, topic, data):
        self.lag_data[topic] = data
        
    def is_healthy(self):
        return True


class TestAsyncPartitionHealthMonitorCoverage:
    """Tests for AsyncPartitionHealthMonitor to improve coverage."""
    
    @pytest.mark.asyncio
    async def test_embedded_factory_with_topics(self):
        """Test embedded() factory method with topics."""
        lag_collector = MockLagCollector()
        topics = ["topic1", "topic2"]
        
        monitor = AsyncPartitionHealthMonitor.embedded(
            lag_collector=lag_collector,
            topics=topics
        )
        
        try:
            assert monitor is not None
            assert monitor._mode == HealthMode.EMBEDDED
            assert monitor._cache is None
            assert monitor._redis_publisher is None
            assert len(monitor._health_data) == 2
            assert "topic1" in monitor._health_data
            assert "topic2" in monitor._health_data
            
        finally:
            if monitor._running:
                await monitor.stop()
    
    @pytest.mark.asyncio
    async def test_embedded_factory_without_topics(self):
        """Test embedded() factory method without topics."""
        lag_collector = MockLagCollector()
        
        monitor = AsyncPartitionHealthMonitor.embedded(
            lag_collector=lag_collector,
            topics=None
        )
        
        try:
            assert monitor is not None
            assert monitor._mode == HealthMode.EMBEDDED
            assert len(monitor._health_data) == 0
            
        finally:
            if monitor._running:
                await monitor.stop()
    
    @pytest.mark.asyncio
    async def test_is_partition_healthy(self):
        """Test is_partition_healthy() method."""
        lag_collector = MockLagCollector()
        lag_collector.set_lag_data("test-topic", {0: 10, 1: 200})
        
        monitor = AsyncPartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=10.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )
        
        monitor.initialize_topics(["test-topic"])
        await monitor._refresh_single_topic("test-topic")
        
        is_healthy_0 = monitor.is_partition_healthy("test-topic", 0)
        is_healthy_1 = monitor.is_partition_healthy("test-topic", 1)
        
        assert is_healthy_0 is True
        assert is_healthy_1 is False


class TestPartitionHealthMonitorCoverage:
    """Tests for PartitionHealthMonitor to improve coverage."""
    
    def test_embedded_factory_with_topics(self):
        """Test embedded() factory method with topics."""
        lag_collector = MockLagCollector()
        topics = ["topic1", "topic2"]
        
        monitor = PartitionHealthMonitor.embedded(
            lag_collector=lag_collector,
            topics=topics
        )
        
        try:
            assert monitor is not None
            assert monitor._mode == HealthMode.EMBEDDED
            assert monitor._cache is None
            assert len(monitor._health_data) == 2
            assert "topic1" in monitor._health_data
            assert "topic2" in monitor._health_data
            
        finally:
            monitor.stop()
    
    def test_embedded_factory_without_topics(self):
        """Test embedded() factory method without topics."""
        lag_collector = MockLagCollector()
        
        monitor = PartitionHealthMonitor.embedded(
            lag_collector=lag_collector,
            topics=None
        )
        
        try:
            assert monitor is not None
            assert monitor._mode == HealthMode.EMBEDDED
            assert len(monitor._health_data) == 0
            
        finally:
            monitor.stop()
    
    def test_is_partition_healthy(self):
        """Test is_partition_healthy() method."""
        lag_collector = MockLagCollector()
        lag_collector.set_lag_data("test-topic", {0: 10, 1: 200})
        
        monitor = PartitionHealthMonitor(
            lag_collector=lag_collector,
            health_threshold=0.5,
            refresh_interval=10.0,
            max_lag_for_health=100,
            mode=HealthMode.EMBEDDED,
        )
        
        monitor.initialize_topics(["test-topic"])
        monitor._refresh_all_topics()
        
        is_healthy_0 = monitor.is_partition_healthy("test-topic", 0)
        is_healthy_1 = monitor.is_partition_healthy("test-topic", 1)
        
        assert is_healthy_0 is True
        assert is_healthy_1 is False


class TestProducerConfigCoverage:
    """Tests for SmartProducerConfig to improve coverage."""
    
    def test_get_clean_kafka_config_with_various_keys(self):
        """Test get_clean_kafka_config() with various key types."""
        config = SmartProducerConfig(
            kafka_config={
                "bootstrap.servers": "localhost:9092",
                "client.id": "test-client",
                "acks": "all",
                "retries": 3,
            },
            topics=["test-topic"],
            consumer_group="test-group",
        )
        
        clean_config = config.get_clean_kafka_config()
        
        assert "bootstrap.servers" in clean_config
        assert "client.id" in clean_config
        assert clean_config["bootstrap.servers"] == "localhost:9092"


class TestHealthModeCoverage:
    """Tests for HealthMode enum to improve coverage."""
    
    def test_from_string_standalone(self):
        """Test from_string() with 'standalone'."""
        mode = HealthMode.from_string("standalone")
        assert mode == HealthMode.STANDALONE
    
    def test_from_string_embedded(self):
        """Test from_string() with 'embedded'."""
        mode = HealthMode.from_string("embedded")
        assert mode == HealthMode.EMBEDDED
    
    def test_from_string_redis_consumer(self):
        """Test from_string() with 'redis_consumer'."""
        mode = HealthMode.from_string("redis_consumer")
        assert mode == HealthMode.REDIS_CONSUMER
    
    def test_from_string_invalid_raises_error(self):
        """Test from_string() with invalid value."""
        with pytest.raises(ValueError):
            HealthMode.from_string("invalid_mode")
