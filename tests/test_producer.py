"""
Tests for the producer base implementation.

This module tests the SmartProducerBase foundation including partition selection,
key caching, metrics collection, and fallback strategies.
"""

import threading
import time
from typing import Dict, List, Optional

import pytest

from kafka_smart_producer.exceptions import PartitionSelectionError
from kafka_smart_producer.producer import (
    DefaultSmartProducerBase,
    KeyPartitionCache,
    MessageMetadata,
    PartitioningStrategy,
    ProducerConfig,
    ProduceResult,
    ProducerMetrics,
)


class MockHealthManager:
    """Mock health manager for testing producer base."""

    def __init__(self, healthy_partitions: Dict[str, List[int]]):
        self._healthy_partitions = healthy_partitions
        self._selection_calls = []
        self._health_check_calls = []

    def select_partition(
        self,
        topic: str,
        key: Optional[bytes] = None,
        available_partitions: Optional[List[int]] = None,
    ) -> int:
        self._selection_calls.append((topic, key, available_partitions))
        healthy = self._healthy_partitions.get(topic, [0])
        if available_partitions:
            healthy = [p for p in healthy if p in available_partitions]
        return healthy[0] if healthy else 0

    def is_partition_healthy(self, topic: str, partition: int) -> bool:
        call = (topic, partition)
        self._health_check_calls.append(call)
        return partition in self._healthy_partitions.get(topic, [])

    def get_selection_calls(self) -> List:
        return self._selection_calls.copy()

    def get_health_check_calls(self) -> List:
        return self._health_check_calls.copy()

    def set_healthy_partitions(self, topic: str, partitions: List[int]):
        """Helper to change health status during tests."""
        self._healthy_partitions[topic] = partitions


class TestProducerConfig:
    """Test ProducerConfig validation and defaults."""

    def test_default_config(self):
        """Test default configuration values."""
        config = ProducerConfig()

        assert config.enable_smart_partitioning is True
        assert config.fallback_strategy == PartitioningStrategy.KEY_HASH
        assert config.health_check_interval_seconds == 5.0
        assert config.enable_key_caching is True
        assert config.key_cache_ttl_seconds == 300.0
        assert config.key_cache_max_size == 10000
        assert config.max_partition_selection_time_ms == 1.0
        assert config.enable_metrics is True
        assert config.unhealthy_partition_threshold == 0.5
        assert config.force_fallback_on_error is True
        assert config.preserve_key_ordering is True
        assert config.key_affinity_mode == "strict"

    def test_custom_config(self):
        """Test custom configuration values."""
        config = ProducerConfig(
            enable_smart_partitioning=False,
            fallback_strategy=PartitioningStrategy.ROUND_ROBIN,
            key_cache_ttl_seconds=600.0,
            key_cache_max_size=5000,
            enable_metrics=False,
            key_affinity_mode="eventual",
        )

        assert config.enable_smart_partitioning is False
        assert config.fallback_strategy == PartitioningStrategy.ROUND_ROBIN
        assert config.key_cache_ttl_seconds == 600.0
        assert config.key_cache_max_size == 5000
        assert config.enable_metrics is False
        assert config.key_affinity_mode == "eventual"

    def test_config_validation(self):
        """Test configuration validation."""
        # Test invalid TTL
        with pytest.raises(ValueError, match="key_cache_ttl_seconds must be positive"):
            ProducerConfig(key_cache_ttl_seconds=0)

        # Test invalid cache size
        with pytest.raises(ValueError, match="key_cache_max_size must be positive"):
            ProducerConfig(key_cache_max_size=-1)

        # Test invalid selection time
        with pytest.raises(
            ValueError, match="max_partition_selection_time_ms must be positive"
        ):
            ProducerConfig(max_partition_selection_time_ms=0)

        # Test invalid threshold
        with pytest.raises(
            ValueError,
            match="unhealthy_partition_threshold must be between 0.0 and 1.0",
        ):
            ProducerConfig(unhealthy_partition_threshold=1.5)

        # Test invalid affinity mode
        with pytest.raises(ValueError, match="key_affinity_mode must be"):
            ProducerConfig(key_affinity_mode="invalid")


class TestMessageMetadata:
    """Test MessageMetadata functionality."""

    def test_basic_creation(self):
        """Test basic message metadata creation."""
        metadata = MessageMetadata(
            topic="test-topic",
            partition=1,
            offset=12345,
            timestamp=1609459200000,
            key=b"test-key",
        )

        assert metadata.topic == "test-topic"
        assert metadata.partition == 1
        assert metadata.offset == 12345
        assert metadata.timestamp == 1609459200000
        assert metadata.key == b"test-key"
        assert metadata.produced_at > 0

    def test_optional_fields(self):
        """Test metadata with optional fields."""
        metadata = MessageMetadata(topic="test-topic", partition=0)

        assert metadata.topic == "test-topic"
        assert metadata.partition == 0
        assert metadata.offset is None
        assert metadata.timestamp is None
        assert metadata.key is None
        assert metadata.produced_at > 0


class TestProduceResult:
    """Test ProduceResult functionality."""

    def test_successful_result(self):
        """Test successful produce result."""
        metadata = MessageMetadata("test-topic", 1)
        result = ProduceResult(metadata=metadata, success=True, latency_ms=2.5)

        assert result.metadata == metadata
        assert result.success is True
        assert result.error is None
        assert result.latency_ms == 2.5

    def test_error_result(self):
        """Test error produce result."""
        metadata = MessageMetadata("test-topic", 1)
        error = Exception("Test error")
        result = ProduceResult(
            metadata=metadata, success=False, error=error, latency_ms=1.0
        )

        assert result.metadata == metadata
        assert result.success is False
        assert result.error == error
        assert result.latency_ms == 1.0


class TestKeyPartitionCache:
    """Test KeyPartitionCache functionality."""

    def create_cache(self, max_size: int = 10, ttl: float = 60.0) -> KeyPartitionCache:
        """Create a test cache with custom configuration."""
        config = ProducerConfig(key_cache_max_size=max_size, key_cache_ttl_seconds=ttl)
        return KeyPartitionCache(config)

    def test_basic_operations(self):
        """Test basic cache get/set operations."""
        cache = self.create_cache()

        # Test cache miss
        assert cache.get_partition(b"key1") is None
        assert cache.size() == 0

        # Test cache set/get
        cache.set_partition(b"key1", 1)
        assert cache.get_partition(b"key1") == 1
        assert cache.size() == 1

        # Test cache update
        cache.set_partition(b"key1", 2)
        assert cache.get_partition(b"key1") == 2
        assert cache.size() == 1

    def test_ttl_expiration(self):
        """Test TTL-based expiration."""
        cache = self.create_cache(ttl=0.1)

        cache.set_partition(b"key1", 1)
        assert cache.get_partition(b"key1") == 1

        # Wait for expiration
        time.sleep(0.15)
        assert cache.get_partition(b"key1") is None
        assert cache.size() == 0

    def test_lru_eviction(self):
        """Test LRU eviction when cache is full."""
        cache = self.create_cache(max_size=2)

        # Fill cache to capacity
        cache.set_partition(b"key1", 1)
        cache.set_partition(b"key2", 2)
        assert cache.size() == 2

        # Add third item, should evict oldest
        cache.set_partition(b"key3", 3)
        assert cache.size() == 2
        assert cache.get_partition(b"key1") is None  # Evicted
        assert cache.get_partition(b"key2") == 2
        assert cache.get_partition(b"key3") == 3

    def test_lru_order_update(self):
        """Test that accessing items updates LRU order."""
        cache = self.create_cache(max_size=2)

        cache.set_partition(b"key1", 1)
        cache.set_partition(b"key2", 2)

        # Access key1 to make it most recently used
        cache.get_partition(b"key1")

        # Add third item, should evict key2 (least recently used)
        cache.set_partition(b"key3", 3)
        assert cache.get_partition(b"key1") == 1  # Still there
        assert cache.get_partition(b"key2") is None  # Evicted
        assert cache.get_partition(b"key3") == 3

    def test_key_invalidation(self):
        """Test individual key invalidation."""
        cache = self.create_cache()

        cache.set_partition(b"key1", 1)
        cache.set_partition(b"key2", 2)
        assert cache.size() == 2

        cache.invalidate_key(b"key1")
        assert cache.get_partition(b"key1") is None
        assert cache.get_partition(b"key2") == 2
        assert cache.size() == 1

    def test_partition_invalidation(self):
        """Test partition-based invalidation."""
        cache = self.create_cache()

        cache.set_partition(b"key1", 1)
        cache.set_partition(b"key2", 1)
        cache.set_partition(b"key3", 2)
        assert cache.size() == 3

        cache.invalidate_partition(1)
        assert cache.get_partition(b"key1") is None
        assert cache.get_partition(b"key2") is None
        assert cache.get_partition(b"key3") == 2
        assert cache.size() == 1

    def test_clear_operation(self):
        """Test cache clear functionality."""
        cache = self.create_cache()

        cache.set_partition(b"key1", 1)
        cache.set_partition(b"key2", 2)
        assert cache.size() == 2

        cache.clear()
        assert cache.size() == 0
        assert cache.get_partition(b"key1") is None
        assert cache.get_partition(b"key2") is None

    def test_thread_safety(self):
        """Test concurrent access to cache."""
        cache = self.create_cache(max_size=1000)
        num_threads = 10
        operations_per_thread = 100
        results = []
        errors = []

        def worker(thread_id: int):
            try:
                for i in range(operations_per_thread):
                    key = f"thread_{thread_id}_key_{i}".encode()
                    partition = thread_id * 100 + i

                    cache.set_partition(key, partition)
                    retrieved = cache.get_partition(key)
                    results.append((key, retrieved))

                    if i % 10 == 0:
                        cache.size()  # Test size calculation under load
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Concurrent access errors: {errors}"
        assert len(results) == num_threads * operations_per_thread

        # Verify no corruption (values may be None due to eviction)
        for key, value in results:
            if value is not None:
                # Extract expected value from key
                parts = key.decode().split("_")
                thread_id = int(parts[1])
                i = int(parts[3])
                expected = thread_id * 100 + i
                assert value == expected


class TestProducerMetrics:
    """Test ProducerMetrics functionality."""

    def test_initial_state(self):
        """Test initial metrics state."""
        metrics = ProducerMetrics()

        assert metrics.messages_produced == 0
        assert metrics.smart_selections == 0
        assert metrics.fallback_selections == 0
        assert metrics.key_cache_hits == 0
        assert metrics.key_cache_misses == 0
        assert metrics.produce_errors == 0
        assert len(metrics.partition_selection_times_ms) == 0
        assert len(metrics.healthy_partitions_used) == 0
        assert len(metrics.unhealthy_partitions_avoided) == 0

    def test_message_recording(self):
        """Test message production recording."""
        metrics = ProducerMetrics()

        # Record smart selection
        metrics.record_message_produced(1, 0.5, True)
        assert metrics.messages_produced == 1
        assert metrics.smart_selections == 1
        assert metrics.fallback_selections == 0
        assert 1 in metrics.healthy_partitions_used
        assert metrics.partition_selection_times_ms == [0.5]

        # Record fallback selection
        metrics.record_message_produced(2, 1.0, False)
        assert metrics.messages_produced == 2
        assert metrics.smart_selections == 1
        assert metrics.fallback_selections == 1
        assert metrics.partition_selection_times_ms == [0.5, 1.0]

    def test_cache_metrics(self):
        """Test cache hit/miss recording."""
        metrics = ProducerMetrics()

        metrics.record_cache_hit()
        metrics.record_cache_hit()
        metrics.record_cache_miss()

        assert metrics.key_cache_hits == 2
        assert metrics.key_cache_misses == 1

    def test_error_recording(self):
        """Test error recording."""
        metrics = ProducerMetrics()

        metrics.record_error()
        metrics.record_error()

        assert metrics.produce_errors == 2

    def test_partition_avoidance(self):
        """Test partition avoidance recording."""
        metrics = ProducerMetrics()

        metrics.record_partition_avoided(1)
        metrics.record_partition_avoided(2)
        metrics.record_partition_avoided(1)  # Duplicate

        assert len(metrics.unhealthy_partitions_avoided) == 2
        assert 1 in metrics.unhealthy_partitions_avoided
        assert 2 in metrics.unhealthy_partitions_avoided

    def test_summary_calculation(self):
        """Test metrics summary calculation."""
        metrics = ProducerMetrics()

        # Record some operations
        metrics.record_message_produced(1, 0.5, True)
        metrics.record_message_produced(2, 1.5, False)
        metrics.record_cache_hit()
        metrics.record_cache_miss()
        metrics.record_error()

        summary = metrics.get_summary()

        assert summary["messages_produced"] == 2
        assert summary["smart_selection_rate"] == 0.5
        assert summary["avg_partition_selection_time_ms"] == 1.0
        assert summary["max_partition_selection_time_ms"] == 1.5
        assert summary["key_cache_hit_rate"] == 0.5
        assert summary["produce_errors"] == 1
        assert summary["unique_healthy_partitions_used"] == 1
        assert summary["uptime_seconds"] > 0

    def test_empty_summary(self):
        """Test summary with no data."""
        metrics = ProducerMetrics()
        summary = metrics.get_summary()

        assert summary["messages_produced"] == 0
        assert summary["smart_selection_rate"] == 0.0
        assert summary["avg_partition_selection_time_ms"] == 0.0
        assert summary["max_partition_selection_time_ms"] == 0.0
        assert summary["key_cache_hit_rate"] == 0.0
        assert summary["produce_errors"] == 0

    def test_reset_functionality(self):
        """Test metrics reset."""
        metrics = ProducerMetrics()

        # Record some data
        metrics.record_message_produced(1, 0.5, True)
        metrics.record_cache_hit()

        # Reset
        metrics.reset()

        assert metrics.messages_produced == 0
        assert metrics.key_cache_hits == 0
        assert len(metrics.partition_selection_times_ms) == 0

    def test_thread_safety(self):
        """Test concurrent metrics updates."""
        metrics = ProducerMetrics()
        num_threads = 10
        operations_per_thread = 100
        errors = []

        def worker():
            try:
                for _ in range(operations_per_thread):
                    metrics.record_message_produced(1, 1.0, True)
                    metrics.record_cache_hit()
                    metrics.record_error()
            except Exception as e:
                errors.append(e)

        threads = []
        for _ in range(num_threads):
            thread = threading.Thread(target=worker)
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Concurrent update errors: {errors}"
        expected_total = num_threads * operations_per_thread
        assert metrics.messages_produced == expected_total
        assert metrics.key_cache_hits == expected_total
        assert metrics.produce_errors == expected_total


class TestDefaultSmartProducerBase:
    """Test DefaultSmartProducerBase functionality."""

    def create_producer_base(
        self,
        config: Optional[ProducerConfig] = None,
        healthy_partitions: Optional[Dict[str, List[int]]] = None,
    ) -> tuple[DefaultSmartProducerBase, MockHealthManager]:
        """Create a test producer base with mock health manager."""
        config = config or ProducerConfig()
        healthy_partitions = healthy_partitions or {"test-topic": [0, 1, 2]}
        health_manager = MockHealthManager(healthy_partitions)
        producer = DefaultSmartProducerBase(config, health_manager, {})
        return producer, health_manager

    def test_smart_partition_selection(self):
        """Test smart partition selection when health manager is available."""
        producer, health_manager = self.create_producer_base()

        partition = producer.select_partition("test-topic")

        # Should use smart partitioning
        assert partition in [0, 1, 2]
        assert len(health_manager.get_selection_calls()) == 1

        # Check metrics
        metrics = producer.get_metrics()
        assert metrics["messages_produced"] == 1
        assert metrics["smart_selection_rate"] == 1.0

    def test_key_caching_behavior(self):
        """Test key-based caching."""
        producer, health_manager = self.create_producer_base()
        key = b"test-key"

        # First call should select and cache
        partition1 = producer.select_partition("test-topic", key=key)
        assert len(health_manager.get_selection_calls()) == 1

        # Second call should use cache
        partition2 = producer.select_partition("test-topic", key=key)
        assert partition1 == partition2
        assert len(health_manager.get_selection_calls()) == 1  # No additional calls

        # Check cache hit metrics
        metrics = producer.get_metrics()
        assert metrics["key_cache_hit_rate"] == 0.5  # 1 hit, 1 miss

    def test_unhealthy_partition_cache_invalidation(self):
        """Test cache invalidation when partition becomes unhealthy."""
        producer, health_manager = self.create_producer_base()
        key = b"test-key"

        # Cache a key to partition mapping
        partition1 = producer.select_partition("test-topic", key=key)

        # Make the cached partition unhealthy
        health_manager.set_healthy_partitions(
            "test-topic", [p for p in [0, 1, 2] if p != partition1]
        )

        # Next selection should invalidate cache and select new partition
        partition2 = producer.select_partition("test-topic", key=key)
        assert partition1 != partition2
        assert len(health_manager.get_selection_calls()) == 2

    def test_fallback_on_health_manager_failure(self):
        """Test fallback when health manager fails."""
        config = ProducerConfig(fallback_strategy=PartitioningStrategy.ROUND_ROBIN)
        producer, health_manager = self.create_producer_base(config)

        # Mock health manager to raise exception
        def failing_select(*args, **kwargs):
            raise Exception("Health manager failed")

        health_manager.select_partition = failing_select

        # Should fall back to round-robin
        partition = producer.select_partition("test-topic")
        assert 0 <= partition <= 2  # Should be valid partition

        # Check metrics show fallback
        metrics = producer.get_metrics()
        assert metrics["smart_selection_rate"] == 0.0

    def test_key_hash_fallback_strategy(self):
        """Test KEY_HASH fallback strategy."""
        config = ProducerConfig(
            enable_smart_partitioning=False,
            fallback_strategy=PartitioningStrategy.KEY_HASH,
        )
        producer, _ = self.create_producer_base(config)

        key = b"test-key"

        # Should consistently hash to same partition
        partition1 = producer.select_partition("test-topic", key=key)
        partition2 = producer.select_partition("test-topic", key=key)
        assert partition1 == partition2
        assert 0 <= partition1 <= 2

    def test_round_robin_fallback_strategy(self):
        """Test ROUND_ROBIN fallback strategy."""
        config = ProducerConfig(
            enable_smart_partitioning=False,
            fallback_strategy=PartitioningStrategy.ROUND_ROBIN,
        )
        producer, _ = self.create_producer_base(config)

        # Should cycle through partitions
        partitions = []
        for _ in range(6):  # Get more than the number of partitions
            partition = producer.select_partition("test-topic")
            partitions.append(partition)

        # Should see all partitions
        assert set(partitions) == {0, 1, 2}

    def test_random_fallback_strategy(self):
        """Test RANDOM fallback strategy."""
        config = ProducerConfig(
            enable_smart_partitioning=False,
            fallback_strategy=PartitioningStrategy.RANDOM,
        )
        producer, _ = self.create_producer_base(config)

        # Should return valid partitions
        partitions = set()
        for _ in range(20):  # Multiple attempts to get randomness
            partition = producer.select_partition("test-topic")
            partitions.add(partition)
            assert 0 <= partition <= 2

        # Should see multiple partitions (random should vary)
        assert len(partitions) > 1

    def test_available_partitions_filtering(self):
        """Test partition selection with available_partitions filter."""
        producer, health_manager = self.create_producer_base()

        # Limit available partitions
        partition = producer.select_partition("test-topic", available_partitions=[1, 2])
        assert partition in [1, 2]

        # Check health manager was called with filter
        calls = health_manager.get_selection_calls()
        assert len(calls) == 1
        assert calls[0][2] == [1, 2]  # available_partitions argument

    def test_no_partitions_available_error(self):
        """Test error when no partitions are available."""
        config = ProducerConfig(enable_smart_partitioning=False)
        producer, _ = self.create_producer_base(config)

        with pytest.raises(PartitionSelectionError):
            producer.select_partition("test-topic", available_partitions=[])

    def test_topic_metadata_caching(self):
        """Test topic metadata caching and refresh."""
        producer, _ = self.create_producer_base()

        # First call should cache metadata
        metadata1 = producer.get_topic_metadata("test-topic")
        assert metadata1["partition_count"] == 3
        assert metadata1["last_refresh"] > 0

        # Second call should use cache
        metadata2 = producer.get_topic_metadata("test-topic")
        assert metadata1["last_refresh"] == metadata2["last_refresh"]

    def test_metrics_disabled(self):
        """Test producer with metrics disabled."""
        config = ProducerConfig(enable_metrics=False)
        producer, _ = self.create_producer_base(config)

        producer.select_partition("test-topic")

        metrics = producer.get_metrics()
        assert metrics == {"metrics_disabled": True}

    def test_key_caching_disabled(self):
        """Test producer with key caching disabled."""
        config = ProducerConfig(enable_key_caching=False)
        producer, health_manager = self.create_producer_base(config)

        key = b"test-key"

        # Multiple calls should not use cache
        producer.select_partition("test-topic", key=key)
        producer.select_partition("test-topic", key=key)

        # Should have made two health manager calls
        assert len(health_manager.get_selection_calls()) == 2

    def test_error_handling_with_force_fallback(self):
        """Test error handling when force_fallback_on_error is True."""
        config = ProducerConfig(force_fallback_on_error=True)
        producer, health_manager = self.create_producer_base(config)

        # Mock health manager to always fail
        def failing_select(*args, **kwargs):
            raise RuntimeError("Simulated failure")

        health_manager.select_partition = failing_select

        # Should not raise exception, use fallback instead
        partition = producer.select_partition("test-topic")
        assert 0 <= partition <= 2

        # Should record error in metrics
        metrics = producer.get_metrics()
        assert metrics["produce_errors"] == 1

    def test_error_handling_without_force_fallback(self):
        """Test error handling when force_fallback_on_error is False."""
        config = ProducerConfig(force_fallback_on_error=False)
        producer, health_manager = self.create_producer_base(config)

        # Mock health manager to always fail
        def failing_select(*args, **kwargs):
            raise RuntimeError("Simulated failure")

        health_manager.select_partition = failing_select

        # Should propagate the exception
        with pytest.raises(RuntimeError, match="Simulated failure"):
            producer.select_partition("test-topic")


class TestIntegration:
    """Integration tests for producer base components."""

    def test_end_to_end_workflow(self):
        """Test complete producer base workflow."""
        config = ProducerConfig(
            enable_smart_partitioning=True,
            enable_key_caching=True,
            enable_metrics=True,
            fallback_strategy=PartitioningStrategy.KEY_HASH,
        )

        healthy_partitions = {"test-topic": [1, 2]}  # Partition 0 is unhealthy
        health_manager = MockHealthManager(healthy_partitions)
        producer = DefaultSmartProducerBase(config, health_manager, {})

        # Test smart selection
        partition1 = producer.select_partition("test-topic", key=b"key1")
        assert partition1 in [1, 2]

        # Test cache hit
        partition2 = producer.select_partition("test-topic", key=b"key1")
        assert partition1 == partition2

        # Test different key
        partition3 = producer.select_partition("test-topic", key=b"key2")
        assert partition3 in [1, 2]

        # Test fallback when health manager fails
        def failing_select(*args, **kwargs):
            raise Exception("Health failure")

        health_manager.select_partition = failing_select

        partition4 = producer.select_partition("test-topic", key=b"key3")
        assert 0 <= partition4 <= 2  # Fallback to all partitions

        # Verify metrics
        metrics = producer.get_metrics()
        assert metrics["messages_produced"] == 4
        assert metrics["key_cache_hit_rate"] == 0.25  # 1 hit out of 4 attempts
        assert metrics["produce_errors"] == 1
        assert metrics["unique_healthy_partitions_used"] >= 1

    def test_concurrent_producer_operations(self):
        """Test concurrent partition selection operations."""
        config = ProducerConfig(enable_key_caching=True, key_cache_max_size=1000)
        producer, _ = self.create_producer_base(config)

        num_threads = 10
        operations_per_thread = 50
        results = []
        errors = []

        def worker(thread_id: int):
            try:
                for i in range(operations_per_thread):
                    key = f"thread_{thread_id}_key_{i}".encode()
                    partition = producer.select_partition("test-topic", key=key)
                    results.append((key, partition))
            except Exception as e:
                errors.append(e)

        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check results
        assert len(errors) == 0, f"Concurrent operation errors: {errors}"
        assert len(results) == num_threads * operations_per_thread

        # Verify all partitions are valid
        for _, partition in results:
            assert 0 <= partition <= 2

        # Verify metrics consistency
        metrics = producer.get_metrics()
        assert metrics["messages_produced"] == len(results)

    def create_producer_base(
        self,
        config: ProducerConfig,
        healthy_partitions: Optional[Dict[str, List[int]]] = None,
    ) -> tuple[DefaultSmartProducerBase, MockHealthManager]:
        """Helper to create producer base for integration tests."""
        healthy_partitions = healthy_partitions or {"test-topic": [0, 1, 2]}
        health_manager = MockHealthManager(healthy_partitions)
        producer = DefaultSmartProducerBase(config, health_manager, {})
        return producer, health_manager
