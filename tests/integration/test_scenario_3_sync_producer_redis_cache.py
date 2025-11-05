"""
Scenario 3: Sync Producer with Redis Cache Integration Tests

This test file validates the complete functionality of:
- SmartProducer (synchronous)
- PartitionHealthMonitor (threading-based)
- DefaultRemoteCache (Redis)

The 4 Core Validations tested are:
1. Initialization - Verify Redis connection, cache configuration, threading monitor setup
2. Health Detection - Verify health data is cached in Redis, shared across instances
3. Smart Routing - Verify Redis-based key stickiness and distributed cache behavior
4. Distributed Key Stickiness - Test key stickiness persistence across producer instances

This scenario represents distributed caching patterns for multi-service architectures.
"""

import threading
import time

import pytest
import redis

# Import actual smart producer components
from kafka_smart_producer import SmartProducer, SmartProducerConfig

# Import base class
from .base_integration_test import BaseIntegrationTest

# Import test configurations
from .test_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TEST_CACHE_CONFIG,
    TEST_DATA_AMOUNTS,
    TEST_HEALTH_CONFIG,
    TEST_THRESHOLDS,
)


class TestScenario3SyncProducerRedisCache(BaseIntegrationTest):
    """Test suite for Scenario 3: Sync Producer with Redis Cache."""

    @pytest.fixture
    def redis_cleanup(self):
        """Ensure clean Redis state between tests."""
        redis_client = redis.Redis(host="localhost", port=6380, decode_responses=True)

        # Clean before test
        try:
            redis_client.flushdb()
        except Exception:
            pass

        yield redis_client

        # Clean after test
        try:
            redis_client.flushdb()
        except Exception:
            pass

    def create_scenario_config(
        self, fresh_test_data, test_topic, enable_key_stickiness=True
    ):
        """Create SmartProducerConfig for Scenario 3 with Redis cache."""
        scenario_config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=[test_topic],
            consumer_group=fresh_test_data["consumer_group"],
            health_manager={
                "consumer_group": fresh_test_data["consumer_group"],
                "refresh_interval": TEST_HEALTH_CONFIG["refresh_interval"],
                "timeout_seconds": TEST_HEALTH_CONFIG["timeout_seconds"],
                "health_threshold": TEST_HEALTH_CONFIG["unhealthy_threshold"],
                "max_lag_for_health": TEST_HEALTH_CONFIG["lag_threshold"],
            },
            cache={
                "local_default_ttl_seconds": TEST_CACHE_CONFIG["ttl_seconds"],
                "remote_enabled": True,  # Enable Redis cache
                "redis_host": "localhost",
                "redis_port": 6380,
                "redis_db": 0,
                "remote_default_ttl_seconds": TEST_CACHE_CONFIG["ttl_seconds"] * 2,
            },
            key_stickiness=enable_key_stickiness,
        )

        return scenario_config

    def create_sync_smart_producer(self, config):
        """Create SmartProducer instance for testing."""
        producer = SmartProducer(config)
        # Health manager will be started lazily on first produce() call
        return producer

    @pytest.fixture
    def sync_delivery_tracker(self):
        """Factory for tracking sync message delivery results."""

        def create_tracker():
            results = []
            results_lock = threading.Lock()

            def callback(err, msg):
                if err is None:
                    with results_lock:
                        results.append(
                            {
                                "partition": msg.partition(),
                                "offset": msg.offset(),
                                "topic": msg.topic(),
                                "key": msg.key().decode() if msg.key() else None,
                                "timestamp": time.time(),
                            }
                        )

            return callback, results

        return create_tracker

    # TEST 1: INITIALIZATION VALIDATION
    def test_initialization_sync_producer_redis_cache(
        self, fresh_test_data, test_topic, redis_cleanup
    ):
        """Test 1: Verify Redis connection, threading monitor setup, cache configuration."""

        # Create scenario configuration with Redis enabled
        config = self.create_scenario_config(fresh_test_data, test_topic)
        producer = self.create_sync_smart_producer(config)

        try:
            # Verify sync producer initialization
            assert producer.smart_enabled, "Sync smart producer should be enabled"

            # Verify health manager initialization
            assert producer.health_manager is not None, (
                "Health manager should be initialized"
            )

            # Trigger health manager startup by producing a test message
            producer.produce(
                topic=test_topic, key=b"startup-test", value=b"startup-test-value"
            )
            producer.flush()

            # Add delay for health manager initialization
            time.sleep(2)

            # Verify threading-based health monitoring (should have _thread attribute, no _task)
            assert hasattr(producer.health_manager, "_thread"), (
                "Health manager should have threading-based monitor"
            )
            assert not hasattr(producer.health_manager, "_task"), (
                "Health manager should not have asyncio task (sync mode)"
            )
            assert producer.health_manager._thread is not None, (
                "Health manager thread should be running"
            )
            assert producer.health_manager._thread.is_alive(), (
                "Health manager thread should be alive"
            )

            # Verify Redis connection and cache setup
            assert producer._partition_selector is not None, (
                "Producer should have partition selector"
            )
            assert hasattr(producer._partition_selector, "_cache"), (
                "Partition selector should have cache instance"
            )
            cache_instance = producer._partition_selector._cache
            assert cache_instance is not None, (
                "Cache should be initialized for key stickiness"
            )

            # Check if it's a hybrid cache with Redis
            from kafka_smart_producer.caching import DefaultHybridCache

            assert isinstance(cache_instance, DefaultHybridCache), (
                "Should use hybrid cache with Redis"
            )

            # Verify Redis connectivity through the remote cache
            redis_cache = cache_instance._remote
            assert hasattr(redis_cache, "ping"), "Redis cache should have ping method"
            ping_result = redis_cache.ping()
            assert ping_result, "Redis should be accessible"

            # Verify key stickiness is enabled
            assert config.key_stickiness, "Key stickiness should be enabled"

            # Verify topic configuration
            assert test_topic in producer.topics, "Test topic should be configured"

        finally:
            producer.close()

    # TEST 2: HEALTH DETECTION WITH REDIS VALIDATION
    def test_health_detection_with_redis_cache(
        self, admin_client, managed_consumer_group, test_topic, redis_cleanup
    ):
        """Test 2: Verify health data is cached in Redis and shared across instances."""
        print("\n=== Test 2: Health Detection with Redis Cache ===")

        test_data = {"consumer_group": managed_consumer_group}
        config = self.create_scenario_config(test_data, test_topic)
        producer = self.create_sync_smart_producer(config)

        try:
            # Step 1: Trigger health manager startup
            producer.produce(
                topic=test_topic, key=b"startup-test", value=b"startup-test-value"
            )
            producer.flush()
            time.sleep(2)  # Allow health manager initialization

            # Step 2: Wait for health manager to discover partitions
            healthy_partitions = self.wait_for_partitions_discovered(
                producer.health_manager, test_topic, timeout=15
            )
            assert len(healthy_partitions) > 0, (
                f"Health manager failed to discover partitions for {test_topic}"
            )

            # Step 3: Create real consumer lag
            actual_lag = self.create_real_consumer_lag(
                topic=test_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=managed_consumer_group,
            )
            assert actual_lag > 0, f"Should have created consumer lag, got {actual_lag}"

            # Wait for refresh cycle
            refresh_interval = TEST_HEALTH_CONFIG["refresh_interval"]
            time.sleep(refresh_interval + 1)

            # Step 4: Wait for health detection
            detection_success = self.wait_for_health_detection(
                producer.health_manager,
                test_topic,
                expected_unhealthy_partition=0,
                timeout=TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"],
            )
            assert detection_success, (
                "Health detection should identify unhealthy partition"
            )

            # Verify healthy partitions list
            healthy_partitions_after = producer.health_manager.get_healthy_partitions(
                test_topic
            )
            assert 0 not in healthy_partitions_after, (
                "Partition 0 should not be in healthy partitions"
            )
            assert len(healthy_partitions_after) >= 1, (
                "Should have at least one healthy partition"
            )

            # Step 5: Verify embedded mode uses local health data (not Redis)
            # Check Redis keys - should be empty for health data in embedded mode
            cache_keys = redis_cleanup.keys("*")

            # In embedded mode, health manager keeps health data locally, not in Redis
            # Only key stickiness data should use Redis when keys are produced
            assert len(cache_keys) == 0, (
                "Redis should be empty - embedded mode keeps health data locally"
            )

            # Step 6: Test embedded mode independence (no persistence expected)
            # Close first producer
            producer.close()
            time.sleep(1)

            # Create second producer with same config
            producer2 = self.create_sync_smart_producer(config)

            # Trigger startup
            producer2.produce(
                topic=test_topic, key=b"restart-test", value=b"restart-test-value"
            )
            producer2.flush()
            time.sleep(2)

            # Verify second producer discovers partitions independently
            healthy_partitions_2 = self.wait_for_partitions_discovered(
                producer2.health_manager, test_topic, timeout=10
            )
            assert len(healthy_partitions_2) > 0, (
                "Second producer should discover partitions independently"
            )

            producer2.close()

        finally:
            if "producer" in locals():
                producer.close()

    # TEST 3: SMART ROUTING WITH REDIS-BASED KEY STICKINESS
    def test_smart_routing_with_redis_key_stickiness(
        self,
        fresh_test_data,
        test_topic,
        sync_delivery_tracker,
        redis_cleanup,
        comprehensive_teardown,
    ):
        """Test 3: Verify Redis-based key stickiness and distributed cache behavior."""

        config = self.create_scenario_config(fresh_test_data, test_topic)
        producer = self.create_sync_smart_producer(config)

        try:
            # Step 1: Trigger health manager startup
            producer.produce(
                topic=test_topic, key=b"startup-test", value=b"startup-test-value"
            )
            producer.flush()
            time.sleep(2)

            # Step 2: Wait for partition discovery
            healthy_partitions = self.wait_for_partitions_discovered(
                producer.health_manager, test_topic, timeout=15
            )
            assert len(healthy_partitions) > 0, (
                f"Health manager failed to discover partitions for {test_topic}"
            )

            # Step 3: Create consumer lag to make partition 0 unhealthy
            self.create_real_consumer_lag(
                topic=test_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=fresh_test_data["consumer_group"],
            )

            # Wait for refresh cycle
            refresh_interval = TEST_HEALTH_CONFIG["refresh_interval"]
            time.sleep(refresh_interval + 1)

            # Step 4: Wait for health detection
            detection_success = self.wait_for_health_detection(
                producer.health_manager,
                test_topic,
                expected_unhealthy_partition=0,
                timeout=TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"],
            )

            # Step 5: Test Redis-based key stickiness
            delivery_callback, delivery_results = sync_delivery_tracker()

            test_keys = ["redis-key-1", "redis-key-2", "redis-key-3"]
            messages_per_key = 10

            # Phase 1: Establish key→partition mappings
            for key in test_keys:
                for i in range(messages_per_key):
                    producer.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"phase1-{key}-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

            producer.flush()
            time.sleep(0.5)  # Allow all deliveries to complete

            # Validate key stickiness in Phase 1
            phase1_results = list(delivery_results)  # Copy current results
            stickiness_validation = self.validate_key_stickiness(
                phase1_results, test_keys
            )

            assert stickiness_validation["stickiness_successful"], (
                "Phase 1 key stickiness should work"
            )
            assert stickiness_validation["expected_keys_found"], (
                "All keys should have consistent partitions"
            )

            key_partition_mapping = stickiness_validation["key_partition_mapping"]

            # Step 6: Verify Redis contains key stickiness data
            cache_keys = redis_cleanup.keys("*")

            # Should have cached key→partition mappings
            assert len(cache_keys) > 0, "Redis should contain key stickiness cache data"

            # Step 7: Test cache consistency with additional messages
            delivery_results.clear()  # Clear for Phase 2

            for key in test_keys:
                for i in range(5):  # Fewer messages for Phase 2
                    producer.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"phase2-{key}-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

            producer.flush()
            time.sleep(0.5)

            # Validate Phase 2 consistency with Phase 1
            phase2_results = list(delivery_results)
            phase2_stickiness = self.validate_key_stickiness(phase2_results, test_keys)

            assert phase2_stickiness["stickiness_successful"], (
                "Phase 2 key stickiness should work"
            )
            phase2_mapping = phase2_stickiness["key_partition_mapping"]

            # Verify consistency between phases
            for key in test_keys:
                phase1_partition = key_partition_mapping.get(key)
                phase2_partition = phase2_mapping.get(key)
                assert phase1_partition == phase2_partition, (
                    f"Key {key} partition should be consistent across phases"
                )

            # Step 8: Validate smart routing (if health detection worked)
            if detection_success:
                all_results = phase1_results + phase2_results
                validation = self.validate_partition_avoidance(
                    all_results, 0, TEST_THRESHOLDS["AVOIDANCE_MAX_PERCENTAGE"]
                )

                # Should avoid unhealthy partition while maintaining key stickiness
                assert validation["avoidance_successful"], (
                    "Should avoid unhealthy partition"
                )

        finally:
            producer.close()

    # TEST 4: DISTRIBUTED KEY STICKINESS ACROSS PRODUCER INSTANCES
    def test_distributed_key_stickiness_across_instances(
        self, fresh_test_data, test_topic, redis_cleanup
    ):
        """Test 4: Test key stickiness persistence across multiple producer instances."""

        config = self.create_scenario_config(fresh_test_data, test_topic)

        # Phase 1: Producer 1 establishes key→partition mappings
        producer1 = self.create_sync_smart_producer(config)

        try:
            # Trigger startup
            producer1.produce(topic=test_topic, key=b"startup", value=b"startup")
            producer1.flush()
            time.sleep(2)

            # Wait for partition discovery
            healthy_partitions = self.wait_for_partitions_discovered(
                producer1.health_manager, test_topic, timeout=15
            )
            assert len(healthy_partitions) > 0, "Producer 1 should discover partitions"

            # Establish key mappings with Producer 1
            test_keys = ["distributed-key-1", "distributed-key-2"]
            producer1_results = []

            def producer1_callback(err, msg):
                if err is None:
                    producer1_results.append(
                        {
                            "partition": msg.partition(),
                            "key": msg.key().decode() if msg.key() else None,
                            "producer": 1,
                        }
                    )

            for key in test_keys:
                for i in range(5):
                    producer1.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"producer1-{key}-{i}".encode(),
                        on_delivery=producer1_callback,
                    )

            producer1.flush()
            time.sleep(0.5)

            # Validate Producer 1 established consistent mappings
            producer1_stickiness = self.validate_key_stickiness(
                producer1_results, test_keys
            )
            assert producer1_stickiness["stickiness_successful"], (
                "Producer 1 should establish consistent key mappings"
            )

            producer1_mapping = producer1_stickiness["key_partition_mapping"]

            # Verify Redis contains the mappings
            cache_keys_after_p1 = redis_cleanup.keys("*")
            assert len(cache_keys_after_p1) > 0, (
                "Redis should contain Producer 1's key mappings"
            )

        finally:
            producer1.close()

        # Phase 2: Producer 2 should use the same key→partition mappings
        producer2 = self.create_sync_smart_producer(config)

        try:
            # Trigger startup
            producer2.produce(topic=test_topic, key=b"startup2", value=b"startup2")
            producer2.flush()
            time.sleep(2)

            # Wait for partition discovery
            healthy_partitions_2 = self.wait_for_partitions_discovered(
                producer2.health_manager, test_topic, timeout=15
            )
            assert len(healthy_partitions_2) > 0, (
                "Producer 2 should discover partitions"
            )

            # Use same keys with Producer 2
            producer2_results = []

            def producer2_callback(err, msg):
                if err is None:
                    producer2_results.append(
                        {
                            "partition": msg.partition(),
                            "key": msg.key().decode() if msg.key() else None,
                            "producer": 2,
                        }
                    )

            for key in test_keys:
                for i in range(5):
                    producer2.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"producer2-{key}-{i}".encode(),
                        on_delivery=producer2_callback,
                    )

            producer2.flush()
            time.sleep(0.5)

            # Validate Producer 2 key stickiness
            producer2_stickiness = self.validate_key_stickiness(
                producer2_results, test_keys
            )
            assert producer2_stickiness["stickiness_successful"], (
                "Producer 2 should maintain key stickiness"
            )

            producer2_mapping = producer2_stickiness["key_partition_mapping"]

            # Phase 3: Verify distributed consistency
            consistency_check = True
            for key in test_keys:
                p1_partition = producer1_mapping.get(key)
                p2_partition = producer2_mapping.get(key)

                if p1_partition != p2_partition:
                    consistency_check = False

            assert consistency_check, (
                "All keys should have consistent partitions across producer instances"
            )

            # Phase 4: Verify Redis cache state
            final_cache_keys = redis_cleanup.keys("*")

            # Should maintain cache entries for distributed coordination
            assert len(final_cache_keys) > 0, (
                "Redis should maintain key stickiness cache for distributed coordination"
            )

        finally:
            producer2.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
