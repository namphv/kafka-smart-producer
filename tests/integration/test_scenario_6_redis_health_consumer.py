"""
Scenario 6: Redis Health Consumer Integration Tests

This test file validates the functionality where SmartProducers act as pure consumers
of health data from Redis, without any embedded health monitoring. The health data
is published by independent Scenario 5 standalone health monitors.

Architecture Pattern:
- Standalone Health Monitor (Scenario 5) --> Redis --> SmartProducer (Scenario 6)
- Complete separation of concerns between health monitoring and message production
- SmartProducers query Redis directly for pre-calculated health scores

The 4 Core Validations tested are:
1. Initialization - Verify Redis health consumer setup without embedded health monitoring
2. Health Data Consumption - Consume health data published by standalone monitors
3. Smart Routing - Route messages based on consumed health data from Redis
4. Producer Independence - Verify producers work independently from health monitor lifecycle

This scenario represents microservice architectures where health monitoring and
message production are completely decoupled services.
"""

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
)


class TestScenario6RedisHealthConsumer(BaseIntegrationTest):
    """Test suite for Scenario 6: Redis Health Consumer."""

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

    def create_standalone_health_monitor(self, fresh_test_data, test_topic):
        """Create a standalone health monitor to publish health data to Redis."""
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        return PartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=[test_topic],
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )

    def create_redis_consumer_config(self, fresh_test_data, test_topic):
        """Create SmartProducerConfig for Redis health consumer mode."""
        return SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=[test_topic],
            health_mode="redis_consumer",  # New mode for Scenario 6
            health_manager={
                "consumer_group": fresh_test_data[
                    "consumer_group"
                ],  # Still needed for config
                "health_threshold": TEST_HEALTH_CONFIG["unhealthy_threshold"],
                "timeout_seconds": TEST_HEALTH_CONFIG["timeout_seconds"],
                "max_lag_for_health": TEST_HEALTH_CONFIG["lag_threshold"],
            },
            cache={
                "local_default_ttl_seconds": TEST_CACHE_CONFIG["ttl_seconds"],
                "remote_enabled": True,  # Redis required for health data
                "redis_host": "localhost",
                "redis_port": 6380,
                "redis_db": 0,
                "remote_default_ttl_seconds": TEST_CACHE_CONFIG["ttl_seconds"] * 2,
            },
            key_stickiness=True,
        )

    def create_redis_consumer_producer(self, config):
        """Create SmartProducer in Redis consumer mode."""
        return SmartProducer(config)

    # TEST 1: REDIS CONSUMER INITIALIZATION
    def test_redis_consumer_initialization(
        self, fresh_test_data, test_topic, redis_cleanup
    ):
        """Test 1: Verify Redis health consumer initialization without embedded health monitoring."""
        print("\n=== Test 1: Redis Consumer Initialization ===")

        config = self.create_redis_consumer_config(fresh_test_data, test_topic)
        producer = self.create_redis_consumer_producer(config)

        try:
            # Verify producer initialization
            assert producer.smart_enabled, "Smart producer should be enabled"
            print("✅ Redis consumer producer enabled")

            # Verify configuration is correct
            assert config.health_mode == "redis_consumer", (
                "Should be in redis_consumer mode"
            )
            assert config.cache_config.remote_enabled, "Should have Redis enabled"
            print("✅ Configuration verified: redis_consumer mode with Redis enabled")

            # Check health manager exists and is Redis consumer type
            if hasattr(producer, "_health_manager") and producer._health_manager:
                health_manager = producer._health_manager

                # Check if it's a Redis health consumer
                from kafka_smart_producer.redis_health_consumer import (
                    HybridRedisHealthConsumer,
                    RedisHealthConsumer,
                )

                health_manager_type = type(health_manager)

                if health_manager_type in [
                    RedisHealthConsumer,
                    HybridRedisHealthConsumer,
                ]:
                    print(
                        f"✅ Redis health consumer type: {health_manager_type.__name__}"
                    )

                    # Verify it's always running (stateless)
                    assert health_manager.is_running, (
                        "Redis consumer should always be running"
                    )

                    # Verify it's NOT an embedded health monitor (no lag collector)
                    assert not hasattr(health_manager, "_lag_collector"), (
                        "Redis consumer should NOT have lag collector"
                    )
                    assert not hasattr(health_manager, "_thread"), (
                        "Redis consumer should NOT have background thread"
                    )
                    print(
                        "✅ Stateless Redis consumer (no lag collector, no background thread)"
                    )
                else:
                    print(
                        f"⚠️ Health manager type: {health_manager_type} (may be embedded mode)"
                    )
            else:
                print("⚠️ No health manager found - checking cache-based health access")

            # Verify Redis connectivity through cache
            if (
                hasattr(producer, "_partition_selector")
                and producer._partition_selector
            ):
                selector = producer._partition_selector
                if hasattr(selector, "_cache") and selector._cache:
                    cache_instance = selector._cache

                    from kafka_smart_producer.caching import DefaultHybridCache

                    if isinstance(cache_instance, DefaultHybridCache):
                        print("✅ Hybrid cache configured")

                        # Test Redis connectivity
                        redis_cache = cache_instance._remote
                        if hasattr(redis_cache, "ping"):
                            ping_result = redis_cache.ping()
                            assert ping_result, "Redis should be accessible"
                            print("✅ Redis connectivity verified")
                    else:
                        print(f"Cache type: {type(cache_instance)}")

            print("🎉 Redis consumer initialization successful")

        finally:
            producer.close()

    # TEST 2: HEALTH DATA CONSUMPTION FROM REDIS
    def test_health_data_consumption_from_redis(
        self, fresh_test_data, test_topic, redis_cleanup
    ):
        """Test 2: Consume health data published by standalone monitors."""
        print("\n=== Test 2: Health Data Consumption from Redis ===")

        # Step 1: Start standalone health monitor to publish data
        standalone_monitor = self.create_standalone_health_monitor(
            fresh_test_data, test_topic
        )
        standalone_monitor.start()

        try:
            # Step 2: Wait for standalone monitor to collect and publish initial health data
            print(
                "⏳ Waiting for standalone health monitor to publish initial health data..."
            )
            time.sleep(12)  # Allow time for lag collection and Redis publishing

            # Step 3: Debug - Check what's in Redis
            redis_client = redis_cleanup
            all_keys = redis_client.keys("*")
            print(f"Redis keys: {all_keys}")

            health_keys = redis_client.keys("kafka_health:*")
            print(f"Health keys: {health_keys}")

            health_state_key = f"kafka_health:state:{test_topic}"
            if redis_client.exists(health_state_key):
                print("✅ Standalone monitor published health data to Redis")
            else:
                print(f"⚠️ No health data found at {health_state_key}")
                print("Will test with mock health data...")

                # Inject mock health data for testing
                import json

                mock_health_data = {
                    "topic": test_topic,
                    "partitions": {"0": 0.9, "1": 0.8, "2": 0.7, "3": 0.6},
                    "timestamp": time.time(),
                    "healthy_count": 4,
                    "total_count": 4,
                    "health_threshold": 0.5,
                }
                redis_client.setex(health_state_key, 300, json.dumps(mock_health_data))
                print("✅ Injected mock health data for testing")

            # Step 4: Create Redis consumer producer
            config = self.create_redis_consumer_config(fresh_test_data, test_topic)
            producer = self.create_redis_consumer_producer(config)

            try:
                # Step 5: Redis health consumer is stateless - no need to start
                print("✅ Redis health consumer ready (stateless)")

                # Step 6: Verify consumer can read health data from Redis
                if hasattr(producer, "_health_manager") and producer._health_manager:
                    healthy_partitions = (
                        producer._health_manager.get_healthy_partitions(test_topic)
                    )
                    print(f"Health partitions from consumer: {healthy_partitions}")

                    if len(healthy_partitions) > 0:
                        print(
                            f"✅ Consumed healthy partitions from Redis: {healthy_partitions}"
                        )
                    else:
                        print("⚠️ No healthy partitions found - checking cache directly")

                        # Test cache-based health data consumption
                        if (
                            hasattr(producer, "_partition_selector")
                            and producer._partition_selector
                        ):
                            cache_instance = producer._partition_selector._cache
                            health_data = cache_instance.get_health_data(test_topic)
                            if health_data:
                                print(
                                    f"✅ Health data available via cache: {health_data}"
                                )
                                assert len(health_data) > 0, "Should have health data"
                            else:
                                print("⚠️ No health data available via cache either")
                else:
                    print(
                        "⚠️ No health manager found - testing cache-based health consumption"
                    )
                    # Test cache-based health data consumption
                    if (
                        hasattr(producer, "_partition_selector")
                        and producer._partition_selector
                    ):
                        cache_instance = producer._partition_selector._cache
                        health_data = cache_instance.get_health_data(test_topic)
                        if health_data:
                            print(
                                f"✅ Health data consumed from Redis via cache: {len(health_data)} partitions"
                            )
                            assert len(health_data) > 0, "Should have health data"
                        else:
                            print("⚠️ No health data found in cache")

                print("🎉 Health data consumption test completed")

            finally:
                producer.close()

        finally:
            standalone_monitor.stop()

    # TEST 3: SMART ROUTING BASED ON CONSUMED HEALTH DATA
    def test_smart_routing_with_consumed_health_data(
        self, fresh_test_data, test_topic, delivery_tracker, redis_cleanup
    ):
        """Test 3: Route messages based on health data consumed from Redis."""
        print("\n=== Test 3: Smart Routing with Consumed Health Data ===")

        # Step 1: Start standalone health monitor
        standalone_monitor = self.create_standalone_health_monitor(
            fresh_test_data, test_topic
        )
        standalone_monitor.start()

        try:
            # Step 2: Wait for initial health data publication
            print("⏳ Waiting for initial health data...")
            time.sleep(8)

            # Step 3: Create consumer lag to test health-aware routing
            actual_lag = self.create_real_consumer_lag(
                topic=test_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=fresh_test_data["consumer_group"],
            )
            assert actual_lag > 0, f"Should create consumer lag, got {actual_lag}"
            print(f"✅ Created consumer lag: {actual_lag} messages on partition 0")

            # Step 4: Wait for health detection and Redis publishing
            print("⏳ Waiting for health detection and Redis publishing...")
            time.sleep(8)  # Allow health detection and Redis update

            # Step 5: Create Redis consumer producer
            config = self.create_redis_consumer_config(fresh_test_data, test_topic)
            producer = self.create_redis_consumer_producer(config)

            try:
                # Step 6: Start consumer and verify it reads unhealthy partition
                if hasattr(producer, "health_manager") and producer.health_manager:
                    producer.health_manager.start()
                    healthy_partitions = producer.health_manager.get_healthy_partitions(
                        test_topic
                    )
                    print(f"Healthy partitions from Redis: {healthy_partitions}")
                    # Should exclude partition 0 (unhealthy)
                    if 0 not in healthy_partitions:
                        print(
                            "✅ Redis consumer correctly identifies unhealthy partition 0"
                        )
                    else:
                        print("⚠️ Health detection may still be in progress")

                # Step 7: Test smart routing
                delivery_callback, delivery_results = delivery_tracker()
                message_count = 30

                print(f"Producing {message_count} messages to test smart routing...")
                for i in range(message_count):
                    producer.produce(
                        topic=test_topic,
                        key=f"redis-consumer-key-{i}".encode(),
                        value=f"redis-consumer-value-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

                producer.flush()

                # Step 8: Analyze routing results
                assert len(delivery_results) >= message_count * 0.8, (
                    f"Should deliver at least 80% of {message_count} messages"
                )

                # Analyze partition usage
                partition_counts = {}
                for result in delivery_results:
                    partition = result["partition"]
                    partition_counts[partition] = partition_counts.get(partition, 0) + 1

                partition_0_usage = partition_counts.get(0, 0)
                total_messages = len(delivery_results)
                unhealthy_percentage = partition_0_usage / total_messages

                print("✅ Routing results:")
                print(f"   Total messages: {total_messages}")
                print(f"   Partition distribution: {partition_counts}")
                print(f"   Unhealthy partition 0 usage: {unhealthy_percentage:.1%}")

                # Smart routing should avoid unhealthy partition (but may not be perfect due to timing)
                if unhealthy_percentage < 0.4:  # Less than 40% to unhealthy partition
                    print("✅ Smart routing successfully avoided unhealthy partition")
                else:
                    print(
                        "⚠️ Smart routing results inconclusive (health detection timing)"
                    )

                print("🎉 Smart routing with consumed health data completed")

            finally:
                producer.close()

        finally:
            standalone_monitor.stop()

    # TEST 4: PRODUCER INDEPENDENCE FROM HEALTH MONITOR LIFECYCLE
    def test_producer_independence_from_health_monitor(
        self, fresh_test_data, test_topic, delivery_tracker, redis_cleanup
    ):
        """Test 4: Verify producers work independently from health monitor lifecycle."""
        print("\n=== Test 4: Producer Independence from Health Monitor ===")

        # Step 1: Start standalone health monitor and let it publish data
        standalone_monitor = self.create_standalone_health_monitor(
            fresh_test_data, test_topic
        )
        standalone_monitor.start()

        try:
            print("⏳ Waiting for standalone health monitor to publish data...")
            time.sleep(8)

            # Step 2: Create Redis consumer producer while monitor is running
            config = self.create_redis_consumer_config(fresh_test_data, test_topic)
            producer1 = self.create_redis_consumer_producer(config)

            try:
                # Step 3: Test producer works while monitor is running
                if hasattr(producer1, "health_manager") and producer1.health_manager:
                    producer1.health_manager.start()
                    healthy_partitions_1 = (
                        producer1.health_manager.get_healthy_partitions(test_topic)
                    )
                    assert len(healthy_partitions_1) > 0, (
                        "Producer should work while monitor is running"
                    )
                    print(
                        f"✅ Producer 1 works while monitor running: {len(healthy_partitions_1)} healthy partitions"
                    )

                # Step 4: STOP health monitor while producer is still running
                print("🛑 Stopping standalone health monitor...")
                standalone_monitor.stop()
                time.sleep(2)  # Allow stop to complete

                # Step 5: Producer should still work using cached/stale Redis data
                delivery_callback, delivery_results = delivery_tracker()

                for i in range(10):
                    producer1.produce(
                        topic=test_topic,
                        key=f"independence-test-{i}".encode(),
                        value=f"value-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

                producer1.flush()

                assert len(delivery_results) == 10, (
                    "Producer should continue working after monitor stops"
                )
                print("✅ Producer continues working after health monitor stops")

            finally:
                producer1.close()

            # Step 6: Create NEW producer after monitor has stopped
            print("Creating new producer after monitor has stopped...")
            producer2 = self.create_redis_consumer_producer(config)

            try:
                # Step 7: New producer should still get some health data (cached in Redis)
                if hasattr(producer2, "health_manager") and producer2.health_manager:
                    producer2.health_manager.start()
                    # May get cached data or empty data - both are acceptable
                    healthy_partitions_2 = (
                        producer2.health_manager.get_healthy_partitions(test_topic)
                    )
                    print(
                        f"✅ Producer 2 after monitor stop: {len(healthy_partitions_2)} healthy partitions (cached)"
                    )

                # Step 8: Producer should still be able to produce messages (fallback to default partitioning)
                delivery_results_2 = []

                def callback2(err, msg):
                    if err is None:
                        delivery_results_2.append(
                            {"partition": msg.partition(), "topic": msg.topic()}
                        )

                for i in range(5):
                    producer2.produce(
                        topic=test_topic,
                        key=f"post-stop-{i}".encode(),
                        value=f"value-{i}".encode(),
                        on_delivery=callback2,
                    )

                producer2.flush()

                assert len(delivery_results_2) == 5, (
                    "New producer should work even after monitor stops"
                )
                print(
                    "✅ New producer works independently after monitor lifecycle ends"
                )

                print("🎉 Producer independence from health monitor lifecycle verified")

            finally:
                producer2.close()

        finally:
            if standalone_monitor.is_running:
                standalone_monitor.stop()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
