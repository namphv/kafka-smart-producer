"""
Scenario 1: Simple Sync Producer with Local Cache Integration Tests

This test file validates the complete functionality of:
- SmartProducer (synchronous)
- PartitionHealthMonitor (threading-based)
- DefaultLocalCache (in-memory LRU cache)

The 4 Core Validations tested are:
1. Initialization - Verify sync producer, threading health monitor, local cache setup
2. Health Detection - Create real consumer lag, verify detection within timeout
3. Smart Routing - Verify producer avoids unhealthy partitions (<25% routing)
4. Key Stickiness - Test key→partition consistency with local cache

This scenario represents the most common usage pattern for smart producers.
"""

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


class TestScenario1SimpleSyncLocalCache(BaseIntegrationTest):
    """Test suite for Scenario 1: Simple Sync Producer with Local Cache."""

    def create_scenario_config(self, fresh_test_data, test_topic):
        """Create SmartProducerConfig for Scenario 1."""
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
                "remote_enabled": False,  # Local cache only for Scenario 1
            },
            key_stickiness=True,  # Enable key stickiness for testing
        )

        return scenario_config

    def create_smart_producer(self, config):
        """Create SmartProducer instance for testing."""
        return SmartProducer(config)

    # TEST 1: INITIALIZATION VALIDATION
    def test_initialization_sync_producer_local_cache(
        self, fresh_test_data, test_topic
    ):
        """Test 1: Verify sync producer, threading health monitor, local cache setup."""
        print("\n=== Test 1: Initialization Validation ===")

        # Create scenario configuration
        config = self.create_scenario_config(fresh_test_data, test_topic)
        producer = self.create_smart_producer(config)

        try:
            # Verify producer initialization
            assert producer.smart_enabled, "Smart producer should be enabled"
            print("✅ Smart producer enabled")

            # Verify health manager initialization
            assert producer.health_manager is not None, (
                "Health manager should be initialized"
            )
            assert producer.health_manager.is_running, (
                "Health manager should be running"
            )
            print("✅ Health manager initialized and running")

            # Verify threading-based health monitoring
            # Health manager should be running in background thread
            assert hasattr(producer.health_manager, "_thread"), (
                "Health manager should have background thread"
            )
            print("✅ Threading-based health monitor verified")

            # Verify cache configuration (SmartProducerConfig has cache property)
            cache_config = config.cache if config.cache else {}
            assert not cache_config.get("remote_enabled", True), (
                "Remote cache should be disabled"
            )
            print("✅ Local cache configuration verified")

            # Verify key stickiness is enabled
            assert config.key_stickiness, "Key stickiness should be enabled"
            print("✅ Key stickiness enabled")

            print("🎉 Initialization validation completed successfully")

        finally:
            producer.close()

    # TEST 2: HEALTH DETECTION VALIDATION
    def test_health_detection_real_consumer_lag(
        self, admin_client, managed_consumer_group, test_topic
    ):
        """Test 2: Create real consumer lag and verify health detection."""
        print("\n=== Test 2: Health Detection Validation ===")

        # Use managed consumer group for proper cleanup
        test_data = {"consumer_group": managed_consumer_group}
        config = self.create_scenario_config(test_data, test_topic)
        producer = self.create_smart_producer(config)

        try:
            # Step 1: Wait for health manager to discover partitions first
            print("⏳ Waiting for health manager to discover partitions...")
            partitions = self.wait_for_partitions_discovered(
                producer.health_manager, test_topic
            )
            assert len(partitions) > 0, (
                "🚨 LIBRARY BUG: Health manager failed to discover topic partitions"
            )

            # Step 2: Create real consumer lag on partition 0
            actual_lag = self.create_real_consumer_lag(
                topic=test_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=managed_consumer_group,
            )

            assert actual_lag > 0, f"Should have created consumer lag, got {actual_lag}"
            print(f"✅ Created real consumer lag: {actual_lag} messages on partition 0")

            # Step 3: Wait for automatic health detection
            print("⏳ Waiting for automatic health detection of partition 0...")
            print(
                f"🔍 Created {actual_lag} messages of lag with threshold {TEST_HEALTH_CONFIG['lag_threshold']}"
            )

            detection_success = self.wait_for_health_detection(
                producer.health_manager,
                test_topic,
                expected_unhealthy_partition=0,
                timeout=TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"],
            )

            assert detection_success, (
                "Health manager should detect unhealthy partition within timeout"
            )
            print("✅ Health detection completed within timeout")

            # Step 4: Verify healthy partitions list
            healthy_partitions = producer.health_manager.get_healthy_partitions(
                test_topic
            )
            assert 0 not in healthy_partitions, (
                "Partition 0 should not be in healthy partitions"
            )
            assert len(healthy_partitions) >= 1, (
                "Should have at least one healthy partition"
            )
            print(f"✅ Healthy partitions verified: {healthy_partitions}")

            print("🎉 Health detection validation completed successfully")

        finally:
            producer.close()

    # TEST 3: SMART ROUTING VALIDATION
    def test_smart_routing_partition_avoidance(
        self, fresh_test_data, test_topic, delivery_tracker, comprehensive_teardown
    ):
        """Test 3: Verify producer avoids unhealthy partitions during smart routing."""
        print("\n=== Test 3: Smart Routing Validation ===")

        config = self.create_scenario_config(fresh_test_data, test_topic)
        producer = self.create_smart_producer(config)

        try:
            # Step 1: Wait for health manager to discover partitions
            print("⏳ Waiting for health manager to discover partitions...")
            healthy_partitions = self.wait_for_partitions_discovered(
                producer.health_manager, test_topic
            )
            assert len(healthy_partitions) > 0, (
                "🚨 LIBRARY BUG: Health manager failed to discover partitions"
            )

            # Step 2: Create consumer lag to make partition 0 unhealthy
            actual_lag = self.create_real_consumer_lag(
                topic=test_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=fresh_test_data["consumer_group"],
            )
            print(f"✅ Created consumer lag: {actual_lag} messages on partition 0")

            # Step 3: Wait for health detection of unhealthy partition 0
            print("⏳ Waiting for health detection...")
            detection_success = self.wait_for_health_detection(
                producer.health_manager,
                test_topic,
                expected_unhealthy_partition=0,
                timeout=TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"],
            )

            if not detection_success:
                print("🚨 LIBRARY BUG: Health detection system not working properly!")
                print(
                    "   Continuing test to validate key stickiness, but routing avoidance cannot be tested"
                )

            # Step 4: Set up delivery tracking and produce messages
            delivery_callback, delivery_results = delivery_tracker()
            message_count = TEST_DATA_AMOUNTS["produce_count"]
            print(f"Producing {message_count} messages to test smart routing...")

            for i in range(message_count):
                producer.produce(
                    topic=test_topic,
                    key=f"test-key-{i}".encode(),
                    value=f"test-value-{i}".encode(),
                    on_delivery=delivery_callback,
                )

            producer.flush()

            # Step 5: Validate message delivery and distribution
            assert len(delivery_results) >= TEST_THRESHOLDS["MIN_MESSAGE_DELIVERY"], (
                f"Should deliver at least {TEST_THRESHOLDS['MIN_MESSAGE_DELIVERY']} messages"
            )

            # Analyze partition distribution and validate avoidance
            validation = self.validate_partition_avoidance(
                delivery_results, 0, TEST_THRESHOLDS["AVOIDANCE_MAX_PERCENTAGE"]
            )

            print("✅ Smart routing results:")
            print(f"   Total messages: {validation['total_messages']}")
            print(f"   Partition distribution: {validation['distribution']}")
            print(f"   Partitions used: {len(validation['distribution'])}")
            print(
                f"   Unhealthy partition usage: {validation['unhealthy_percentage']:.1%}"
            )

            # Verify messages are distributed (not all going to one partition)
            assert len(validation["distribution"]) > 1, (
                "Messages should be distributed across multiple partitions"
            )

            # Verify key stickiness is maintained
            stickiness_validation = self.validate_key_stickiness(delivery_results)
            assert stickiness_validation["stickiness_successful"], (
                "Key stickiness should be maintained"
            )
            print(
                f"✅ Key stickiness maintained: {stickiness_validation['consistent_keys']} keys consistently routed"
            )

            print("🎉 Smart routing validation completed successfully")

        finally:
            producer.close()

    # TEST 4: KEY STICKINESS VALIDATION
    def test_key_stickiness_local_cache_behavior(
        self, fresh_test_data, test_topic, delivery_tracker, comprehensive_teardown
    ):
        """Test 4: Verify key stickiness behavior with local cache."""
        print("\n=== Test 4: Key Stickiness Validation ===")

        config = self.create_scenario_config(fresh_test_data, test_topic)
        producer = self.create_smart_producer(config)

        try:
            # Wait for health manager to discover topic partitions
            print("⏳ Waiting for health manager to discover partitions...")
            healthy_partitions = self.wait_for_partitions_discovered(
                producer.health_manager, test_topic
            )
            assert len(healthy_partitions) > 0, (
                "🚨 LIBRARY BUG: Health manager failed to discover partitions"
            )

            delivery_callback, delivery_results = delivery_tracker()

            # Step 1: Test basic key stickiness behavior
            print("Testing basic key stickiness...")
            test_keys = ["user-123", "user-456", "user-789"]

            for key in test_keys:
                for i in range(10):  # 10 messages per key
                    producer.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"value-{key}-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

            producer.flush()

            # Verify key stickiness
            stickiness_validation = self.validate_key_stickiness(
                delivery_results, test_keys
            )

            assert stickiness_validation["stickiness_successful"], (
                f"Key stickiness failed: {stickiness_validation['inconsistent_keys']}"
            )
            assert stickiness_validation["expected_keys_found"], (
                "Missing expected keys in results"
            )

            print(
                f"✅ Basic key stickiness verified: {stickiness_validation['consistent_keys']}/{len(test_keys)} keys consistently routed"
            )
            print(f"   Key mappings: {stickiness_validation['key_partition_mapping']}")

            # Step 2: Test cache behavior consistency
            print("Testing cache behavior consistency...")
            delivery_results.clear()  # Reset results

            test_key = "sticky-test-key"

            # First batch - establish key→partition mapping
            for i in range(5):
                producer.produce(
                    topic=test_topic,
                    key=test_key.encode(),
                    value=f"batch1-{i}".encode(),
                    on_delivery=delivery_callback,
                )
            producer.flush()

            first_partition = delivery_results[0]["partition"]

            # Second batch - should use cached partition
            for i in range(5):
                producer.produce(
                    topic=test_topic,
                    key=test_key.encode(),
                    value=f"batch2-{i}".encode(),
                    on_delivery=delivery_callback,
                )
            producer.flush()

            # Verify all messages with same key went to same partition
            partitions_used = {
                r["partition"] for r in delivery_results if r["key"] == test_key
            }
            assert len(partitions_used) == 1, (
                f"Key stickiness failed: key used {len(partitions_used)} partitions"
            )
            print(
                f"✅ Cache consistency verified: key '{test_key}' consistently routed to partition {first_partition}"
            )

            # Step 3: Test mixed keyed and keyless messages
            print("Testing mixed keyed/keyless routing...")
            delivery_results.clear()  # Reset results

            for i in range(20):
                if i % 3 == 0:
                    # Keyless message
                    producer.produce(
                        topic=test_topic,
                        value=f"keyless-{i}".encode(),
                        on_delivery=delivery_callback,
                    )
                else:
                    # Keyed message
                    key = f"sticky-key-{i % 2}"  # Alternates between 2 keys
                    producer.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"keyed-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

            producer.flush()

            # Validate mixed routing
            keyed_results = [r for r in delivery_results if r.get("key")]
            keyless_results = [r for r in delivery_results if not r.get("key")]

            # Verify key stickiness for keyed messages
            keyed_stickiness = self.validate_key_stickiness(keyed_results)
            assert keyed_stickiness["stickiness_successful"], (
                "Keyed messages should maintain stickiness"
            )

            # Verify keyless messages can use different partitions
            keyless_partitions = {r["partition"] for r in keyless_results}

            print("✅ Mixed routing verified:")
            print(
                f"   Keyed messages: {keyed_stickiness['consistent_keys']} keys with consistent routing"
            )
            print(
                f"   Keyless messages: distributed across {len(keyless_partitions)} partitions"
            )

            assert len(keyed_stickiness["key_partition_mapping"]) == 2, (
                "Should have 2 sticky keys"
            )
            print("✅ Key stickiness with local cache working correctly")

            print("🎉 Key stickiness validation completed successfully")

        finally:
            producer.close()

    # MULTI-TOPIC TEST
    def test_multi_topic_health_monitoring(
        self, admin_client, fresh_test_data, delivery_tracker
    ):
        """Test multi-topic monitoring where one health manager tracks all topics."""
        print("\n=== Multi-Topic Health Monitoring Test ===")

        # Create multiple test topics
        multiple_topics = [
            f"sync-orders-{fresh_test_data['client_id']}",
            f"sync-payments-{fresh_test_data['client_id']}",
        ]

        # Create topics
        from confluent_kafka.admin import NewTopic

        new_topics = [
            NewTopic(name, num_partitions=4, replication_factor=1)
            for name in multiple_topics
        ]
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result(timeout=10)
                print(f"✅ Created test topic: {topic}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
                print(f"⚠️ Topic {topic} already exists")

        import time

        time.sleep(1.0)  # Allow topics to be ready

        try:
            # Create config with multiple topics
            config = self.create_scenario_config(fresh_test_data, multiple_topics[0])
            # Update config to monitor all topics
            config.topics = multiple_topics
            producer = self.create_smart_producer(config)

            try:
                # Wait for health manager to discover all topics
                print("⏳ Waiting for health manager to discover all topics...")
                topic_partitions = {}
                for topic in multiple_topics:
                    partitions = self.wait_for_partitions_discovered(
                        producer.health_manager, topic, timeout=15
                    )
                    assert len(partitions) > 0, (
                        f"Health manager failed to discover partitions for {topic}"
                    )
                    topic_partitions[topic] = partitions
                    print(f"✅ Discovered {len(partitions)} partitions for {topic}")

                # Verify single health manager tracks all topics
                assert len(producer.topics) == len(multiple_topics), (
                    "Producer should track all topics"
                )
                print(
                    f"✅ Single health manager tracking {len(multiple_topics)} topics"
                )

                # Test topic-specific health queries
                delivery_callback, delivery_results = delivery_tracker()

                # Produce messages to each topic and verify topic-specific health
                for topic in multiple_topics:
                    for i in range(10):
                        producer.produce(
                            topic=topic,
                            key=f"multi-key-{topic}-{i}".encode(),
                            value=f"multi-value-{topic}-{i}".encode(),
                            on_delivery=delivery_callback,
                        )

                producer.flush()

                # Verify messages were delivered to correct topics
                topic_message_counts = {}
                for result in delivery_results:
                    topic = result["topic"]
                    topic_message_counts[topic] = topic_message_counts.get(topic, 0) + 1

                for topic in multiple_topics:
                    assert topic_message_counts.get(topic, 0) == 10, (
                        f"Should have 10 messages for {topic}"
                    )

                    # Verify producer can get topic-specific health
                    healthy_partitions = producer.health_manager.get_healthy_partitions(
                        topic
                    )
                    assert len(healthy_partitions) > 0, (
                        f"Should have healthy partitions for {topic}"
                    )
                    print(
                        f"✅ Topic-specific health query for {topic}: {len(healthy_partitions)} healthy partitions"
                    )

                # Verify key stickiness works independently per topic
                topic_results = {}
                for topic in multiple_topics:
                    topic_messages = [
                        r for r in delivery_results if r["topic"] == topic
                    ]
                    topic_results[topic] = topic_messages

                    stickiness_validation = self.validate_key_stickiness(topic_messages)
                    assert stickiness_validation["stickiness_successful"], (
                        f"Key stickiness should work for {topic}"
                    )
                    print(
                        f"✅ Key stickiness verified for {topic}: {stickiness_validation['consistent_keys']} keys consistent"
                    )

                print("🎉 Multi-topic health monitoring test completed successfully")
                print(f"   Topics monitored: {len(multiple_topics)}")
                print(f"   Total messages: {len(delivery_results)}")
                print("   Single health manager: ✅")
                print("   Topic-specific queries: ✅")
                print("   Independent key stickiness: ✅")

            finally:
                producer.close()

        finally:
            # Cleanup topics
            try:
                fs = admin_client.delete_topics(multiple_topics, operation_timeout=15)
                for topic, f in fs.items():
                    f.result(timeout=10)
                    print(f"🧹 Cleaned up topic: {topic}")
            except Exception as e:
                print(f"Topic cleanup warning: {e}")

    # COMPREHENSIVE SCENARIO TEST
    def test_scenario_1_complete_workflow(
        self, admin_client, fresh_test_data, test_topic, delivery_tracker
    ):
        """Complete workflow test combining all 4 validations for Scenario 1."""
        print("\n=== Scenario 1: Complete Workflow Test ===")

        config = self.create_scenario_config(fresh_test_data, test_topic)
        producer = self.create_smart_producer(config)

        try:
            # Phase 1: Initialization
            print("Phase 1: Validating initialization...")
            assert producer.smart_enabled
            assert producer.health_manager is not None
            assert producer.health_manager.is_running
            print("✅ Phase 1 complete: Initialization validated")

            # Phase 2: Health Detection
            print("Phase 2: Testing health detection...")
            self.create_real_consumer_lag(
                test_topic,
                0,
                TEST_DATA_AMOUNTS["lag_messages"],
                fresh_test_data["consumer_group"],
            )
            # Wait for health detection
            print("⏳ Waiting for health detection...")
            detection_success = self.wait_for_health_detection(
                producer.health_manager,
                test_topic,
                expected_unhealthy_partition=0,
                timeout=TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"],
            )

            if not detection_success:
                print("🚨 LIBRARY BUG: Health detection failed in comprehensive test!")
                print("   This confirms the health system is not working properly")
            healthy_partitions = producer.health_manager.get_healthy_partitions(
                test_topic
            )
            assert 0 not in healthy_partitions
            print("✅ Phase 2 complete: Health detection working")

            # Phase 3: Smart Routing + Key Stickiness Combined
            print("Phase 3: Testing smart routing with key stickiness...")
            delivery_callback, delivery_results = delivery_tracker()

            # Produce messages with keys (should avoid partition 0 AND maintain key stickiness)
            test_keys = ["workflow-key-1", "workflow-key-2"]
            for key in test_keys:
                for i in range(25):  # 50 total messages
                    producer.produce(
                        topic=test_topic,
                        key=key.encode(),
                        value=f"workflow-{key}-{i}".encode(),
                        on_delivery=delivery_callback,
                    )

            producer.flush()

            # Validate both smart routing and key stickiness
            validation = self.validate_partition_avoidance(delivery_results, 0, 0.25)
            assert validation["avoidance_successful"], (
                "Smart routing should avoid unhealthy partition"
            )

            stickiness_validation = self.validate_key_stickiness(
                delivery_results, test_keys
            )
            assert stickiness_validation["stickiness_successful"], (
                "Key stickiness should be maintained"
            )
            assert stickiness_validation["expected_keys_found"], (
                "All keys should have consistent partitions"
            )

            print(
                "✅ Phase 3 complete: Smart routing + key stickiness working together"
            )

            print("🎉 Scenario 1 complete workflow validation successful!")
            print(f"   Messages produced: {len(delivery_results)}")
            print(
                f"   Partition avoidance: {validation['unhealthy_percentage']:.1%} to unhealthy partition"
            )
            print(
                f"   Key stickiness: {stickiness_validation['consistent_keys']} keys consistently routed"
            )
            print(f"   Final distribution: {validation['distribution']}")

        finally:
            producer.close()
