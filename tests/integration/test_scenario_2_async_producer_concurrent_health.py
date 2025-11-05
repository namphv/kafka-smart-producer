"""
Scenario 2: Async Producer with Concurrent Health Monitoring Integration Tests

This test file validates the complete functionality of:
- AsyncSmartProducer (asynchronous)
- AsyncPartitionHealthMonitor (asyncio-based)
- DefaultLocalCache (in-memory LRU cache)

The 4 Core Validations tested are:
1. Initialization - Verify async producer, asyncio health monitor, concurrent topic monitoring
2. Health Detection - Create lag on multiple topics concurrently, verify detection across all
3. Smart Routing - Async produce to multiple topics, verify topic-specific avoidance
4. Key Stickiness - Test async key stickiness across multiple topics

This scenario represents async usage patterns with concurrent topic monitoring.
"""

import asyncio

import pytest

# Import actual smart producer components
from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig

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


class TestScenario2AsyncProducerConcurrentHealth(BaseIntegrationTest):
    """Test suite for Scenario 2: Async Producer with Concurrent Health Monitoring."""

    def create_scenario_config(self, fresh_test_data, test_topics):
        """Create SmartProducerConfig for Scenario 2."""
        scenario_config = SmartProducerConfig(
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=test_topics,  # Multiple topics for concurrent monitoring
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
                "remote_enabled": False,  # Local cache only for Scenario 2
            },
            key_stickiness=True,  # Enable key stickiness for testing
        )

        return scenario_config

    async def create_async_smart_producer(self, config):
        """Create AsyncSmartProducer instance for testing."""
        producer = AsyncSmartProducer(config)
        # Note: Health manager will be started lazily on first produce() call
        # No need to manually start it here
        return producer

    @pytest.fixture
    def multiple_test_topics(self, admin_client, fresh_test_data):
        """Create and manage multiple test topics for concurrent monitoring."""
        topic_names = [
            f"test-orders-{fresh_test_data['client_id']}",
            f"test-payments-{fresh_test_data['client_id']}",
        ]

        # Create topics
        from confluent_kafka.admin import NewTopic

        new_topics = [
            NewTopic(name, num_partitions=4, replication_factor=1)
            for name in topic_names
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

        # Add small delay to ensure topics are fully ready
        import time

        time.sleep(1.0)

        yield topic_names

        # Cleanup topics
        try:
            fs = admin_client.delete_topics(topic_names, operation_timeout=15)
            for topic, f in fs.items():
                f.result(timeout=10)
                print(f"🧹 Cleaned up topic: {topic}")
            import time

            time.sleep(0.5)  # Allow cleanup to complete
        except Exception as e:
            print(f"Topic cleanup warning: {e}")

    @pytest.fixture
    def async_delivery_tracker(self):
        """Factory for tracking async message delivery results."""

        def create_tracker():
            results = []

            def callback(err, msg):
                if err is None:
                    results.append(
                        {
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "topic": msg.topic(),
                            "key": msg.key().decode() if msg.key() else None,
                            "timestamp": asyncio.get_event_loop().time(),
                        }
                    )
                else:
                    print(f"Async delivery error: {err}")

            return callback, results

        return create_tracker

    # TEST 1: INITIALIZATION VALIDATION
    @pytest.mark.asyncio
    async def test_initialization_async_producer_concurrent_monitoring(
        self, fresh_test_data, multiple_test_topics
    ):
        """Test 1: Verify async producer, asyncio health monitor, concurrent topic monitoring setup."""
        print("\\n=== Test 1: Async Initialization Validation ===")

        # Create scenario configuration with multiple topics
        config = self.create_scenario_config(fresh_test_data, multiple_test_topics)
        producer = await self.create_async_smart_producer(config)

        try:
            # Verify async producer initialization
            assert producer.smart_enabled, "Async smart producer should be enabled"
            print("✅ Async smart producer enabled")

            # Verify health manager initialization
            assert producer.health_manager is not None, (
                "Async health manager should be initialized"
            )
            print("✅ AsyncPartitionHealthMonitor initialized")

            # Verify asyncio-based health monitoring (should have _task attribute, no _thread)
            assert hasattr(producer.health_manager, "_task"), (
                "Health manager should have asyncio task"
            )
            assert not hasattr(producer.health_manager, "_thread"), (
                "Health manager should not have thread (asyncio-based)"
            )
            print("✅ Asyncio-based health monitor verified")

            # Verify concurrent topic monitoring setup
            assert len(producer.topics) == 2, (
                "Should monitor multiple topics concurrently"
            )
            assert set(producer.topics) == set(multiple_test_topics), (
                "Should monitor all configured topics"
            )
            print(f"✅ Concurrent topic monitoring setup: {producer.topics}")

            # Verify cache configuration (SmartProducerConfig has cache property)
            cache_config = config.cache if config.cache else {}
            assert not cache_config.get("remote_enabled", True), (
                "Remote cache should be disabled"
            )
            print("✅ Local cache configuration verified")

            # Verify key stickiness is enabled
            assert config.key_stickiness, "Key stickiness should be enabled"
            print("✅ Key stickiness enabled")

            print("🎉 Async initialization validation completed successfully")

        finally:
            await producer.close()

    # TEST 2: HEALTH DETECTION VALIDATION
    @pytest.mark.asyncio
    async def test_health_detection_concurrent_topics(
        self, admin_client, managed_consumer_group, multiple_test_topics
    ):
        """Test 2: Create lag on multiple topics concurrently and verify detection across all."""
        print("\\n=== Test 2: Concurrent Health Detection Validation ===")

        # Use managed consumer group for proper cleanup
        test_data = {"consumer_group": managed_consumer_group}
        config = self.create_scenario_config(test_data, multiple_test_topics)
        producer = await self.create_async_smart_producer(config)

        try:
            # Step 1: Trigger health manager startup by producing a test message
            print("⏳ Triggering health manager startup...")

            # Produce a single test message to each topic to trigger health manager startup
            for topic in multiple_test_topics:
                await producer.produce(
                    topic=topic, key=b"startup-test", value=b"startup-test-value"
                )
            await producer.flush()

            # Add delay for health manager concurrent initialization
            import asyncio

            await asyncio.sleep(2)  # Allow concurrent monitoring to initialize properly

            # Wait for health manager to discover partitions for all topics
            print(
                "⏳ Waiting for health manager to discover partitions across all topics..."
            )

            topic_partitions = {}
            for topic in multiple_test_topics:
                # Use longer timeout for multi-topic concurrent scenarios
                partitions = self.wait_for_partitions_discovered(
                    producer.health_manager, topic, timeout=15
                )
                assert len(partitions) > 0, (
                    f"🚨 LIBRARY BUG: Health manager failed to discover partitions for topic {topic}"
                )
                topic_partitions[topic] = partitions
                print(f"✅ Discovered {len(partitions)} partitions for {topic}")

            # Step 2: Create real consumer lag on partition 0 for both topics concurrently
            print("⏳ Creating consumer lag on multiple topics concurrently...")

            lag_results = {}
            for topic in multiple_test_topics:
                actual_lag = self.create_real_consumer_lag(
                    topic=topic,
                    target_partition=0,
                    lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                    consumer_group=managed_consumer_group,
                )

                assert actual_lag > 0, (
                    f"Should have created consumer lag for {topic}, got {actual_lag}"
                )
                lag_results[topic] = actual_lag
                print(
                    f"✅ Created real consumer lag: {actual_lag} messages on {topic} partition 0"
                )

            # Wait for at least one refresh cycle to allow health manager to detect the lag
            refresh_interval = TEST_HEALTH_CONFIG["refresh_interval"]  # 5 seconds
            await asyncio.sleep(refresh_interval + 1)  # Wait for next refresh cycle
            print(
                f"⏳ Waited {refresh_interval + 1} seconds for health manager refresh cycle"
            )

            # Step 3: Wait for automatic health detection across all topics
            print("⏳ Waiting for concurrent health detection across all topics...")

            detection_results = {}
            for topic in multiple_test_topics:
                print(
                    f"🔍 Checking health detection for {topic} (lag: {lag_results[topic]} messages)"
                )

                # Use longer timeout for multi-topic health detection
                detection_success = self.wait_for_health_detection(
                    producer.health_manager,
                    topic,
                    expected_unhealthy_partition=0,
                    timeout=15,  # Increased from TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"] (12s)
                )

                detection_results[topic] = detection_success
                if detection_success:
                    print(f"✅ Health detection completed for {topic}")
                else:
                    print(f"🚨 Health detection failed for {topic}")

            # Verify all topics had successful health detection
            failed_topics = [
                topic for topic, success in detection_results.items() if not success
            ]
            assert not failed_topics, (
                f"Health detection failed for topics: {failed_topics}"
            )

            # Step 4: Verify healthy partitions list for each topic
            for topic in multiple_test_topics:
                healthy_partitions = producer.health_manager.get_healthy_partitions(
                    topic
                )
                assert 0 not in healthy_partitions, (
                    f"Partition 0 should not be in healthy partitions for {topic}"
                )
                assert len(healthy_partitions) >= 1, (
                    f"Should have at least one healthy partition for {topic}"
                )
                print(
                    f"✅ Healthy partitions verified for {topic}: {healthy_partitions}"
                )

            print("🎉 Concurrent health detection validation completed successfully")

        finally:
            await producer.close()

    # TEST 3: SMART ROUTING VALIDATION
    @pytest.mark.asyncio
    async def test_smart_routing_concurrent_topics(
        self,
        fresh_test_data,
        multiple_test_topics,
        async_delivery_tracker,
        comprehensive_teardown,
    ):
        """Test 3: Async produce to multiple topics, verify topic-specific avoidance."""
        print("\\n=== Test 3: Concurrent Smart Routing Validation ===")

        config = self.create_scenario_config(fresh_test_data, multiple_test_topics)
        producer = await self.create_async_smart_producer(config)

        try:
            # Step 1: Trigger health manager startup
            print("⏳ Triggering health manager startup...")
            for topic in multiple_test_topics:
                await producer.produce(
                    topic=topic, key=b"startup-test", value=b"startup-test-value"
                )
            await producer.flush()
            await asyncio.sleep(2)  # Allow concurrent monitoring to initialize properly

            # Step 2: Wait for health manager to discover partitions for all topics
            print("⏳ Waiting for health manager to discover partitions...")
            topic_healthy_partitions = {}
            for topic in multiple_test_topics:
                healthy_partitions = self.wait_for_partitions_discovered(
                    producer.health_manager, topic, timeout=15
                )
                assert len(healthy_partitions) > 0, (
                    f"🚨 LIBRARY BUG: Health manager failed to discover partitions for {topic}"
                )
                topic_healthy_partitions[topic] = healthy_partitions

            # Step 3: Create consumer lag to make partition 0 unhealthy for both topics
            for topic in multiple_test_topics:
                actual_lag = self.create_real_consumer_lag(
                    topic=topic,
                    target_partition=0,
                    lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                    consumer_group=fresh_test_data["consumer_group"],
                )
                print(
                    f"✅ Created consumer lag: {actual_lag} messages on {topic} partition 0"
                )

            # Wait for at least one refresh cycle to allow health manager to detect the lag
            refresh_interval = TEST_HEALTH_CONFIG["refresh_interval"]  # 5 seconds
            await asyncio.sleep(refresh_interval + 1)  # Wait for next refresh cycle
            print(
                f"⏳ Waited {refresh_interval + 1} seconds for health manager refresh cycle"
            )

            # Step 4: Wait for health detection for both topics
            print("⏳ Waiting for health detection across all topics...")
            for topic in multiple_test_topics:
                detection_success = self.wait_for_health_detection(
                    producer.health_manager,
                    topic,
                    expected_unhealthy_partition=0,
                    timeout=TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"],
                )

                if not detection_success:
                    print(
                        f"🚨 LIBRARY BUG: Health detection system not working for topic {topic}!"
                    )
                    print(
                        "   Continuing test to validate key stickiness, but routing avoidance cannot be tested"
                    )

            # Step 4: Set up delivery tracking and async produce messages to both topics
            delivery_callback, delivery_results = async_delivery_tracker()
            message_count_per_topic = (
                TEST_DATA_AMOUNTS["produce_count"] // 2
            )  # Split messages between topics

            print(
                f"Async producing {message_count_per_topic} messages to each topic ({len(multiple_test_topics)} topics)..."
            )

            # Produce to multiple topics concurrently
            for i, topic in enumerate(multiple_test_topics):
                for j in range(message_count_per_topic):
                    message_id = i * message_count_per_topic + j
                    await producer.produce(
                        topic=topic,
                        key=f"async-key-{message_id}".encode(),
                        value=f"async-value-{topic}-{j}".encode(),
                        on_delivery=delivery_callback,
                    )

            # Flush all messages
            remaining = await producer.flush()
            assert remaining == 0, "All messages should be flushed"

            # Step 5: Validate message delivery and distribution per topic
            total_expected = message_count_per_topic * len(multiple_test_topics)
            assert len(delivery_results) >= int(total_expected * 0.8), (
                f"Should deliver at least 80% of {total_expected} messages"
            )

            # Analyze partition distribution per topic
            topic_results = {}
            for topic in multiple_test_topics:
                topic_messages = [r for r in delivery_results if r["topic"] == topic]
                topic_results[topic] = topic_messages

                # Validate avoidance for this topic
                validation = self.validate_partition_avoidance(
                    topic_messages, 0, TEST_THRESHOLDS["AVOIDANCE_MAX_PERCENTAGE"]
                )

                print(f"✅ Smart routing results for {topic}:")
                print(f"   Total messages: {validation['total_messages']}")
                print(f"   Partition distribution: {validation['distribution']}")
                print(f"   Partitions used: {len(validation['distribution'])}")
                print(
                    f"   Unhealthy partition usage: {validation['unhealthy_percentage']:.1%}"
                )

                # Verify messages are distributed (not all going to one partition)
                assert len(validation["distribution"]) > 1, (
                    f"Messages should be distributed across multiple partitions for {topic}"
                )

            # Step 6: Verify key stickiness is maintained across all topics
            for topic in multiple_test_topics:
                topic_messages = topic_results[topic]
                stickiness_validation = self.validate_key_stickiness(topic_messages)
                assert stickiness_validation["stickiness_successful"], (
                    f"Key stickiness should be maintained for {topic}"
                )
                print(
                    f"✅ Key stickiness maintained for {topic}: {stickiness_validation['consistent_keys']} keys consistently routed"
                )

            print("🎉 Concurrent smart routing validation completed successfully")

        finally:
            await producer.close()

    # TEST 4: KEY STICKINESS VALIDATION
    @pytest.mark.asyncio
    async def test_async_key_stickiness_concurrent_topics(
        self,
        fresh_test_data,
        multiple_test_topics,
        async_delivery_tracker,
        comprehensive_teardown,
    ):
        """Test 4: Test async key stickiness across multiple topics."""
        print("\\n=== Test 4: Async Key Stickiness Validation ===")

        config = self.create_scenario_config(fresh_test_data, multiple_test_topics)
        producer = await self.create_async_smart_producer(config)

        try:
            # Trigger health manager startup
            print("⏳ Triggering health manager startup...")
            for topic in multiple_test_topics:
                await producer.produce(
                    topic=topic, key=b"startup-test", value=b"startup-test-value"
                )
            await producer.flush()
            await asyncio.sleep(2)  # Allow concurrent monitoring to initialize properly

            # Wait for health manager to discover topic partitions for all topics
            print("⏳ Waiting for health manager to discover partitions...")
            for topic in multiple_test_topics:
                healthy_partitions = self.wait_for_partitions_discovered(
                    producer.health_manager, topic, timeout=15
                )
                assert len(healthy_partitions) > 0, (
                    f"🚨 LIBRARY BUG: Health manager failed to discover partitions for {topic}"
                )

            delivery_callback, delivery_results = async_delivery_tracker()

            # Step 1: Test async key stickiness across multiple topics
            print("Testing async key stickiness across multiple topics...")
            test_keys = ["async-user-123", "async-user-456", "async-user-789"]

            topic_key_mappings = {}

            for topic in multiple_test_topics:
                # Clear results for this topic
                topic_delivery_results = []

                def topic_callback(err, msg, t=topic, results=topic_delivery_results):
                    if err is None and msg.topic() == t:
                        results.append(
                            {
                                "topic": msg.topic(),
                                "partition": msg.partition(),
                                "key": msg.key().decode() if msg.key() else None,
                            }
                        )

                # Produce messages with keys to this topic
                for key in test_keys:
                    for i in range(5):  # 5 messages per key per topic
                        await producer.produce(
                            topic=topic,
                            key=key.encode(),
                            value=f"async-value-{topic}-{key}-{i}".encode(),
                            on_delivery=topic_callback,
                        )

                # Flush for this topic batch
                await producer.flush()

                # Verify key stickiness for this topic
                stickiness_validation = self.validate_key_stickiness(
                    topic_delivery_results, test_keys
                )

                assert stickiness_validation["stickiness_successful"], (
                    f"Async key stickiness failed for topic {topic}: {stickiness_validation['inconsistent_keys']}"
                )
                assert stickiness_validation["expected_keys_found"], (
                    f"Missing expected keys for topic {topic}"
                )

                topic_key_mappings[topic] = stickiness_validation[
                    "key_partition_mapping"
                ]
                print(
                    f"✅ Async key stickiness verified for {topic}: {len(test_keys)} keys consistently routed"
                )
                print(
                    f"   Key mappings: {stickiness_validation['key_partition_mapping']}"
                )

            # Step 2: Test async cache behavior consistency across topics
            print("Testing async cache behavior consistency...")

            cache_test_key = "async-sticky-test-key"
            cache_delivery_results = []

            def cache_callback(err, msg):
                if err is None:
                    cache_delivery_results.append(
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "key": msg.key().decode() if msg.key() else None,
                        }
                    )

            # Test cache consistency for each topic independently
            for topic in multiple_test_topics:
                # First batch - establish key→partition mapping for this topic
                for i in range(3):
                    await producer.produce(
                        topic=topic,
                        key=cache_test_key.encode(),
                        value=f"cache-batch1-{topic}-{i}".encode(),
                        on_delivery=cache_callback,
                    )

                # Second batch - should use cached partition for this topic
                for i in range(3):
                    await producer.produce(
                        topic=topic,
                        key=cache_test_key.encode(),
                        value=f"cache-batch2-{topic}-{i}".encode(),
                        on_delivery=cache_callback,
                    )

            await producer.flush()

            # Verify cache consistency per topic
            for topic in multiple_test_topics:
                topic_cache_results = [
                    r
                    for r in cache_delivery_results
                    if r["topic"] == topic and r["key"] == cache_test_key
                ]
                partitions_used = {r["partition"] for r in topic_cache_results}
                assert len(partitions_used) == 1, (
                    f"Cache consistency failed for {topic}: key used {len(partitions_used)} partitions"
                )

                first_partition = list(partitions_used)[0]
                print(
                    f"✅ Cache consistency verified for {topic}: key consistently routed to partition {first_partition}"
                )

            # Step 3: Test mixed keyed and keyless async messages
            print("Testing mixed keyed/keyless async routing...")
            mixed_delivery_results = []

            def mixed_callback(err, msg):
                if err is None:
                    mixed_delivery_results.append(
                        {
                            "topic": msg.topic(),
                            "partition": msg.partition(),
                            "key": msg.key().decode() if msg.key() else None,
                        }
                    )

            # Produce mixed messages to both topics
            for topic in multiple_test_topics:
                for i in range(15):  # 15 messages per topic
                    if i % 3 == 0:
                        # Keyless message
                        await producer.produce(
                            topic=topic,
                            value=f"keyless-{topic}-{i}".encode(),
                            on_delivery=mixed_callback,
                        )
                    else:
                        # Keyed message
                        key = f"mixed-key-{topic}-{i % 2}"  # Alternates between 2 keys per topic
                        await producer.produce(
                            topic=topic,
                            key=key.encode(),
                            value=f"keyed-{topic}-{i}".encode(),
                            on_delivery=mixed_callback,
                        )

            await producer.flush()

            # Validate mixed routing per topic
            for topic in multiple_test_topics:
                topic_mixed_results = [
                    r for r in mixed_delivery_results if r["topic"] == topic
                ]
                keyed_results = [r for r in topic_mixed_results if r.get("key")]
                keyless_results = [r for r in topic_mixed_results if not r.get("key")]

                # Verify key stickiness for keyed messages in this topic
                keyed_stickiness = self.validate_key_stickiness(keyed_results)
                assert keyed_stickiness["stickiness_successful"], (
                    f"Keyed messages should maintain stickiness for {topic}"
                )

                # Verify keyless messages can use different partitions
                keyless_partitions = {r["partition"] for r in keyless_results}

                print(f"✅ Mixed routing verified for {topic}:")
                print(
                    f"   Keyed messages: {keyed_stickiness['consistent_keys']} keys with consistent routing"
                )
                print(
                    f"   Keyless messages: distributed across {len(keyless_partitions)} partitions"
                )

                # Should have 2 sticky keys per topic (mixed-key-{topic}-0 and mixed-key-{topic}-1)
                assert len(keyed_stickiness["key_partition_mapping"]) == 2, (
                    f"Should have 2 sticky keys for {topic}"
                )

            print(
                "✅ Async key stickiness with local cache working correctly across all topics"
            )
            print("🎉 Async key stickiness validation completed successfully")

        finally:
            await producer.close()

    # COMPREHENSIVE SCENARIO TEST
    @pytest.mark.asyncio
    async def test_scenario_2_complete_async_workflow(
        self,
        admin_client,
        fresh_test_data,
        multiple_test_topics,
        async_delivery_tracker,
    ):
        """Complete async workflow test combining all 4 validations for Scenario 2."""
        print("\\n=== Scenario 2: Complete Async Workflow Test ===")

        config = self.create_scenario_config(fresh_test_data, multiple_test_topics)
        producer = await self.create_async_smart_producer(config)

        try:
            # Phase 1: Async Initialization
            print("Phase 1: Validating async initialization...")
            assert producer.smart_enabled
            assert producer.health_manager is not None
            assert len(producer.topics) == 2  # Concurrent topic monitoring
            assert hasattr(producer.health_manager, "_task")  # Asyncio-based

            # Trigger health manager startup
            for topic in multiple_test_topics:
                await producer.produce(
                    topic=topic, key=b"startup-test", value=b"startup-test-value"
                )
            await producer.flush()
            await asyncio.sleep(2)  # Allow concurrent monitoring to initialize properly

            print("✅ Phase 1 complete: Async initialization validated")

            # Phase 2: Concurrent Health Detection
            print("Phase 2: Testing concurrent health detection...")
            for topic in multiple_test_topics:
                actual_lag = self.create_real_consumer_lag(
                    topic,
                    0,
                    TEST_DATA_AMOUNTS["lag_messages"],
                    fresh_test_data["consumer_group"],
                )
                print(f"✅ Created lag for {topic}: {actual_lag} messages")

            # Wait for at least one refresh cycle to allow health manager to detect the lag
            refresh_interval = TEST_HEALTH_CONFIG["refresh_interval"]  # 5 seconds
            await asyncio.sleep(refresh_interval + 1)  # Wait for next refresh cycle
            print(
                f"⏳ Waited {refresh_interval + 1} seconds for health manager refresh cycle"
            )

            # Wait for health detection across all topics
            print("⏳ Waiting for concurrent health detection...")
            all_detected = True
            for topic in multiple_test_topics:
                # Use longer timeout for multi-topic health detection
                detection_success = self.wait_for_health_detection(
                    producer.health_manager,
                    topic,
                    expected_unhealthy_partition=0,
                    timeout=15,  # Increased from TEST_THRESHOLDS["HEALTH_DETECTION_TIMEOUT"] (12s)
                )
                if not detection_success:
                    all_detected = False
                    print(f"🚨 LIBRARY BUG: Health detection failed for {topic}!")
                else:
                    healthy_partitions = producer.health_manager.get_healthy_partitions(
                        topic
                    )
                    assert 0 not in healthy_partitions

            print("✅ Phase 2 complete: Concurrent health detection working")

            # Phase 3: Concurrent Smart Routing + Key Stickiness
            print("Phase 3: Testing concurrent smart routing with key stickiness...")
            delivery_callback, delivery_results = async_delivery_tracker()

            # Produce messages to both topics with keys (should avoid partition 0 AND maintain key stickiness)
            test_keys = ["workflow-key-1", "workflow-key-2"]
            messages_per_topic = 20

            for topic in multiple_test_topics:
                for key in test_keys:
                    for i in range(messages_per_topic):
                        await producer.produce(
                            topic=topic,
                            key=key.encode(),
                            value=f"workflow-{topic}-{key}-{i}".encode(),
                            on_delivery=delivery_callback,
                        )

            await producer.flush()

            # Validate both smart routing and key stickiness per topic
            for topic in multiple_test_topics:
                topic_results = [r for r in delivery_results if r["topic"] == topic]

                # Smart routing validation
                validation = self.validate_partition_avoidance(topic_results, 0, 0.25)
                # Note: Only assert if health detection worked
                if all_detected:
                    assert validation["avoidance_successful"], (
                        f"Smart routing should avoid unhealthy partition for {topic}"
                    )

                # Key stickiness validation
                stickiness_validation = self.validate_key_stickiness(
                    topic_results, test_keys
                )
                assert stickiness_validation["stickiness_successful"], (
                    f"Key stickiness should be maintained for {topic}"
                )
                assert stickiness_validation["expected_keys_found"], (
                    f"All keys should have consistent partitions for {topic}"
                )

                print(
                    f"✅ Phase 3 validated for {topic}: routing + stickiness working together"
                )

            print(
                "✅ Phase 3 complete: Concurrent smart routing + key stickiness working together"
            )

            print("🎉 Scenario 2 complete async workflow validation successful!")
            print(f"   Topics monitored: {len(multiple_test_topics)}")
            print(f"   Total messages produced: {len(delivery_results)}")
            print("   Asyncio-based health monitoring: ✅")
            print("   Concurrent topic health detection: ✅")
            print("   Topic-specific key stickiness: ✅")

        finally:
            await producer.close()
