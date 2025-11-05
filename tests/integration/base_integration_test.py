"""
Base Integration Test Class

Provides common fixtures, helper methods, and utilities for all Smart Producer
integration test scenarios. This reduces duplication and provides a consistent
testing foundation.
"""

import time
from collections import Counter

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Import test configurations
from .test_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    REDIS_HOST,
    REDIS_PORT,
    TEST_THRESHOLDS,
)


class BaseIntegrationTest:
    """
    Base class for Smart Producer integration tests.

    Provides common fixtures, helper methods, and validation utilities
    that can be reused across different test scenarios.
    """

    # Common Fixtures
    @pytest.fixture(scope="class")
    def admin_client(self):
        """Create Kafka admin client for integration testing."""
        client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        yield client

    @pytest.fixture
    def fresh_test_data(self):
        """Generate fresh test data for each test to ensure isolation."""
        test_id = int(time.time() * 1000) % TEST_THRESHOLDS["UNIQUE_ID_MODULO"]
        return {
            "consumer_group": f"integration-group-{test_id}",
            "client_id": f"integration-client-{test_id}",
            "topic": "test-orders",
        }

    @pytest.fixture
    def managed_consumer_group(self, admin_client, fresh_test_data):
        """Create and manage consumer group with proper cleanup."""
        group_id = fresh_test_data["consumer_group"]
        yield group_id

        # CRITICAL: Clean up consumer group after test to prevent contamination
        print(f"🧹 Cleaning up consumer group: {group_id}")
        try:
            # Delete consumer group and wait for completion
            fs = admin_client.delete_consumer_groups([group_id], request_timeout=10)
            for group, f in fs.items():
                f.result(timeout=5)
                print(f"✅ Successfully deleted consumer group: {group}")
        except Exception as e:
            print(f"⚠️ Consumer group cleanup warning for '{group_id}': {e}")
            # This is expected if group doesn't exist or is already deleted

    @pytest.fixture
    def comprehensive_teardown(self, fresh_test_data):
        """Comprehensive teardown including Redis cache cleanup."""
        yield  # Run the test first

        # Redis cache cleanup (if remote cache is enabled)
        try:
            import redis

            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

            # Clean up any test-related cache keys
            test_patterns = [
                f"*{fresh_test_data['client_id']}*",
                f"*{fresh_test_data['consumer_group']}*",
                f"*{fresh_test_data['topic']}*",
                "test-*",  # General test keys
                "*integration*",  # Integration-specific keys
            ]

            for pattern in test_patterns:
                keys = r.keys(pattern)
                if keys:
                    r.delete(*keys)
                    print(f"🧹 Cleaned {len(keys)} Redis keys matching '{pattern}'")

        except (ImportError, redis.exceptions.ConnectionError):
            # Redis not available or not configured - skip cleanup
            pass
        except Exception as e:
            print(f"⚠️ Redis cleanup warning: {e}")

        # Force garbage collection to ensure clean state
        import gc

        gc.collect()
        print("🧹 Comprehensive teardown completed")

    @pytest.fixture
    def delivery_tracker(self):
        """Factory for tracking real message delivery results."""

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
                            "timestamp": time.time(),
                        }
                    )
                else:
                    print(f"Delivery error: {err}")

            return callback, results

        return create_tracker

    @pytest.fixture
    def test_topic(self, admin_client, fresh_test_data):
        """Create and manage test topic for scenario tests."""
        topic_name = f"{fresh_test_data['topic']}-{fresh_test_data['client_id']}"

        # Create topic
        new_topic = NewTopic(topic_name, num_partitions=4, replication_factor=1)
        fs = admin_client.create_topics([new_topic])
        for topic, f in fs.items():
            try:
                f.result(timeout=10)
                print(f"✅ Created test topic: {topic}")
            except Exception as e:
                if "already exists" not in str(e).lower():
                    raise
                print(f"⚠️ Topic {topic} already exists")

        yield topic_name

        # Cleanup topic with proper timeout and verification
        try:
            fs = admin_client.delete_topics([topic_name], operation_timeout=15)
            for topic, f in fs.items():
                f.result(timeout=10)  # Add timeout to prevent hangs
                print(f"🧹 Cleaned up topic: {topic}")
            # Small delay to ensure Kafka internal cleanup completes
            time.sleep(0.5)
        except Exception as e:
            print(f"Topic cleanup warning: {e}")

    # Core Helper Methods
    def create_real_consumer_lag(
        self, topic, target_partition, lag_messages, consumer_group
    ):
        """
        Create real consumer lag by producing and partially consuming messages.

        This is the standard method used across all integration tests to create
        realistic consumer lag scenarios for health detection testing.
        """
        # Step 1: Produce messages to target partition
        producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "acks": "1"})

        for i in range(lag_messages):
            producer.produce(
                topic=topic,
                partition=target_partition,
                key=f"lag-key-{i}".encode(),
                value=f"lag-data-{i}".encode(),
            )
        producer.flush()
        print(f"Produced {lag_messages} messages to partition {target_partition}")

        # Step 2: Create consumer and consume only some messages
        consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
                "auto.commit.interval.ms": 1000,
            }
        )

        consumer.subscribe([topic])

        # Step 3: Consume fewer messages than produced to create lag
        consume_count = max(1, lag_messages // 3)  # Leave 2/3 as lag
        consumed = 0

        while consumed < consume_count:
            msg = consumer.poll(timeout=2.0)
            if msg is not None and not msg.error():
                if msg.partition() == target_partition:
                    consumed += 1
                    print(f"Consumed message {consumed}/{consume_count}")

        consumer.commit()
        consumer.close()

        actual_lag = lag_messages - consumed
        print(
            f"Created lag: produced {lag_messages}, consumed {consumed}, lag {actual_lag}"
        )
        return actual_lag

    def wait_for_partitions_discovered(self, health_manager, topic, timeout=10):
        """Wait for health manager to discover topic partitions."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            partitions = health_manager.get_healthy_partitions(topic)
            if len(partitions) > 0:
                print(
                    f"Health manager discovered {len(partitions)} partitions for {topic}: {partitions}"
                )
                return partitions
            time.sleep(0.2)  # Check more frequently

        print(
            f"Partition discovery timeout after {timeout}s - this indicates a library bug"
        )
        return []

    def wait_for_health_detection(
        self, health_manager, topic, expected_unhealthy_partition, timeout=12
    ):
        """Wait for health manager to detect unhealthy partition."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            healthy_partitions = health_manager.get_healthy_partitions(topic)
            if expected_unhealthy_partition not in healthy_partitions:
                print(
                    f"Health manager detected unhealthy partition {expected_unhealthy_partition}"
                )
                return True
            time.sleep(0.5)

        print(f"Health detection timeout after {timeout}s")
        print(
            f"Current healthy partitions: {health_manager.get_healthy_partitions(topic)}"
        )
        print("🚨 This suggests a bug in the health detection system!")
        return False

    # Validation Methods
    def validate_partition_avoidance(
        self, delivery_results, unhealthy_partition, max_percentage=0.25
    ):
        """Validate messages avoid unhealthy partition within threshold."""
        total = len(delivery_results)
        unhealthy_count = sum(
            1 for r in delivery_results if r["partition"] == unhealthy_partition
        )
        unhealthy_percentage = unhealthy_count / total if total > 0 else 0

        # Efficient O(n) partition distribution calculation
        distribution = Counter(r["partition"] for r in delivery_results)

        return {
            "total_messages": total,
            "unhealthy_messages": unhealthy_count,
            "unhealthy_percentage": unhealthy_percentage,
            "avoidance_successful": unhealthy_percentage <= max_percentage,
            "distribution": dict(distribution),
        }

    def validate_key_stickiness(self, delivery_results, expected_keys=None):
        """
        Validate key stickiness behavior across delivery results.

        Returns dictionary with key→partition mappings and validation status.
        """
        key_partition_mapping = {}
        inconsistent_keys = []

        for result in delivery_results:
            key = result.get("key")
            partition = result["partition"]

            # Only process results that have actual keys (not None)
            if key is not None:
                if key in key_partition_mapping:
                    if key_partition_mapping[key] != partition:
                        inconsistent_keys.append(key)
                else:
                    key_partition_mapping[key] = partition

        validation_result = {
            "key_partition_mapping": key_partition_mapping,
            "consistent_keys": len(key_partition_mapping) - len(inconsistent_keys),
            "inconsistent_keys": inconsistent_keys,
            "stickiness_successful": len(inconsistent_keys) == 0,
        }

        if expected_keys:
            validation_result["expected_keys_found"] = all(
                key in key_partition_mapping for key in expected_keys
            )

        return validation_result
