#!/usr/bin/env python3
"""
Integration tests for Scenario 5: Standalone Health Monitor Service

Tests both sync and async standalone health monitors running as independent services
that collect consumer lag data, calculate health scores, and publish to Redis.
These services operate completely independently of any producers.

Test Structure:
- Sync Standalone Tests (1-3): Threading-based health monitoring service
- Async Standalone Tests (4-6): Asyncio-based health monitoring service
- Focus on lag collection, health calculation, and Redis publishing
- No producer testing - pure health monitoring service validation

Note: Tests use real Kafka operations and KafkaAdminLagCollector integration.
No mocking of lag data or health calculations.
"""

import asyncio
import os
import time

import pytest
import redis

# Set up environment for standalone health monitor Redis configuration
os.environ["REDIS_PORT"] = "6380"
from .base_integration_test import BaseIntegrationTest
from .test_config import KAFKA_BOOTSTRAP_SERVERS, TEST_DATA_AMOUNTS, TEST_HEALTH_CONFIG


class TestScenario5StandaloneHealthMonitors(BaseIntegrationTest):
    """Test both sync and async standalone health monitors."""

    def create_redis_config(self):
        """Create Redis configuration for standalone monitors."""
        return {"host": "localhost", "port": 6380, "decode_responses": True}

    def get_redis_client(self):
        """Get Redis client for validation."""
        return redis.Redis(host="localhost", port=6380, decode_responses=True)

    def cleanup_redis_health_data(self):
        """Clean up health data from Redis between tests."""
        redis_client = self.get_redis_client()
        try:
            # Delete all health-related keys
            health_keys = redis_client.keys("kafka_health:*")
            if health_keys:
                redis_client.delete(*health_keys)
            print(f"🧹 Cleaned up {len(health_keys)} Redis health keys")
        except Exception as e:
            print(f"Redis cleanup warning: {e}")

    @pytest.fixture
    def redis_cleanup(self):
        """Fixture to ensure Redis is clean before and after tests."""
        self.cleanup_redis_health_data()
        yield
        self.cleanup_redis_health_data()

    @pytest.fixture
    def test_topics(self, admin_client, fresh_test_data):
        """Create test topics for standalone monitoring."""
        topic_names = [
            f"standalone-orders-{fresh_test_data['client_id']}",
            f"standalone-payments-{fresh_test_data['client_id']}",
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

        # Add delay to ensure topics are fully ready
        time.sleep(1.0)

        yield topic_names

        # Cleanup topics
        try:
            fs = admin_client.delete_topics(topic_names, operation_timeout=15)
            for topic, f in fs.items():
                f.result(timeout=10)
                print(f"🧹 Cleaned up topic: {topic}")
        except Exception as e:
            print(f"Topic cleanup warning: {e}")

    # ==========================================
    # SYNC STANDALONE HEALTH MONITOR TESTS
    # ==========================================

    def test_sync_standalone_initialization(
        self, fresh_test_data, test_topics, redis_cleanup
    ):
        """Test 1: Verify sync standalone health monitor starts correctly."""
        print("\n=== Test 1: Sync Standalone Initialization ===")

        from kafka_smart_producer.health_mode import HealthMode
        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        # Create sync standalone monitor
        sync_monitor = PartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=test_topics,
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )

        try:
            # Verify initialization state
            assert sync_monitor._mode == HealthMode.STANDALONE, (
                "Should be in standalone mode"
            )
            assert not sync_monitor.is_running, "Should not be running initially"
            assert hasattr(sync_monitor, "_lag_collector"), "Should have lag collector"
            assert hasattr(sync_monitor, "_redis_publisher"), (
                "Should have Redis publisher"
            )
            print("✅ Sync standalone monitor created with correct configuration")

            # Start the monitor service
            sync_monitor.start()

            # Verify running state
            assert sync_monitor.is_running, "Should be running after start()"
            assert sync_monitor._thread is not None, "Should have background thread"
            assert sync_monitor._thread.is_alive(), "Background thread should be alive"
            print("✅ Sync standalone monitor started with background thread")

            # Wait for initial lag collection cycle
            print("⏳ Waiting for initial lag collection cycle...")
            time.sleep(8)  # Allow 1+ refresh cycles

            # Verify lag collector is working and health data is available
            for topic in test_topics:
                partitions = sync_monitor.get_healthy_partitions(topic)
                assert len(partitions) > 0, f"No partitions discovered for {topic}"
                print(
                    f"✅ Lag collector working for {topic}: {len(partitions)} partitions discovered"
                )

            print("🎉 Sync standalone initialization successful")

        finally:
            sync_monitor.stop()

    def test_sync_lag_collection_and_health_publishing(
        self, fresh_test_data, test_topics, redis_cleanup
    ):
        """Test 2: Verify sync standalone monitor collects lag data and publishes health scores to Redis."""
        print("\n=== Test 2: Sync Lag Collection & Health Publishing ===")

        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        topic = test_topics[0]  # Use first topic for testing

        sync_monitor = PartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=[topic],
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )
        sync_monitor.start()

        try:
            # Wait for initial lag collection
            print("⏳ Waiting for initial lag collection...")
            time.sleep(8)

            # Verify lag collector is functioning
            initial_partitions = sync_monitor.get_healthy_partitions(topic)
            assert len(initial_partitions) > 0, (
                "No partitions discovered by lag collector"
            )
            print(f"✅ Lag collector discovered {len(initial_partitions)} partitions")

            # Verify Redis contains health data from interval publishing
            redis_client = self.get_redis_client()
            health_state_key = f"kafka_health:state:{topic}"

            # Check that Redis contains the expected health state key
            assert redis_client.exists(health_state_key), (
                f"Health state not published to Redis key: {health_state_key}"
            )
            health_data_raw = redis_client.get(health_state_key)
            assert health_data_raw is not None, "Health data should exist in Redis"

            # Parse the JSON health data (remove "json:" prefix if present)
            import json

            if health_data_raw.startswith("json:"):
                health_data_raw = health_data_raw[5:]  # Remove "json:" prefix
            health_data = json.loads(health_data_raw)
            assert "partitions" in health_data, "Health data should contain partitions"
            assert "healthy_count" in health_data, (
                "Health data should contain healthy_count"
            )
            assert "total_count" in health_data, (
                "Health data should contain total_count"
            )
            print(f"✅ Initial health data published to Redis: {health_state_key}")

            # Create consumer lag to test health calculation
            print("⏳ Creating consumer lag to test health calculation...")
            lag_created = self.create_real_consumer_lag(
                topic=topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],  # Above lag_threshold
                consumer_group=fresh_test_data["consumer_group"],
            )
            assert lag_created > 0, (
                f"Failed to create consumer lag for testing, got {lag_created}"
            )
            print(f"✅ Created consumer lag: {lag_created} messages on partition 0")

            # Wait for lag collection and health calculation
            print("⏳ Waiting for health calculation and detection...")
            health_detected = self.wait_for_health_detection(
                sync_monitor, topic, expected_unhealthy_partition=0, timeout=15
            )
            assert health_detected, (
                "Sync standalone monitor failed to detect unhealthy partition from lag data"
            )
            print("✅ Health calculation successful - unhealthy partition detected")

            # Verify updated health data was published to Redis
            updated_health_data_raw = redis_client.get(health_state_key)
            assert updated_health_data_raw is not None, (
                "Updated health data should exist in Redis"
            )

            # Parse partition data to verify health scores changed (remove "json:" prefix if present)
            import json

            if updated_health_data_raw.startswith("json:"):
                updated_health_data_raw = updated_health_data_raw[
                    5:
                ]  # Remove "json:" prefix
            updated_health_data = json.loads(updated_health_data_raw)
            partitions_data = updated_health_data[
                "partitions"
            ]  # Already a dict, no need for json.loads
            assert len(partitions_data) > 0, "Should have partition health scores"
            print(
                f"✅ Updated health scores published to Redis: {len(partitions_data)} partitions"
            )

            print("🎉 Sync lag collection and health publishing completed successfully")

        finally:
            sync_monitor.stop()

    def test_sync_multi_topic_monitoring(
        self, fresh_test_data, test_topics, redis_cleanup
    ):
        """Test 3: Verify sync standalone monitor handles multiple topics independently."""
        print("\n=== Test 3: Sync Multi-Topic Monitoring ===")

        from kafka_smart_producer.partition_health_monitor import PartitionHealthMonitor

        sync_monitor = PartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=test_topics,
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )
        sync_monitor.start()

        try:
            # Wait for initial lag collection on all topics
            print("⏳ Waiting for initial lag collection on all topics...")
            time.sleep(8)

            # Verify each topic is being monitored independently
            redis_client = self.get_redis_client()
            for topic in test_topics:
                partitions = sync_monitor.get_healthy_partitions(topic)
                assert len(partitions) > 0, f"Topic {topic} not being monitored"

                # Verify Redis contains health data for this topic
                health_state_key = f"kafka_health:state:{topic}"
                assert redis_client.exists(health_state_key), (
                    f"No Redis health data for {topic}"
                )
                print(
                    f"✅ Topic {topic}: {len(partitions)} partitions monitored, Redis data published"
                )

            # Create lag on one topic only
            target_topic = test_topics[0]  # First topic
            print(f"⏳ Creating lag on {target_topic} only...")
            lag_created = self.create_real_consumer_lag(
                topic=target_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=fresh_test_data["consumer_group"],
            )
            assert lag_created > 0, "Failed to create consumer lag"
            print(f"✅ Created lag on {target_topic}: {lag_created} messages")

            # Wait for health detection on affected topic
            health_detected = self.wait_for_health_detection(
                sync_monitor, target_topic, expected_unhealthy_partition=0, timeout=15
            )
            assert health_detected, f"Failed to detect health change on {target_topic}"
            print(f"✅ Health change detected on {target_topic}")

            # Verify other topics remain healthy
            other_topic = test_topics[1]  # Second topic
            other_partitions = sync_monitor.get_healthy_partitions(other_topic)
            assert len(other_partitions) > 0, (
                f"Other topic {other_topic} should remain healthy"
            )
            print(
                f"✅ Other topic {other_topic} remains healthy: {len(other_partitions)} partitions"
            )

            # Verify Redis contains health data for both topics with independent states
            target_health_key = f"kafka_health:state:{target_topic}"
            other_health_key = f"kafka_health:state:{other_topic}"

            target_health = redis_client.get(target_health_key)
            other_health = redis_client.get(other_health_key)

            assert target_health != other_health, (
                "Topics should have independent health states"
            )
            print("✅ Redis contains independent health data for both topics")

            print("🎉 Multi-topic monitoring completed successfully")

        finally:
            sync_monitor.stop()

    # ==========================================
    # ASYNC STANDALONE HEALTH MONITOR TESTS
    # ==========================================

    @pytest.mark.asyncio
    async def test_async_standalone_initialization(
        self, fresh_test_data, test_topics, redis_cleanup
    ):
        """Test 4: Verify async standalone health monitor starts correctly."""
        print("\n=== Test 4: Async Standalone Initialization ===")

        from kafka_smart_producer.async_partition_health_monitor import (
            AsyncPartitionHealthMonitor,
        )
        from kafka_smart_producer.health_mode import HealthMode

        # Create async standalone monitor
        async_monitor = await AsyncPartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=test_topics,
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )

        try:
            # Verify initialization state
            assert async_monitor._mode == HealthMode.STANDALONE, (
                "Should be in standalone mode"
            )
            assert not async_monitor.is_running, "Should not be running initially"
            assert hasattr(async_monitor, "_lag_collector"), "Should have lag collector"
            assert hasattr(async_monitor, "_redis_publisher"), (
                "Should have Redis publisher"
            )
            print("✅ Async standalone monitor created with correct configuration")

            # Start the monitor service
            await async_monitor.start()

            # Verify running state
            assert async_monitor.is_running, "Should be running after start()"
            assert async_monitor._task is not None, "Should have background task"
            assert not async_monitor._task.done(), "Background task should be running"
            print("✅ Async standalone monitor started with background task")

            # Wait for initial lag collection cycle
            print("⏳ Waiting for initial lag collection cycle...")
            await asyncio.sleep(8)  # Allow 1+ refresh cycles

            # Verify lag collector is working and health data is available
            for topic in test_topics:
                partitions = async_monitor.get_healthy_partitions(topic)
                assert len(partitions) > 0, f"No partitions discovered for {topic}"
                print(
                    f"✅ Lag collector working for {topic}: {len(partitions)} partitions discovered"
                )

            print("🎉 Async standalone initialization successful")

        finally:
            await async_monitor.stop()

    @pytest.mark.asyncio
    async def test_async_lag_collection_and_health_publishing(
        self, fresh_test_data, test_topics, redis_cleanup
    ):
        """Test 5: Verify async standalone monitor collects lag data and publishes health scores to Redis."""
        print("\n=== Test 5: Async Lag Collection & Health Publishing ===")

        from kafka_smart_producer.async_partition_health_monitor import (
            AsyncPartitionHealthMonitor,
        )

        topic = test_topics[0]  # Use first topic for testing

        async_monitor = await AsyncPartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=[topic],
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )
        await async_monitor.start()

        try:
            # Wait for initial lag collection
            print("⏳ Waiting for initial lag collection...")
            await asyncio.sleep(8)

            # Verify lag collector is functioning
            initial_partitions = async_monitor.get_healthy_partitions(topic)
            assert len(initial_partitions) > 0, (
                "No partitions discovered by lag collector"
            )
            print(f"✅ Lag collector discovered {len(initial_partitions)} partitions")

            # Verify Redis contains health data from interval publishing
            redis_client = self.get_redis_client()
            health_state_key = f"kafka_health:state:{topic}"

            # Check that Redis contains the expected health state key
            assert redis_client.exists(health_state_key), (
                f"Health state not published to Redis key: {health_state_key}"
            )
            health_data_raw = redis_client.get(health_state_key)
            assert health_data_raw is not None, "Health data should exist in Redis"

            # Parse the JSON health data (remove "json:" prefix if present)
            import json

            if health_data_raw.startswith("json:"):
                health_data_raw = health_data_raw[5:]  # Remove "json:" prefix
            health_data = json.loads(health_data_raw)
            assert "partitions" in health_data, "Health data should contain partitions"
            assert "healthy_count" in health_data, (
                "Health data should contain healthy_count"
            )
            assert "total_count" in health_data, (
                "Health data should contain total_count"
            )
            print(f"✅ Initial health data published to Redis: {health_state_key}")

            # Create consumer lag to test health calculation
            print("⏳ Creating consumer lag to test health calculation...")
            lag_created = self.create_real_consumer_lag(
                topic=topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],  # Above lag_threshold
                consumer_group=fresh_test_data["consumer_group"],
            )
            assert lag_created > 0, (
                f"Failed to create consumer lag for testing, got {lag_created}"
            )
            print(f"✅ Created consumer lag: {lag_created} messages on partition 0")

            # Wait for lag collection and health calculation using async helper
            print("⏳ Waiting for health calculation and detection...")
            health_detected = await self.wait_for_health_detection_async(
                async_monitor, topic, expected_unhealthy_partition=0, timeout=15
            )
            assert health_detected, (
                "Async standalone monitor failed to detect unhealthy partition from lag data"
            )
            print("✅ Health calculation successful - unhealthy partition detected")

            # Verify updated health data was published to Redis
            updated_health_data_raw = redis_client.get(health_state_key)
            assert updated_health_data_raw is not None, (
                "Updated health data should exist in Redis"
            )

            # Parse partition data to verify health scores changed (remove "json:" prefix if present)
            import json

            if updated_health_data_raw.startswith("json:"):
                updated_health_data_raw = updated_health_data_raw[
                    5:
                ]  # Remove "json:" prefix
            updated_health_data = json.loads(updated_health_data_raw)
            partitions_data = updated_health_data[
                "partitions"
            ]  # Already a dict, no need for json.loads
            assert len(partitions_data) > 0, "Should have partition health scores"
            print(
                f"✅ Updated health scores published to Redis: {len(partitions_data)} partitions"
            )

            print(
                "🎉 Async lag collection and health publishing completed successfully"
            )

        finally:
            await async_monitor.stop()

    @pytest.mark.asyncio
    async def test_async_multi_topic_monitoring(
        self, fresh_test_data, test_topics, redis_cleanup
    ):
        """Test 6: Verify async standalone monitor handles multiple topics independently."""
        print("\n=== Test 6: Async Multi-Topic Monitoring ===")

        from kafka_smart_producer.async_partition_health_monitor import (
            AsyncPartitionHealthMonitor,
        )

        async_monitor = await AsyncPartitionHealthMonitor.standalone(
            consumer_group=fresh_test_data["consumer_group"],
            kafka_config={"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS},
            topics=test_topics,
            health_threshold=TEST_HEALTH_CONFIG["unhealthy_threshold"],
            refresh_interval=TEST_HEALTH_CONFIG["refresh_interval"],
            max_lag_for_health=TEST_HEALTH_CONFIG["lag_threshold"],
        )
        await async_monitor.start()

        try:
            # Wait for initial lag collection on all topics
            print("⏳ Waiting for initial lag collection on all topics...")
            await asyncio.sleep(8)

            # Verify each topic is being monitored independently
            redis_client = self.get_redis_client()
            for topic in test_topics:
                partitions = async_monitor.get_healthy_partitions(topic)
                assert len(partitions) > 0, f"Topic {topic} not being monitored"

                # Verify Redis contains health data for this topic
                health_state_key = f"kafka_health:state:{topic}"
                assert redis_client.exists(health_state_key), (
                    f"No Redis health data for {topic}"
                )
                print(
                    f"✅ Topic {topic}: {len(partitions)} partitions monitored, Redis data published"
                )

            # Create lag on one topic only
            target_topic = test_topics[0]  # First topic
            print(f"⏳ Creating lag on {target_topic} only...")
            lag_created = self.create_real_consumer_lag(
                topic=target_topic,
                target_partition=0,
                lag_messages=TEST_DATA_AMOUNTS["lag_messages"],
                consumer_group=fresh_test_data["consumer_group"],
            )
            assert lag_created > 0, "Failed to create consumer lag"
            print(f"✅ Created lag on {target_topic}: {lag_created} messages")

            # Wait for health detection on affected topic using async helper
            health_detected = await self.wait_for_health_detection_async(
                async_monitor, target_topic, expected_unhealthy_partition=0, timeout=15
            )
            assert health_detected, f"Failed to detect health change on {target_topic}"
            print(f"✅ Health change detected on {target_topic}")

            # Verify other topics remain healthy
            other_topic = test_topics[1]  # Second topic
            other_partitions = async_monitor.get_healthy_partitions(other_topic)
            assert len(other_partitions) > 0, (
                f"Other topic {other_topic} should remain healthy"
            )
            print(
                f"✅ Other topic {other_topic} remains healthy: {len(other_partitions)} partitions"
            )

            # Verify Redis contains health data for both topics with independent states
            target_health_key = f"kafka_health:state:{target_topic}"
            other_health_key = f"kafka_health:state:{other_topic}"

            target_health = redis_client.get(target_health_key)
            other_health = redis_client.get(other_health_key)

            assert target_health != other_health, (
                "Topics should have independent health states"
            )
            print("✅ Redis contains independent health data for both topics")

            print("🎉 Async multi-topic monitoring completed successfully")

        finally:
            await async_monitor.stop()

    # ==========================================
    # HELPER METHODS FOR ASYNC TESTING
    # ==========================================

    async def wait_for_health_detection_async(
        self, health_manager, topic, expected_unhealthy_partition, timeout=12
    ):
        """Async version of wait_for_health_detection."""
        start_time = asyncio.get_event_loop().time()

        while (asyncio.get_event_loop().time() - start_time) < timeout:
            healthy_partitions = health_manager.get_healthy_partitions(topic)
            if (
                expected_unhealthy_partition not in healthy_partitions
                and len(healthy_partitions) > 0
            ):
                print(
                    f"Async health detection successful: partition {expected_unhealthy_partition} no longer in {healthy_partitions}"
                )
                return True
            await asyncio.sleep(0.5)

        print(f"Async health detection timeout after {timeout}s")
        return False
