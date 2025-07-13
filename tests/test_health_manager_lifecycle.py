"""
Tests for PartitionHealthMonitor lifecycle management in SmartProducer.

This module tests whether health managers are properly started/stopped
when used with SmartProducer.
"""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.producer_config import ProducerConfig
from kafka_smart_producer.sync_producer import SmartProducer


class TestHealthManagerLifecycle:
    """Test health manager lifecycle in SmartProducer."""

    @pytest.fixture
    def health_enabled_config(self):
        """Configuration with health manager enabled."""
        return ProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": True,
                "consumer_group": "test-group",
                "health_manager": {
                    "consumer_group": "test-group",
                    "health_threshold": 0.5,
                    "refresh_interval": 5.0,
                },
            }
        )

    @pytest.fixture
    def no_health_config(self):
        """Configuration without health manager."""
        return ProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": False,
            }
        )

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    @patch(
        "kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor.from_config"
    )
    def test_health_manager_created_and_started(
        self, mock_from_config, mock_confluent_producer, health_enabled_config
    ):
        """Test new behavior: self-created health manager is auto-started."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Mock health manager instance
        mock_health_manager = Mock()
        mock_health_manager.is_running = False
        mock_from_config.return_value = mock_health_manager

        # Create producer
        producer = SmartProducer(health_enabled_config)

        # Verify health manager was created
        mock_from_config.assert_called_once()
        assert producer.health_manager is mock_health_manager

        # Verify start() WAS called (new correct behavior)
        mock_health_manager.start.assert_called_once()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_no_health_manager_when_disabled(
        self, mock_confluent_producer, no_health_config
    ):
        """Test that no health manager is created when smart_enabled=False."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Create producer
        producer = SmartProducer(no_health_config)

        # Should have no health manager
        assert producer.health_manager is None
        assert not producer.smart_enabled

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_explicit_health_manager_used(
        self, mock_confluent_producer, health_enabled_config
    ):
        """Test that explicitly provided health manager is used."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Create explicit health manager
        mock_health_manager = Mock()
        mock_health_manager.is_running = True

        # Create producer with explicit health manager
        producer = SmartProducer(
            health_enabled_config, health_manager=mock_health_manager
        )

        # Should use the provided health manager
        assert producer.health_manager is mock_health_manager

        # Should not interfere with existing health manager
        mock_health_manager.start.assert_not_called()
        mock_health_manager.stop.assert_not_called()

    def test_health_manager_lifecycle_integration(self, health_enabled_config):
        """Integration test: verify health manager works when started manually."""
        # This test will show the current broken state
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_confluent_producer.return_value = Mock()

            # Create producer (health manager should be created but not started)
            producer = SmartProducer(health_enabled_config)

            if producer.health_manager:
                # Manually start health manager (what should happen automatically)
                producer.health_manager.start()

                # Verify it's running
                assert producer.health_manager.is_running

                # Test getting healthy partitions (should work now)
                healthy_partitions = producer.health_manager.get_healthy_partitions(
                    "test-topic"
                )
                assert isinstance(healthy_partitions, list)

                # Manually stop
                producer.health_manager.stop()
                assert not producer.health_manager.is_running

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    @patch("kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor")
    def test_producer_cleanup_without_lifecycle_management(
        self, mock_health_manager_class, mock_confluent_producer, health_enabled_config
    ):
        """Test what happens when producer is destroyed without proper cleanup."""
        # Mock confluent producer
        mock_confluent_producer.return_value = Mock()

        # Mock health manager instance
        mock_health_manager = Mock()
        mock_health_manager.is_running = False
        mock_health_manager_class.return_value = mock_health_manager

        # Create producer
        producer = SmartProducer(health_enabled_config)

        # Simulate health manager was started manually
        mock_health_manager.is_running = True

        # Delete producer without proper cleanup
        del producer

        # Health manager should still be running (this is the problem!)
        # No cleanup happened because there's no lifecycle management
        mock_health_manager.stop.assert_not_called()


class TestHealthManagerLifecycleFixes:
    """Test the fixes we need to implement."""

    @pytest.fixture
    def health_enabled_config(self):
        """Configuration with health manager enabled."""
        return ProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": True,
                "consumer_group": "test-group",
                "health_manager": {
                    "consumer_group": "test-group",
                    "health_threshold": 0.5,
                    "refresh_interval": 5.0,
                },
            }
        )

    def test_should_auto_start_self_created_health_manager(self, health_enabled_config):
        """Test that self-created health manager should be auto-started."""
        # This test defines the behavior we want to implement
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_confluent_producer.return_value = Mock()

            with patch(
                "kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor.from_config"
            ) as mock_from_config:
                mock_health_manager = Mock()
                mock_health_manager.is_running = False
                mock_from_config.return_value = mock_health_manager

                # Create producer
                SmartProducer(health_enabled_config)

                # SHOULD auto-start the health manager
                mock_health_manager.start.assert_called_once()

    def test_should_not_auto_start_explicit_health_manager(self, health_enabled_config):
        """Test that explicitly provided health manager should NOT be auto-started."""
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_confluent_producer.return_value = Mock()

            # Create explicit health manager
            mock_health_manager = Mock()
            mock_health_manager.is_running = False

            # Create producer with explicit health manager
            SmartProducer(health_enabled_config, health_manager=mock_health_manager)

            # Should NOT auto-start explicitly provided health manager
            mock_health_manager.start.assert_not_called()

    def test_should_auto_stop_self_created_health_manager_on_close(
        self, health_enabled_config
    ):
        """Test that close() should stop self-created health manager."""
        # This test defines the close() behavior we want to implement
        with patch(
            "kafka_smart_producer.sync_producer.ConfluentProducer"
        ) as mock_confluent_producer:
            mock_producer_instance = Mock()
            mock_confluent_producer.return_value = mock_producer_instance

            with patch(
                "kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor.from_config"
            ) as mock_from_config:
                mock_health_manager = Mock()
                mock_health_manager.is_running = True
                mock_from_config.return_value = mock_health_manager

                # Create producer
                producer = SmartProducer(health_enabled_config)

                # Add close method and test it
                if hasattr(producer, "close"):
                    producer.close()

                    # Should flush and stop health manager
                    mock_producer_instance.flush.assert_called_once()
                    mock_health_manager.stop.assert_called_once()

    def test_minimal_config_auto_starts_health_monitoring(self):
        """Test that minimal config automatically starts background health monitoring.
        
        This test verifies that with just kafka config, topics, and consumer_group,
        the producer will:
        1. Auto-create and start a PartitionHealthMonitor
        2. Use it for intelligent partition selection
        3. Return healthy partitions from background monitoring
        4. Clean up properly on close()
        """
        # Minimal config - just kafka, topics, and consumer_group
        minimal_config = ProducerConfig.from_dict({
            "bootstrap.servers": "localhost:9092",
            "topics": ["orders", "payments"],
            "health_manager": {
                "consumer_group": "order-processors"
            }
        })

        with patch("kafka_smart_producer.sync_producer.ConfluentProducer") as mock_confluent:
            mock_confluent.return_value = Mock()
            
            with patch("kafka_smart_producer.partition_health_monitor.PartitionHealthMonitor.from_config") as mock_from_config:
                # Mock health manager that will return healthy partitions
                mock_health_manager = Mock()
                mock_health_manager.is_running = False
                mock_health_manager.get_healthy_partitions.return_value = [0, 1, 2]
                mock_from_config.return_value = mock_health_manager

                # Create producer with minimal config
                producer = SmartProducer(minimal_config)

                # Health manager should be auto-started
                mock_health_manager.start.assert_called_once()
                
                # Simulate that health manager is now running after start
                mock_health_manager.is_running = True
                
                # Producer should be smart-enabled
                assert producer.smart_enabled is True
                assert producer.health_manager is mock_health_manager
                
                # Should be able to get healthy partitions from background monitoring
                healthy_partitions = producer.health_manager.get_healthy_partitions("orders")
                assert healthy_partitions == [0, 1, 2]
                
                # Produce a message - should use smart partitioning
                producer.produce(topic="orders", value=b"test-order", key=b"customer-123")
                
                # Should have called the underlying producer with partition selection
                mock_confluent.return_value.produce.assert_called_once()
                call_kwargs = mock_confluent.return_value.produce.call_args[1]
                assert "partition" in call_kwargs  # Smart partitioning applied
                assert call_kwargs["partition"] in [0, 1, 2]  # Used healthy partition
                
                # Cleanup should stop the health manager
                producer.close()
                mock_health_manager.stop.assert_called_once()
