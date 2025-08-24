"""
Simple integration test for health manager lifecycle.
"""

from unittest.mock import patch

import pytest

from kafka_smart_producer.producer_config import SmartProducerConfig
from kafka_smart_producer.sync_producer import SmartProducer


class TestSimpleHealthManagerLifecycle:
    """Simple test for health manager lifecycle."""

    @pytest.fixture
    def health_config(self):
        """Config with health manager enabled."""
        return SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": True,
                "health_manager": {
                    "consumer_group": "test-group",
                    "health_threshold": 0.5,
                    "refresh_interval": 10.0,
                    "timeout_seconds": 5.0,
                },
            }
        )

    @pytest.fixture
    def no_health_config(self):
        """Config with no health manager."""
        return SmartProducerConfig.from_dict(
            {
                "bootstrap.servers": "localhost:9092",
                "topics": ["test-topic"],
                "smart_enabled": False,
            }
        )

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_with_health_manager_lifecycle(self, mock_producer, health_config):
        """Test that health manager is auto-started and can be manually closed."""
        mock_producer.return_value = mock_producer

        # Create producer - health manager should auto-start
        producer = SmartProducer(health_config)

        # Should have health manager
        assert producer.health_manager is not None
        assert producer.smart_enabled

        # Should be able to close cleanly
        producer.close()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_without_health_manager(self, mock_producer, no_health_config):
        """Test producer without health manager."""
        mock_producer.return_value = mock_producer

        # Create producer
        producer = SmartProducer(no_health_config)

        # Should have no health manager
        assert producer.health_manager is None
        assert not producer.smart_enabled

        # Should still be able to close cleanly
        producer.close()

    @patch("kafka_smart_producer.sync_producer.ConfluentProducer")
    def test_explicit_health_manager_not_managed(self, mock_producer, health_config):
        """Test that explicit health manager is not managed by producer."""
        mock_producer.return_value = mock_producer

        # Create external health manager (mock)
        from unittest.mock import Mock

        external_hm = Mock()
        external_hm.is_running = False

        # Create producer with explicit health manager
        producer = SmartProducer(health_config, health_manager=external_hm)

        # Should use the external health manager
        assert producer.health_manager is external_hm

        # Should not have called start on external health manager
        external_hm.start.assert_not_called()

        # Close should not call stop on external health manager
        producer.close()
        external_hm.stop.assert_not_called()
