"""Tests for RedisHealthPublisher and RedisHealthConsumer (contrib)."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture
def mock_redis():
    with patch(
        "kafka_smart_producer.contrib.redis_health_consumer._import_redis"
    ) as mock_import:
        mock_mod = MagicMock()
        mock_client = MagicMock()
        mock_mod.from_url.return_value = mock_client
        mock_import.return_value = mock_mod
        yield mock_client


class TestRedisHealthPublisher:
    def test_publish_scores(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthPublisher,
        )

        publisher = RedisHealthPublisher(ttl=120.0)
        scores = {0: 1.0, 1: 0.5, 2: 0.0}
        publisher.publish_scores("my-topic", scores)

        # Check setex was called
        setex_call = mock_redis.setex.call_args
        key = setex_call[0][0]
        ttl = setex_call[0][1]
        data = json.loads(setex_call[0][2])

        assert key == "ksp:health:my-topic"
        assert ttl == 120
        assert data["scores"] == {"0": 1.0, "1": 0.5, "2": 0.0}
        assert "timestamp" in data

        # Check publish was called
        mock_redis.publish.assert_called_once()
        pub_channel = mock_redis.publish.call_args[0][0]
        assert pub_channel == "ksp:health:update:my-topic"

    def test_publish_error_silent(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthPublisher,
        )

        mock_redis.setex.side_effect = Exception("connection lost")
        publisher = RedisHealthPublisher()
        publisher.publish_scores("topic", {0: 1.0})  # should not raise

    def test_is_healthy(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthPublisher,
        )

        mock_redis.ping.return_value = True
        publisher = RedisHealthPublisher()
        assert publisher.is_healthy() is True


class TestRedisHealthConsumer:
    def test_get_lag_data(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        data = {
            "scores": {"0": 1.0, "1": 0.5, "2": 0.0},
            "timestamp": time.time(),
        }
        mock_redis.get.return_value = json.dumps(data)

        consumer = RedisHealthConsumer()
        lag = consumer.get_lag_data("my-topic")

        # score 1.0 -> lag 0, score 0.5 -> lag 500, score 0.0 -> lag 1000
        assert lag == {0: 0, 1: 500, 2: 1000}
        mock_redis.get.assert_called_with("ksp:health:my-topic")

    def test_get_lag_data_missing(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        mock_redis.get.return_value = None
        consumer = RedisHealthConsumer()
        assert consumer.get_lag_data("my-topic") == {}

    def test_get_lag_data_stale(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        data = {
            "scores": {"0": 1.0},
            "timestamp": time.time() - 300,  # 5 minutes old
        }
        mock_redis.get.return_value = json.dumps(data)

        consumer = RedisHealthConsumer(max_age=120.0)
        assert consumer.get_lag_data("my-topic") == {}

    def test_get_lag_data_error_returns_empty(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        mock_redis.get.side_effect = Exception("connection lost")
        consumer = RedisHealthConsumer()
        assert consumer.get_lag_data("my-topic") == {}

    def test_get_health_scores(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        data = {
            "scores": {"0": 1.0, "1": 0.5, "2": 0.0},
            "timestamp": time.time(),
        }
        mock_redis.get.return_value = json.dumps(data)

        consumer = RedisHealthConsumer()
        scores = consumer.get_health_scores("my-topic")
        assert scores == {0: 1.0, 1: 0.5, 2: 0.0}

    def test_get_health_scores_stale(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        data = {
            "scores": {"0": 1.0},
            "timestamp": time.time() - 300,
        }
        mock_redis.get.return_value = json.dumps(data)

        consumer = RedisHealthConsumer(max_age=120.0)
        assert consumer.get_health_scores("my-topic") is None

    def test_get_health_scores_missing(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        mock_redis.get.return_value = None
        consumer = RedisHealthConsumer()
        assert consumer.get_health_scores("my-topic") is None

    def test_is_healthy(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        mock_redis.ping.return_value = True
        consumer = RedisHealthConsumer()
        assert consumer.is_healthy() is True

    def test_is_healthy_when_down(self, mock_redis):
        from kafka_smart_producer.contrib.redis_health_consumer import (
            RedisHealthConsumer,
        )

        mock_redis.ping.side_effect = Exception("down")
        consumer = RedisHealthConsumer()
        assert consumer.is_healthy() is False
