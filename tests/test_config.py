"""Tests for SmartProducerConfig."""

import pytest

from kafka_smart_producer.config import SmartProducerConfig


class TestSmartProducerConfig:
    def _minimal(self, **overrides):
        defaults = {
            "kafka_config": {"bootstrap.servers": "localhost:9092"},
            "topics": ["test-topic"],
        }
        defaults.update(overrides)
        return SmartProducerConfig(**defaults)

    def test_minimal_config(self):
        cfg = self._minimal()
        assert cfg.topics == ["test-topic"]
        assert cfg.health_threshold == 0.5
        assert not cfg.has_health_monitoring
        assert not cfg.has_redis

    def test_with_consumer_group(self):
        cfg = self._minimal(consumer_group="my-group")
        assert cfg.has_health_monitoring
        assert not cfg.has_redis

    def test_with_redis(self):
        cfg = self._minimal(
            consumer_group="my-group",
            redis_url="redis://localhost:6379/0",
        )
        assert cfg.has_health_monitoring
        assert cfg.has_redis

    def test_smart_disabled(self):
        cfg = self._minimal(consumer_group="my-group", smart_enabled=False)
        assert not cfg.has_health_monitoring

    def test_get_kafka_config_strips_internal_keys(self):
        cfg = self._minimal()
        kafka = cfg.get_kafka_config()
        assert "bootstrap.servers" in kafka
        assert "topics" not in kafka

    def test_invalid_kafka_config(self):
        with pytest.raises(ValueError, match="kafka_config must be a dict"):
            SmartProducerConfig(kafka_config="bad", topics=["t"])

    def test_empty_topics(self):
        with pytest.raises(ValueError, match="topics must be a non-empty list"):
            SmartProducerConfig(kafka_config={"bootstrap.servers": "x"}, topics=[])

    def test_invalid_threshold(self):
        with pytest.raises(ValueError, match="health_threshold"):
            self._minimal(health_threshold=1.5)

    def test_invalid_refresh_interval(self):
        with pytest.raises(ValueError, match="refresh_interval"):
            self._minimal(refresh_interval=-1)

    def test_invalid_max_lag(self):
        with pytest.raises(ValueError, match="max_lag"):
            self._minimal(max_lag=0)

    def test_invalid_cache_max_size(self):
        with pytest.raises(ValueError, match="cache_max_size"):
            self._minimal(cache_max_size=0)

    def test_invalid_topics_type(self):
        with pytest.raises(ValueError, match="all topics must be strings"):
            self._minimal(topics=[123])

    def test_invalid_collector_timeout(self):
        with pytest.raises(ValueError, match="collector_timeout"):
            self._minimal(collector_timeout=0)

    def test_invalid_cache_ttl(self):
        with pytest.raises(ValueError, match="cache_ttl"):
            self._minimal(cache_ttl=-1)

    def test_defaults(self):
        cfg = self._minimal()
        assert cfg.refresh_interval == 30.0
        assert cfg.max_lag == 1000
        assert cfg.cache_max_size == 1000
        assert cfg.cache_ttl == 300.0
        assert cfg.redis_ttl == 900.0
        assert cfg.key_stickiness is True
