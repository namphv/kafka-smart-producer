"""Tests for Redis cache and HybridCache (contrib)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from kafka_smart_producer.cache.local import LocalLRUCache


@pytest.fixture
def mock_redis():
    with patch("kafka_smart_producer.contrib.redis_cache._import_redis") as mock_import:
        mock_mod = MagicMock()
        mock_client = MagicMock()
        mock_mod.from_url.return_value = mock_client
        mock_import.return_value = mock_mod
        yield mock_client


class TestRemoteCache:
    def test_get_hit(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        mock_redis.get.return_value = json.dumps(42)
        cache = RemoteCache()
        assert cache.get("mykey") == 42
        mock_redis.get.assert_called_with("ksp:mykey")

    def test_get_miss(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        mock_redis.get.return_value = None
        cache = RemoteCache()
        assert cache.get("mykey") is None

    def test_get_error_returns_none(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        mock_redis.get.side_effect = Exception("connection lost")
        cache = RemoteCache()
        assert cache.get("mykey") is None

    def test_set(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        cache = RemoteCache(default_ttl=600.0)
        cache.set("mykey", {"partition": 3})
        mock_redis.setex.assert_called_with(
            "ksp:mykey", 600, json.dumps({"partition": 3})
        )

    def test_set_custom_ttl(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        cache = RemoteCache(default_ttl=600.0)
        cache.set("mykey", 5, ttl_seconds=30.0)
        mock_redis.setex.assert_called_with("ksp:mykey", 30, json.dumps(5))

    def test_set_error_silent(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        mock_redis.setex.side_effect = Exception("connection lost")
        cache = RemoteCache()
        cache.set("mykey", 5)  # should not raise

    def test_delete(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        cache = RemoteCache()
        cache.delete("mykey")
        mock_redis.delete.assert_called_with("ksp:mykey")

    def test_custom_prefix(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        cache = RemoteCache(key_prefix="app:")
        cache.get("mykey")
        mock_redis.get.assert_called_with("app:mykey")

    def test_is_healthy(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        mock_redis.ping.return_value = True
        cache = RemoteCache()
        assert cache.is_healthy() is True

    def test_is_healthy_when_down(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import RemoteCache

        mock_redis.ping.side_effect = Exception("down")
        cache = RemoteCache()
        assert cache.is_healthy() is False


class TestHybridCache:
    def test_get_from_l1(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

        local = LocalLRUCache(max_size=100, default_ttl=60.0)
        remote = RemoteCache()
        hybrid = HybridCache(local=local, remote=remote)

        local.set("key1", 42)
        assert hybrid.get("key1") == 42
        mock_redis.get.assert_not_called()  # didn't need L2

    def test_get_l1_miss_l2_hit(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

        local = LocalLRUCache(max_size=100, default_ttl=60.0)
        remote = RemoteCache()
        hybrid = HybridCache(local=local, remote=remote)

        mock_redis.get.return_value = json.dumps(99)
        result = hybrid.get("key1")
        assert result == 99
        # Should have populated L1
        assert local.get("key1") == 99

    def test_get_both_miss(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

        local = LocalLRUCache(max_size=100, default_ttl=60.0)
        remote = RemoteCache()
        hybrid = HybridCache(local=local, remote=remote)

        mock_redis.get.return_value = None
        assert hybrid.get("key1") is None

    def test_set_writes_both(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

        local = LocalLRUCache(max_size=100, default_ttl=60.0)
        remote = RemoteCache(default_ttl=900.0)
        hybrid = HybridCache(local=local, remote=remote)

        hybrid.set("key1", 7)
        assert local.get("key1") == 7
        mock_redis.setex.assert_called_once()

    def test_delete_from_both(self, mock_redis):
        from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

        local = LocalLRUCache(max_size=100, default_ttl=60.0)
        remote = RemoteCache()
        hybrid = HybridCache(local=local, remote=remote)

        local.set("key1", 42)
        hybrid.delete("key1")
        assert local.get("key1") is None
        mock_redis.delete.assert_called_with("ksp:key1")
