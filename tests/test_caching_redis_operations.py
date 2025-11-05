"""
Unit tests for Redis caching operations to improve coverage.

Tests DefaultRemoteCache and DefaultHybridCache with mocked Redis.
"""

import json
from unittest.mock import Mock
import pytest

from kafka_smart_producer.caching import (
    CacheConfig,
    DefaultHybridCache,
    DefaultLocalCache,
    DefaultRemoteCache,
)


class TestDefaultRemoteCacheOperations:
    """Test DefaultRemoteCache with mocked Redis."""

    def test_serialize_integer_value(self):
        """Test _serialize_value() with integer (optimized path)."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test integer serialization
        assert cache._serialize_value(42) == "42"
        assert cache._serialize_value(0) == "0"
        assert cache._serialize_value(-100) == "-100"

    def test_serialize_string_value(self):
        """Test _serialize_value() with string."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test string serialization
        assert cache._serialize_value("hello") == "hello"
        assert cache._serialize_value("42") == "42"

    def test_serialize_float_value(self):
        """Test _serialize_value() with float."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test float serialization
        assert cache._serialize_value(3.14) == "3.14"
        assert cache._serialize_value(0.0) == "0.0"

    def test_serialize_dict_value(self):
        """Test _serialize_value() with dict (JSON path)."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test dict serialization
        data = {"key": "value", "number": 123}
        result = cache._serialize_value(data)

        # Should have json: prefix
        assert result.startswith("json:")
        # Parse the JSON part
        parsed = json.loads(result[5:])
        assert parsed["key"] == "value"
        assert parsed["number"] == 123

    def test_serialize_list_value(self):
        """Test _serialize_value() with list (JSON path)."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test list serialization
        data = [1, 2, 3, "test"]
        result = cache._serialize_value(data)

        # Should have json: prefix
        assert result.startswith("json:")
        parsed = json.loads(result[5:])
        assert parsed == [1, 2, 3, "test"]

    def test_deserialize_integer_string(self):
        """Test _deserialize_value() with integer string."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test integer deserialization
        assert cache._deserialize_value("42") == 42
        assert cache._deserialize_value("0") == 0
        assert cache._deserialize_value("-100") == -100

    def test_deserialize_plain_string(self):
        """Test _deserialize_value() with plain string (not integer)."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Non-integer strings are returned as-is
        assert cache._deserialize_value("hello") == "hello"
        assert cache._deserialize_value("3.14") == "3.14"

    def test_deserialize_json_string(self):
        """Test _deserialize_value() with JSON string."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test JSON deserialization with json: prefix
        json_str = 'json:{"key": "value", "number": 123}'
        result = cache._deserialize_value(json_str)

        assert isinstance(result, dict)
        assert result["key"] == "value"
        assert result["number"] == 123

    def test_get_existing_key(self):
        """Test get() with existing key."""
        mock_redis = Mock()
        # Redis returns bytes
        mock_redis.get.return_value = b"42"
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test get
        result = cache.get("test-key")

        assert result == 42
        mock_redis.get.assert_called_once_with("test-key")

    def test_get_missing_key(self):
        """Test get() with missing key."""
        mock_redis = Mock()
        mock_redis.get.return_value = None
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test get non-existent
        result = cache.get("missing-key")

        assert result is None

    def test_get_json_value(self):
        """Test get() with JSON value."""
        mock_redis = Mock()
        # Redis returns bytes with json: prefix
        mock_redis.get.return_value = b'json:{"key": "value"}'
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        result = cache.get("test-key")

        assert isinstance(result, dict)
        assert result["key"] == "value"

    def test_set_with_ttl(self):
        """Test set() with TTL."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test set with TTL
        cache.set("test-key", 42, ttl_seconds=60)

        # Verify setex was called
        mock_redis.setex.assert_called_once_with("test-key", 60, "42")

    def test_set_without_ttl_uses_default(self):
        """Test set() without TTL uses default from config."""
        mock_redis = Mock()
        config = CacheConfig(remote_default_ttl_seconds=300)
        cache = DefaultRemoteCache(mock_redis, config)

        # Test set without TTL
        cache.set("test-key", 42)

        # Should use default TTL
        mock_redis.setex.assert_called_once_with("test-key", 300, "42")

    def test_set_dict_value(self):
        """Test set() with dict value."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        data = {"key": "value"}
        cache.set("test-key", data, ttl_seconds=60)

        # Verify serialized with json: prefix
        call_args = mock_redis.setex.call_args[0]
        assert call_args[2].startswith("json:")

    def test_delete_key(self):
        """Test delete() operation."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test delete
        cache.delete("test-key")

        mock_redis.delete.assert_called_once_with("test-key")

    def test_ping_success(self):
        """Test ping() returns True when Redis is available."""
        mock_redis = Mock()
        mock_redis.ping.return_value = True
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        assert cache.ping() is True

    def test_ping_failure(self):
        """Test ping() returns False when Redis fails."""
        mock_redis = Mock()
        mock_redis.ping.side_effect = Exception("Connection failed")
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        assert cache.ping() is False

    def test_publish_health_data(self):
        """Test publish_health_data() to Redis."""
        mock_redis = Mock()
        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test publish health data
        health_data = {0: 0.9, 1: 0.8, 2: 0.7}
        cache.publish_health_data("test-topic", health_data)

        # Verify setex was called with state key and healthy key
        assert mock_redis.setex.call_count == 2

        # Check first call - state key
        first_call = mock_redis.setex.call_args_list[0]
        assert first_call[0][0] == "kafka_health:state:test-topic"
        assert first_call[0][1] == 300  # TTL

    def test_get_health_data(self):
        """Test get_health_data() from Redis."""
        mock_redis = Mock()

        # Return JSON with partitions data
        health_payload = {
            "topic": "test-topic",
            "partitions": {"0": 0.9, "1": 0.8},
            "timestamp": 1234567890,
            "healthy_count": 2,
            "total_count": 2,
            "health_threshold": 0.5,
        }
        mock_redis.get.return_value = json.dumps(health_payload).encode("utf-8")

        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        # Test get health data
        result = cache.get_health_data("test-topic")

        # Should convert string keys to int keys
        assert isinstance(result, dict)
        assert 0 in result
        assert result[0] == 0.9
        assert result[1] == 0.8

    def test_get_health_data_missing(self):
        """Test get_health_data() when no data exists."""
        mock_redis = Mock()
        mock_redis.get.return_value = None

        config = CacheConfig()
        cache = DefaultRemoteCache(mock_redis, config)

        result = cache.get_health_data("test-topic")
        assert result is None


class TestDefaultHybridCacheOperations:
    """Test DefaultHybridCache with mocked components."""

    def test_get_from_local_cache_hit(self):
        """Test get() when value exists in local cache."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        mock_redis = Mock()

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        # Populate local cache
        local.set("test-key", 42, ttl_seconds=60)

        # Get from hybrid (should hit local, not remote)
        result = hybrid.get("test-key")

        assert result == 42
        # Remote should NOT be called
        mock_redis.get.assert_not_called()

    def test_get_from_remote_cache_miss_local(self):
        """Test get() when local cache misses, fetches from remote."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        mock_redis = Mock()
        mock_redis.get.return_value = b"99"

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        # Get from hybrid (local miss, remote hit)
        result = hybrid.get("test-key")

        assert result == 99
        # Remote should be called
        mock_redis.get.assert_called_once()
        # Value should be cached locally after remote hit
        assert local.get("test-key") == 99

    def test_get_miss_both_caches(self):
        """Test get() when both caches miss."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        mock_redis = Mock()
        mock_redis.get.return_value = None

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        result = hybrid.get("missing-key")
        assert result is None

    def test_set_to_both_caches(self):
        """Test set() writes to both local and remote."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        mock_redis = Mock()

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        # Set in hybrid
        hybrid.set("test-key", 42, ttl_seconds=60)

        # Verify both caches have the value
        assert local.get("test-key") == 42
        mock_redis.setex.assert_called_once()

    def test_delete_from_both_caches(self):
        """Test delete() removes from both local and remote."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        local.set("test-key", 42, ttl_seconds=60)

        mock_redis = Mock()

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        # Delete from hybrid
        hybrid.delete("test-key")

        # Verify both caches deleted
        assert local.get("test-key") is None
        mock_redis.delete.assert_called_once()

    def test_publish_health_data_to_remote(self):
        """Test publish_health_data() delegates to remote."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        mock_redis = Mock()

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        # Publish health data
        health_data = {0: 0.9, 1: 0.8}
        hybrid.publish_health_data("test-topic", health_data)

        # Verify remote was called (should call setex twice: state + healthy)
        assert mock_redis.setex.call_count == 2

    def test_get_health_data_from_remote(self):
        """Test get_health_data() fetches from remote."""
        local = DefaultLocalCache(CacheConfig(local_max_size=100, local_default_ttl_seconds=60))
        mock_redis = Mock()

        health_payload = {
            "topic": "test-topic",
            "partitions": {"0": 0.9},
            "timestamp": 1234567890,
        }
        mock_redis.get.return_value = json.dumps(health_payload).encode("utf-8")

        config = CacheConfig()
        remote = DefaultRemoteCache(mock_redis, config)
        hybrid = DefaultHybridCache(local_cache=local, remote_cache=remote, config=config)

        # Get health data
        result = hybrid.get_health_data("test-topic")

        assert isinstance(result, dict)
        assert 0 in result
        assert result[0] == 0.9
