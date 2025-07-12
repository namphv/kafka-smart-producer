"""
Tests for BasePartitionSelector logic.

This module tests the core partition selection algorithm that implements
the improved logic flow for smart partition selection.
"""

from unittest.mock import Mock, patch

import pytest

from kafka_smart_producer.producer_utils import BasePartitionSelector


class TestBasePartitionSelector:
    """Test the BasePartitionSelector logic comprehensively."""

    @pytest.fixture
    def mock_health_manager(self):
        """Mock health manager with configurable healthy partitions."""
        health_manager = Mock()
        health_manager.get_healthy_partitions.return_value = [0, 2, 4]
        return health_manager

    @pytest.fixture
    def mock_cache(self):
        """Mock cache with configurable get/set behavior."""
        cache = Mock()
        cache.get.return_value = None  # Default: cache miss
        return cache

    def test_key_with_stickiness_cache_hit(self, mock_health_manager, mock_cache):
        """Test key + stickiness + cache hit scenario."""
        # Setup cache hit and health manager
        mock_cache.get.return_value = 3
        mock_health_manager.get_healthy_partitions.return_value = [0, 2, 4]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        result = selector.select_partition("test-topic", b"user-123")

        # Should return cached partition
        assert result == 3

        # Should check cache
        mock_cache.get.assert_called_once_with("test-topic:user-123")

        # Should call health manager (always calls _get_selected_partition first)
        mock_health_manager.get_healthy_partitions.assert_called_once_with("test-topic")

        # Should NOT cache set (cache hit)
        mock_cache.set.assert_not_called()

    def test_key_with_stickiness_cache_miss(self, mock_health_manager, mock_cache):
        """Test key + stickiness + cache miss scenario."""
        # Setup cache miss
        mock_cache.get.return_value = None
        mock_health_manager.get_healthy_partitions.return_value = [1, 3, 5]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        result = selector.select_partition("test-topic", b"user-456")

        # Should return one of the healthy partitions
        assert result in [1, 3, 5]

        # Should check cache first
        mock_cache.get.assert_called_once_with("test-topic:user-456")

        # Should get healthy partitions
        mock_health_manager.get_healthy_partitions.assert_called_once_with("test-topic")

        # Should cache the selected partition
        mock_cache.set.assert_called_once_with("test-topic:user-456", result)

    def test_key_without_stickiness(self, mock_health_manager, mock_cache):
        """Test key + no stickiness scenario."""
        mock_health_manager.get_healthy_partitions.return_value = [0, 1, 4]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=False,
        )

        result = selector.select_partition("test-topic", b"event-789")

        # Should return one of the healthy partitions
        assert result in [0, 1, 4]

        # Should get healthy partitions
        mock_health_manager.get_healthy_partitions.assert_called_once_with("test-topic")

        # Should NOT check or use cache
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()

    def test_no_key(self, mock_health_manager, mock_cache):
        """Test no key scenario."""
        mock_health_manager.get_healthy_partitions.return_value = [2, 3, 5]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,  # Irrelevant when no key
        )

        result = selector.select_partition("test-topic", None)

        # Should return one of the healthy partitions
        assert result in [2, 3, 5]

        # Should get healthy partitions
        mock_health_manager.get_healthy_partitions.assert_called_once_with("test-topic")

        # Should NOT use cache (no key to cache)
        mock_cache.get.assert_not_called()
        mock_cache.set.assert_not_called()

    def test_no_health_manager(self, mock_cache):
        """Test behavior when no health manager is available."""
        mock_cache.get.return_value = None  # Cache miss

        selector = BasePartitionSelector(
            health_manager=None, cache=mock_cache, use_key_stickiness=True
        )

        result = selector.select_partition("test-topic", b"any-key")

        # Should return None (let Kafka handle default partitioning)
        assert result is None

        # Should check cache first even without health manager
        mock_cache.get.assert_called_once_with("test-topic:any-key")

        # Should NOT cache set (no healthy partition to cache)
        mock_cache.set.assert_not_called()

    def test_cache_miss_no_healthy_partitions(self, mock_health_manager, mock_cache):
        """Test cache miss with no healthy partitions available."""
        # Setup cache miss and no healthy partitions
        mock_cache.get.return_value = None
        mock_health_manager.get_healthy_partitions.return_value = []

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        result = selector.select_partition("test-topic", b"user-123")

        # Should return None (no healthy partitions to select)
        assert result is None

        # Should check cache and health manager
        mock_cache.get.assert_called_once_with("test-topic:user-123")
        mock_health_manager.get_healthy_partitions.assert_called_once_with("test-topic")

        # Should NOT cache anything
        mock_cache.set.assert_not_called()

    def test_health_manager_exception(self, mock_health_manager, mock_cache):
        """Test exception handling in health manager calls."""
        mock_health_manager.get_healthy_partitions.side_effect = Exception(
            "Health check failed"
        )

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=False,
        )

        result = selector.select_partition("test-topic", b"test-key")

        # Should return None and not crash
        assert result is None

    def test_cache_exception_on_get(self, mock_health_manager, mock_cache):
        """Test exception handling in cache get operations."""
        mock_cache.get.side_effect = Exception("Cache get failed")
        mock_health_manager.get_healthy_partitions.return_value = [1, 2]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        result = selector.select_partition("test-topic", b"user-123")

        # Should fallback to health manager selection
        assert result in [1, 2]

        # Should still try to cache the result
        mock_cache.set.assert_called_once()

    def test_cache_exception_on_set(self, mock_health_manager, mock_cache):
        """Test exception handling in cache set operations."""
        mock_cache.get.return_value = None
        mock_cache.set.side_effect = Exception("Cache set failed")
        mock_health_manager.get_healthy_partitions.return_value = [0, 1]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        result = selector.select_partition("test-topic", b"user-456")

        # Should still return healthy partition despite cache set failure
        assert result in [0, 1]

    def test_force_refresh_on_empty_partitions(self, mock_health_manager, mock_cache):
        """Test force refresh when no healthy partitions are found."""
        mock_health_manager.get_healthy_partitions.return_value = []
        mock_health_manager.force_refresh_threadsafe = Mock()

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=False,
        )

        result = selector.select_partition("test-topic", b"test-key")

        # Should return None
        assert result is None

        # Should trigger force refresh
        mock_health_manager.force_refresh_threadsafe.assert_called_once_with(
            "test-topic"
        )

    def test_force_refresh_fallback(self, mock_health_manager, mock_cache):
        """Test fallback to force_refresh if force_refresh_threadsafe unavailable."""
        mock_health_manager.get_healthy_partitions.return_value = []
        mock_health_manager.force_refresh = Mock()

        # Mock hasattr to return False for force_refresh_threadsafe
        with patch("builtins.hasattr", return_value=False):
            selector = BasePartitionSelector(
                health_manager=mock_health_manager,
                cache=mock_cache,
                use_key_stickiness=False,
            )

            result = selector.select_partition("test-topic", b"test-key")

            # Should return None
            assert result is None

            # Should trigger regular force refresh
            mock_health_manager.force_refresh.assert_called_once_with("test-topic")

    def test_unicode_key_handling(self, mock_health_manager, mock_cache):
        """Test handling of unicode keys in cache operations."""
        mock_cache.get.return_value = 2

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        # Test with unicode key
        unicode_key = "用户-123".encode()
        result = selector.select_partition("test-topic", unicode_key)

        assert result == 2

        # Should handle unicode in cache key
        expected_cache_key = "test-topic:用户-123"
        mock_cache.get.assert_called_once_with(expected_cache_key)

    def test_malformed_key_handling(self, mock_health_manager, mock_cache):
        """Test handling of malformed byte keys."""
        mock_cache.get.return_value = None
        mock_health_manager.get_healthy_partitions.return_value = [1]

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        # Test with malformed bytes that can't decode to UTF-8
        malformed_key = b"\xff\xfe\xfd"
        result = selector.select_partition("test-topic", malformed_key)

        assert result == 1

        # Should handle decode errors gracefully using replacement characters
        mock_cache.get.assert_called_once()
        cache_key_used = mock_cache.get.call_args[0][0]
        assert cache_key_used.startswith("test-topic:")

    def test_deterministic_selection_with_same_key(
        self, mock_health_manager, mock_cache
    ):
        """Test that same key consistently gets same partition when cached."""
        mock_cache.get.return_value = 3

        selector = BasePartitionSelector(
            health_manager=mock_health_manager,
            cache=mock_cache,
            use_key_stickiness=True,
        )

        # Multiple calls with same key should return same result
        key = b"consistent-key"
        result1 = selector.select_partition("test-topic", key)
        result2 = selector.select_partition("test-topic", key)
        result3 = selector.select_partition("test-topic", key)

        assert result1 == result2 == result3 == 3

        # Cache should be checked each time
        assert mock_cache.get.call_count == 3
