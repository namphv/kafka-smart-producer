"""Tests for LocalLRUCache."""

import time

import pytest

from kafka_smart_producer.cache.local import LocalLRUCache


class TestLocalLRUCache:
    def test_get_set(self):
        cache = LocalLRUCache()
        cache.set("k", "v")
        assert cache.get("k") == "v"

    def test_get_missing_returns_none(self):
        cache = LocalLRUCache()
        assert cache.get("missing") is None

    def test_delete(self):
        cache = LocalLRUCache()
        cache.set("k", "v")
        cache.delete("k")
        assert cache.get("k") is None

    def test_delete_missing_is_noop(self):
        cache = LocalLRUCache()
        cache.delete("missing")  # should not raise

    def test_ttl_expiration(self):
        cache = LocalLRUCache(default_ttl=0.05)
        cache.set("k", "v")
        assert cache.get("k") == "v"
        time.sleep(0.06)
        assert cache.get("k") is None

    def test_custom_ttl_per_entry(self):
        cache = LocalLRUCache(default_ttl=300.0)
        cache.set("k", "v", ttl_seconds=0.05)
        assert cache.get("k") == "v"
        time.sleep(0.06)
        assert cache.get("k") is None

    def test_lru_eviction(self):
        cache = LocalLRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        # "a" is oldest
        cache.set("d", 4)
        assert cache.get("a") is None
        assert cache.get("b") == 2
        assert cache.get("d") == 4

    def test_lru_access_refreshes_position(self):
        cache = LocalLRUCache(max_size=3)
        cache.set("a", 1)
        cache.set("b", 2)
        cache.set("c", 3)
        # Access "a" to make it most recent
        cache.get("a")
        # Now "b" is oldest
        cache.set("d", 4)
        assert cache.get("a") == 1  # kept
        assert cache.get("b") is None  # evicted

    def test_overwrite_existing_key(self):
        cache = LocalLRUCache()
        cache.set("k", "old")
        cache.set("k", "new")
        assert cache.get("k") == "new"

    def test_clear(self):
        cache = LocalLRUCache()
        cache.set("a", 1)
        cache.set("b", 2)
        cache.clear()
        assert len(cache) == 0
        assert cache.get("a") is None

    def test_len(self):
        cache = LocalLRUCache()
        assert len(cache) == 0
        cache.set("a", 1)
        assert len(cache) == 1

    def test_invalid_max_size(self):
        with pytest.raises(ValueError, match="max_size"):
            LocalLRUCache(max_size=0)

    def test_invalid_ttl(self):
        with pytest.raises(ValueError, match="default_ttl"):
            LocalLRUCache(default_ttl=0)

    def test_stores_various_types(self):
        cache = LocalLRUCache()
        cache.set("int", 42)
        cache.set("list", [1, 2, 3])
        cache.set("dict", {"a": 1})
        assert cache.get("int") == 42
        assert cache.get("list") == [1, 2, 3]
        assert cache.get("dict") == {"a": 1}
