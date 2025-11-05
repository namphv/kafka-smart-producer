"""Tests verifying protocol contracts via duck typing."""

from kafka_smart_producer.cache.protocols import Cache
from kafka_smart_producer.health.protocols import (
    HealthScorer,
    HealthSignalCollector,
    PartitionSelector,
)


class FakeCollector:
    def get_lag_data(self, topic: str) -> dict[int, int]:
        return {0: 100, 1: 200}

    def is_healthy(self) -> bool:
        return True


class FakeScorer:
    def calculate_scores(self, lag_data: dict[int, int]) -> dict[int, float]:
        return dict.fromkeys(lag_data, 1.0)


class FakeSelector:
    def select(self, healthy_partitions, key=None):
        return healthy_partitions[0] if healthy_partitions else None


class FakeCache:
    def __init__(self):
        self._data = {}

    def get(self, key):
        return self._data.get(key)

    def set(self, key, value, ttl_seconds=None):
        self._data[key] = value

    def delete(self, key):
        self._data.pop(key, None)


class TestProtocolConformance:
    def test_collector_satisfies_protocol(self):
        collector: HealthSignalCollector = FakeCollector()
        assert collector.get_lag_data("topic") == {0: 100, 1: 200}
        assert collector.is_healthy() is True

    def test_scorer_satisfies_protocol(self):
        scorer: HealthScorer = FakeScorer()
        scores = scorer.calculate_scores({0: 100})
        assert scores == {0: 1.0}

    def test_selector_satisfies_protocol(self):
        selector: PartitionSelector = FakeSelector()
        assert selector.select([0, 1, 2]) == 0
        assert selector.select([]) is None
        assert selector.select([5], key=b"k") == 5

    def test_cache_satisfies_protocol(self):
        cache: Cache = FakeCache()
        assert cache.get("k") is None
        cache.set("k", 42)
        assert cache.get("k") == 42
        cache.delete("k")
        assert cache.get("k") is None
