"""Tests for RandomHealthySelector."""

from kafka_smart_producer.partition.selector import RandomHealthySelector


class TestRandomHealthySelector:
    def test_selects_from_candidates(self):
        selector = RandomHealthySelector()
        result = selector.select([0, 1, 2])
        assert result in [0, 1, 2]

    def test_returns_none_for_empty_list(self):
        selector = RandomHealthySelector()
        assert selector.select([]) is None

    def test_single_partition(self):
        selector = RandomHealthySelector()
        assert selector.select([5]) == 5

    def test_key_is_ignored(self):
        selector = RandomHealthySelector()
        result = selector.select([0, 1], key=b"some-key")
        assert result in [0, 1]

    def test_distribution_is_reasonable(self):
        selector = RandomHealthySelector()
        counts = {0: 0, 1: 0, 2: 0}
        for _ in range(1000):
            p = selector.select([0, 1, 2])
            counts[p] += 1
        # Each should get roughly 333, allow wide margin
        for count in counts.values():
            assert count > 200
