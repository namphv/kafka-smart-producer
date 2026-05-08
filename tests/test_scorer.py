"""Tests for LinearHealthScorer."""

import pytest

from kafka_smart_producer.health.scorer import LinearHealthScorer


class TestLinearHealthScorer:
    def test_zero_lag_is_fully_healthy(self):
        scorer = LinearHealthScorer(max_lag=1000)
        assert scorer.calculate_scores({0: 0}) == {0: 1.0}

    def test_max_lag_is_unhealthy(self):
        scorer = LinearHealthScorer(max_lag=1000)
        assert scorer.calculate_scores({0: 1000}) == {0: 0.0}

    def test_over_max_lag_is_zero(self):
        scorer = LinearHealthScorer(max_lag=1000)
        assert scorer.calculate_scores({0: 5000}) == {0: 0.0}

    def test_linear_interpolation(self):
        scorer = LinearHealthScorer(max_lag=1000)
        scores = scorer.calculate_scores({0: 500})
        assert scores[0] == pytest.approx(0.5)

    def test_quarter_lag(self):
        scorer = LinearHealthScorer(max_lag=1000)
        scores = scorer.calculate_scores({0: 250})
        assert scores[0] == pytest.approx(0.75)

    def test_multiple_partitions(self):
        scorer = LinearHealthScorer(max_lag=100)
        scores = scorer.calculate_scores({0: 0, 1: 50, 2: 100, 3: 200})
        assert scores == {0: 1.0, 1: pytest.approx(0.5), 2: 0.0, 3: 0.0}

    def test_negative_lag_treated_as_zero(self):
        scorer = LinearHealthScorer(max_lag=1000)
        scores = scorer.calculate_scores({0: -10})
        assert scores[0] == 1.0

    def test_empty_lag_data(self):
        scorer = LinearHealthScorer(max_lag=1000)
        assert scorer.calculate_scores({}) == {}

    def test_invalid_max_lag(self):
        with pytest.raises(ValueError, match="max_lag"):
            LinearHealthScorer(max_lag=0)

    def test_custom_max_lag(self):
        scorer = LinearHealthScorer(max_lag=10)
        scores = scorer.calculate_scores({0: 5})
        assert scores[0] == pytest.approx(0.5)
