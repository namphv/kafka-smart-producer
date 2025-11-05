"""Default health scorer: linear lag-to-score mapping."""

from __future__ import annotations


class LinearHealthScorer:
    """
    Scores partition health using linear interpolation.

    Score = 1.0 - (lag / max_lag), clamped to [0.0, 1.0].
    - lag=0 -> score=1.0 (perfectly healthy)
    - lag>=max_lag -> score=0.0 (unhealthy)
    """

    def __init__(self, max_lag: int = 1000):
        if max_lag <= 0:
            raise ValueError("max_lag must be positive")
        self._max_lag = max_lag

    def calculate_scores(self, lag_data: dict[int, int]) -> dict[int, float]:
        scores = {}
        for partition_id, lag in lag_data.items():
            if lag <= 0:
                scores[partition_id] = 1.0
            elif lag >= self._max_lag:
                scores[partition_id] = 0.0
            else:
                scores[partition_id] = 1.0 - (lag / self._max_lag)
        return scores
