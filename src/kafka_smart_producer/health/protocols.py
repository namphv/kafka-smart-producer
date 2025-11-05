"""Protocols for pluggable health monitoring components."""

from __future__ import annotations

from typing import Protocol


class HealthSignalCollector(Protocol):
    """Collects health signal data (e.g., consumer lag) for topic partitions."""

    def get_lag_data(self, topic: str) -> dict[int, int]:
        """
        Collect lag data for all partitions of a topic.

        Args:
            topic: Kafka topic name

        Returns:
            Dict mapping partition_id -> lag_count

        Raises:
            LagDataUnavailableError: When data cannot be retrieved
        """
        ...

    def is_healthy(self) -> bool:
        """Check if the collector is operational. Must complete in <100ms."""
        ...


class HealthScorer(Protocol):
    """Transforms raw lag data into health scores (0.0-1.0)."""

    def calculate_scores(self, lag_data: dict[int, int]) -> dict[int, float]:
        """
        Calculate health scores from lag data.

        Args:
            lag_data: Dict mapping partition_id -> lag_count

        Returns:
            Dict mapping partition_id -> health_score (0.0=unhealthy, 1.0=healthy)
        """
        ...


class PartitionSelector(Protocol):
    """Selects a partition from candidates."""

    def select(
        self,
        healthy_partitions: list[int],
        key: bytes | None = None,
    ) -> int | None:
        """
        Select a partition from the healthy candidates.

        Args:
            healthy_partitions: List of healthy partition IDs
            key: Optional message key for affinity-based selection

        Returns:
            Selected partition ID, or None to use default Kafka partitioning
        """
        ...
