"""
Health management for Kafka Smart Producer.

This module provides the HealthManager implementation that orchestrates lag data
collection, health score calculation, and intelligent partition selection.
"""

import asyncio
import logging
import random
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Protocol, Union

from .caching import CacheConfig, DefaultLocalCache
from .exceptions import (
    HealthCalculationError,
    HealthManagerError,
    LagDataUnavailableError,
    PartitionSelectionError,
)
from .protocols import CacheBackend, HotPartitionCalculator, LagDataCollector
from .threading import (
    SimpleBackgroundRefresh,
    create_async_background_task,
    create_sync_background_refresh,
)

logger = logging.getLogger(__name__)


class PartitionSelectionStrategy(Enum):
    """Strategies for selecting partitions when multiple healthy options exist."""

    RANDOM = "random"
    ROUND_ROBIN = "round_robin"
    LEAST_LAG = "least_lag"
    WEIGHTED_RANDOM = "weighted_random"


@dataclass
class PartitionHealth:
    """Health information for a single partition."""

    partition_id: int
    health_score: float  # 0.0 = unhealthy, 1.0 = healthy
    lag_count: int
    last_updated: float  # timestamp
    is_healthy: bool  # derived from score and threshold


@dataclass
class TopicHealth:
    """Health information for all partitions of a topic."""

    topic: str
    partitions: Dict[int, PartitionHealth]
    last_refresh: float
    healthy_partitions: List[int]  # cached list of healthy partition IDs
    total_partitions: int


@dataclass
class HealthManagerConfig:
    """Configuration for HealthManager behavior."""

    refresh_interval_seconds: float = 5.0
    health_threshold: float = 0.5  # Minimum score to consider partition healthy
    cache_ttl_seconds: float = 60.0
    selection_strategy: PartitionSelectionStrategy = PartitionSelectionStrategy.RANDOM
    default_partition_count: int = 3  # For topics with no metadata
    max_refresh_failures: int = 5  # Max consecutive failures before marking unhealthy


class HealthManager(Protocol):
    """Central coordinator for partition health monitoring and selection."""

    async def start(self) -> None:
        """Start background health monitoring."""
        ...

    async def stop(self) -> None:
        """Stop background health monitoring and cleanup resources."""
        ...

    def get_topic_health(self, topic: str) -> Optional[TopicHealth]:
        """Get current health state for a topic."""
        ...

    def select_partition(
        self,
        topic: str,
        available_partitions: Optional[List[int]] = None,
    ) -> int:
        """
        Select optimal partition for message delivery based on consumer lag.

        Args:
            topic: Target topic name
            available_partitions: Available partition list (optional)

        Returns:
            Selected partition ID

        Raises:
            PartitionSelectionError: When no partitions are available
        """
        ...

    def is_partition_healthy(self, topic: str, partition_id: int) -> bool:
        """Check if specific partition is currently healthy."""
        ...

    def force_refresh(self, topic: str) -> None:
        """Force immediate refresh of topic health data."""
        ...

    def get_health_summary(self) -> Dict[str, Any]:
        """Get health summary for monitoring/debugging."""
        ...


class DefaultHealthManager:
    """Default implementation of HealthManager using simplified threading."""

    def __init__(
        self,
        lag_collector: LagDataCollector,
        health_calculator: HotPartitionCalculator,
        cache: Optional[Union[DefaultLocalCache, CacheBackend]] = None,
        config: Optional[HealthManagerConfig] = None,
        explicit_async_mode: Optional[bool] = None,
    ):
        # Input validation
        if lag_collector is None:
            raise ValueError("lag_collector cannot be None")
        if health_calculator is None:
            raise ValueError("health_calculator cannot be None")

        self._lag_collector = lag_collector
        self._health_calculator = health_calculator
        self._cache = cache or DefaultLocalCache(
            CacheConfig(
                local_max_size=1000,
                local_default_ttl_seconds=300.0,
                stats_collection_enabled=False,  # No stats needed
            )
        )
        self._config = config or HealthManagerConfig()
        self._explicit_async_mode = explicit_async_mode

        # Background refresh management
        self._background_refresh: Optional[
            Union[SimpleBackgroundRefresh, asyncio.Task[None]]
        ] = None
        self._is_async_context = False
        self._running = False

        # Health data storage with cleanup tracking
        self._topic_metadata: Dict[str, TopicHealth] = {}
        self._selection_state: Dict[str, int] = {}  # For round-robin tracking
        self._failure_counts: Dict[str, int] = {}  # Track refresh failures
        self._last_cleanup: float = time.time()

        # Thread safety - use regular lock for better performance
        self._lock = threading.Lock()
        self._selection_locks: Dict[str, threading.Lock] = {}  # Per-topic locks

    async def start(self) -> None:
        """Start background refresh using explicit threading model."""
        with self._lock:
            if self._running:
                logger.debug("HealthManager already running")
                return

            self._running = True

            # Use explicit async mode if provided, otherwise detect
            if self._explicit_async_mode is not None:
                self._is_async_context = self._explicit_async_mode
            else:
                # Safe runtime detection as fallback
                try:
                    asyncio.get_running_loop()
                    self._is_async_context = True
                except RuntimeError:
                    self._is_async_context = False

            if self._is_async_context:
                logger.debug("Starting HealthManager in async context")
                # Create async background task
                self._background_refresh = create_async_background_task(
                    self._refresh_all_topics,
                    interval=self._config.refresh_interval_seconds,
                )
            else:
                logger.debug("Starting HealthManager in sync context")
                # Create sync background refresh
                self._background_refresh = create_sync_background_refresh(
                    self._refresh_all_topics,
                    interval=self._config.refresh_interval_seconds,
                )
                self._background_refresh.start()

        logger.info(
            f"HealthManager started with {self._config.refresh_interval_seconds}s "
            "refresh interval"
        )

    async def stop(self) -> None:
        """Stop background refresh and cleanup."""
        with self._lock:
            if not self._running:
                return

            self._running = False

            if self._background_refresh:
                if self._is_async_context:
                    # Cancel async task
                    if isinstance(self._background_refresh, asyncio.Task):
                        self._background_refresh.cancel()
                        try:
                            await self._background_refresh
                        except asyncio.CancelledError:
                            pass
                else:
                    # Stop sync background refresh
                    if isinstance(self._background_refresh, SimpleBackgroundRefresh):
                        self._background_refresh.stop()

                self._background_refresh = None

        logger.info("HealthManager stopped")

    def get_topic_health(self, topic: str) -> Optional[TopicHealth]:
        """Get cached topic health or return None if not available."""
        with self._lock:
            return self._topic_metadata.get(topic)

    def select_partition(
        self,
        topic: str,
        available_partitions: Optional[List[int]] = None,
    ) -> int:
        """Select partition using configured strategy and health data."""
        try:
            topic_health = self.get_topic_health(topic)

            if not topic_health:
                # No health data available, use fallback strategy
                return self._fallback_partition_selection(topic, available_partitions)

            healthy_partitions = topic_health.healthy_partitions.copy()

            if not healthy_partitions:
                # No healthy partitions, use fallback
                return self._fallback_partition_selection(topic, available_partitions)

            # Filter by available_partitions if provided
            if available_partitions:
                healthy_partitions = [
                    p for p in healthy_partitions if p in available_partitions
                ]
                if not healthy_partitions:
                    return self._fallback_partition_selection(
                        topic, available_partitions
                    )

            # Apply selection strategy based on consumer lag
            return self._apply_selection_strategy(topic, healthy_partitions)

        except Exception as e:
            logger.warning(f"Error in partition selection for {topic}: {e}")
            return self._fallback_partition_selection(topic, available_partitions)

    def is_partition_healthy(self, topic: str, partition_id: int) -> bool:
        """Check if partition meets health threshold."""
        topic_health = self.get_topic_health(topic)
        if not topic_health:
            return True  # Assume healthy if no data available

        partition_health = topic_health.partitions.get(partition_id)
        if not partition_health:
            return True  # Assume healthy if partition not monitored

        return partition_health.is_healthy

    def force_refresh(self, topic: str) -> None:
        """Force immediate refresh for specific topic."""
        try:
            self._refresh_topic_health(topic)
            logger.debug(f"Forced refresh completed for topic: {topic}")
        except Exception as e:
            logger.error(f"Error during force refresh for {topic}: {e}")

    def get_health_summary(self) -> Dict[str, Any]:
        """Return comprehensive health summary."""
        with self._lock:
            summary = {
                "running": self._running,
                "async_context": self._is_async_context,
                "topics": len(self._topic_metadata),
                "total_partitions": sum(
                    th.total_partitions for th in self._topic_metadata.values()
                ),
                "healthy_partitions": sum(
                    len(th.healthy_partitions) for th in self._topic_metadata.values()
                ),
                "last_refresh": max(
                    (th.last_refresh for th in self._topic_metadata.values()), default=0
                ),
                "background_refresh_running": self._is_background_running(),
                "config": {
                    "refresh_interval": self._config.refresh_interval_seconds,
                    "health_threshold": self._config.health_threshold,
                    "selection_strategy": self._config.selection_strategy.value,
                },
                "topics_detail": {
                    topic: {
                        "total_partitions": health.total_partitions,
                        "healthy_partitions": len(health.healthy_partitions),
                        "last_refresh": health.last_refresh,
                        "partition_scores": {
                            pid: p.health_score for pid, p in health.partitions.items()
                        },
                    }
                    for topic, health in self._topic_metadata.items()
                },
            }
            return summary

    def _is_background_running(self) -> bool:
        """Check if background refresh is running."""
        if not self._background_refresh:
            return False

        if self._is_async_context:
            return (
                isinstance(self._background_refresh, asyncio.Task)
                and not self._background_refresh.done()
            )
        else:
            return (
                isinstance(self._background_refresh, SimpleBackgroundRefresh)
                and self._background_refresh.is_running()
            )

    def _refresh_all_topics(self) -> None:
        """Refresh health data for all monitored topics."""
        with self._lock:
            topics_to_refresh = list(self._topic_metadata.keys())

        # Periodic cleanup of stale data to prevent memory leaks
        self._cleanup_stale_data()

        # If no topics are being monitored yet, nothing to refresh
        if not topics_to_refresh:
            return

        for topic in topics_to_refresh:
            try:
                self._refresh_topic_health(topic)
                # Reset failure count on success
                self._failure_counts.pop(topic, None)
            except Exception as e:
                logger.warning(f"Error refreshing {topic}: {e}")

                # Track failure count
                self._failure_counts[topic] = self._failure_counts.get(topic, 0) + 1

                # If too many failures, remove topic from monitoring
                if self._failure_counts[topic] >= self._config.max_refresh_failures:
                    logger.error(
                        f"Too many failures for {topic}, removing from monitoring"
                    )
                    with self._lock:
                        self._topic_metadata.pop(topic, None)
                        self._failure_counts.pop(topic, None)
                        # Clean up related state
                        self._selection_state.pop(topic, None)
                        self._selection_state.pop(f"fallback:{topic}", None)
                        self._selection_locks.pop(topic, None)

    def _refresh_topic_health(self, topic: str) -> None:
        """Refresh health data for a specific topic."""
        try:
            # Get lag data - prefer sync method for compatibility
            if hasattr(self._lag_collector, "get_lag_data_sync"):
                lag_data = self._lag_collector.get_lag_data_sync(topic)
            else:
                # CRITICAL FIX: Remove dangerous asyncio.run() usage
                # This was causing deadlocks in production
                raise HealthManagerError(
                    "Lag collector must implement get_lag_data_sync for background "
                    "refresh. Async-only collectors are not supported in background "
                    "refresh context."
                )

            # Calculate health scores
            health_scores = self._health_calculator.calculate_scores(lag_data)

            # Build partition health objects
            now = time.time()
            partitions = {}
            healthy_partitions = []

            for partition_id, lag_count in lag_data.items():
                health_score = health_scores.get(partition_id, 0.0)
                is_healthy = health_score >= self._config.health_threshold

                partitions[partition_id] = PartitionHealth(
                    partition_id=partition_id,
                    health_score=health_score,
                    lag_count=lag_count,
                    last_updated=now,
                    is_healthy=is_healthy,
                )

                if is_healthy:
                    healthy_partitions.append(partition_id)

            # Update topic health
            topic_health = TopicHealth(
                topic=topic,
                partitions=partitions,
                last_refresh=now,
                healthy_partitions=healthy_partitions,
                total_partitions=len(partitions),
            )

            with self._lock:
                self._topic_metadata[topic] = topic_health

            # Cache for quick access if cache supports it
            if hasattr(self._cache, "set"):
                try:
                    cache_key = f"topic_health:{topic}"
                    self._cache.set(
                        cache_key, topic_health, int(self._config.cache_ttl_seconds)
                    )
                except Exception as e:
                    logger.debug(f"Cache set failed for {topic}: {e}")

        except (LagDataUnavailableError, HealthCalculationError) as e:
            logger.warning(f"Health refresh failed for {topic}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error refreshing {topic}: {e}")
            raise HealthManagerError(f"Health refresh failed for {topic}") from e

    def _apply_selection_strategy(
        self, topic: str, healthy_partitions: List[int]
    ) -> int:
        """Apply configured partition selection strategy based on consumer lag."""
        if self._config.selection_strategy == PartitionSelectionStrategy.RANDOM:
            return random.choice(healthy_partitions)

        elif self._config.selection_strategy == PartitionSelectionStrategy.ROUND_ROBIN:
            # Use per-topic lock for better performance
            topic_lock = self._get_topic_lock(topic)
            with topic_lock:
                current = self._selection_state.get(topic, 0)
                partition = healthy_partitions[current % len(healthy_partitions)]
                self._selection_state[topic] = current + 1
                return partition

        elif self._config.selection_strategy == PartitionSelectionStrategy.LEAST_LAG:
            topic_health = self._topic_metadata[topic]
            min_lag_partition = min(
                healthy_partitions, key=lambda p: topic_health.partitions[p].lag_count
            )
            return min_lag_partition

        elif (
            self._config.selection_strategy
            == PartitionSelectionStrategy.WEIGHTED_RANDOM
        ):
            topic_health = self._topic_metadata[topic]
            weights = [
                topic_health.partitions[p].health_score for p in healthy_partitions
            ]
            return self._weighted_random_choice(healthy_partitions, weights)

        else:
            # Default to random
            return random.choice(healthy_partitions)

    def _fallback_partition_selection(
        self,
        topic: str,
        available_partitions: Optional[List[int]],
    ) -> int:
        """Fallback selection when no health data available."""
        if available_partitions is not None:
            partitions = available_partitions
        else:
            partitions = list(range(self._config.default_partition_count))

        if not partitions:
            raise PartitionSelectionError(f"No partitions available for topic {topic}")

        # Use round-robin with per-topic lock
        topic_lock = self._get_topic_lock(topic)
        with topic_lock:
            current = self._selection_state.get(f"fallback:{topic}", 0)
            partition = partitions[current % len(partitions)]
            self._selection_state[f"fallback:{topic}"] = current + 1
            return partition

    def _weighted_random_choice(self, choices: List[int], weights: List[float]) -> int:
        """Select item from choices using weights."""
        total = sum(weights)
        if total <= 0:
            return random.choice(choices)

        r = random.uniform(0, total)
        cumulative = 0.0
        for choice, weight in zip(choices, weights):
            cumulative += weight
            if r <= cumulative:
                return choice

        return choices[-1]  # Fallback

    def _get_topic_lock(self, topic: str) -> threading.Lock:
        """Get or create a per-topic lock for fine-grained synchronization."""
        if topic not in self._selection_locks:
            with self._lock:
                if topic not in self._selection_locks:
                    self._selection_locks[topic] = threading.Lock()
        return self._selection_locks[topic]

    def _cleanup_stale_data(self) -> None:
        """Clean up stale data to prevent memory leaks."""
        now = time.time()
        # Run cleanup every 5 minutes
        if now - self._last_cleanup < 300:
            return

        self._last_cleanup = now
        stale_cutoff = now - (self._config.cache_ttl_seconds * 2)  # 2x TTL for safety

        with self._lock:
            # Remove stale topic metadata
            stale_topics = [
                topic
                for topic, health in self._topic_metadata.items()
                if health.last_refresh < stale_cutoff
            ]

            for topic in stale_topics:
                logger.debug(f"Cleaning up stale data for topic: {topic}")
                self._topic_metadata.pop(topic, None)
                self._selection_state.pop(topic, None)
                self._selection_state.pop(f"fallback:{topic}", None)
                self._failure_counts.pop(topic, None)
                self._selection_locks.pop(topic, None)

            # Limit selection state size to prevent unbounded growth
            if len(self._selection_state) > 1000:
                logger.warning("Selection state growing large, clearing oldest entries")
                # Keep only most recent 500 entries
                items = list(self._selection_state.items())
                self._selection_state.clear()
                self._selection_state.update(items[-500:])


# Exception classes specific to health management
class NoHealthyPartitionsError(PartitionSelectionError):
    """Raised when no healthy partitions are available for selection."""


class HealthRefreshError(HealthManagerError):
    """Raised when health data refresh fails."""
