"""
Unified Partition Health Monitor supporting both sync (threading) and async (asyncio).


Core responsibilities:
- Background health data refresh (thread or asyncio task)
- Thread-safe health data access for partition selection
- Graceful degradation when health data unavailable
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Any

from ..health.protocols import HealthScorer, HealthSignalCollector
from ..health.scorer import LinearHealthScorer

logger = logging.getLogger(__name__)


class PartitionHealthMonitor:
    """
    Monitors partition health and provides healthy partition lists.

    Supports two execution modes:
    - Sync: background daemon thread (start_sync/stop_sync)
    - Async: asyncio task (start_async/stop_async)

    Health data access (get_healthy_partitions) is always thread-safe
    and can be called from any context.
    """

    def __init__(
        self,
        collector: HealthSignalCollector,
        scorer: HealthScorer | None = None,
        health_threshold: float = 0.5,
        refresh_interval: float = 30.0,
        max_lag: int = 1000,
    ) -> None:
        self._collector = collector
        self._scorer = scorer or LinearHealthScorer(max_lag=max_lag)
        self._health_threshold = health_threshold
        self._refresh_interval = refresh_interval

        # Thread-safe health data: {topic: {partition_id: score}}
        self._health_data: dict[str, dict[int, float]] = {}
        self._last_refresh: dict[str, float] = {}
        self._lock = threading.Lock()

        # Sync mode state
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # Async mode state
        self._async_task: asyncio.Task[None] | None = None

        self._running = False

    # ── Topic Management ──

    def initialize_topics(self, topics: list[str]) -> None:
        with self._lock:
            for topic in topics:
                if topic not in self._health_data:
                    self._health_data[topic] = {}

    # ── Health Data Access (thread-safe, called from producers) ──

    def get_healthy_partitions(self, topic: str) -> list[int]:
        with self._lock:
            scores = self._health_data.get(topic, {})
        return [pid for pid, score in scores.items() if score >= self._health_threshold]

    def is_partition_healthy(self, topic: str, partition_id: int) -> bool:
        with self._lock:
            scores = self._health_data.get(topic, {})
            score = scores.get(partition_id, 1.0)  # default healthy
        return score >= self._health_threshold

    def get_health_scores(self, topic: str) -> dict[int, float]:
        with self._lock:
            return dict(self._health_data.get(topic, {}))

    # ── Sync Mode (threading) ──

    def start_sync(self) -> None:
        if self._running:
            return
        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._sync_refresh_loop, daemon=True, name="health-monitor"
        )
        self._thread.start()
        logger.info("PartitionHealthMonitor started (sync/threading)")

    def stop_sync(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=self._refresh_interval + 2.0)
            self._thread = None
        logger.info("PartitionHealthMonitor stopped (sync)")

    def _sync_refresh_loop(self) -> None:
        self._refresh_all_topics()
        while not self._stop_event.is_set():
            if self._stop_event.wait(self._refresh_interval):
                break
            self._refresh_all_topics()

    # ── Async Mode (asyncio) ──

    async def start_async(self) -> None:
        if self._running:
            return
        self._running = True
        self._async_task = asyncio.create_task(self._async_refresh_loop())
        logger.info("PartitionHealthMonitor started (async/asyncio)")

    async def stop_async(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._async_task:
            self._async_task.cancel()
            try:
                await self._async_task
            except asyncio.CancelledError:
                pass
            self._async_task = None
        logger.info("PartitionHealthMonitor stopped (async)")

    async def _async_refresh_loop(self) -> None:
        await self._async_refresh_all_topics()
        while self._running:
            try:
                await asyncio.sleep(self._refresh_interval)
                if not self._running:  # pragma: no cover
                    break
                await self._async_refresh_all_topics()
            except asyncio.CancelledError:
                break
            except Exception as e:  # pragma: no cover
                logger.error(f"Error in async health refresh: {e}")

    async def _async_refresh_all_topics(self) -> None:
        with self._lock:
            topics = list(self._health_data.keys())

        # Refresh all topics concurrently
        tasks = [self._async_refresh_topic(t) for t in topics]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _async_refresh_topic(self, topic: str) -> None:
        try:
            loop = asyncio.get_event_loop()
            scores = await loop.run_in_executor(None, self._collect_and_score, topic)
            if scores is not None:
                with self._lock:
                    self._health_data[topic] = scores
                    self._last_refresh[topic] = time.time()
        except Exception as e:  # pragma: no cover
            logger.warning(f"Failed to refresh topic '{topic}': {e}")

    # ── Core Refresh Logic (shared) ──

    def _refresh_all_topics(self) -> None:
        with self._lock:
            topics = list(self._health_data.keys())

        for topic in topics:
            try:
                scores = self._collect_and_score(topic)
                if scores is not None:
                    with self._lock:
                        self._health_data[topic] = scores
                        self._last_refresh[topic] = time.time()
            except Exception as e:  # pragma: no cover
                logger.warning(f"Failed to refresh topic '{topic}': {e}")

    def _collect_and_score(self, topic: str) -> dict[int, float] | None:
        """Collect lag data and calculate health scores. Returns None on failure."""
        try:
            lag_data = self._collector.get_lag_data(topic)
            if not lag_data:
                return {}
            return self._scorer.calculate_scores(lag_data)
        except Exception as e:
            logger.warning(f"Collection/scoring failed for '{topic}': {e}")
            return None

    def force_refresh(self, topic: str) -> None:
        """Force immediate refresh for a topic (sync, blocking)."""
        with self._lock:
            if topic not in self._health_data:
                return
        scores = self._collect_and_score(topic)
        if scores is not None:
            with self._lock:
                self._health_data[topic] = scores
                self._last_refresh[topic] = time.time()

    # ── Lifecycle Helpers ──

    @property
    def is_running(self) -> bool:
        return self._running

    def get_summary(self) -> dict[str, Any]:
        with self._lock:
            total = sum(len(t) for t in self._health_data.values())
            healthy = sum(
                sum(1 for s in t.values() if s >= self._health_threshold)
                for t in self._health_data.values()
            )
            return {
                "running": self._running,
                "topics": len(self._health_data),
                "total_partitions": total,
                "healthy_partitions": healthy,
                "threshold": self._health_threshold,
                "refresh_interval": self._refresh_interval,
            }

    def __enter__(self) -> PartitionHealthMonitor:
        self.start_sync()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop_sync()

    async def __aenter__(self) -> PartitionHealthMonitor:
        await self.start_async()
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.stop_async()

    def __repr__(self) -> str:
        return (
            f"PartitionHealthMonitor("
            f"threshold={self._health_threshold}, "
            f"interval={self._refresh_interval}s, "
            f"running={self._running})"
        )
