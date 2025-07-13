"""
Asynchronous Partition Health Monitor using asyncio for background monitoring.
"""

import asyncio
import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, AsyncIterator, Dict, List, Optional, Type, Union

from .health_config import HealthManagerConfig
from .health_mode import HealthMode
from .health_utils import (
    calculate_health_scores,
    collect_and_calculate_health,
    create_cache_from_config,
    create_lag_collector_from_config,
    filter_healthy_partitions,
)

if TYPE_CHECKING:
    from .caching import DefaultHybridCache, DefaultLocalCache, DefaultRemoteCache
    from .protocols import LagDataCollector

CacheBackend = Union["DefaultLocalCache", "DefaultRemoteCache", "DefaultHybridCache"]

logger = logging.getLogger(__name__)


class AsyncPartitionHealthMonitor:
    """
    Asynchronous health manager using asyncio for background monitoring.

    This manager runs background health monitoring using asyncio tasks
    and provides async-safe access to health data for partition selection.
    """

    def __init__(
        self,
        lag_collector: "LagDataCollector",
        cache: Optional[CacheBackend] = None,
        health_threshold: float = 0.5,
        refresh_interval: float = 5.0,
        max_lag_for_health: int = 1000,
        mode: HealthMode = HealthMode.STANDALONE,
        redis_health_publisher: Optional["DefaultRemoteCache"] = None,
    ) -> None:
        """
        Initialize async health manager.

        Args:
            lag_collector: Sync lag data collector implementation
            cache: Optional cache backend for health data
            health_threshold: Minimum health score to consider partition healthy
                                 (0.0-1.0)
            refresh_interval: Seconds between health data refreshes
            max_lag_for_health: Maximum lag for 0.0 health score (linear scaling)
            mode: Operation mode - HealthMode.STANDALONE (default) or
                  HealthMode.EMBEDDED
            redis_health_publisher: Optional Redis publisher for standalone mode
        """
        self._lag_collector = lag_collector
        self._cache = cache
        self._health_threshold = health_threshold
        self._refresh_interval = refresh_interval
        self._max_lag_for_health = max_lag_for_health
        self._mode = mode
        self._redis_publisher = redis_health_publisher

        # Dual-safe health data storage (both thread-safe and async-safe)
        # Format: {topic: {partition_id: health_score}}
        self._health_data: Dict[str, Dict[int, float]] = {}
        self._last_refresh: Dict[str, float] = {}

        # Dual locking strategy for hybrid sync/async access
        self._thread_lock = (
            threading.RLock()
        )  # For executor thread access (producer calls)
        self._async_lock = asyncio.Lock()  # For event loop operations

        # Health streams for reactive patterns
        self._health_streams: Dict[str, asyncio.Queue[Dict[int, float]]] = {}

        # Asyncio control
        self._task: Optional[asyncio.Task[None]] = None
        self._running = False

        cache_type = type(cache).__name__ if cache else "in-memory"
        redis_status = "enabled" if redis_health_publisher else "disabled"
        logger.info(
            f"AsyncPartitionHealthMonitor initialized - mode={mode}, "
            f"threshold={health_threshold}, interval={refresh_interval}s, "
            f"max_lag={max_lag_for_health}, cache={cache_type}, "
            f"redis_publisher={redis_status}, reactive_streams=enabled"
        )

    @classmethod
    def embedded(
        cls, lag_collector: "LagDataCollector", topics: Optional[List[str]] = None
    ) -> "AsyncPartitionHealthMonitor":
        """
        Create AsyncPartitionHealthMonitor for embedded mode (producer integration).

        This factory method creates a lightweight health manager optimized for
        integration with AsyncSmartProducer. No Redis publishing, minimal features.

        Args:
            lag_collector: Lag data collector instance
            topics: Optional list of topics to monitor initially

        Returns:
            AsyncPartitionHealthMonitor configured for embedded mode

        Example:
            lag_collector = KafkaAdminLagCollector(...)
            health_manager = AsyncPartitionHealthMonitor.embedded(
                lag_collector, ["orders", "payments"]
            )
        """
        manager = cls(
            lag_collector=lag_collector,
            cache=None,  # No cache for embedded mode
            health_threshold=0.5,
            refresh_interval=5.0,
            max_lag_for_health=1000,
            mode=HealthMode.EMBEDDED,
            redis_health_publisher=None,  # No Redis for embedded mode
        )

        # Initialize with topics if provided
        if topics:
            manager._initialize_topics(topics)

        return manager

    @classmethod
    async def standalone(
        cls,
        consumer_group: str,
        kafka_config: Dict[str, Any],
        topics: Optional[List[str]] = None,
        health_threshold: float = 0.5,
        refresh_interval: float = 5.0,
        max_lag_for_health: int = 1000,
    ) -> "AsyncPartitionHealthMonitor":
        """
        Create AsyncPartitionHealthMonitor for standalone mode (monitoring service).

        This factory method creates a full-featured health manager for running
        as an independent monitoring service with Redis publishing and health streams.

        Args:
            consumer_group: Kafka consumer group to monitor
            kafka_config: Kafka configuration (bootstrap.servers, security, etc.)
            topics: Optional list of topics to monitor initially
            health_threshold: Minimum health score for healthy partitions
            refresh_interval: Seconds between health refreshes
            max_lag_for_health: Maximum lag for 0.0 health score

        Returns:
            AsyncPartitionHealthMonitor configured for standalone mode

        Example:
            health_manager = await AsyncPartitionHealthMonitor.standalone(
                consumer_group="my-consumers",
                kafka_config={"bootstrap.servers": "localhost:9092"},
                topics=["orders", "payments"]
            )
        """
        from .caching import CacheFactory
        from .lag_collector import KafkaAdminLagCollector

        # Create lag collector
        lag_collector = KafkaAdminLagCollector(
            bootstrap_servers=kafka_config.get("bootstrap.servers", "localhost:9092"),
            consumer_group=consumer_group,
            **{k: v for k, v in kafka_config.items() if k != "bootstrap.servers"},
        )

        # Create Redis publisher for standalone mode
        redis_publisher = None
        try:
            # Try to create Redis publisher with default config
            redis_config = {
                "redis_host": "localhost",
                "redis_port": 6379,
                "redis_db": 0,
            }
            redis_publisher = CacheFactory.create_remote_cache(redis_config)
            if redis_publisher:
                logger.info("Redis health publisher enabled for standalone mode")
        except Exception as e:
            logger.warning(
                f"Failed to create Redis publisher: {e}. Continuing without Redis."
            )

        # Create health manager
        manager = cls(
            lag_collector=lag_collector,
            cache=None,  # No additional cache needed
            health_threshold=health_threshold,
            refresh_interval=refresh_interval,
            max_lag_for_health=max_lag_for_health,
            mode=HealthMode.STANDALONE,
            redis_health_publisher=redis_publisher,
        )

        # Initialize with topics if provided
        if topics:
            manager._initialize_topics(topics)

        return manager

    @classmethod
    def from_config(
        cls, health_config: "HealthManagerConfig", kafka_config: Dict[str, Any]
    ) -> "AsyncPartitionHealthMonitor":
        """
        Factory method to create AsyncPartitionHealthMonitor from unified configuration.

        Args:
            health_config: Health manager configuration
            kafka_config: Producer's Kafka configuration (bootstrap.servers,
                          security, etc.)

        Returns:
            Configured AsyncPartitionHealthMonitor instance

        Raises:
            ValueError: If configuration is invalid
            RuntimeError: If component creation fails
        """
        if not isinstance(kafka_config, dict):
            raise ValueError("Kafka configuration must be a dictionary")

        if not isinstance(health_config, HealthManagerConfig):
            raise ValueError(
                "Health configuration must be a HealthManagerConfig instance"
            )

        # Extract common settings (already validated by HealthManagerConfig)
        health_threshold = health_config.health_threshold
        refresh_interval = health_config.refresh_interval
        max_lag_for_health = health_config.max_lag_for_health

        # Create components using helper functions
        lag_collector = create_lag_collector_from_config(health_config, kafka_config)
        cache = create_cache_from_config(health_config)

        # Determine operation mode and Redis publisher
        mode_str = health_config.get_async_option("mode", "standalone")
        mode = (
            HealthMode.from_string(mode_str) if isinstance(mode_str, str) else mode_str
        )
        redis_publisher = None

        if mode == HealthMode.STANDALONE:
            # Create Redis publisher for standalone mode
            from .caching import CacheFactory

            # Use cache config for Redis connection if available
            if hasattr(health_config, "cache") and health_config.cache:
                redis_publisher = CacheFactory.create_remote_cache(
                    health_config.cache.__dict__
                )
                if redis_publisher:
                    logger.info("Redis health publisher enabled for standalone mode")
                else:
                    logger.warning(
                        "Failed to create Redis publisher, continuing without Redis"
                    )

        # Log async capability detection
        is_async_native = hasattr(lag_collector, "get_lag_data_async")
        logger.info(
            f"AsyncPartitionHealthMonitor mode: {mode}, lag collector: "
            f"{'async-native' if is_async_native else 'executor-wrapped'}"
        )

        return cls(
            lag_collector=lag_collector,
            cache=cache,
            health_threshold=health_threshold,
            refresh_interval=refresh_interval,
            max_lag_for_health=max_lag_for_health,
            mode=mode,
            redis_health_publisher=redis_publisher,
        )

    async def start(self) -> None:
        """
        Start health monitoring in async context.

        Uses asyncio.create_task for scheduling with run_in_executor
        for the sync lag collection operations.
        """
        if self._running:
            logger.warning("AsyncPartitionHealthMonitor already running")
            return

        self._running = True
        self._task = asyncio.create_task(self._refresh_loop())
        logger.info("AsyncPartitionHealthMonitor started")

    async def stop(self) -> None:
        """Stop health monitoring in async context."""
        if not self._running:
            return

        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        # Clean up health streams
        for queue in self._health_streams.values():
            while not queue.empty():
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

        logger.info("AsyncPartitionHealthMonitor stopped")

    def get_healthy_partitions(self, topic: str) -> List[int]:
        """
        Get list of healthy partitions for a topic.

        THREAD-SAFE: Called from AsyncSmartProducer's executor threads.
        This is the performance-critical path for partition selection.

        Args:
            topic: Kafka topic name

        Returns:
            List of partition IDs with health score >= threshold
        """
        with self._thread_lock:
            topic_health = self._health_data.get(topic, {})

        healthy_partitions = filter_healthy_partitions(
            topic_health, self._health_threshold
        )

        logger.debug(
            f"Topic '{topic}' healthy partitions: {healthy_partitions} "
            f"(threshold: {self._health_threshold})"
        )
        return healthy_partitions

    def is_partition_healthy(self, topic: str, partition_id: int) -> bool:
        """
        Check if a specific partition is healthy.

        THREAD-SAFE: Called from AsyncSmartProducer's executor threads.

        Args:
            topic: Kafka topic name
            partition_id: Partition ID to check

        Returns:
            True if partition health score >= threshold, False otherwise
        """
        with self._thread_lock:
            topic_health = self._health_data.get(topic, {})
            health_score = topic_health.get(partition_id, 1.0)  # Default to healthy

        return health_score >= self._health_threshold

    async def get_health_summary(self) -> Dict[str, Any]:
        """
        Get current health summary for monitoring/debugging.

        Returns:
            Dictionary with health statistics and topic details
        """
        async with self._async_lock:
            with self._thread_lock:
                total_partitions = sum(
                    len(topic) for topic in self._health_data.values()
                )
                healthy_partitions = sum(
                    sum(
                        1 for score in topic.values() if score >= self._health_threshold
                    )
                    for topic in self._health_data.values()
                )

                summary = {
                    "running": self._running,
                    "execution_context": "async",
                    "topics": len(self._health_data),
                    "total_partitions": total_partitions,
                    "healthy_partitions": healthy_partitions,
                    "health_threshold": self._health_threshold,
                    "refresh_interval": self._refresh_interval,
                    "topics_detail": {
                        topic: {
                            "partition_count": len(partitions),
                            "healthy_count": sum(
                                1
                                for score in partitions.values()
                                if score >= self._health_threshold
                            ),
                            "last_refresh": self._last_refresh.get(topic, 0),
                            "partition_scores": dict(partitions),
                        }
                        for topic, partitions in self._health_data.items()
                    },
                }

                # Add stream status
                summary["health_streams"] = {
                    "enabled_topics": list(self._health_streams.keys()),
                    "total_streams": len(self._health_streams),
                    "queue_sizes": {
                        topic: queue.qsize()
                        for topic, queue in self._health_streams.items()
                    },
                }

        return summary

    def _initialize_topics(self, topics: List[str]) -> None:
        """
        Initialize health monitoring for a list of topics.

        Args:
            topics: List of Kafka topic names to monitor
        """
        with self._thread_lock:
            for topic in topics:
                if topic not in self._health_data:
                    self._health_data[topic] = {}
                    # Create health stream for reactive patterns
                    if topic not in self._health_streams:
                        self._health_streams[topic] = asyncio.Queue[Dict[int, float]](
                            maxsize=100
                        )
                    logger.info(f"Initialized health monitoring for topic '{topic}'")

    async def force_refresh(self, topic: str) -> None:
        """
        Force immediate refresh of health data for a topic.

        Args:
            topic: Kafka topic name to refresh (must be already initialized)
        """
        try:
            # Check if topic is initialized
            with self._thread_lock:
                if topic not in self._health_data:
                    logger.warning(
                        f"Topic '{topic}' not initialized for health monitoring"
                    )
                    return

            # Perform immediate refresh using run_in_executor
            await self._refresh_single_topic(topic)
            logger.info(f"Forced refresh completed for topic '{topic}'")

        except Exception as e:
            logger.error(f"Force refresh failed for topic '{topic}': {e}")
            raise

    def force_refresh_threadsafe(self, topic: str) -> None:
        """
        Force immediate refresh of health data for a topic from executor threads.

        INTERNAL USE: Called from AsyncSmartProducer's executor threads.

        Args:
            topic: Kafka topic name to refresh (must be already initialized)
        """
        try:
            # Check if topic is initialized (thread-safe)
            with self._thread_lock:
                if topic not in self._health_data:
                    logger.warning(
                        f"Topic '{topic}' not initialized for health monitoring"
                    )
                    return

            # Schedule async refresh without blocking executor thread
            try:
                loop = asyncio.get_event_loop()
                asyncio.run_coroutine_threadsafe(
                    self._refresh_single_topic(topic), loop
                )
            except RuntimeError:
                pass  # No event loop running

        except Exception as e:
            logger.debug(f"Force refresh (threadsafe) failed for topic '{topic}': {e}")

    async def start_monitoring(self, topics: List[str]) -> None:
        """
        Start concurrent monitoring of multiple topics.

        This is the primary async value: concurrent monitoring instead of sequential.

        Args:
            topics: List of topic names to monitor concurrently
        """
        if self._running:
            logger.warning("AsyncPartitionHealthMonitor already running")
            return

        self._running = True

        # Initialize health data and streams for all topics
        async with self._async_lock:
            with self._thread_lock:
                for topic in topics:
                    if topic not in self._health_data:
                        self._health_data[topic] = {}
                    if topic not in self._health_streams:
                        self._health_streams[topic] = asyncio.Queue[Dict[int, float]](
                            maxsize=100
                        )

        # Start concurrent monitoring task
        self._task = asyncio.create_task(self._monitor_all_topics(topics))
        logger.info(f"Started concurrent monitoring for {len(topics)} topics")

    async def _monitor_all_topics(self, topics: List[str]) -> None:
        """Concurrent monitoring of all topics - THIS IS THE ASYNC VALUE!"""
        logger.debug("Starting concurrent topic monitoring")

        # Perform initial concurrent refresh
        await self._refresh_all_topics_concurrent(topics)

        while self._running:
            try:
                await asyncio.sleep(self._refresh_interval)
                if not self._running:
                    break

                # Concurrent refresh - monitor all topics simultaneously
                await self._refresh_all_topics_concurrent(topics)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in concurrent topic monitoring: {e}")
                # Continue running despite errors
                await asyncio.sleep(self._refresh_interval * 2)  # Backoff on errors

        logger.debug("Concurrent topic monitoring finished")

    async def _refresh_loop(self) -> None:
        """Legacy refresh loop for backward compatibility."""
        logger.debug("Starting async health refresh loop (legacy mode)")

        # Perform initial refresh
        await self._refresh_all_topics()

        while self._running:
            try:
                await asyncio.sleep(self._refresh_interval)
                if not self._running:
                    break

                await self._refresh_all_topics()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in async health refresh loop: {e}")
                # Continue running despite errors

        logger.debug("Async health refresh loop finished")

    async def _refresh_all_topics_concurrent(self, topics: List[str]) -> None:
        """Refresh all topics concurrently - MAJOR ASYNC VALUE ADD!"""
        if not topics:
            return

        logger.debug(f"Starting concurrent refresh of {len(topics)} topics")

        # Create concurrent refresh tasks for all topics
        refresh_tasks = [self._refresh_single_topic(topic) for topic in topics]

        # Execute all refreshes concurrently - this is the performance win!
        results = await asyncio.gather(*refresh_tasks, return_exceptions=True)

        # Log any failures
        for topic, result in zip(topics, results):
            if isinstance(result, Exception):
                logger.warning(
                    f"Concurrent refresh failed for topic '{topic}': {result}"
                )

        logger.debug(f"Completed concurrent refresh of {len(topics)} topics")

    async def _refresh_all_topics(self) -> None:
        """Legacy sequential refresh for backward compatibility."""
        with self._thread_lock:
            topics_to_refresh = list(self._health_data.keys())

        for topic in topics_to_refresh:
            try:
                await self._refresh_single_topic(topic)
            except Exception as e:
                logger.warning(f"Failed to refresh health for topic '{topic}': {e}")

    async def _refresh_single_topic(self, topic: str) -> Dict[int, float]:
        """Refresh health data for a single topic."""
        try:
            # Support both sync and async lag collectors - THIS IS PHASE 5!
            if hasattr(self._lag_collector, "get_lag_data_async"):
                # Use async collector if available - native async support
                logger.debug(f"Using async lag collector for topic '{topic}'")
                lag_data = await self._lag_collector.get_lag_data_async(topic)
                health_data = calculate_health_scores(
                    lag_data, self._max_lag_for_health
                )
            else:
                # Fallback to sync collector in executor
                logger.debug(
                    f"Using sync lag collector in executor for topic '{topic}'"
                )
                loop = asyncio.get_event_loop()
                health_data = await loop.run_in_executor(
                    None,
                    collect_and_calculate_health,
                    self._lag_collector,
                    topic,
                    self._max_lag_for_health,
                )

            # Update health data with dual locking
            with self._thread_lock:
                old_health = self._health_data.get(topic, {})
                self._health_data[topic] = health_data
                self._last_refresh[topic] = time.time()

            # Publish to health stream if data changed
            if health_data != old_health and topic in self._health_streams:
                try:
                    self._health_streams[topic].put_nowait(health_data)
                except asyncio.QueueFull:
                    # Drop old updates if queue is full
                    pass

            # Publish to Redis if in standalone mode and data changed
            if (
                self._mode == HealthMode.STANDALONE
                and self._redis_publisher
                and health_data != old_health
            ):
                await self._publish_to_redis(topic, health_data)

            return health_data

        except Exception as e:
            logger.warning(f"Failed to refresh topic '{topic}': {e}")
            raise

    @property
    def is_running(self) -> bool:
        """Check if health manager is currently running."""
        return self._running

    async def __aenter__(self) -> "AsyncPartitionHealthMonitor":
        """Async context manager entry."""
        await self.start()
        return self

    async def health_stream(self, topic: str) -> AsyncIterator[Dict[int, float]]:
        """
        Stream real-time health updates for a topic.

        This is a major async value add: real-time reactive health monitoring.
        Use this for building reactive systems that respond to health changes.

        Args:
            topic: Topic name to stream health updates for

        Yields:
            Dict mapping partition_id -> health_score for each health update

        Raises:
            ValueError: If topic is not being monitored

        Example:
            async for health_update in health_manager.health_stream("events"):
                unhealthy_partitions = [
                    pid for pid, score in health_update.items() if score < 0.3
                ]
                if unhealthy_partitions:
                    await alert.send(f"Partitions {unhealthy_partitions} unhealthy!")
        """
        if topic not in self._health_streams:
            raise ValueError(
                f"Topic '{topic}' is not being monitored. "
                f"Available topics: {list(self._health_streams.keys())}"
            )

        queue = self._health_streams[topic]
        logger.debug(f"Starting health stream for topic '{topic}'")

        try:
            while True:
                # Wait for next health update
                health_update = await queue.get()
                yield health_update
                queue.task_done()
        except asyncio.CancelledError:
            logger.debug(f"Health stream cancelled for topic '{topic}'")
            raise
        except Exception as e:
            logger.error(f"Error in health stream for topic '{topic}': {e}")
            raise

    # Advanced streaming methods removed in Phase 2 simplification
    # Users can build these patterns using health_stream() if needed

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.stop()

    # Batch operations removed in Phase 2 simplification
    # Users can achieve batch operations using asyncio.gather() if needed

    async def _publish_to_redis(
        self, topic: str, health_data: Dict[int, float]
    ) -> None:
        """
        Publish health data to Redis for standalone mode.

        Args:
            topic: Kafka topic name
            health_data: Dict mapping partition_id -> health_score
        """
        if not self._redis_publisher:
            return

        try:
            current_time = time.time()

            # Prepare health data payload
            health_payload = {
                "topic": topic,
                "partitions": json.dumps(health_data),
                "timestamp": current_time,
                "healthy_count": sum(
                    1
                    for score in health_data.values()
                    if score >= self._health_threshold
                ),
                "total_count": len(health_data),
            }

            # Store current health state with TTL
            state_key = f"kafka_health:state:{topic}"
            loop = asyncio.get_event_loop()

            # Use executor for sync Redis operations
            await loop.run_in_executor(
                None,
                self._redis_publisher.set,
                state_key,
                health_payload,
                300,  # 5 minute TTL
            )

            # Also store healthy partitions list for quick producer access
            healthy_partitions = [
                pid
                for pid, score in health_data.items()
                if score >= self._health_threshold
            ]
            healthy_key = f"kafka_health:healthy:{topic}"
            await loop.run_in_executor(
                None,
                self._redis_publisher.set,
                healthy_key,
                json.dumps(healthy_partitions),
                300,  # 5 minute TTL
            )

            logger.debug(
                f"Published health to Redis for topic '{topic}': "
                f"{len(health_data)} partitions, {len(healthy_partitions)} healthy"
            )

        except Exception as e:
            logger.warning(
                f"Failed to publish health to Redis for topic '{topic}': {e}"
            )
            # Don't raise - Redis publishing failures shouldn't crash monitoring

    # Introspection methods removed in Phase 2 simplification
    # These were diagnostic overhead that users rarely need

    def __repr__(self) -> str:
        return (
            f"AsyncPartitionHealthMonitor("
            f"threshold={self._health_threshold}, "
            f"interval={self._refresh_interval}s, "
            f"running={self._running}"
            f")"
        )
