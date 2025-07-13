"""
Partition Health Monitor using threading for background monitoring.
"""

import json
import logging
import threading
import time
from typing import TYPE_CHECKING, Any, Optional, Union

from .health_config import HealthManagerConfig
from .health_mode import HealthMode
from .health_utils import (
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


class PartitionHealthMonitor:
    """
    Partition health monitor using threading for background monitoring.

    This monitor runs background health monitoring using a daemon thread
    and provides thread-safe access to health data for partition selection.
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
        Initialize sync health manager.

        Args:
            lag_collector: Sync lag data collector implementation
            cache: Optional cache backend for health data
            health_threshold: Minimum health score to consider partition healthy
                                 (0.0-1.0)
            refresh_interval: Seconds between health data refreshes
            mode: Operation mode - HealthMode.STANDALONE (default) or
                  HealthMode.EMBEDDED
            redis_health_publisher: Optional Redis publisher for standalone mode
            max_lag_for_health: Maximum lag for 0.0 health score (linear scaling)
        """
        self._lag_collector = lag_collector
        self._cache = cache
        self._health_threshold = health_threshold
        self._refresh_interval = refresh_interval
        self._max_lag_for_health = max_lag_for_health
        self._mode = mode
        self._redis_publisher = redis_health_publisher

        # Thread-safe health data storage
        # Format: {topic: {partition_id: health_score}}
        self._health_data: dict[str, dict[int, float]] = {}
        self._last_refresh: dict[str, float] = {}
        self._lock = threading.Lock()

        # Threading control
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._running = False

        cache_type = type(cache).__name__ if cache else "in-memory"
        redis_status = "enabled" if redis_health_publisher else "disabled"
        logger.info(
            f"PartitionHealthMonitor initialized - mode={mode}, "
            f"threshold={health_threshold}, interval={refresh_interval}s, "
            f"max_lag={max_lag_for_health}, cache={cache_type}, "
            f"redis_publisher={redis_status}"
        )

    @classmethod
    def embedded(
        cls, lag_collector: "LagDataCollector", topics: Optional[list[str]] = None
    ) -> "PartitionHealthMonitor":
        """
        Create PartitionHealthMonitor for embedded mode (producer integration).

        This factory method creates a lightweight health manager optimized for
        integration with SmartProducer. No Redis publishing, minimal features.

        Args:
            lag_collector: Lag data collector instance
            topics: Optional list of topics to monitor initially

        Returns:
            PartitionHealthMonitor configured for embedded mode

        Example:
            lag_collector = KafkaAdminLagCollector(...)
            health_manager = PartitionHealthMonitor.embedded(
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
    def standalone(
        cls,
        consumer_group: str,
        kafka_config: dict[str, Any],
        topics: Optional[list[str]] = None,
        health_threshold: float = 0.5,
        refresh_interval: float = 5.0,
        max_lag_for_health: int = 1000,
    ) -> "PartitionHealthMonitor":
        """
        Create PartitionHealthMonitor for standalone mode (monitoring service).

        This factory method creates a full-featured health manager for running
        as an independent monitoring service with Redis publishing.

        Args:
            consumer_group: Kafka consumer group to monitor
            kafka_config: Kafka configuration (bootstrap.servers, security, etc.)
            topics: Optional list of topics to monitor initially
            health_threshold: Minimum health score for healthy partitions
            refresh_interval: Seconds between health refreshes
            max_lag_for_health: Maximum lag for 0.0 health score

        Returns:
            PartitionHealthMonitor configured for standalone mode

        Example:
            health_manager = PartitionHealthMonitor.standalone(
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
        cls, health_config: "HealthManagerConfig", kafka_config: dict[str, Any]
    ) -> "PartitionHealthMonitor":
        """
        Factory method to create PartitionHealthMonitor from unified configuration.

        Args:
            health_config: Health manager configuration
            kafka_config: Producer's Kafka configuration (kafka authentication config)

        Returns:
            Configured PartitionHealthMonitor instance

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
        mode_str = health_config.get_sync_option("mode", "standalone")
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

        return cls(
            lag_collector=lag_collector,
            cache=cache,
            health_threshold=health_threshold,
            refresh_interval=refresh_interval,
            max_lag_for_health=max_lag_for_health,
            mode=mode,
            redis_health_publisher=redis_publisher,
        )

    def start(self) -> None:
        """
        Start health monitoring in sync context.

        Uses a daemon thread for background health monitoring.
        """
        if self._running:
            logger.warning("PartitionHealthMonitor already running")
            return

        self._running = True
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._refresh_loop, daemon=True, name="sync-health-manager"
        )
        self._thread.start()
        logger.info("PartitionHealthMonitor started")

    def stop(self) -> None:
        """Stop health monitoring in sync context."""
        if not self._running:
            return

        self._running = False
        self._stop_event.set()

        if self._thread:
            self._thread.join(timeout=self._refresh_interval + 2.0)
            if self._thread.is_alive():
                logger.warning(
                    "PartitionHealthMonitor thread did not terminate gracefully"
                )
            self._thread = None

        logger.info("PartitionHealthMonitor stopped")

    def get_healthy_partitions(self, topic: str) -> list[int]:
        """
        Get list of healthy partitions for a topic.

        This method is thread-safe and can be called from any context.
        Called by producers during partition selection.

        Args:
            topic: Kafka topic name

        Returns:
            List of partition IDs with health score >= threshold
        """
        with self._lock:
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

        Args:
            topic: Kafka topic name
            partition_id: Partition ID to check

        Returns:
            True if partition health score >= threshold, False otherwise
        """
        with self._lock:
            topic_health = self._health_data.get(topic, {})
            health_score = topic_health.get(partition_id, 1.0)  # Default to healthy

        return health_score >= self._health_threshold

    def get_health_summary(self) -> dict[str, Any]:
        """
        Get current health summary for monitoring/debugging.

        Returns:
            Dictionary with health statistics and topic details
        """
        with self._lock:
            total_partitions = sum(len(topic) for topic in self._health_data.values())
            healthy_partitions = sum(
                sum(1 for score in topic.values() if score >= self._health_threshold)
                for topic in self._health_data.values()
            )

            summary = {
                "running": self._running,
                "execution_context": "sync",
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

        return summary

    def _initialize_topics(self, topics: list[str]) -> None:
        """
        Initialize health monitoring for a list of topics.

        Args:
            topics: List of Kafka topic names to monitor
        """
        with self._lock:
            for topic in topics:
                if topic not in self._health_data:
                    self._health_data[topic] = {}
                    logger.info(f"Initialized health monitoring for topic '{topic}'")

    def force_refresh(self, topic: str) -> None:
        """
        Force immediate refresh of health data for a topic.

        Args:
            topic: Kafka topic name to refresh (must be already initialized)
        """
        try:
            # Check if topic is initialized
            with self._lock:
                if topic not in self._health_data:
                    logger.warning(
                        f"Topic '{topic}' not initialized for health monitoring"
                    )
                    return

            # Perform immediate refresh
            health_data = collect_and_calculate_health(
                self._lag_collector, topic, self._max_lag_for_health
            )

            with self._lock:
                self._health_data[topic] = health_data
                self._last_refresh[topic] = time.time()

            logger.info(f"Forced refresh completed for topic '{topic}'")

        except Exception as e:
            logger.error(f"Force refresh failed for topic '{topic}': {e}")
            raise

    def _refresh_loop(self) -> None:
        """Main refresh loop for daemon thread."""
        logger.debug("Starting sync health refresh loop")

        # Perform initial refresh
        self._refresh_all_topics()

        while not self._stop_event.is_set():
            try:
                # Interruptible sleep
                if self._stop_event.wait(self._refresh_interval):
                    break  # Stop event was set

                self._refresh_all_topics()

            except Exception as e:
                logger.error(f"Error in sync health refresh loop: {e}")
                # Continue running despite errors

        logger.debug("Sync health refresh loop finished")

    def _refresh_all_topics(self) -> None:
        """Refresh all topics in sync context."""
        with self._lock:
            topics_to_refresh = list(self._health_data.keys())

        for topic in topics_to_refresh:
            try:
                health_data = collect_and_calculate_health(
                    self._lag_collector, topic, self._max_lag_for_health
                )

                with self._lock:
                    old_health = self._health_data.get(topic, {})
                    self._health_data[topic] = health_data
                    self._last_refresh[topic] = time.time()

                # Publish to Redis if in standalone mode and data changed
                if (
                    self._mode == HealthMode.STANDALONE
                    and self._redis_publisher
                    and health_data != old_health
                ):
                    self._publish_to_redis(topic, health_data)

            except Exception as e:
                logger.warning(f"Failed to refresh health for topic '{topic}': {e}")

    def _publish_to_redis(self, topic: str, health_data: dict[int, float]) -> None:
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
            self._redis_publisher.set(state_key, health_payload, 300)  # 5 minute TTL

            # Also store healthy partitions list for quick producer access
            healthy_partitions = [
                pid
                for pid, score in health_data.items()
                if score >= self._health_threshold
            ]
            healthy_key = f"kafka_health:healthy:{topic}"
            self._redis_publisher.set(healthy_key, json.dumps(healthy_partitions), 300)

            logger.debug(
                f"Published health to Redis for topic '{topic}': "
                f"{len(health_data)} partitions, {len(healthy_partitions)} healthy"
            )

        except Exception as e:
            logger.warning(
                f"Failed to publish health to Redis for topic '{topic}': {e}"
            )
            # Don't raise - Redis publishing failures shouldn't crash monitoring

    @property
    def is_running(self) -> bool:
        """Check if health manager is currently running."""
        return self._running

    def __enter__(self) -> "PartitionHealthMonitor":
        """Context manager entry."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Any,
    ) -> None:
        """Context manager exit."""
        self.stop()

    def __repr__(self) -> str:
        return (
            f"PartitionHealthMonitor("
            f"threshold={self._health_threshold}, "
            f"interval={self._refresh_interval}s, "
            f"running={self._running}"
            f")"
        )
