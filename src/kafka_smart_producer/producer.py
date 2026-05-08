"""Smart Kafka producers with health-aware partition selection."""

from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from confluent_kafka import Producer as ConfluentProducer

from .cache.local import LocalLRUCache
from .config import SmartProducerConfig
from .health.monitor import PartitionHealthMonitor
from .health.protocols import HealthScorer, HealthSignalCollector, PartitionSelector
from .partition.selector import RandomHealthySelector

logger = logging.getLogger(__name__)


def _build_monitor(
    config: SmartProducerConfig,
    collector: HealthSignalCollector | None = None,
    scorer: HealthScorer | None = None,
) -> PartitionHealthMonitor | None:
    """Build a PartitionHealthMonitor from config, or return None if not configured."""
    if not config.has_health_monitoring:
        return None

    if collector is None:
        from .health.collectors.kafka_admin import KafkaAdminLagCollector

        collector = KafkaAdminLagCollector(
            bootstrap_servers=config.kafka_config.get(
                "bootstrap.servers", "localhost:9092"
            ),
            consumer_group=config.consumer_group,
            timeout=config.collector_timeout,
        )

    monitor = PartitionHealthMonitor(
        collector=collector,
        scorer=scorer,
        health_threshold=config.health_threshold,
        refresh_interval=config.refresh_interval,
        max_lag=config.max_lag,
    )
    monitor.initialize_topics(config.topics)
    return monitor


class SmartProducer:
    """
    Synchronous Kafka producer with health-aware partition selection.

    Wraps confluent-kafka Producer. Falls back to default partitioning
    when health data is unavailable.
    """

    def __init__(
        self,
        config: SmartProducerConfig,
        monitor: PartitionHealthMonitor | None = None,
        collector: HealthSignalCollector | None = None,
        scorer: HealthScorer | None = None,
        selector: PartitionSelector | None = None,
    ) -> None:
        self._config = config
        self._producer = ConfluentProducer(config.get_kafka_config())
        self._selector = selector or RandomHealthySelector()

        # Health monitor
        self._monitor_owned = monitor is None
        self._monitor = monitor or _build_monitor(config, collector, scorer)

        # Key stickiness cache
        self._cache: LocalLRUCache | None = None
        if config.key_stickiness and self._monitor:
            self._cache = LocalLRUCache(
                max_size=config.cache_max_size, default_ttl=config.cache_ttl
            )

        # Auto-start if we created the monitor
        if self._monitor and self._monitor_owned:
            self._monitor.start_sync()

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        on_delivery: Callable[..., None] | None = None,
        timestamp: int | None = None,
        headers: dict[str, bytes] | None = None,
    ) -> None:
        if partition is None and self._monitor:
            partition = self._select_partition(topic, key)

        kwargs: dict[str, Any] = {"topic": topic}
        if value is not None:
            kwargs["value"] = value
        if key is not None:
            kwargs["key"] = key
        if partition is not None:
            kwargs["partition"] = partition
        if on_delivery is not None:  # pragma: no cover
            kwargs["on_delivery"] = on_delivery
        if timestamp is not None:  # pragma: no cover
            kwargs["timestamp"] = timestamp
        if headers is not None:  # pragma: no cover
            kwargs["headers"] = headers

        self._producer.produce(**kwargs)
        self._producer.poll(0)

    def flush(self, timeout: float | None = None) -> int:
        if timeout is not None:
            return self._producer.flush(timeout)
        return self._producer.flush()  # pragma: no cover

    def close(self) -> None:
        try:
            self._producer.flush()
            if self._monitor and self._monitor_owned and self._monitor.is_running:
                self._monitor.stop_sync()
        except Exception as e:  # pragma: no cover
            logger.error(f"Error during SmartProducer close: {e}")

    def _select_partition(self, topic: str, key: bytes | None) -> int | None:
        try:
            # Key stickiness: check cache first
            if key and self._cache is not None:
                cache_key = f"{topic}:{key!r}"
                cached = self._cache.get(cache_key)
                if cached is not None:
                    # Verify cached partition is still healthy
                    if self._monitor.is_partition_healthy(topic, int(cached)):
                        return int(cached)
                    # Cached partition unhealthy — fall through to re-select
                    self._cache.delete(cache_key)

            # Select from healthy partitions
            healthy = self._monitor.get_healthy_partitions(topic)
            selected = self._selector.select(healthy, key)

            # Cache the selection
            if selected is not None and key and self._cache is not None:
                cache_key = f"{topic}:{key!r}"
                self._cache.set(cache_key, selected)

            return selected
        except Exception as e:  # pragma: no cover
            logger.debug(f"Partition selection failed: {e}")
            return None

    @property
    def monitor(self) -> PartitionHealthMonitor | None:
        return self._monitor

    def __enter__(self) -> SmartProducer:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._producer, name)


class AsyncSmartProducer:
    """
    Asynchronous Kafka producer with health-aware partition selection.

    Uses asyncio for non-blocking produce and health monitoring.
    """

    def __init__(
        self,
        config: SmartProducerConfig,
        monitor: PartitionHealthMonitor | None = None,
        collector: HealthSignalCollector | None = None,
        scorer: HealthScorer | None = None,
        selector: PartitionSelector | None = None,
        max_workers: int = 4,
    ) -> None:
        self._config = config
        self._producer = ConfluentProducer(config.get_kafka_config())
        self._selector = selector or RandomHealthySelector()
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="async-producer"
        )
        self._closed = False

        # Health monitor
        self._monitor_owned = monitor is None
        self._monitor = monitor or _build_monitor(config, collector, scorer)
        self._monitor_started = False

        # Key stickiness cache
        self._cache: LocalLRUCache | None = None
        if config.key_stickiness and self._monitor:
            self._cache = LocalLRUCache(
                max_size=config.cache_max_size, default_ttl=config.cache_ttl
            )

    async def _ensure_started(self) -> None:
        if self._monitor and not self._monitor_started and self._monitor_owned:
            await self._monitor.start_async()
            self._monitor_started = True

    async def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        partition: int | None = None,
        on_delivery: Callable[..., None] | None = None,
        timestamp: int | None = None,
        headers: dict[str, bytes] | None = None,
    ) -> Any:
        if self._closed:
            raise RuntimeError("AsyncSmartProducer is closed")

        await self._ensure_started()

        loop = asyncio.get_event_loop()
        delivery_future = loop.create_future()

        def _delivery_cb(err: Any, msg: Any) -> None:
            def _complete() -> None:
                try:
                    if on_delivery:  # pragma: no cover
                        on_delivery(err, msg)
                    if not delivery_future.done():
                        if err:  # pragma: no cover
                            delivery_future.set_exception(
                                Exception(f"Delivery failed: {err}")
                            )
                        else:
                            delivery_future.set_result(msg)
                except Exception as e:  # pragma: no cover
                    if not delivery_future.done():
                        delivery_future.set_exception(e)

            loop.call_soon_threadsafe(_complete)

        if partition is None and self._monitor:
            partition = self._select_partition(topic, key)

        kwargs: dict[str, Any] = {"topic": topic, "on_delivery": _delivery_cb}
        if value is not None:
            kwargs["value"] = value
        if key is not None:
            kwargs["key"] = key
        if partition is not None:
            kwargs["partition"] = partition
        if timestamp is not None:  # pragma: no cover
            kwargs["timestamp"] = timestamp
        if headers is not None:  # pragma: no cover
            kwargs["headers"] = headers

        await loop.run_in_executor(
            self._executor, lambda: self._producer.produce(**kwargs)
        )

        # Poll until delivered
        async def _poll_loop() -> None:
            for _ in range(100):
                await loop.run_in_executor(self._executor, self._producer.poll, 0.1)
                if delivery_future.done():  # pragma: no cover
                    break
                await asyncio.sleep(0.01)  # pragma: no cover

        poll_task = asyncio.create_task(_poll_loop())
        try:
            return await asyncio.wait_for(delivery_future, timeout=10.0)
        finally:
            poll_task.cancel()
            try:
                await poll_task
            except asyncio.CancelledError:
                pass

    async def flush(self, timeout: float | None = None) -> int:
        if self._closed:  # pragma: no cover
            return 0
        loop = asyncio.get_event_loop()
        if timeout is not None:
            return await loop.run_in_executor(
                self._executor, self._producer.flush, timeout
            )
        return await loop.run_in_executor(  # pragma: no cover
            self._executor, self._producer.flush
        )

    async def close(self) -> None:
        if self._closed:  # pragma: no cover
            return
        self._closed = True
        try:
            if self._monitor and self._monitor_owned:
                await self._monitor.stop_async()
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(self._executor, self._producer.flush)
        except Exception as e:  # pragma: no cover
            logger.error(f"Error during AsyncSmartProducer close: {e}")
        finally:
            self._executor.shutdown(wait=True)

    def _select_partition(self, topic: str, key: bytes | None) -> int | None:
        # Same logic as sync — called from event loop but monitor access is thread-safe
        try:
            if key and self._cache is not None:
                cache_key = f"{topic}:{key!r}"
                cached = self._cache.get(cache_key)
                if cached is not None:
                    if self._monitor.is_partition_healthy(topic, int(cached)):
                        return int(cached)
                    self._cache.delete(cache_key)

            healthy = self._monitor.get_healthy_partitions(topic)
            selected = self._selector.select(healthy, key)

            if selected is not None and key and self._cache is not None:
                cache_key = f"{topic}:{key!r}"
                self._cache.set(cache_key, selected)

            return selected
        except Exception as e:  # pragma: no cover
            logger.debug(f"Partition selection failed: {e}")
            return None

    @property
    def monitor(self) -> PartitionHealthMonitor | None:
        return self._monitor

    @property
    def closed(self) -> bool:
        return self._closed

    async def __aenter__(self) -> AsyncSmartProducer:
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()
