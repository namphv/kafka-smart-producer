# Kafka Smart Producer

[![CI](https://github.com/namphv/kafka-smart-producer/workflows/CI/badge.svg)](https://github.com/namphv/kafka-smart-producer/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/kafka-smart-producer.svg)](https://badge.fury.io/py/kafka-smart-producer)
[![Python Version](https://img.shields.io/pypi/pyversions/kafka-smart-producer.svg)](https://pypi.org/project/kafka-smart-producer/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A Python library that extends `confluent-kafka-python` with lag-aware partition selection. Instead of round-robin or hash-based routing, it monitors real consumer lag and routes messages to the healthiest partitions — avoiding hot partitions automatically.

## Installation

```bash
pip install kafka-smart-producer
```

For Redis-based distributed caching (Scenarios 3-6):

```bash
pip install kafka-smart-producer[redis]
```

## Quick Start

```python
from kafka_smart_producer import SmartProducer, SmartProducerConfig

config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders"],
    consumer_group="order-processors",  # enables health monitoring
)

with SmartProducer(config) as producer:
    producer.produce("orders", key=b"customer-123", value=b"order-data")
    producer.flush()
```

That's it. Health monitoring starts in the background and messages are routed to healthy partitions. When health data is unavailable it falls back to Kafka's default partitioner automatically.

## How It Works

```
collector.get_lag_data(topic) -> {partition: lag}
  -> scorer.calculate_scores(lag_data) -> {partition: score 0.0-1.0}
    -> selector.select(healthy_partitions, key) -> partition_id
```

Three extension axes — all swappable via protocols:

- **HealthSignalCollector** — where health data comes from (Kafka AdminClient, Redis, custom)
- **HealthScorer** — how lag becomes a 0.0-1.0 score (default: linear)
- **PartitionSelector** — how to pick among healthy partitions (default: random)

## Configuration

```python
from kafka_smart_producer import SmartProducerConfig

config = SmartProducerConfig(
    # Required
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders", "payments"],

    # Health monitoring (None = smart routing disabled)
    consumer_group="order-processors",
    health_threshold=0.5,       # min score to be "healthy"
    refresh_interval=30.0,      # seconds between refreshes
    max_lag=1000,               # lag that maps to score 0.0
    collector_timeout=10.0,     # timeout for lag collection

    # Caching
    cache_max_size=1000,
    cache_ttl=300.0,
    key_stickiness=True,        # same key -> same partition (via cache)

    # Redis (requires [redis] extra)
    redis_url=None,             # e.g. "redis://localhost:6379/0"
    redis_ttl=900.0,

    # Feature flags
    smart_enabled=True,         # False = plain confluent-kafka producer
)
```

## Deployment Scenarios

### Scenario 1 — Simple Sync Producer

```python
from kafka_smart_producer import SmartProducer, SmartProducerConfig

config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders"],
    consumer_group="order-processors",
)

with SmartProducer(config) as producer:
    producer.produce("orders", key=b"user-1", value=b"data")
    producer.flush()
```

### Scenario 2 — Async Concurrent Producer

```python
from kafka_smart_producer import AsyncSmartProducer, SmartProducerConfig

config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders", "payments", "inventory"],  # monitored concurrently
    consumer_group="processors",
)

async with AsyncSmartProducer(config) as producer:
    await producer.produce("orders", value=b"data")
    await producer.flush()
```

### Scenario 3 — Sync Producer + Redis (Distributed Key Stickiness)

```python
from kafka_smart_producer import SmartProducer, SmartProducerConfig
from kafka_smart_producer.cache.local import LocalLRUCache
from kafka_smart_producer.contrib.redis_cache import HybridCache, RemoteCache

config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders"],
    consumer_group="processors",
    key_stickiness=True,
)

producer = SmartProducer(config)

# Replace local cache with hybrid L1+L2
local = LocalLRUCache(max_size=1000, default_ttl=300.0)
remote = RemoteCache(redis_url="redis://localhost:6379/0")
producer._cache = HybridCache(local=local, remote=remote)

# Key-partition mappings now survive producer restarts
producer.produce("orders", key=b"user-1", value=b"data")
producer.flush()
producer.close()
```

### Scenario 4 — Async Producer + Redis

Same as Scenario 3 but with `AsyncSmartProducer`.

### Scenario 5 — Standalone Health Monitor

Run health monitoring as an independent service that publishes to Redis. Useful when producers run in restricted environments without Kafka AdminClient access.

```python
from kafka_smart_producer import (
    KafkaAdminLagCollector,
    LinearHealthScorer,
    PartitionHealthMonitor,
)
from kafka_smart_producer.contrib.redis_health_consumer import RedisHealthPublisher

collector = KafkaAdminLagCollector(
    bootstrap_servers="localhost:9092",
    consumer_group="my-consumers",
)
monitor = PartitionHealthMonitor(
    collector=collector,
    scorer=LinearHealthScorer(max_lag=1000),
    health_threshold=0.5,
    refresh_interval=10.0,
)
monitor.initialize_topics(["orders", "payments"])
publisher = RedisHealthPublisher(redis_url="redis://localhost:6379/0")

monitor.start_sync()
try:
    while True:
        scores = monitor.get_health_scores("orders")
        publisher.publish_scores("orders", scores)
        time.sleep(10)
finally:
    monitor.stop_sync()
```

### Scenario 6 — Redis Health Consumer

Producer reads health data from Redis (published by Scenario 5) instead of Kafka directly.

```python
from kafka_smart_producer import PartitionHealthMonitor, SmartProducer, SmartProducerConfig
from kafka_smart_producer.contrib.redis_health_consumer import RedisHealthConsumer

# Use Redis as the health signal source
redis_collector = RedisHealthConsumer(redis_url="redis://localhost:6379/0")
monitor = PartitionHealthMonitor(
    collector=redis_collector,
    health_threshold=0.5,
)
monitor.initialize_topics(["orders"])

config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders"],
    consumer_group="processors",
)

with SmartProducer(config, monitor=monitor) as producer:
    producer.produce("orders", value=b"data")
    producer.flush()
```

## Extending the Library

All three core components are swappable via protocols:

```python
from kafka_smart_producer.health.protocols import HealthSignalCollector, HealthScorer
from kafka_smart_producer.health.monitor import PartitionHealthMonitor

class PrometheusCollector:
    """Custom collector pulling lag from Prometheus."""
    def get_lag_data(self, topic: str) -> dict[int, int]:
        # query Prometheus, return {partition: lag}
        ...
    def is_healthy(self) -> bool:
        return True

class PercentileScorer:
    """Score partitions by lag percentile across the topic."""
    def calculate_scores(self, lag_data: dict[int, int]) -> dict[int, float]:
        ...

monitor = PartitionHealthMonitor(
    collector=PrometheusCollector(),
    scorer=PercentileScorer(),
)
```

## Project Structure

```
src/kafka_smart_producer/
├── __init__.py                    # Public API
├── producer.py                    # SmartProducer, AsyncSmartProducer
├── config.py                      # SmartProducerConfig
├── exceptions.py                  # Exception hierarchy
├── health/
│   ├── protocols.py               # HealthSignalCollector, HealthScorer, PartitionSelector
│   ├── monitor.py                 # PartitionHealthMonitor (unified sync+async)
│   ├── scorer.py                  # LinearHealthScorer
│   └── collectors/
│       └── kafka_admin.py         # KafkaAdminLagCollector
├── partition/
│   └── selector.py                # RandomHealthySelector
├── cache/
│   ├── protocols.py               # Cache protocol
│   └── local.py                   # LocalLRUCache (stdlib-only)
└── contrib/                       # Optional, requires [redis] extra
    ├── redis_cache.py             # RemoteCache, HybridCache
    └── redis_health_consumer.py   # RedisHealthPublisher, RedisHealthConsumer
```

## Testing

```bash
# Unit tests (no external services required)
uv run pytest tests/ --ignore=tests/integration

# Integration tests (requires Docker)
cd tests/integration && docker compose up -d
cd ../.. && uv run pytest tests/integration/ -v

# With coverage
uv run pytest tests/ --ignore=tests/integration --cov=kafka_smart_producer

# Linting
uv run ruff check src/ tests/
```

Integration tests use Docker Compose to run real Kafka and Redis instances. Each of the 6 scenarios has a dedicated test file.

## Design Principles

- **Minimal hard dependencies** — only `confluent-kafka`. Redis is an optional extra.
- **Graceful degradation** — always falls back to default Kafka partitioning when health data is unavailable.
- **Unified sync/async** — one `PartitionHealthMonitor` class supports both threading and asyncio modes.
- **Protocol-based** — all extension points (`HealthSignalCollector`, `HealthScorer`, `PartitionSelector`, `Cache`) are Python protocols, not abstract base classes.
- **Performance** — sub-millisecond overhead on `produce()` calls via LRU caching.

## License

MIT
