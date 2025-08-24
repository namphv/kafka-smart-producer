# Kafka Smart Producer

[![CI](https://github.com/namphv/kafka-smart-producer/workflows/CI/badge.svg)](https://github.com/namphv/kafka-smart-producer/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/namphv/kafka-smart-producer/branch/main/graph/badge.svg)](https://codecov.io/gh/namphv/kafka-smart-producer)
[![PyPI version](https://badge.fury.io/py/kafka-smart-producer.svg)](https://badge.fury.io/py/kafka-smart-producer)
[![Python Version](https://img.shields.io/pypi/pyversions/kafka-smart-producer.svg)](https://pypi.org/project/kafka-smart-producer/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Kafka Smart Producer** is a Python library that extends `confluent-kafka-python` with intelligent, real-time, lag-aware partition selection. It solves the "hot partition" problem by monitoring consumer health and routing messages to the healthiest partitions.

## 🚀 Key Features

- **Intelligent Partition Selection**: Routes messages to healthy partitions based on real-time consumer lag monitoring
- **Performance Optimized**: Sub-millisecond overhead with smart caching strategies
- **Dual API Support**: Both synchronous and asynchronous producer implementations
- **Flexible Architecture**: Protocol-based design with extensible lag collection and health calculation
- **Graceful Degradation**: Falls back to default partitioner when health data is unavailable
- **Simple Configuration**: Minimal setup with sensible defaults, advanced configuration when needed

## 📦 Installation

```bash
pip install kafka-smart-producer
```

### Optional Dependencies

For Redis-based distributed caching:

```bash
pip install kafka-smart-producer[redis]
```

For development:

```bash
pip install kafka-smart-producer[dev]
```

## 🔧 Quick Start

### Minimal Configuration

```python
from kafka_smart_producer import SmartProducer, SmartProducerConfig

# Simple setup - just specify Kafka, topics, and consumer group
config = SmartProducerConfig.from_dict({
    "bootstrap.servers": "localhost:9092",
    "topics": ["orders", "payments"],
    "consumer_group": "order-processors"  # Automatically enables health monitoring
})

# Create producer with automatic health monitoring
with SmartProducer(config) as producer:
    # Messages are automatically routed to healthy partitions
    producer.produce(
        topic="orders",
        key=b"customer-123",
        value=b"order-data"
    )

    # Manual flush for guaranteed delivery
    producer.flush()
```

### Advanced Configuration

```python
# Full control over health monitoring and caching
config = SmartProducerConfig.from_dict({
    "bootstrap.servers": "localhost:9092",
    "topics": ["orders", "payments"],
    "health_manager": {
        "consumer_group": "order-processors",
        "health_threshold": 0.3,      # More sensitive to lag
        "refresh_interval": 3.0,      # Faster refresh
        "max_lag_for_health": 500,    # Lower lag threshold
    },
    "cache": {
        "local_max_size": 2000,
        "local_ttl_seconds": 600,
        "remote_enabled": True,       # Redis caching
        "redis_host": "localhost",
        "redis_port": 6379
    }
})

with SmartProducer(config) as producer:
    # Get health information
    healthy_partitions = producer.health_manager.get_healthy_partitions("orders")
    health_summary = producer.health_manager.get_health_summary()

    # Produce with smart partitioning
    producer.produce(topic="orders", key=b"key", value=b"value")
```

### Async Producer

```python
from kafka_smart_producer import AsyncSmartProducer

async def main():
    config = SmartProducerConfig.from_dict({
        "bootstrap.servers": "localhost:9092",
        "topics": ["orders"],
        "consumer_group": "processors"
    })

    async with AsyncSmartProducer(config) as producer:
        await producer.produce(topic="orders", key=b"key", value=b"value")
        await producer.flush()

# Run with asyncio.run(main())
```

## 🏗️ Architecture

### Core Components

1. **LagDataCollector Protocol**: Fetches consumer lag data from various sources
   - `KafkaAdminLagCollector`: Uses Kafka AdminClient (default)
   - Extensible for custom data sources (Redis, Prometheus, etc.)

2. **Health Calculation**: Transforms lag data into health scores using linear scaling
   - Built-in linear health scoring algorithm (0.0-1.0 based on lag thresholds)
   - Configurable via `HealthManagerConfig` (health_threshold, max_lag_for_health)

3. **HealthManager**: Central coordinator for health monitoring
   - `PartitionHealthMonitor`: Sync implementation with threading
   - `AsyncPartitionHealthMonitor`: Async implementation with asyncio
   - Configured via `HealthManagerConfig` with unified sync/async settings

### Caching Strategy

- **L1 Cache**: In-memory LRU cache for sub-millisecond lookups
- **L2 Cache**: Optional Redis-based distributed cache
- **Strategy**: Read-through pattern with TTL-based invalidation

## 🔄 How It Works

1. **Health Monitoring**: Background threads/tasks continuously monitor consumer lag for configured topics
2. **Health Scoring**: Lag data is converted to health scores (0.0-1.0) using configurable algorithms
3. **Partition Selection**: During message production, the producer selects partitions with health scores above the threshold
4. **Caching**: Health data is cached to minimize latency impact on message production
5. **Fallback**: If no healthy partitions are available, falls back to confluent-kafka's default partitioner

## 📊 Performance

- **Overhead**: <1ms additional latency per message
- **Throughput**: Minimal impact on producer throughput
- **Memory**: Efficient caching with configurable TTL and size limits
- **Network**: Optional Redis caching for distributed deployments

## 📋 Usage Scenarios

The library provides complete flexibility with **2 producer types** × **2 health monitor types** × **3 cache types** = **12 distinct usage scenarios** to fit different architectural needs.

### Producer Types

#### 1. **SmartProducer (Sync)**

- **Usage**: Drop-in replacement for `confluent_kafka.Producer`
- **Health Monitor**: Uses `PartitionHealthMonitor` (threading-based)
- **Context**: Synchronous applications, traditional blocking I/O

#### 2. **AsyncSmartProducer (Async)**

- **Usage**: Async/await API with `asyncio` integration
- **Health Monitor**: Uses `AsyncPartitionHealthMonitor` (asyncio-based)
- **Context**: Asynchronous applications, high-concurrency workloads

### Health Monitor Types

#### 1. **PartitionHealthMonitor (Sync)**

- **Threading**: Uses daemon threads for background monitoring
- **API**: Thread-safe methods with locks
- **Modes**:
  - `HealthMode.EMBEDDED` - Lightweight for producer integration
  - `HealthMode.STANDALONE` - Full-featured monitoring service with Redis publishing

#### 2. **AsyncPartitionHealthMonitor (Async)**

- **Concurrency**: Uses `asyncio.Task` for background monitoring
- **API**: Async methods with dual locking (thread-safe + async-safe)
- **Features**:
  - Concurrent topic monitoring
  - Health streams for reactive patterns
  - Same modes as sync version

### Cache Types

#### 1. **DefaultLocalCache (In-Memory)**

- **Implementation**: LRU cache using `cachetools.LRUCache`
- **Features**: O(1) operations, TTL support, thread-safe
- **Usage**: Fast local caching, no external dependencies

#### 2. **DefaultRemoteCache (Redis)**

- **Implementation**: Redis-based distributed cache
- **Features**: Persistence, sharing across producers, SSL support
- **Usage**: Multi-producer environments, distributed systems

#### 3. **DefaultHybridCache (Local + Remote)**

- **Implementation**: L1 (local) + L2 (Redis) cache
- **Features**: Read-through pattern, local promotion, fail-fast
- **Usage**: Best performance with distributed consistency

### Configuration Matrix

| Producer Type      | Health Monitor              | Cache Type | Use Case                  |
| ------------------ | --------------------------- | ---------- | ------------------------- |
| SmartProducer      | PartitionHealthMonitor      | Local      | Simple sync apps          |
| SmartProducer      | PartitionHealthMonitor      | Redis      | Distributed sync systems  |
| SmartProducer      | PartitionHealthMonitor      | Hybrid     | High-performance sync     |
| AsyncSmartProducer | AsyncPartitionHealthMonitor | Local      | Simple async apps         |
| AsyncSmartProducer | AsyncPartitionHealthMonitor | Redis      | Distributed async systems |
| AsyncSmartProducer | AsyncPartitionHealthMonitor | Hybrid     | High-performance async    |

### Complete Usage Examples

#### Scenario 1: Simple Sync Producer with Local Cache

```python
config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders"],
    consumer_group="order-processors"
)
producer = SmartProducer(config)
```

#### Scenario 2: Async Producer with Concurrent Health Monitoring

```python
config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders", "payments", "inventory"],
    consumer_group="processors"
)
producer = AsyncSmartProducer(config)
```

#### Scenario 3: Sync Producer with Redis Cache

```python
config = SmartProducerConfig(
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders"],
    consumer_group="processors",
    cache_config=CacheConfig(
        remote_enabled=True,
        redis_host="redis.example.com"
    )
)
producer = SmartProducer(config)
```

#### Scenario 4: Async Producer with Health Streams

```python
producer = AsyncSmartProducer(config)
async for health_update in producer.health_manager.health_stream("orders"):
    unhealthy = [p for p, score in health_update.items() if score < 0.3]
    if unhealthy:
        await alert.send(f"Partitions {unhealthy} unhealthy!")
```

#### Scenario 5: Standalone Health Monitor Service

```python
health_monitor = PartitionHealthMonitor.standalone(
    consumer_group="my-consumers",
    kafka_config={"bootstrap.servers": "localhost:9092"},
    topics=["orders", "payments"]
)
# Runs as independent monitoring service with Redis publishing
```

#### Scenario 6: Custom Health Monitor Integration

```python
lag_collector = KafkaAdminLagCollector(...)
health_monitor = PartitionHealthMonitor.embedded(lag_collector, ["orders"])
producer = SmartProducer(config, health_manager=health_monitor)
```

## 🔧 Configuration Options

### SmartProducerConfig

| Parameter        | Type      | Default  | Description                                              |
| ---------------- | --------- | -------- | -------------------------------------------------------- |
| `kafka_config`   | dict      | Required | Standard confluent-kafka producer config                 |
| `topics`         | list[str] | Required | Topics for smart partitioning                            |
| `consumer_group` | str       | None     | Consumer group for health monitoring (simplified config) |
| `health_manager` | dict      | None     | Detailed health manager configuration                    |
| `cache`          | dict      | None     | Caching configuration                                    |
| `smart_enabled`  | bool      | True     | Enable/disable smart partitioning                        |
| `key_stickiness` | bool      | True     | Enable partition stickiness for keys                     |

### Health Manager Configuration

| Parameter            | Type  | Default  | Description                                 |
| -------------------- | ----- | -------- | ------------------------------------------- |
| `consumer_group`     | str   | Required | Consumer group to monitor                   |
| `health_threshold`   | float | 0.5      | Minimum health score for healthy partitions |
| `refresh_interval`   | float | 30.0     | Seconds between health data refreshes       |
| `max_lag_for_health` | int   | 1000     | Maximum lag for 0.0 health score            |
| `timeout_seconds`    | float | 20.0     | Timeout for lag collection operations       |
| `cache_enabled`      | bool  | True     | Enable caching of health data               |
| `cache_max_size`     | int   | 1000     | Maximum cache entries                       |
| `cache_ttl_seconds`  | int   | 300      | Cache TTL in seconds                        |

### Cache Configuration

| Parameter            | Type  | Default   | Description                    |
| -------------------- | ----- | --------- | ------------------------------ |
| `local_max_size`     | int   | 1000      | LRU cache maximum entries      |
| `local_ttl_seconds`  | float | 300.0     | Local cache TTL                |
| `remote_enabled`     | bool  | False     | Enable Redis distributed cache |
| `remote_ttl_seconds` | float | 900.0     | Redis cache TTL                |
| `redis_host`         | str   | localhost | Redis server hostname          |
| `redis_port`         | int   | 6379      | Redis server port              |
| `redis_db`           | int   | 0         | Redis database number          |
| `redis_password`     | str   | None      | Redis password (optional)      |
| `redis_ssl_enabled`  | bool  | False     | Enable Redis SSL/TLS           |

## 🧪 Testing

```bash
# Install with dev dependencies
pip install kafka-smart-producer[dev]

# Run tests
pytest

# Run with coverage
pytest --cov=kafka_smart_producer

# Type checking
mypy src/

# Linting
ruff check .
```

## 📊 Code Coverage

The project maintains **70.5%** test coverage with comprehensive unit tests. Coverage reports are generated automatically in CI/CD and uploaded to Codecov.

### Running Coverage Locally

```bash
# Run tests with coverage report
uv run pytest tests/ --ignore=tests/integration/ --cov=src --cov-report=html --cov-report=term

# View detailed HTML coverage report
open coverage_html/index.html
```

### Coverage Breakdown

- **High Coverage (>85%)**: Core producer classes, configuration, and protocols
- **Medium Coverage (70-85%)**: Health monitoring and partition selection logic
- **Areas for Improvement (<70%)**: Redis caching, advanced streaming, and error handling paths

The coverage threshold is set to 70% to ensure code quality while allowing for reasonable development velocity. Integration tests are excluded from coverage as they test end-to-end scenarios with external dependencies.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙋‍♂️ Support

- 📖 [Documentation](https://github.com/pham-nam/kafka-smart-producer)
- 🐛 [Issue Tracker](https://github.com/pham-nam/kafka-smart-producer/issues)
- 💬 [Discussions](https://github.com/pham-nam/kafka-smart-producer/discussions)

## 🔄 Version History

### 0.1.0 (Initial Release)

- Core smart partitioning functionality
- Sync and async producer implementations
- Health monitoring with threading/asyncio
- Flexible caching with local and Redis support
- Protocol-based extensible architecture
- Comprehensive test suite
