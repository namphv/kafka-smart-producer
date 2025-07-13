# Kafka Smart Producer

[![CI](https://github.com/namphv/kafka-smart-producer/workflows/CI/badge.svg)](https://github.com/namphv/kafka-smart-producer/actions/workflows/ci.yml)
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

2. **HotPartitionCalculator Protocol**: Transforms lag data into health scores
   - `ThresholdHotPartitionCalculator`: Basic threshold-based scoring (default)
   - Extensible for custom health algorithms

3. **HealthManager**: Central coordinator for health monitoring
   - `PartitionHealthMonitor`: Sync implementation with threading
   - `AsyncPartitionHealthMonitor`: Async implementation with asyncio

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
| `refresh_interval`   | float | 5.0      | Seconds between health data refreshes       |
| `max_lag_for_health` | int   | 1000     | Maximum lag for 0.0 health score            |
| `timeout_seconds`    | float | 5.0      | Timeout for lag collection operations       |

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
