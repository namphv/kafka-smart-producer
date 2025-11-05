# Integration Tests

This directory contains integration tests that use real Kafka services via Docker to test real-world usage scenarios.

## Setup

### Prerequisites

- Docker and Docker Compose
- Python environment with project dependencies

### Running Integration Tests

1. **Start Kafka services:**

   ```bash
   cd tests/integration
   docker compose up -d
   ```

2. **Wait for services to be ready:**

   ```bash
   # Check service health
   docker compose ps

   # Verify topics are created
   docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
   ```

3. **Run integration tests:**

   ```bash
   # From project root - Run all integration tests
   uv run pytest tests/integration/ -v

   # ⭐ Run CRITICAL core functionality tests first
   uv run pytest tests/integration/test_core_smart_features.py -v -s

   # Run specific test class
   uv run pytest tests/integration/test_real_world_usage.py::TestRealWorldSyncProducer -v

   # Run with output
   uv run pytest tests/integration/ -v -s
   ```

4. **Clean up:**
   ```bash
   cd tests/integration
   docker compose down -v
   ```

## Test Structure

### Docker Services

- **Zookeeper**: Kafka coordination service
- **Kafka**: Single broker with 4 default partitions
- **Kafka Setup**: Initializes test topics:
  - `test-topic` (4 partitions)
  - `multi-partition-topic` (6 partitions)
  - `simple-topic` (2 partitions)

### Test Categories

1. **Core Smart Features** (`TestCoreSmartFeatures`) ⭐ **CRITICAL**
   - **Partition avoidance verification** - Does the producer actually avoid unhealthy partitions?
   - **Health monitoring detection** - Does health monitoring detect real consumer lag?
   - **Graceful degradation** - Does producer work when health monitoring fails?
   - **True drop-in replacement** - Is SmartProducer 100% compatible with Producer?
   - **Key consistency with health** - Do same keys route consistently while respecting health?

2. **Smart Functionality Validation** (`TestSmartFunctionalityVerification`)
   - Advanced partition routing verification with lag scenarios
   - Health manager real data collection testing
   - Comprehensive async producer smart routing
   - Partition health scoring validation

3. **Real World Sync Producer** (`TestRealWorldSyncProducer`)
   - Basic producer lifecycle with health monitoring
   - Consumer lag scenarios and intelligent routing
   - Partition selection intelligence

4. **Real World Async Producer** (`TestRealWorldAsyncProducer`)
   - Async producer lifecycle
   - Concurrent message production workloads

5. **Health Monitoring Behavior** (`TestHealthMonitoringBehavior`)
   - Background health monitor operation
   - Graceful degradation when health monitoring fails

6. **Drop-in Compatibility** (`TestDropInCompatibility`)
   - API compatibility with confluent-kafka-python
   - Configuration compatibility

### Test Scenarios

- **Basic functionality**: Producer startup, message production, shutdown
- **Health monitoring**: Background lag collection and partition health scoring
- **Consumer lag simulation**: Create realistic lag scenarios to test intelligent routing
- **Concurrent workloads**: Multiple producers and consumers with realistic message volumes
- **Error scenarios**: Network issues, consumer group problems, graceful degradation
- **Drop-in replacement**: Verify 100% API compatibility with confluent-kafka-python

## Configuration

Tests use minimal configuration to simulate real-world usage:

```python
SmartProducerConfig(
    kafka_config={
        "bootstrap.servers": "localhost:9092",
        "client.id": "integration-test-producer",
    },
    consumer_group="integration-test-group",
)
```

Health monitoring automatically starts in the background and provides partition health data to optimize message routing.

## Debugging

### View Kafka logs:

```bash
docker compose logs kafka
```

### Check topic details:

```bash
docker compose exec kafka kafka-topics --describe --topic test-topic --bootstrap-server localhost:9092
```

### Monitor consumer groups:

```bash
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker compose exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group integration-test-group
```

### Manual testing:

```bash
# Produce test message
docker compose exec kafka kafka-console-producer --topic test-topic --bootstrap-server localhost:9092

# Consume messages
docker compose exec kafka kafka-console-consumer --topic test-topic --from-beginning --bootstrap-server localhost:9092
```
