#!/usr/bin/env python3
"""
Example demonstrating simplified SmartProducer configuration.

This example shows how to use the new top-level consumer_group configuration
for easy setup of health monitoring without complex nested configuration.
"""

from kafka_smart_producer.producer_config import SmartProducerConfig
from kafka_smart_producer.sync_producer import SmartProducer


def main():
    """Demonstrate simplified configuration."""

    # Method 1: Minimal configuration with top-level consumer_group
    # This automatically creates and starts a PartitionHealthMonitor
    minimal_config = SmartProducerConfig.from_dict(
        {
            "bootstrap.servers": "localhost:9092",
            "topics": ["orders", "payments", "inventory"],
            "consumer_group": "order-processors",  # Top-level consumer group
        }
    )

    print("=== Minimal Configuration Example ===")
    print(f"Config created: {minimal_config}")
    print(f"Health config auto-created: {minimal_config.health_config is not None}")
    print(
        f"Consumer group: {minimal_config.health_config.consumer_group if minimal_config.health_config else 'None'}"
    )

    # Create producer with minimal config
    with SmartProducer(minimal_config) as producer:
        print(f"Producer smart enabled: {producer.smart_enabled}")
        print(
            f"Health manager running: {producer.health_manager.is_running if producer.health_manager else False}"
        )

        # Produce some messages
        for i in range(5):
            producer.produce(
                topic="orders",
                key=f"customer-{i}".encode(),
                value=f"order-{i}".encode(),
            )

        # Manual flush for guaranteed delivery
        producer.flush()
        print("Messages produced successfully with smart partitioning")

    print("\n=== Advanced Configuration Example ===")

    # Method 2: Advanced configuration with explicit health_manager
    # This gives you full control over health monitoring settings
    advanced_config = SmartProducerConfig.from_dict(
        {
            "bootstrap.servers": "localhost:9092",
            "topics": ["orders", "payments"],
            "health_manager": {
                "consumer_group": "order-processors",
                "health_threshold": 0.3,  # More sensitive to lag
                "refresh_interval": 3.0,  # Faster refresh
                "max_lag_for_health": 500,  # Lower lag threshold
            },
            "cache": {
                "local_max_size": 2000,
                "local_ttl_seconds": 600,
            },
        }
    )

    print(f"Advanced config: {advanced_config}")
    print(f"Health threshold: {advanced_config.health_config.health_threshold}")
    print(f"Refresh interval: {advanced_config.health_config.refresh_interval}s")

    with SmartProducer(advanced_config) as producer:
        print(f"Health manager running: {producer.health_manager.is_running}")

        # Get health summary
        health_summary = producer.health_manager.get_health_summary()
        print(f"Health summary: {health_summary}")


if __name__ == "__main__":
    main()
