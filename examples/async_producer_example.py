"""
Example usage of AsyncSmartProducer with asyncio.

This example demonstrates how to use the AsyncSmartProducer in various
async contexts including FastAPI, batch processing, and concurrent operations.
"""

import asyncio
import json
import time
from typing import Any, Dict

from kafka_smart_producer import AsyncSmartProducer


async def basic_async_example():
    """Basic usage example of AsyncSmartProducer."""
    print("=== Basic AsyncSmartProducer Example ===")

    # Configure the producer
    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "async-example-producer",
        "acks": "all",
        "retries": 3,
        "smart.partitioning.enabled": True,
        "smart.cache.ttl.ms": 300000,  # 5 minutes
    }

    # Create async producer
    producer = AsyncSmartProducer(config)

    try:
        # Produce messages asynchronously
        for i in range(10):
            await producer.produce(
                topic="user-events",
                key=f"user{i}".encode(),
                value=json.dumps(
                    {
                        "user_id": f"user{i}",
                        "event": "page_view",
                        "timestamp": time.time(),
                    }
                ).encode(),
            )
            print(f"Produced message {i}")

        # Flush remaining messages
        remaining = await producer.flush()
        print(f"Flush completed, {remaining} messages remaining")

    finally:
        await producer.close()
        print("Producer closed")


async def concurrent_production_example():
    """Example of concurrent message production."""
    print("\n=== Concurrent Production Example ===")

    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "concurrent-producer",
        "smart.partitioning.enabled": True,
    }

    # Use async context manager
    async with AsyncSmartProducer(config) as producer:
        # Create multiple concurrent tasks
        tasks = []
        for i in range(50):
            task = producer.produce(
                topic="events",
                key=f"key{i % 10}".encode(),  # 10 different keys
                value=f"concurrent_event_{i}".encode(),
            )
            tasks.append(task)

        # Wait for all messages to be produced
        start_time = time.time()
        await asyncio.gather(*tasks)
        end_time = time.time()

        print(
            f"Produced 50 messages concurrently in {end_time - start_time:.2f} seconds"
        )


async def batch_processing_example():
    """Example of processing events in batches."""
    print("\n=== Batch Processing Example ===")

    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "batch-processor",
        "batch.size": 16384,
        "linger.ms": 10,
        "compression.type": "snappy",
    }

    # Simulate event data
    events = [
        {
            "user_id": f"user{i % 100}",
            "event_type": "purchase" if i % 5 == 0 else "page_view",
            "timestamp": time.time() + i,
            "data": {"product_id": f"prod_{i}", "price": 9.99 + i},
        }
        for i in range(1000)
    ]

    producer = AsyncSmartProducer(config)

    try:
        # Process events in batches
        batch_size = 50
        for i in range(0, len(events), batch_size):
            batch = events[i : i + batch_size]

            # Process batch concurrently
            tasks = [
                producer.produce(
                    topic="user-events",
                    key=event["user_id"].encode(),
                    value=json.dumps(event).encode(),
                    timestamp=int(event["timestamp"] * 1000),
                )
                for event in batch
            ]

            await asyncio.gather(*tasks)

            # Small delay between batches
            await asyncio.sleep(0.1)

            print(f"Processed batch {i // batch_size + 1}/{len(events) // batch_size}")

        # Final flush
        await producer.flush()
        print("Batch processing completed")

    finally:
        await producer.close()


async def fastapi_integration_example():
    """Example of integrating AsyncSmartProducer with FastAPI."""
    print("\n=== FastAPI Integration Example ===")

    # This would typically be done in your FastAPI app
    class EventService:
        def __init__(self):
            self.producer = None

        async def start(self):
            """Start the producer (called in FastAPI startup event)."""
            config = {
                "bootstrap.servers": "localhost:9092",
                "client.id": "fastapi-producer",
                "smart.partitioning.enabled": True,
            }

            self.producer = AsyncSmartProducer(config)
            print("EventService started")

        async def stop(self):
            """Stop the producer (called in FastAPI shutdown event)."""
            if self.producer:
                await self.producer.close()
                print("EventService stopped")

        async def publish_event(self, event_data: Dict[str, Any]):
            """Publish an event asynchronously."""
            if not self.producer:
                raise RuntimeError("EventService not started")

            await self.producer.produce(
                topic="api-events",
                key=event_data["user_id"].encode(),
                value=json.dumps(event_data).encode(),
            )

        async def get_stats(self):
            """Get producer statistics."""
            if not self.producer:
                return {"status": "not_started"}

            return {
                "status": "running",
                "cache_stats": self.producer.get_cache_stats(),
                "closed": self.producer.closed,
            }

    # Simulate FastAPI lifecycle
    service = EventService()

    # Startup
    await service.start()

    try:
        # Simulate API requests
        events = [
            {"user_id": "user1", "action": "login", "timestamp": time.time()},
            {"user_id": "user2", "action": "purchase", "amount": 99.99},
            {"user_id": "user1", "action": "logout", "timestamp": time.time()},
        ]

        # Publish events (non-blocking)
        tasks = [service.publish_event(event) for event in events]
        await asyncio.gather(*tasks)

        # Get stats
        stats = await service.get_stats()
        print(f"Service stats: {stats}")

    finally:
        # Shutdown
        await service.stop()


async def error_handling_example():
    """Example of error handling with AsyncSmartProducer."""
    print("\n=== Error Handling Example ===")

    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "error-handling-producer",
        "message.timeout.ms": 5000,
        "retries": 1,
    }

    producer = AsyncSmartProducer(config)

    try:
        # Custom delivery callback
        async def handle_delivery(err, msg):
            if err:
                print(f"Delivery failed: {err}")
            else:
                print(
                    f"Message delivered to {msg.topic()}[{msg.partition()}] "
                    f"at offset {msg.offset()}"
                )

        # Produce with error handling
        try:
            await producer.produce(
                topic="test-topic",
                key=b"test-key",
                value=b"test-message",
                on_delivery=handle_delivery,
            )
            print("Message produced successfully")

        except Exception as e:
            print(f"Failed to produce message: {e}")

        # Test closed producer
        await producer.close()

        try:
            await producer.produce("test-topic", value=b"should-fail")
        except RuntimeError as e:
            print(f"Expected error: {e}")

    finally:
        # Ensure cleanup
        if not producer.closed:
            await producer.close()


async def monitoring_example():
    """Example of monitoring producer performance."""
    print("\n=== Monitoring Example ===")

    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "monitoring-producer",
        "smart.partitioning.enabled": True,
    }

    producer = AsyncSmartProducer(config)

    try:
        # Produce messages and monitor
        start_time = time.time()

        tasks = []
        for i in range(100):
            task = producer.produce(
                topic="monitoring-topic",
                key=f"key{i % 10}".encode(),
                value=f"message_{i}".encode(),
            )
            tasks.append(task)

        await asyncio.gather(*tasks)

        end_time = time.time()

        # Get performance metrics
        stats = producer.get_cache_stats()

        print("Performance metrics:")
        print("  Messages produced: 100")
        print(f"  Time taken: {end_time - start_time:.2f} seconds")
        print(f"  Messages per second: {100 / (end_time - start_time):.2f}")
        print(f"  Cache stats: {stats}")

        # Clear cache for testing
        producer.clear_cache()
        print("Cache cleared")

    finally:
        await producer.close()


async def main():
    """Run all examples."""
    print("AsyncSmartProducer Examples")
    print("=" * 50)

    examples = [
        basic_async_example,
        concurrent_production_example,
        batch_processing_example,
        fastapi_integration_example,
        error_handling_example,
        monitoring_example,
    ]

    for example in examples:
        try:
            await example()
        except Exception as e:
            print(f"Example {example.__name__} failed: {e}")

        # Small delay between examples
        await asyncio.sleep(0.5)

    print("\nAll examples completed!")


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())
