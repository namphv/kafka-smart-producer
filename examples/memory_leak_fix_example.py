"""
Example showing the memory leak fix in Kafka Smart Producer.

This example demonstrates how the bounded cache prevents memory leaks
in long-running applications with many unique keys.
"""

import time

from kafka_smart_producer import SmartProducer


def demonstrate_memory_leak_fix():
    """Demonstrate that the cache is bounded and prevents memory leaks."""

    print("=== Kafka Smart Producer - Memory Leak Fix Demo ===")

    # Configure producer with bounded cache
    config = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "memory-leak-demo",
        # Cache configuration - prevents memory leaks
        "smart.cache.max.size": 100,  # Maximum 100 cached entries
        "smart.cache.ttl.ms": 60000,  # 1 minute TTL
        "smart.partitioning.enabled": True,
    }

    # NOTE: This example doesn't actually connect to Kafka
    # It demonstrates the cache behavior

    print(
        f"Creating SmartProducer with cache max size: {config['smart.cache.max.size']}"
    )

    try:
        producer = SmartProducer(config)

        print("✅ SmartProducer created successfully")

        # Get initial cache stats
        stats = producer.get_cache_stats()
        print(f"Initial cache stats: {stats}")

        # Simulate processing many unique keys (would cause memory leak in old version)
        print("\n📊 Simulating high-throughput application with many unique keys...")

        # This simulates a real-world scenario where an application processes
        # many different users/keys over time
        unique_keys_processed = 0

        for batch in range(10):  # Process 10 batches
            batch_start = time.time()

            # Process 100 unique keys per batch
            for i in range(100):
                key_id = batch * 100 + i
                topic = f"user-events-{key_id % 5}"  # 5 different topics
                key = f"user_{key_id}".encode()

                # This would normally call producer.produce(), but we'll just
                # simulate the cache behavior
                cache_key = f"{topic}:{key.hex()}"
                producer._key_cache.set(cache_key, key_id % 3)  # Store partition

                unique_keys_processed += 1

            batch_time = time.time() - batch_start

            # Check cache stats after each batch
            stats = producer.get_cache_stats()

            print(
                f"Batch {batch + 1}: Processed {unique_keys_processed} unique keys, "
                f"Cache size: {stats['total_entries']} (max: {stats['max_size']}), "
                f"Batch time: {batch_time:.3f}s"
            )

            # Verify cache is bounded
            assert stats["total_entries"] <= stats["max_size"]

        print(f"\n✅ Successfully processed {unique_keys_processed} unique keys")
        print(f"💾 Cache remained bounded at {stats['total_entries']} entries")
        print(
            f"🚫 Memory leak prevented - cache never exceeded "
            f"{stats['max_size']} entries"
        )

        # Show cache efficiency
        print("\n📈 Cache efficiency:")
        print(f"  - Total unique keys processed: {unique_keys_processed}")
        print(f"  - Cache entries: {stats['total_entries']}")
        print(
            f"  - Memory usage: Bounded (would be {unique_keys_processed} "
            f"entries without fix)"
        )

        # Demonstrate cache clearing
        print("\n🗑️ Cache can be cleared manually:")
        producer.clear_cache()
        final_stats = producer.get_cache_stats()
        print(f"  - Cache size after clear: {final_stats['total_entries']}")

    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback

        traceback.print_exc()


def demonstrate_cache_configuration():
    """Show different cache configuration options."""

    print("\n=== Cache Configuration Examples ===")

    configs = [
        {
            "name": "High-throughput application",
            "config": {
                "bootstrap.servers": "localhost:9092",
                "smart.cache.max.size": 5000,  # Large cache for high throughput
                "smart.cache.ttl.ms": 300000,  # 5 minutes
            },
        },
        {
            "name": "Memory-constrained environment",
            "config": {
                "bootstrap.servers": "localhost:9092",
                "smart.cache.max.size": 100,  # Small cache to save memory
                "smart.cache.ttl.ms": 60000,  # 1 minute
            },
        },
        {
            "name": "Disabled caching",
            "config": {
                "bootstrap.servers": "localhost:9092",
                "smart.partitioning.enabled": False,  # Disables cache entirely
            },
        },
    ]

    for example in configs:
        print(f"\n📋 {example['name']}:")

        # Show configuration
        for key, value in example["config"].items():
            if key.startswith("smart."):
                print(f"  {key}: {value}")

        # Create producer to show resulting cache stats
        try:
            producer = SmartProducer.__new__(SmartProducer)
            smart_config = producer._extract_smart_config(example["config"].copy())

            print(
                f"  Result: Cache "
                f"{'enabled' if smart_config['enabled'] else 'disabled'}"
            )
            if smart_config["enabled"]:
                print(f"    - Max size: {smart_config['cache_max_size']} entries")
                print(f"    - TTL: {smart_config['cache_ttl_ms']}ms")

        except Exception as e:
            print(f"  Error: {e}")


if __name__ == "__main__":
    demonstrate_memory_leak_fix()
    demonstrate_cache_configuration()

    print("\n🎉 Memory leak fix demonstration complete!")
    print("💡 The SmartProducer now uses bounded cache to prevent memory leaks")
    print("📚 Configure cache size based on your application's needs")
