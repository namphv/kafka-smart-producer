#!/usr/bin/env python3
"""
Partition Selection Logic Example

This demonstrates the improved partition selection logic in the Smart Producer.
The new logic follows this clear flow:

1. Get selected_partition if smart_enabled (from healthy partitions)
2. Apply key stickiness logic:
   - If key + key_stickiness: check cache first, fallback to selected_partition
   - If key + no key_stickiness: use selected_partition directly
   - If no key: use selected_partition

This provides cleaner separation between health-based selection and caching.
"""

import asyncio


def demonstrate_partition_logic():
    """Demonstrate the new partition selection logic."""
    print("🎯 NEW PARTITION SELECTION LOGIC")
    print("=" * 50)

    print("\n📋 Logic Flow:")
    print("1. Get selected_partition if _smart_enabled")
    print("2. If key and key_stickiness:")
    print("   - Check cache first")
    print("   - If cache miss, use selected_partition and cache it")
    print("3. If key and not key_stickiness:")
    print("   - Use selected_partition directly (no caching)")
    print("4. If no key:")
    print("   - Use selected_partition")
    print("5. Fall back to Kafka default partitioning if no selected_partition")


def sync_producer_examples():
    """Examples using sync producer."""
    print("\n🔄 Sync Producer Examples")

    from kafka_smart_producer import SmartProducer

    # Example 1: With health manager and key stickiness
    config1 = {
        "bootstrap.servers": "localhost:9092",
        "topics": ["orders"],
        "health_manager": {"consumer_group": "order-consumers"},
        "key_stickiness": True,
        "smart_enabled": True,
    }

    print("\n1️⃣ Smart Producer with Health Manager + Key Stickiness:")
    try:
        producer1 = SmartProducer(config1)
        print(f"✅ Created: {producer1}")
        print(f"   - Smart enabled: {producer1.smart_enabled}")
        print(f"   - Key stickiness: {producer1._config.key_stickiness}")

        # Simulate messages with keys
        print("\n   📤 Message Flow:")
        print(
            "   - Message with key='user-123': Will check cache → use healthy partition → cache result"
        )
        print("   - Message with key='user-123': Will hit cache → use cached partition")
        print(
            "   - Message with key='user-456': Will check cache → use healthy partition → cache result"
        )
        print("   - Message without key: Will use healthy partition directly")

    except Exception as e:
        print(f"❌ Config 1 failed: {e}")

    # Example 2: With health manager but no key stickiness
    config2 = {
        "bootstrap.servers": "localhost:9092",
        "topics": ["events"],
        "health_manager": {"consumer_group": "event-consumers"},
        "key_stickiness": False,
        "smart_enabled": True,
    }

    print("\n2️⃣ Smart Producer with Health Manager but NO Key Stickiness:")
    try:
        producer2 = SmartProducer(config2)
        print(f"✅ Created: {producer2}")
        print(f"   - Smart enabled: {producer2.smart_enabled}")
        print(f"   - Key stickiness: {producer2._config.key_stickiness}")

        print("\n   📤 Message Flow:")
        print(
            "   - Message with key='event-123': Will use healthy partition directly (no caching)"
        )
        print(
            "   - Message with key='event-456': Will use healthy partition directly (no caching)"
        )
        print("   - Message without key: Will use healthy partition directly")

    except Exception as e:
        print(f"❌ Config 2 failed: {e}")

    # Example 3: No health manager (smart disabled)
    config3 = {
        "bootstrap.servers": "localhost:9092",
        "topics": ["logs"],
        "smart_enabled": False,  # Or no health_manager
    }

    print("\n3️⃣ Producer with Smart Partitioning Disabled:")
    try:
        producer3 = SmartProducer(config3)
        print(f"✅ Created: {producer3}")
        print(f"   - Smart enabled: {producer3.smart_enabled}")

        print("\n   📤 Message Flow:")
        print("   - All messages: Use Kafka's default partitioning")
        print("   - No health checks, no caching, no smart selection")

    except Exception as e:
        print(f"❌ Config 3 failed: {e}")


async def async_producer_examples():
    """Examples using async producer."""
    print("\n⚡ Async Producer Examples")

    from kafka_smart_producer import AsyncSmartProducer

    config = {
        "bootstrap.servers": "localhost:9092",
        "topics": ["async-events"],
        "health_manager": {"consumer_group": "async-consumers"},
        "key_stickiness": True,
        "smart_enabled": True,
    }

    try:
        async_producer = AsyncSmartProducer(config)
        print(f"✅ Async Producer Created: {async_producer}")
        print(f"   - Smart enabled: {async_producer.smart_enabled}")
        print(f"   - Key stickiness: {async_producer._config.key_stickiness}")

        print("\n   📤 Async Message Flow:")
        print("   - Same logic as sync producer")
        print("   - But all operations are async (health checks, caching)")
        print("   - Uses thread pool executor for blocking operations")

    except Exception as e:
        print(f"❌ Async config failed: {e}")


def partition_selection_scenarios():
    """Demonstrate different partition selection scenarios."""
    print("\n🎲 Partition Selection Scenarios")

    scenarios = [
        {
            "name": "Key + Stickiness + Cache Hit",
            "key": "user-123",
            "stickiness": True,
            "cache_hit": True,
            "healthy_partitions": [0, 2, 4],
            "cached_partition": 2,
            "expected": "Use cached partition 2",
        },
        {
            "name": "Key + Stickiness + Cache Miss",
            "key": "user-456",
            "stickiness": True,
            "cache_hit": False,
            "healthy_partitions": [1, 3, 5],
            "cached_partition": None,
            "expected": "Use healthy partition (random from [1,3,5]) + cache it",
        },
        {
            "name": "Key + No Stickiness",
            "key": "event-789",
            "stickiness": False,
            "cache_hit": False,
            "healthy_partitions": [0, 1, 4],
            "cached_partition": None,
            "expected": "Use healthy partition (random from [0,1,4])",
        },
        {
            "name": "No Key",
            "key": None,
            "stickiness": True,
            "cache_hit": False,
            "healthy_partitions": [2, 3, 5],
            "cached_partition": None,
            "expected": "Use healthy partition (random from [2,3,5])",
        },
        {
            "name": "Smart Disabled",
            "key": "any-key",
            "stickiness": True,
            "cache_hit": False,
            "healthy_partitions": [],  # Smart disabled
            "cached_partition": None,
            "expected": "Use Kafka default partitioning",
        },
    ]

    for i, scenario in enumerate(scenarios, 1):
        print(f"\n{i}️⃣ {scenario['name']}:")
        print(f"   Key: {scenario['key']}")
        print(f"   Key Stickiness: {scenario['stickiness']}")
        print(f"   Cache Hit: {scenario['cache_hit']}")
        print(f"   Healthy Partitions: {scenario['healthy_partitions']}")
        print(f"   Expected: {scenario['expected']}")


def performance_benefits():
    """Explain performance benefits of the new logic."""
    print("\n⚡ Performance Benefits")

    benefits = [
        "🚀 Early Exit: Skip smart logic if explicit partition provided",
        "🎯 Direct Access: Clear separation between health selection and caching",
        "⚡ Cache First: Check cache before expensive health calculations",
        "🔄 Fallback Chain: Graceful degradation when components unavailable",
        "🧠 Smart Defaults: Sensible behavior for all key/stickiness combinations",
        "📊 Predictable: Same logic flow for both sync and async producers",
    ]

    for benefit in benefits:
        print(f"   {benefit}")


if __name__ == "__main__":
    demonstrate_partition_logic()
    sync_producer_examples()
    asyncio.run(async_producer_examples())
    partition_selection_scenarios()
    performance_benefits()

    print("\n🎉 Partition Selection Logic Demo Complete!")
    print("\n💡 Key Improvements:")
    print("✅ SIMPLE API: producer.produce() → _partition_selector.select_partition()")
    print("✅ CLEAR FLOW: Explicit steps in BasePartitionSelector")
    print("✅ CACHE FIRST: Check cache before health calculations")
    print("✅ ENCAPSULATED: All logic contained in partition selector")
    print("✅ SAME LOGIC: Consistent between sync and async")
    print("✅ PERFORMANCE: Minimal overhead on critical path")
