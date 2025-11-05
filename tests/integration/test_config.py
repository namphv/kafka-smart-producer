"""
Test Configuration Constants for Integration Tests.

Shared configuration values used across all integration test scenarios.
"""

import os

# Test-optimized health configuration
TEST_HEALTH_CONFIG = {
    "refresh_interval": 5.0,  # 5 seconds (vs 30s production)
    "timeout_seconds": 3.0,  # 3 seconds (vs 20s production) - must be < refresh_interval
    "lag_threshold": 5,  # 5 messages (vs 100+ production)
    "unhealthy_threshold": 0.3,  # 30% threshold (vs 70% production)
}

# Test cache configuration
TEST_CACHE_CONFIG = {
    "ttl_seconds": 6,  # 6 seconds cache TTL - must be > refresh_interval
    "local_enabled": True,
    "remote_enabled": False,  # Override per scenario
}

# Small data amounts for testing
TEST_DATA_AMOUNTS = {
    "lag_messages": 8,  # Creates clear unhealthy signal
    "produce_count": 100,  # Sufficient for 15% threshold validation (minimum 67 messages)
    "wait_cycles": 2,  # 2 refresh cycles = 10 seconds
}

# Test validation thresholds
TEST_THRESHOLDS = {
    "AVOIDANCE_MAX_PERCENTAGE": 0.25,  # Max 25% to unhealthy partition (more realistic)
    "HEALTH_DETECTION_TIMEOUT": 12,  # Health detection timeout seconds (2+ refresh cycles)
    "MIN_MESSAGE_DELIVERY": 80,  # Minimum successful deliveries (80% of produce_count)
    "UNIQUE_ID_MODULO": 10000,  # For test ID generation
}

# Environment configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6380"))
