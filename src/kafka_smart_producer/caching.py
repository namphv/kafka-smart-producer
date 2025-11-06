"""
Sync-only caching system for Kafka Smart Producer.

This module provides local and remote cache implementations
with read-through patterns for partition health data caching.

Note: CacheFactory has been extracted to cache_factory.py to reduce file complexity.
"""

import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from cachetools import LRUCache

from .exceptions import CacheError

logger = logging.getLogger(__name__)


class CacheEntry:
    """Container for cached values with metadata."""

    def __init__(self, value: Any, ttl_seconds: Optional[float] = None):
        self.value = value
        self.created_at = time.time()
        self.ttl_seconds = ttl_seconds

    def is_expired(self) -> bool:
        """Check if entry has exceeded its TTL."""
        if self.ttl_seconds is None:
            return False
        if self.ttl_seconds <= 0:
            return True  # Zero or negative TTL means immediate expiration
        return (time.time() - self.created_at) > self.ttl_seconds


class LocalCache(Protocol):
    """Local in-memory cache interface (LRU with TTL)."""

    def get(self, key: str) -> Optional[Any]:
        """Get value from local cache. Returns None if not found or expired."""
        ...

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set value in local cache with optional TTL."""
        ...

    def delete(self, key: str) -> None:
        """Delete key from local cache."""
        ...


class RemoteCache(Protocol):
    """Distributed cache interface (Redis-based)."""

    def get(self, key: str) -> Optional[Any]:
        """Get value from remote cache."""
        ...

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set value in remote cache with optional TTL."""
        ...

    def delete(self, key: str) -> None:
        """Delete key from remote cache."""
        ...

    def ping(self) -> bool:
        """Check if remote cache is available."""
        ...

    def publish_health_data(
        self, topic: str, health_data: dict[int, float], health_threshold: float = 0.5
    ) -> None:
        """Publish health data to distributed cache."""
        ...

    def get_health_data(self, topic: str) -> Optional[dict[int, float]]:
        """Retrieve health data from distributed cache."""
        ...


class HybridCache(Protocol):
    """Combined local + remote cache with read-through pattern."""

    def get(self, key: str) -> Optional[Any]:
        """
        Get value using read-through pattern: local -> remote -> None.
        Updates local on remote hit.
        """
        ...

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set value in both local and remote caches."""
        ...

    def delete(self, key: str) -> None:
        """Delete key from both local and remote caches."""
        ...

    def publish_health_data(
        self, topic: str, health_data: dict[int, float], health_threshold: float = 0.5
    ) -> None:
        """
        Publish health data to distributed cache for sharing across producer instances.

        Args:
            topic: Kafka topic name
            health_data: Dict mapping partition_id -> health_score (0.0-1.0)
            health_threshold: Minimum score to consider partition healthy
        """
        ...

    def get_health_data(self, topic: str) -> Optional[dict[int, float]]:
        """
        Retrieve health data from distributed cache.

        Args:
            topic: Kafka topic name

        Returns:
            Dict mapping partition_id -> health_score or None if not found
        """
        ...


@dataclass(frozen=True)
class CacheConfig:
    """Configuration for cache behavior."""

    # Local cache settings
    local_max_size: int = 1000
    local_default_ttl_seconds: float = 300.0

    # Remote cache settings
    remote_enabled: bool = False
    remote_default_ttl_seconds: float = 900.0

    # Redis connection settings
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_password: Optional[str] = None

    # Redis security settings
    redis_ssl_enabled: bool = False
    redis_ssl_cert_reqs: str = "required"
    redis_ssl_ca_certs: Optional[str] = None
    redis_ssl_certfile: Optional[str] = None
    redis_ssl_keyfile: Optional[str] = None

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.local_max_size <= 0:
            raise ValueError("local_max_size must be positive")
        if self.local_default_ttl_seconds <= 0:
            raise ValueError("local_default_ttl_seconds must be positive")
        if self.remote_default_ttl_seconds <= 0:
            raise ValueError("remote_default_ttl_seconds must be positive")
        if self.redis_port <= 0 or self.redis_port > 65535:
            raise ValueError("redis_port must be between 1 and 65535")
        if self.redis_db < 0:
            raise ValueError("redis_db must be non-negative")


class DefaultLocalCache:
    """O(1) LRU cache implementation with TTL support using cachetools.LRUCache."""

    def __init__(self, config: CacheConfig):
        self._config = config
        self._lock = threading.RLock()

        # Use cachetools.LRUCache as storage engine - eliminates manual node management
        self._cache: LRUCache[str, CacheEntry] = LRUCache(maxsize=config.local_max_size)

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            try:
                entry = self._cache[key]  # This handles LRU promotion automatically

                if entry.is_expired():
                    # Remove expired entry
                    del self._cache[key]
                    return None

                # Entry is valid and LRU promotion happened automatically
                return entry.value

            except KeyError:
                # Key not found
                return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        ttl = (
            ttl_seconds
            if ttl_seconds is not None
            else self._config.local_default_ttl_seconds
        )

        with self._lock:
            # Create cache entry with TTL
            entry = CacheEntry(value, ttl)

            # LRUCache handles eviction automatically when maxsize is reached
            self._cache[key] = entry

    def delete(self, key: str) -> None:
        with self._lock:
            try:
                del self._cache[key]
            except KeyError:
                # Key doesn't exist - silently ignore
                pass


class DefaultHybridCache:
    """Hybrid cache with local and remote cache - remote is required."""

    def __init__(
        self,
        local_cache: LocalCache,
        remote_cache: RemoteCache,  # Required, not Optional
        config: CacheConfig,
    ):
        if remote_cache is None:
            raise ValueError("Hybrid cache requires a remote cache instance")

        self._local = local_cache
        self._remote = remote_cache
        self._config = config

    def get(self, key: str) -> Optional[Any]:
        """Read-through cache lookup: local -> remote -> None."""
        # Try local first
        value = self._local.get(key)
        if value is not None:
            return value

        # Try remote - fail fast on errors
        try:
            value = self._remote.get(key)
            if value is not None:
                # Promote to local
                self._local.set(key, value, self._config.local_default_ttl_seconds)
                return value
        except Exception as e:
            logger.warning(f"Remote cache get failed for key {key}: {e}")
            # Let the exception bubble up - fail fast

        return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set in both local and remote - fail fast on remote errors."""
        # Always set in local first
        self._local.set(
            key, value, ttl_seconds or self._config.local_default_ttl_seconds
        )

        # Set in remote - fail fast on errors
        try:
            self._remote.set(
                key, value, ttl_seconds or self._config.remote_default_ttl_seconds
            )
        except Exception as e:
            logger.warning(f"Remote cache set failed for key {key}: {e}")
            # Let the exception bubble up - fail fast

    def delete(self, key: str) -> None:
        """Delete from both caches - fail fast on remote errors."""
        # Always delete from local
        self._local.delete(key)

        # Delete from remote - fail fast on errors
        try:
            self._remote.delete(key)
        except Exception as e:
            logger.warning(f"Remote cache delete failed for key {key}: {e}")
            # Let the exception bubble up - fail fast

    def publish_health_data(
        self, topic: str, health_data: dict[int, float], health_threshold: float = 0.5
    ) -> None:
        """Publish health data using the remote cache (no local caching)."""
        # Health data is only published to remote cache for distributed sharing
        # Local cache is not used for health data to avoid stale health information
        if hasattr(self._remote, "publish_health_data"):
            self._remote.publish_health_data(topic, health_data, health_threshold)
        else:
            logger.warning("Remote cache does not support health data publishing")

    def get_health_data(self, topic: str) -> Optional[dict[int, float]]:
        """Retrieve health data from remote cache only (no local caching)."""
        # Health data is only retrieved from remote cache to ensure freshness
        # Local cache is not used for health data to avoid stale health information
        if hasattr(self._remote, "get_health_data"):
            return self._remote.get_health_data(topic)
        else:
            logger.warning("Remote cache does not support health data retrieval")
            return None


class DefaultRemoteCache:
    """Redis-based remote cache implementation with sync Redis client."""

    def __init__(self, redis_client: Any, config: CacheConfig):
        """
        Initialize with a pre-configured sync Redis client.

        Args:
            redis_client: redis.Redis instance (sync client)
            config: Cache configuration
        """
        self._redis = redis_client
        self._config = config

    def _serialize_value(self, value: Any) -> str:
        """Serialize value with optimization for integer partition IDs."""
        if isinstance(value, int):
            return str(value)
        elif isinstance(value, (str, float)):
            return str(value)
        else:
            # Fallback to JSON for complex types
            import json

            return f"json:{json.dumps(value)}"

    def _deserialize_value(self, value: str) -> Any:
        """Deserialize value, handling integer optimization and JSON fallback."""
        if value.startswith("json:"):
            import json

            return json.loads(value[5:])
        try:
            return int(value)
        except ValueError:
            return value

    def get(self, key: str) -> Optional[Any]:
        """Get value from Redis with optimized deserialization."""
        if not self._redis:
            return None

        try:
            value = self._redis.get(key)
            if value is not None:
                return self._deserialize_value(value.decode("utf-8"))
            return None
        except Exception as e:
            logger.debug(f"Failed to get cache key {key}: {e}")
            return None

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set value in Redis with optimized serialization."""
        if not self._redis:
            return
        try:
            serialized_value = self._serialize_value(value)
            ttl = ttl_seconds or self._config.remote_default_ttl_seconds
            if ttl:
                self._redis.setex(key, int(ttl), serialized_value)
            else:
                self._redis.set(key, serialized_value)
        except Exception as e:
            logger.debug(f"Failed to set cache key {key}: {e}")

    def delete(self, key: str) -> None:
        """Delete key from Redis."""
        if not self._redis:
            return
        try:
            self._redis.delete(key)
        except Exception as e:
            logger.debug(f"Failed to delete cache key {key}: {e}")

    def ping(self) -> bool:
        """Check if Redis is available."""
        if not self._redis:
            return False

        try:
            self._redis.ping()
            return True
        except Exception:
            return False

    def publish_health_data(
        self, topic: str, health_data: dict[int, float], health_threshold: float = 0.5
    ) -> None:
        """Publish health data to Redis with structured keys for easy retrieval."""
        if not self._redis:
            return

        try:
            import json
            import time

            current_time = time.time()

            # Store complete health data with metadata
            health_payload = {
                "topic": topic,
                "partitions": health_data,
                "timestamp": current_time,
                "healthy_count": sum(
                    1 for score in health_data.values() if score >= health_threshold
                ),
                "total_count": len(health_data),
                "health_threshold": health_threshold,
            }

            # Store complete health state
            state_key = f"kafka_health:state:{topic}"
            self._redis.setex(state_key, 300, json.dumps(health_payload))  # 5 min TTL

            # Store healthy partitions list for quick access
            healthy_partitions = [
                partition_id
                for partition_id, score in health_data.items()
                if score >= health_threshold
            ]
            healthy_key = f"kafka_health:healthy:{topic}"
            self._redis.setex(
                healthy_key, 300, json.dumps(healthy_partitions)
            )  # 5 min TTL

            logger.debug(
                f"Published health data for {topic}: "
                f"{len(healthy_partitions)}/{len(health_data)} healthy partitions"
            )

        except Exception as e:
            logger.warning(f"Failed to publish health data for {topic}: {e}")

    def get_health_data(self, topic: str) -> Optional[dict[int, float]]:
        """Retrieve health data from Redis."""
        if not self._redis:
            return None

        try:
            import json

            # Get complete health state
            state_key = f"kafka_health:state:{topic}"
            state_data = self._redis.get(state_key)

            if state_data:
                # Handle both string (decode_responses=True) and bytes data
                if isinstance(state_data, bytes):
                    health_payload = json.loads(state_data.decode("utf-8"))
                else:
                    health_payload = json.loads(state_data)
                partitions_data = health_payload.get("partitions", {})

                # Convert string keys back to integers
                return {
                    int(partition_id): score
                    for partition_id, score in partitions_data.items()
                }

            return None

        except Exception as e:
            logger.debug(f"Failed to get health data for {topic}: {e}")
            return None


class CacheUnavailableError(CacheError):
    """Raised when cache backend is unavailable."""


class CacheTimeoutError(CacheError):
    """Raised when cache operation times out."""
