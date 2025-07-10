"""
Hybrid caching system for Kafka Smart Producer.

This module provides local and remote cache implementations
with read-through patterns for partition health data caching.
"""

import fnmatch
import logging
import os
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Protocol

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
        return (time.time() - self.created_at) > self.ttl_seconds


class _LRUNode:
    """Doubly-linked list node for O(1) LRU operations."""

    def __init__(self, key: str, entry: CacheEntry):
        self.key = key
        self.entry = entry
        self.prev: Optional[_LRUNode] = None
        self.next: Optional[_LRUNode] = None


class CacheLevel(Enum):
    """Cache levels in the hierarchy."""

    LOCAL = "local"
    REMOTE = "remote"


class CacheStats:
    """Thread-safe statistics for cache performance monitoring."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.hits_local = 0
        self.hits_remote = 0
        self.misses = 0
        self.sets = 0
        self.deletes = 0
        self.evictions = 0
        self.errors = 0
        self.total_latency_ms = 0.0
        self.last_reset = time.time()

    def hit_local(self, latency_ms: float) -> None:
        with self._lock:
            self.hits_local += 1
            self.total_latency_ms += latency_ms

    def hit_remote(self, latency_ms: float) -> None:
        with self._lock:
            self.hits_remote += 1
            self.total_latency_ms += latency_ms

    def miss(self, latency_ms: float) -> None:
        with self._lock:
            self.misses += 1
            self.total_latency_ms += latency_ms

    def set_operation(self) -> None:
        with self._lock:
            self.sets += 1

    def delete_operation(self) -> None:
        with self._lock:
            self.deletes += 1

    def eviction(self) -> None:
        with self._lock:
            self.evictions += 1

    def error(self) -> None:
        with self._lock:
            self.errors += 1

    def get_hit_ratio(self) -> float:
        with self._lock:
            total_ops = self.hits_local + self.hits_remote + self.misses
            if total_ops == 0:
                return 0.0
            return (self.hits_local + self.hits_remote) / total_ops

    def get_avg_latency_ms(self) -> float:
        with self._lock:
            total_ops = self.hits_local + self.hits_remote + self.misses
            if total_ops == 0:
                return 0.0
            return self.total_latency_ms / total_ops

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self.hits_local = 0
            self.hits_remote = 0
            self.misses = 0
            self.sets = 0
            self.deletes = 0
            self.evictions = 0
            self.errors = 0
            self.total_latency_ms = 0.0
            self.last_reset = time.time()


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

    def clear(self) -> None:
        """Clear all entries from local cache."""
        ...

    def size(self) -> int:
        """Get current number of entries in cache."""
        ...

    def get_stats(self) -> CacheStats:
        """Get cache performance statistics."""
        ...


class RemoteCache(Protocol):
    """Distributed cache interface (Redis-based)."""

    async def get(self, key: str) -> Optional[Any]:
        """Get value from remote cache asynchronously."""
        ...

    def get_sync(self, key: str) -> Optional[Any]:
        """Get value from remote cache synchronously."""
        ...

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in remote cache with optional TTL."""
        ...

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in remote cache synchronously."""
        ...

    async def delete(self, key: str) -> None:
        """Delete key from remote cache."""
        ...

    def delete_sync(self, key: str) -> None:
        """Delete key from remote cache synchronously."""
        ...

    async def ping(self) -> bool:
        """Check if remote cache is available."""
        ...

    def ping_sync(self) -> bool:
        """Check if remote cache is available synchronously."""
        ...

    def get_stats(self) -> CacheStats:
        """Get cache performance statistics."""
        ...


class HybridCache(Protocol):
    """Combined local + remote cache with read-through pattern."""

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value using read-through pattern: local -> remote -> None.
        Updates local on remote hit.
        """
        ...

    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous version of get() for sync contexts."""
        ...

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in both local and remote caches."""
        ...

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Synchronous version of set()."""
        ...

    async def delete(self, key: str) -> None:
        """Delete key from both local and remote caches."""
        ...

    def delete_sync(self, key: str) -> None:
        """Synchronous version of delete()."""
        ...

    def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate cache entries matching pattern (e.g., 'topic:*')."""
        ...

    def get_combined_stats(self) -> Dict[str, CacheStats]:
        """Get statistics for both cache levels."""
        ...

    def is_remote_available(self) -> bool:
        """Check if remote cache is currently available."""
        ...


@dataclass
class CacheConfig:
    """Configuration for cache behavior."""

    # Message-type specific TTLs
    ordered_message_ttl: float = (
        3600.0  # 1 hour for consistency (only for ordered messages)
    )

    # Local cache settings
    local_max_size: int = 1000
    local_default_ttl_seconds: float = 300.0

    # Remote cache settings
    remote_enabled: bool = True
    remote_default_ttl_seconds: float = 900.0

    # Redis security settings (optional)
    redis_ssl_enabled: bool = False
    redis_ssl_cert_reqs: str = "required"
    redis_ssl_ca_certs: Optional[str] = None
    redis_ssl_certfile: Optional[str] = None
    redis_ssl_keyfile: Optional[str] = None

    # Monitoring
    stats_collection_enabled: bool = True

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.local_max_size <= 0:
            raise ValueError("local_max_size must be positive")
        if self.local_default_ttl_seconds <= 0:
            raise ValueError("local_default_ttl_seconds must be positive")
        if self.remote_default_ttl_seconds <= 0:
            raise ValueError("remote_default_ttl_seconds must be positive")
        if self.ordered_message_ttl <= 0:
            raise ValueError("ordered_message_ttl must be positive")


class DefaultLocalCache:
    """O(1) LRU cache implementation with TTL support using doubly-linked list."""

    def __init__(self, config: CacheConfig):
        self._config = config
        self._data: Dict[str, _LRUNode] = {}
        self._lock = threading.RLock()
        self._stats = CacheStats()

        # Create sentinel nodes for O(1) operations
        self._head = _LRUNode("", CacheEntry(None))
        self._tail = _LRUNode("", CacheEntry(None))
        self._head.next = self._tail
        self._tail.prev = self._head

    def get(self, key: str) -> Optional[Any]:
        start_time = time.time()

        with self._lock:
            node = self._data.get(key)
            if node is None:
                latency_ms = (time.time() - start_time) * 1000
                if self._config.stats_collection_enabled:
                    self._stats.miss(latency_ms)
                return None

            if node.entry.is_expired():
                # Remove expired entry
                self._remove_node(node)
                latency_ms = (time.time() - start_time) * 1000
                if self._config.stats_collection_enabled:
                    self._stats.miss(latency_ms)
                return None

            # Move to head (most recently used) - O(1) operation
            self._move_to_head(node)

            latency_ms = (time.time() - start_time) * 1000
            if self._config.stats_collection_enabled:
                self._stats.hit_local(latency_ms)
            return node.entry.value

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        ttl = ttl_seconds or self._config.local_default_ttl_seconds

        with self._lock:
            existing_node = self._data.get(key)
            if existing_node:
                # Update existing entry and move to head
                existing_node.entry = CacheEntry(value, ttl)
                self._move_to_head(existing_node)
            else:
                # Create new entry
                entry = CacheEntry(value, ttl)
                new_node = _LRUNode(key, entry)
                self._data[key] = new_node
                self._add_to_head(new_node)

                # Evict if over capacity
                if len(self._data) > self._config.local_max_size:
                    self._evict_tail()

            if self._config.stats_collection_enabled:
                self._stats.set_operation()

    def delete(self, key: str) -> None:
        with self._lock:
            node = self._data.get(key)
            if node:
                self._remove_node(node)
                if self._config.stats_collection_enabled:
                    self._stats.delete_operation()

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._head.next = self._tail
            self._tail.prev = self._head

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def get_stats(self) -> CacheStats:
        return self._stats

    def _add_to_head(self, node: _LRUNode) -> None:
        """Add node right after head. Must hold lock."""
        node.prev = self._head
        node.next = self._head.next
        if self._head.next is not None:
            self._head.next.prev = node
        self._head.next = node

    def _remove_node(self, node: _LRUNode) -> None:
        """Remove node from linked list and hash map. Must hold lock."""
        # Remove from linked list - O(1)
        if node.prev is not None:
            node.prev.next = node.next
        if node.next is not None:
            node.next.prev = node.prev

        # Remove from hash map
        self._data.pop(node.key, None)

    def _move_to_head(self, node: _LRUNode) -> None:
        """Move existing node to head (most recent). Must hold lock."""
        # Remove from current position
        if node.prev is not None:
            node.prev.next = node.next
        if node.next is not None:
            node.next.prev = node.prev

        # Add to head
        self._add_to_head(node)

    def _evict_tail(self) -> None:
        """Remove least recently used node (tail). Must hold lock."""
        lru_node = self._tail.prev
        if lru_node is not None and lru_node != self._head:  # Don't remove sentinel
            self._remove_node(lru_node)
            if self._config.stats_collection_enabled:
                self._stats.eviction()

    def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate entries matching pattern."""
        with self._lock:
            keys_to_delete = [
                key for key in self._data.keys() if fnmatch.fnmatch(key, pattern)
            ]
            for key in keys_to_delete:
                node = self._data.get(key)
                if node:
                    self._remove_node(node)

    # Compatibility methods for unified cache interface
    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous get (same as get for local cache)."""
        return self.get(key)

    def delete_sync(self, key: str) -> None:
        """Synchronous delete (same as delete for local cache)."""
        self.delete(key)

    def set_with_ordered_ttl(self, key: str, value: Any) -> None:
        """Set value with TTL for ordered messages."""
        # Use default TTL for local cache
        self.set(key, value, self._config.local_default_ttl_seconds)

    def is_remote_available(self) -> bool:
        """Check if remote cache is available (always False for local cache)."""
        return False

    def get_combined_stats(self) -> Dict[str, CacheStats]:
        """Get statistics (local cache only)."""
        return {"local": self._stats}


class DefaultHybridCache:
    """Enhanced hybrid cache with local and remote cache support."""

    def __init__(
        self,
        local_cache: LocalCache,
        remote_cache: Optional[RemoteCache],
        config: CacheConfig,
    ):
        self._local = local_cache
        self._remote = remote_cache
        self._config = config
        self._remote_available = True
        self._last_remote_check = 0.0

    async def get(self, key: str) -> Optional[Any]:
        """Read-through cache lookup: local -> remote -> None."""
        # Try local first
        value = self._local.get(key)
        if value is not None:
            return value

        # Try remote if available
        if self._is_remote_enabled() and self._remote is not None:
            try:
                value = await self._remote.get(key)
                if value is not None:
                    # Promote to local
                    self._local.set(key, value, self._config.local_default_ttl_seconds)
                    return value
            except Exception:
                self._mark_remote_unavailable()
                # Continue to return None

        return None

    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous version of get()."""
        # Try local first
        value = self._local.get(key)
        if value is not None:
            return value

        # Try remote if available
        if self._is_remote_enabled() and self._remote is not None:
            try:
                value = self._remote.get_sync(key)
                if value is not None:
                    # Promote to local
                    self._local.set(key, value, self._config.local_default_ttl_seconds)
                    return value
            except Exception:
                self._mark_remote_unavailable()

        return None

    def set_with_ordered_ttl(self, key: str, value: Any) -> None:
        """Set value with TTL for ordered messages (key stickiness)."""
        ttl = self._config.ordered_message_ttl
        self.set_sync(key, value, ttl)

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set in both local and remote."""
        # Always set in local
        self._local.set(
            key, value, ttl_seconds or self._config.local_default_ttl_seconds
        )

        # Set in remote if available
        if self._is_remote_enabled() and self._remote is not None:
            try:
                await self._remote.set(
                    key, value, ttl_seconds or self._config.remote_default_ttl_seconds
                )
            except Exception:
                self._mark_remote_unavailable()
                # Local set still succeeded

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Synchronous version of set()."""
        # Always set in local
        self._local.set(
            key, value, ttl_seconds or self._config.local_default_ttl_seconds
        )

        # Set in remote if available
        if self._is_remote_enabled() and self._remote is not None:
            try:
                self._remote.set_sync(
                    key, value, ttl_seconds or self._config.remote_default_ttl_seconds
                )
            except Exception:
                self._mark_remote_unavailable()

    async def delete(self, key: str) -> None:
        """Delete from both caches."""
        self._local.delete(key)

        if self._is_remote_enabled() and self._remote is not None:
            try:
                await self._remote.delete(key)
            except Exception:
                self._mark_remote_unavailable()

    def delete_sync(self, key: str) -> None:
        """Synchronous version of delete()."""
        self._local.delete(key)

        if self._is_remote_enabled() and self._remote is not None:
            try:
                self._remote.delete_sync(key)
            except Exception:
                self._mark_remote_unavailable()

    def clear_pattern(self, pattern: str) -> None:
        """Safely clear cache entries matching pattern."""
        # Clear from local cache
        if hasattr(self._local, "invalidate_pattern"):
            self._local.invalidate_pattern(pattern)

        # Clear from remote cache safely
        if self._is_remote_enabled() and self._remote is not None:
            try:
                self._remote.delete_sync(pattern)
            except Exception as e:
                logger.warning(f"Failed to clear remote cache pattern {pattern}: {e}")

    def clear(self) -> None:
        """Clear both local and remote caches safely."""
        # Clear local cache
        self._local.clear()

        # Clear remote cache with app-specific pattern
        if self._is_remote_enabled() and self._remote is not None:
            try:
                # Use app-specific prefix to avoid clearing other apps' data
                app_pattern = "kafka_smart_producer:*"
                self._remote.delete_sync(app_pattern)
            except Exception as e:
                logger.warning(f"Failed to clear remote cache: {e}")

    def get_combined_stats(self) -> Dict[str, CacheStats]:
        """Get statistics from both cache levels."""
        stats = {"local": self._local.get_stats()}
        if self._remote:
            stats["remote"] = self._remote.get_stats()
        return stats

    def is_remote_available(self) -> bool:
        """Check current remote availability."""
        return self._remote_available and self._remote is not None

    def _is_remote_enabled(self) -> bool:
        """Check if remote should be used."""
        if not self._config.remote_enabled or self._remote is None:
            return False

        # Periodically check remote health if marked unavailable
        now = time.time()
        if not self._remote_available and (now - self._last_remote_check) > 30.0:
            self._check_remote_health()

        return self._remote_available

    def _mark_remote_unavailable(self) -> None:
        """Mark remote as unavailable due to error."""
        self._remote_available = False
        self._last_remote_check = time.time()

    def _check_remote_health(self) -> None:
        """Check if remote has recovered."""
        try:
            if self._remote is not None and self._remote.ping_sync():
                self._remote_available = True
        except Exception as e:
            logger.debug(f"Remote health check failed: {e}")  # Remain unavailable

        self._last_remote_check = time.time()


class DefaultRemoteCache:
    """Redis-based remote cache implementation with optional security features."""

    def __init__(self, redis_config: Dict[str, Any]):
        self._redis_config = redis_config
        self._redis = None
        self._stats = CacheStats()

        # Try to initialize Redis connection
        try:
            import redis

            # Build basic connection config
            connection_config = {
                "host": redis_config.get("host", "localhost"),
                "port": redis_config.get("port", 6379),
                "db": redis_config.get("db", 0),
                "decode_responses": True,
                "socket_connect_timeout": 2.0,
                "socket_timeout": 2.0,
            }

            # Add password if provided (from config or environment)
            password = redis_config.get("password") or os.getenv("REDIS_PASSWORD")
            if password:
                connection_config["password"] = password

            # Add SSL/TLS configuration if enabled
            if redis_config.get("ssl_enabled", False):
                connection_config.update(
                    {
                        "ssl": True,
                        "ssl_cert_reqs": redis_config.get("ssl_cert_reqs", "required"),
                        "ssl_ca_certs": redis_config.get("ssl_ca_certs"),
                        "ssl_certfile": redis_config.get("ssl_certfile"),
                        "ssl_keyfile": redis_config.get("ssl_keyfile"),
                    }
                )
                logger.info("Redis SSL/TLS enabled")

            self._redis = redis.Redis(**connection_config)
            # Test connection
            self._redis.ping()

        except Exception as e:
            logger.warning(f"Redis connection failed: {e}")
            self._redis = None

    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis with optimized deserialization."""
        if not self._redis:
            return None

        try:
            start_time = time.time()
            value = self._redis.get(key)
            latency_ms = (time.time() - start_time) * 1000

            if value is not None:
                self._stats.hit_remote(latency_ms)

                # Optimize for integer partition IDs
                if value.startswith("json:"):
                    import json

                    return json.loads(value[5:])
                else:
                    try:
                        return int(value)
                    except ValueError:
                        return value
            else:
                self._stats.miss(latency_ms)
                return None
        except Exception:
            self._stats.error()
            return None

    def get_sync(self, key: str) -> Optional[Any]:
        """Get value from Redis with optimized deserialization."""
        if not self._redis:
            return None

        try:
            start_time = time.time()
            value = self._redis.get(key)
            latency_ms = (time.time() - start_time) * 1000

            if value is not None:
                self._stats.hit_remote(latency_ms)

                # Optimize for integer partition IDs
                if value.startswith("json:"):
                    import json

                    return json.loads(value[5:])
                else:
                    try:
                        return int(value)
                    except ValueError:
                        return value
            else:
                self._stats.miss(latency_ms)
                return None
        except Exception:
            self._stats.error()
            return None

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in Redis with optimized serialization."""
        if not self._redis:
            return

        try:
            # Optimize for integer partition IDs (most common case)
            if isinstance(value, int):
                serialized_value = str(value)
            elif isinstance(value, (str, float)):
                serialized_value = str(value)
            else:
                # Fallback to JSON for complex types
                import json

                serialized_value = f"json:{json.dumps(value)}"

            if ttl_seconds:
                self._redis.setex(key, int(ttl_seconds), serialized_value)
            else:
                self._redis.set(key, serialized_value)

            self._stats.set_operation()
        except Exception:
            self._stats.error()

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in Redis with optimized serialization."""
        if not self._redis:
            return

        try:
            # Optimize for integer partition IDs (most common case)
            if isinstance(value, int):
                serialized_value = str(value)
            elif isinstance(value, (str, float)):
                serialized_value = str(value)
            else:
                # Fallback to JSON for complex types
                import json

                serialized_value = f"json:{json.dumps(value)}"

            if ttl_seconds:
                self._redis.setex(key, int(ttl_seconds), serialized_value)
            else:
                self._redis.set(key, serialized_value)

            self._stats.set_operation()
        except Exception:
            self._stats.error()

    async def delete(self, key: str) -> None:
        """Delete key from Redis asynchronously."""
        if not self._redis:
            return

        try:
            self._redis.delete(key)
            self._stats.delete_operation()
        except Exception:
            self._stats.error()

    def delete_sync(self, key: str) -> None:
        """Delete key or pattern from Redis safely."""
        if not self._redis:
            return

        try:
            if "*" in key or "?" in key:
                # Use SCAN for safe pattern-based deletion
                deleted_count = 0
                for k in self._redis.scan_iter(match=key, count=100):
                    self._redis.delete(k)
                    deleted_count += 1
                logger.debug(f"Deleted {deleted_count} keys matching pattern: {key}")
            else:
                self._redis.delete(key)

            self._stats.delete_operation()
        except Exception:
            self._stats.error()

    async def ping(self) -> bool:
        """Check if Redis is available asynchronously."""
        if not self._redis:
            return False

        try:
            self._redis.ping()
            return True
        except Exception:
            return False

    def ping_sync(self) -> bool:
        """Check if Redis is available synchronously."""
        if not self._redis:
            return False

        try:
            self._redis.ping()
            return True
        except Exception:
            return False

    def get_stats(self) -> CacheStats:
        """Get cache performance statistics."""
        return self._stats

    # Compatibility methods for unified cache interface
    def set_with_ordered_ttl(self, key: str, value: Any) -> None:
        """Set value with TTL for ordered messages."""
        # Use a longer TTL for ordered messages (1 hour)
        self.set_sync(key, value, 3600.0)

    def is_remote_available(self) -> bool:
        """Check if remote cache is available."""
        return self._redis is not None

    def clear(self) -> None:
        """Clear all cache entries safely."""
        if not self._redis:
            return
        try:
            # Use app-specific pattern to avoid clearing other apps' data
            for key in self._redis.scan_iter(match="kafka_smart_producer:*", count=100):
                self._redis.delete(key)
        except Exception as e:
            # Ignore errors during clear but log for debugging
            logger.debug(f"Error clearing Redis cache: {e}")

    def get_combined_stats(self) -> Dict[str, CacheStats]:
        """Get statistics (remote cache only)."""
        return {"remote": self._stats}


class CacheUnavailableError(CacheError):
    """Raised when cache backend is unavailable."""


class CacheTimeoutError(CacheError):
    """Raised when cache operation times out."""


class CacheFactory:
    """Factory for creating configured cache instances."""

    @staticmethod
    def create_local_cache(smart_config: Dict[str, Any]) -> "DefaultLocalCache":
        """Create standalone local cache instance."""
        cache_config = CacheConfig(
            local_max_size=smart_config.get("cache_max_size", 1000),
            local_default_ttl_seconds=smart_config.get("cache_ttl_ms", 300000) / 1000.0,
            ordered_message_ttl=smart_config.get("ordered_message_ttl", 3600.0),
            stats_collection_enabled=True,
        )
        return DefaultLocalCache(cache_config)

    @staticmethod
    def create_remote_cache(
        smart_config: Dict[str, Any],
    ) -> Optional["DefaultRemoteCache"]:
        """Create standalone remote cache instance if configuration is valid."""
        try:
            redis_config = {
                "host": smart_config["redis_host"],
                "port": smart_config["redis_port"],
                "db": smart_config["redis_db"],
                "password": smart_config.get("redis_password"),
                "ssl_enabled": smart_config.get("redis_ssl_enabled", False),
                "ssl_cert_reqs": smart_config.get("redis_ssl_cert_reqs", "required"),
                "ssl_ca_certs": smart_config.get("redis_ssl_ca_certs"),
                "ssl_certfile": smart_config.get("redis_ssl_certfile"),
                "ssl_keyfile": smart_config.get("redis_ssl_keyfile"),
            }

            remote_cache_instance = DefaultRemoteCache(redis_config)
            if remote_cache_instance._redis is not None:
                ssl_status = (
                    "with SSL"
                    if smart_config.get("redis_ssl_enabled")
                    else "without SSL"
                )
                logger.info(
                    f"Redis remote cache created: "
                    f"{smart_config['redis_host']}:{smart_config['redis_port']} "
                    f"({ssl_status})"
                )
                return remote_cache_instance
            else:
                logger.warning("Redis connection failed, returning None")
                return None
        except KeyError as e:
            logger.error(f"Missing required Redis configuration key: {e}")
            return None
        except Exception as e:
            logger.error(f"Failed to initialize remote cache: {e}")
            return None

    @staticmethod
    def create_hybrid_cache(
        smart_config: Dict[str, Any], enable_redis: bool = False
    ) -> "DefaultHybridCache":
        """Create secure hybrid cache with proper SSL/TLS support."""
        cache_config = CacheConfig(
            local_max_size=smart_config["cache_max_size"],
            local_default_ttl_seconds=smart_config["cache_ttl_ms"] / 1000.0,
            remote_default_ttl_seconds=smart_config["redis_ttl_seconds"],
            ordered_message_ttl=smart_config["ordered_message_ttl"],
            stats_collection_enabled=True,
        )

        # Create local cache using factory method
        local_cache = CacheFactory.create_local_cache(smart_config)

        # Create remote cache using factory method if enabled
        remote_cache = None
        if enable_redis:
            remote_cache = CacheFactory.create_remote_cache(smart_config)
            if remote_cache is None:
                logger.warning(
                    "Redis connection failed, falling back to local cache only"
                )

        return DefaultHybridCache(local_cache, remote_cache, cache_config)
