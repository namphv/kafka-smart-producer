"""
Hybrid caching system for Kafka Smart Producer.

This module provides local and remote cache implementations
with read-through patterns for partition health data caching.
"""

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, Protocol

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

    def clear(self) -> None:
        """Clear all entries from local cache."""
        ...

    def size(self) -> int:
        """Get current number of entries in cache."""
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

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._cache)

    # Removed manual LRU node management methods - cachetools.LRUCache handles this

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

    def clear(self) -> None:
        """Clear both local and remote caches safely."""
        # Clear local cache
        self._local.clear()

        # Clear remote cache with app-specific pattern
        if self._is_remote_enabled() and self._remote is not None:
            try:
                # Use app-specific prefix to avoid clearing other apps' data
                app_pattern = "kafka_smart_producer:*"
                if hasattr(self._remote, "delete_pattern_sync"):
                    self._remote.delete_pattern_sync(app_pattern)
                else:
                    # Fallback for older implementations
                    self._remote.delete_sync(app_pattern)
            except Exception as e:
                logger.warning(f"Failed to clear remote cache: {e}")

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
    """Redis-based remote cache implementation with async support and serialization."""

    def __init__(self, redis_client: Any):
        """
        Initialize with a pre-configured Redis client.

        Args:
            redis_client: Either redis.asyncio.Redis or redis.Redis instance
        """
        self._redis = redis_client

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

    async def get(self, key: str) -> Optional[Any]:
        """Get value from Redis asynchronously with optimized deserialization."""
        if not self._redis:
            return None

        try:
            value = await self._redis.get(key)
            if value is not None:
                return self._deserialize_value(value)
            else:
                return None
        except Exception:
            return None

    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous version of get() - delegates to async method."""
        try:
            return asyncio.run(self.get(key))
        except RuntimeError:
            # Fallback for when already in async context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self.get(key))
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in Redis asynchronously with optimized serialization."""
        if not self._redis:
            return

        try:
            serialized_value = self._serialize_value(value)

            if ttl_seconds:
                await self._redis.setex(key, int(ttl_seconds), serialized_value)
            else:
                await self._redis.set(key, serialized_value)
        except Exception as e:
            logger.debug(f"Failed to set cache key {key}: {e}")

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Synchronous version of set() - delegates to async method."""
        try:
            asyncio.run(self.set(key, value, ttl_seconds))
        except RuntimeError:
            # Fallback for when already in async context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.set(key, value, ttl_seconds))
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    async def delete(self, key: str) -> None:
        """Delete key from Redis asynchronously."""
        if not self._redis:
            return

        try:
            await self._redis.delete(key)
        except Exception as e:
            logger.debug(f"Failed to delete cache key {key}: {e}")

    def delete_sync(self, key: str) -> None:
        """Synchronous version of delete() - delegates to async method."""
        try:
            asyncio.run(self.delete(key))
        except RuntimeError:
            # Fallback for when already in async context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.delete(key))
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    async def ping(self) -> bool:
        """Check if Redis is available asynchronously."""
        if not self._redis:
            return False

        try:
            await self._redis.ping()
            return True
        except Exception:
            return False

    def ping_sync(self) -> bool:
        """Synchronous version of ping() - delegates to async method."""
        try:
            return asyncio.run(self.ping())
        except RuntimeError:
            # Fallback for when already in async context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(self.ping())
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    # Compatibility methods for unified cache interface
    def set_with_ordered_ttl(self, key: str, value: Any) -> None:
        """Set value with TTL for ordered messages."""
        # Use a longer TTL for ordered messages (1 hour)
        self.set_sync(key, value, 3600.0)

    async def delete_pattern(self, pattern: str) -> None:
        """Delete keys matching pattern from Redis asynchronously."""
        if not self._redis:
            return

        try:
            deleted_count = 0
            async for key in self._redis.scan_iter(match=pattern, count=100):
                await self._redis.delete(key)
                deleted_count += 1
            logger.debug(f"Deleted {deleted_count} keys matching pattern: {pattern}")
        except Exception as e:
            logger.debug(f"Failed to delete cache keys matching pattern {pattern}: {e}")

    def delete_pattern_sync(self, pattern: str) -> None:
        """Delete keys matching pattern from Redis synchronously."""
        try:
            asyncio.run(self.delete_pattern(pattern))
        except RuntimeError:
            # Fallback for when already in async context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.delete_pattern(pattern))
            finally:
                loop.close()
                asyncio.set_event_loop(None)

    def is_remote_available(self) -> bool:
        """Check if remote cache is available."""
        return True  # Remote cache is always available by design

    def clear(self) -> None:
        """Clear all cache entries safely."""
        # Use pattern-based deletion for app-specific clearing
        self.delete_pattern_sync("kafka_smart_producer:*")


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
        )
        return DefaultLocalCache(cache_config)

    @staticmethod
    def create_remote_cache(
        smart_config: Dict[str, Any],
    ) -> Optional["DefaultRemoteCache"]:
        """Create standalone remote cache instance if configuration is valid."""
        try:
            # Import redis.asyncio here to avoid import errors if redis is not available
            import redis.asyncio as redis

            # Build Redis connection parameters
            redis_kwargs = {
                "host": smart_config["redis_host"],
                "port": smart_config["redis_port"],
                "db": smart_config["redis_db"],
            }

            # Add optional parameters
            if smart_config.get("redis_password"):
                redis_kwargs["password"] = smart_config["redis_password"]

            # Handle SSL configuration
            if smart_config.get("redis_ssl_enabled", False):
                redis_kwargs["ssl"] = True
                redis_kwargs["ssl_cert_reqs"] = smart_config.get(
                    "redis_ssl_cert_reqs", "required"
                )

                if smart_config.get("redis_ssl_ca_certs"):
                    redis_kwargs["ssl_ca_certs"] = smart_config["redis_ssl_ca_certs"]
                if smart_config.get("redis_ssl_certfile"):
                    redis_kwargs["ssl_certfile"] = smart_config["redis_ssl_certfile"]
                if smart_config.get("redis_ssl_keyfile"):
                    redis_kwargs["ssl_keyfile"] = smart_config["redis_ssl_keyfile"]

            # Create async Redis client
            redis_client = redis.Redis(**redis_kwargs)

            # Test connection by attempting ping
            try:
                # For connection testing, we need to use sync ping
                import redis as sync_redis

                # Create a temp sync client for testing
                sync_kwargs = redis_kwargs.copy()
                if "ssl" in sync_kwargs:
                    sync_kwargs.pop("ssl")
                    if smart_config.get("redis_ssl_enabled", False):
                        sync_kwargs["ssl"] = True

                test_client = sync_redis.Redis(**sync_kwargs)
                test_client.ping()
                test_client.close()

            except Exception as e:
                logger.warning(f"Redis connection test failed: {e}")
                return None

            # Create cache instance
            remote_cache_instance = DefaultRemoteCache(redis_client)

            ssl_status = (
                "with SSL" if smart_config.get("redis_ssl_enabled") else "without SSL"
            )
            logger.info(
                f"Redis remote cache created: "
                f"{smart_config['redis_host']}:{smart_config['redis_port']} "
                f"({ssl_status})"
            )
            return remote_cache_instance

        except ImportError:
            logger.error("Redis package not available. Install with: pip install redis")
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
