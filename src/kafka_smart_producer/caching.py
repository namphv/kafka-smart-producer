"""
Hybrid caching system for Kafka Smart Producer.

This module provides L1 (local) and L2 (distributed) cache implementations
with read-through patterns for partition health data caching.
"""

import fnmatch
import logging
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

    L1_LOCAL = "l1_local"
    L2_DISTRIBUTED = "l2_distributed"


class CacheStats:
    """Thread-safe statistics for cache performance monitoring."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.hits_l1 = 0
        self.hits_l2 = 0
        self.misses = 0
        self.sets = 0
        self.deletes = 0
        self.evictions = 0
        self.errors = 0
        self.total_latency_ms = 0.0
        self.last_reset = time.time()

    def hit_l1(self, latency_ms: float) -> None:
        with self._lock:
            self.hits_l1 += 1
            self.total_latency_ms += latency_ms

    def hit_l2(self, latency_ms: float) -> None:
        with self._lock:
            self.hits_l2 += 1
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
            total_ops = self.hits_l1 + self.hits_l2 + self.misses
            if total_ops == 0:
                return 0.0
            return (self.hits_l1 + self.hits_l2) / total_ops

    def get_avg_latency_ms(self) -> float:
        with self._lock:
            total_ops = self.hits_l1 + self.hits_l2 + self.misses
            if total_ops == 0:
                return 0.0
            return self.total_latency_ms / total_ops

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self.hits_l1 = 0
            self.hits_l2 = 0
            self.misses = 0
            self.sets = 0
            self.deletes = 0
            self.evictions = 0
            self.errors = 0
            self.total_latency_ms = 0.0
            self.last_reset = time.time()


class L1Cache(Protocol):
    """Local in-memory cache interface (LRU with TTL)."""

    def get(self, key: str) -> Optional[Any]:
        """Get value from L1 cache. Returns None if not found or expired."""
        ...

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set value in L1 cache with optional TTL."""
        ...

    def delete(self, key: str) -> None:
        """Delete key from L1 cache."""
        ...

    def clear(self) -> None:
        """Clear all entries from L1 cache."""
        ...

    def size(self) -> int:
        """Get current number of entries in cache."""
        ...

    def get_stats(self) -> CacheStats:
        """Get cache performance statistics."""
        ...


class L2Cache(Protocol):
    """Distributed cache interface (Redis-based)."""

    async def get(self, key: str) -> Optional[Any]:
        """Get value from L2 cache asynchronously."""
        ...

    def get_sync(self, key: str) -> Optional[Any]:
        """Get value from L2 cache synchronously."""
        ...

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in L2 cache with optional TTL."""
        ...

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in L2 cache synchronously."""
        ...

    async def delete(self, key: str) -> None:
        """Delete key from L2 cache."""
        ...

    def delete_sync(self, key: str) -> None:
        """Delete key from L2 cache synchronously."""
        ...

    async def ping(self) -> bool:
        """Check if L2 cache is available."""
        ...

    def ping_sync(self) -> bool:
        """Check if L2 cache is available synchronously."""
        ...

    def get_stats(self) -> CacheStats:
        """Get cache performance statistics."""
        ...


class HybridCache(Protocol):
    """Combined L1 + L2 cache with read-through pattern."""

    async def get(self, key: str) -> Optional[Any]:
        """
        Get value using read-through pattern: L1 -> L2 -> None.
        Updates L1 on L2 hit.
        """
        ...

    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous version of get() for sync contexts."""
        ...

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set value in both L1 and L2 caches."""
        ...

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Synchronous version of set()."""
        ...

    async def delete(self, key: str) -> None:
        """Delete key from both L1 and L2 caches."""
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

    def is_l2_available(self) -> bool:
        """Check if L2 cache is currently available."""
        ...


@dataclass
class CacheConfig:
    """Configuration for cache behavior."""

    # L1 Cache settings
    l1_max_size: int = 1000
    l1_default_ttl_seconds: float = 300.0  # 5 minutes

    # L2 Cache settings
    l2_enabled: bool = True
    l2_default_ttl_seconds: float = 900.0  # 15 minutes

    # Monitoring
    stats_collection_enabled: bool = True

    def __post_init__(self) -> None:
        """Validate configuration parameters."""
        if self.l1_max_size <= 0:
            raise ValueError("l1_max_size must be positive")
        if self.l1_default_ttl_seconds <= 0:
            raise ValueError("l1_default_ttl_seconds must be positive")
        if self.l2_default_ttl_seconds <= 0:
            raise ValueError("l2_default_ttl_seconds must be positive")


class DefaultL1Cache:
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
                self._stats.hit_l1(latency_ms)
            return node.entry.value

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        ttl = ttl_seconds or self._config.l1_default_ttl_seconds

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
                if len(self._data) > self._config.l1_max_size:
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


class DefaultHybridCache:
    """Default implementation of HybridCache."""

    def __init__(
        self,
        l1_cache: L1Cache,
        l2_cache: Optional[L2Cache],
        config: CacheConfig,
    ):
        self._l1 = l1_cache
        self._l2 = l2_cache
        self._config = config
        self._l2_available = True
        self._last_l2_check = 0.0

    async def get(self, key: str) -> Optional[Any]:
        """Read-through cache lookup: L1 -> L2 -> None."""
        # Try L1 first
        value = self._l1.get(key)
        if value is not None:
            return value

        # Try L2 if available
        if self._is_l2_enabled() and self._l2 is not None:
            try:
                value = await self._l2.get(key)
                if value is not None:
                    # Promote to L1
                    self._l1.set(key, value, self._config.l1_default_ttl_seconds)
                    return value
            except Exception:
                self._mark_l2_unavailable()
                # Continue to return None

        return None

    def get_sync(self, key: str) -> Optional[Any]:
        """Synchronous version of get()."""
        # Try L1 first
        value = self._l1.get(key)
        if value is not None:
            return value

        # Try L2 if available
        if self._is_l2_enabled() and self._l2 is not None:
            try:
                value = self._l2.get_sync(key)
                if value is not None:
                    # Promote to L1
                    self._l1.set(key, value, self._config.l1_default_ttl_seconds)
                    return value
            except Exception:
                self._mark_l2_unavailable()

        return None

    async def set(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Set in both L1 and L2."""
        # Always set in L1
        self._l1.set(key, value, ttl_seconds or self._config.l1_default_ttl_seconds)

        # Set in L2 if available
        if self._is_l2_enabled() and self._l2 is not None:
            try:
                await self._l2.set(
                    key, value, ttl_seconds or self._config.l2_default_ttl_seconds
                )
            except Exception:
                self._mark_l2_unavailable()
                # L1 set still succeeded

    def set_sync(
        self, key: str, value: Any, ttl_seconds: Optional[float] = None
    ) -> None:
        """Synchronous version of set()."""
        # Always set in L1
        self._l1.set(key, value, ttl_seconds or self._config.l1_default_ttl_seconds)

        # Set in L2 if available
        if self._is_l2_enabled() and self._l2 is not None:
            try:
                self._l2.set_sync(
                    key, value, ttl_seconds or self._config.l2_default_ttl_seconds
                )
            except Exception:
                self._mark_l2_unavailable()

    async def delete(self, key: str) -> None:
        """Delete from both caches."""
        self._l1.delete(key)

        if self._is_l2_enabled() and self._l2 is not None:
            try:
                await self._l2.delete(key)
            except Exception:
                self._mark_l2_unavailable()

    def delete_sync(self, key: str) -> None:
        """Synchronous version of delete()."""
        self._l1.delete(key)

        if self._is_l2_enabled() and self._l2 is not None:
            try:
                self._l2.delete_sync(key)
            except Exception:
                self._mark_l2_unavailable()

    def invalidate_pattern(self, pattern: str) -> None:
        """Invalidate entries matching pattern (L1 only for now)."""
        # Use proper L1 cache interface
        if hasattr(self._l1, "invalidate_pattern"):
            self._l1.invalidate_pattern(pattern)
        else:
            # Fallback for caches without pattern support
            # This is still expensive but properly encapsulated
            if hasattr(self._l1, "_data") and hasattr(self._l1, "_lock"):
                with self._l1._lock:
                    keys_to_delete = [
                        key
                        for key in self._l1._data.keys()
                        if fnmatch.fnmatch(key, pattern)
                    ]
                    for key in keys_to_delete:
                        self._l1.delete(key)

    def get_combined_stats(self) -> Dict[str, CacheStats]:
        """Get statistics from both cache levels."""
        stats = {"l1": self._l1.get_stats()}
        if self._l2:
            stats["l2"] = self._l2.get_stats()
        return stats

    def is_l2_available(self) -> bool:
        """Check current L2 availability."""
        return self._l2_available and self._l2 is not None

    def _is_l2_enabled(self) -> bool:
        """Check if L2 should be used."""
        if not self._config.l2_enabled or self._l2 is None:
            return False

        # Periodically check L2 health if marked unavailable
        now = time.time()
        if not self._l2_available and (now - self._last_l2_check) > 30.0:
            self._check_l2_health()

        return self._l2_available

    def _mark_l2_unavailable(self) -> None:
        """Mark L2 as unavailable due to error."""
        self._l2_available = False
        self._last_l2_check = time.time()

    def _check_l2_health(self) -> None:
        """Check if L2 has recovered."""
        try:
            if self._l2 is not None and self._l2.ping_sync():
                self._l2_available = True
        except Exception as e:
            logger.debug(f"L2 health check failed: {e}")  # Remain unavailable

        self._last_l2_check = time.time()


# Cache-specific exception classes
class CacheUnavailableError(CacheError):
    """Raised when cache backend is unavailable."""


class CacheTimeoutError(CacheError):
    """Raised when cache operation times out."""
