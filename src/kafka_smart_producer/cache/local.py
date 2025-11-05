"""Local in-memory LRU cache with TTL support. No external dependencies."""

import threading
import time
from collections import OrderedDict
from typing import Any, Optional


class LocalLRUCache:
    """
    Thread-safe LRU cache with TTL expiration.

    Uses stdlib OrderedDict for O(1) get/set/eviction.
    """

    def __init__(self, max_size: int = 1000, default_ttl: float = 300.0):
        if max_size <= 0:
            raise ValueError("max_size must be positive")
        if default_ttl <= 0:
            raise ValueError("default_ttl must be positive")

        self._max_size = max_size
        self._default_ttl = default_ttl
        self._lock = threading.Lock()
        # Stores (value, expires_at) tuples
        self._data: OrderedDict[str, tuple[Any, float]] = OrderedDict()

    def get(self, key: str) -> Optional[Any]:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None

            value, expires_at = entry
            if time.monotonic() >= expires_at:
                del self._data[key]
                return None

            # Move to end (most recently used)
            self._data.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        expires_at = time.monotonic() + ttl

        with self._lock:
            # Remove existing entry first (to update position)
            if key in self._data:
                del self._data[key]

            # Evict oldest if at capacity
            while len(self._data) >= self._max_size:
                self._data.popitem(last=False)

            self._data[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)
