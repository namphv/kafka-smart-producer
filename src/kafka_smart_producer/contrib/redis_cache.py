"""Redis-backed cache and hybrid L1+L2 cache for distributed key stickiness."""

from __future__ import annotations

import json
import logging
from typing import Any

from ..cache.local import LocalLRUCache

logger = logging.getLogger(__name__)


def _import_redis():  # pragma: no cover
    """Lazy import redis to keep it optional."""
    try:
        import redis

        return redis
    except ImportError as err:
        raise ImportError(
            "redis is required for RemoteCache. "
            "Install it with: pip install kafka-smart-producer[redis]"
        ) from err


class RemoteCache:
    """
    Redis-backed cache implementing the Cache protocol.

    Values are JSON-serialized for cross-process compatibility.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        default_ttl: float = 900.0,
        key_prefix: str = "ksp:",
    ) -> None:
        redis_mod = _import_redis()
        self._client = redis_mod.from_url(redis_url, decode_responses=True)
        self._default_ttl = default_ttl
        self._key_prefix = key_prefix

    def get(self, key: str) -> Any | None:
        try:
            raw = self._client.get(self._key_prefix + key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as e:
            logger.debug(f"RemoteCache get failed for '{key}': {e}")
            return None

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self._default_ttl
        try:
            self._client.setex(self._key_prefix + key, int(ttl), json.dumps(value))
        except Exception as e:
            logger.debug(f"RemoteCache set failed for '{key}': {e}")

    def delete(self, key: str) -> None:
        try:
            self._client.delete(self._key_prefix + key)
        except Exception as e:  # pragma: no cover
            logger.debug(f"RemoteCache delete failed for '{key}': {e}")

    def is_healthy(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False


class HybridCache:
    """
    Two-level cache: L1 (local LRU) + L2 (Redis).

    Read: check L1 -> miss -> check L2 -> hit -> populate L1
    Write: write to both L1 and L2
    Delete: delete from both
    """

    def __init__(
        self,
        local: LocalLRUCache,
        remote: RemoteCache,
    ) -> None:
        self._local = local
        self._remote = remote

    def get(self, key: str) -> Any | None:
        # L1 check
        value = self._local.get(key)
        if value is not None:
            return value

        # L2 check
        value = self._remote.get(key)
        if value is not None:
            # Populate L1
            self._local.set(key, value)
            return value

        return None

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        self._local.set(key, value, ttl_seconds)
        self._remote.set(key, value, ttl_seconds)

    def delete(self, key: str) -> None:
        self._local.delete(key)
        self._remote.delete(key)
