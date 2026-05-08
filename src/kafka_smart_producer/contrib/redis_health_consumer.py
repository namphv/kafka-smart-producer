"""Redis-based health data consumer for distributed health monitoring (Scenario 5/6)."""

from __future__ import annotations

import json
import logging
import time

logger = logging.getLogger(__name__)


def _import_redis():  # pragma: no cover
    """Lazy import redis to keep it optional."""
    try:
        import redis

        return redis
    except ImportError as err:
        raise ImportError(
            "redis is required for RedisHealthConsumer. "
            "Install it with: pip install kafka-smart-producer[redis]"
        ) from err


class RedisHealthPublisher:
    """
    Publishes health scores to Redis for consumption by other processes.

    Used by a standalone PartitionHealthMonitor (Scenario 5) to share
    health data across a distributed system.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "ksp:health:",
        ttl: float = 120.0,
        channel_prefix: str = "ksp:health:update:",
    ) -> None:
        redis_mod = _import_redis()
        self._client = redis_mod.from_url(redis_url, decode_responses=True)
        self._key_prefix = key_prefix
        self._ttl = ttl
        self._channel_prefix = channel_prefix

    def publish_scores(self, topic: str, scores: dict[int, float]) -> None:
        """Publish health scores to Redis and notify subscribers."""
        key = f"{self._key_prefix}{topic}"
        data = {
            "scores": {str(k): v for k, v in scores.items()},
            "timestamp": time.time(),
        }
        try:
            self._client.setex(key, int(self._ttl), json.dumps(data))
            self._client.publish(f"{self._channel_prefix}{topic}", json.dumps(data))
        except Exception as e:  # pragma: no cover
            logger.warning(f"Failed to publish health scores for '{topic}': {e}")

    def is_healthy(self) -> bool:
        try:
            return self._client.ping()
        except Exception:  # pragma: no cover
            return False


class RedisHealthConsumer:
    """
    Consumes health scores from Redis instead of collecting directly from Kafka.

    Implements HealthSignalCollector protocol by reading pre-computed health
    scores stored in Redis by a RedisHealthPublisher (Scenario 6).

    This allows producers to get health data without direct Kafka AdminClient
    access, useful in environments where producers run in restricted contexts.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        key_prefix: str = "ksp:health:",
        max_age: float = 120.0,
    ) -> None:
        redis_mod = _import_redis()
        self._client = redis_mod.from_url(redis_url, decode_responses=True)
        self._key_prefix = key_prefix
        self._max_age = max_age

    def get_lag_data(self, topic: str) -> dict[int, int]:
        """
        Read health scores from Redis and convert to synthetic lag values.

        Since we already have health scores (0.0-1.0), we return synthetic
        lag values that will produce the same scores when run through a scorer.
        A score of 1.0 -> lag 0, score of 0.0 -> lag 1000.
        """
        key = f"{self._key_prefix}{topic}"
        try:
            raw = self._client.get(key)
            if raw is None:
                return {}

            data = json.loads(raw)
            timestamp = data.get("timestamp", 0)

            # Check staleness
            if time.time() - timestamp > self._max_age:
                logger.debug(
                    f"Health data for '{topic}' is stale "
                    f"(age={time.time() - timestamp:.0f}s, max={self._max_age}s)"
                )
                return {}

            # Convert scores back to synthetic lag values
            # score = 1.0 - (lag / max_lag), so lag = (1.0 - score) * max_lag
            # Using max_lag=1000 as the synthetic reference
            scores = data.get("scores", {})
            return {
                int(pid): int((1.0 - score) * 1000) for pid, score in scores.items()
            }
        except Exception as e:
            logger.warning(f"Failed to read health data for '{topic}': {e}")
            return {}

    def get_health_scores(self, topic: str) -> dict[int, float] | None:
        """Read health scores directly from Redis (bypasses lag conversion)."""
        key = f"{self._key_prefix}{topic}"
        try:
            raw = self._client.get(key)
            if raw is None:
                return None

            data = json.loads(raw)
            timestamp = data.get("timestamp", 0)

            if time.time() - timestamp > self._max_age:
                return None

            scores = data.get("scores", {})
            return {int(pid): float(score) for pid, score in scores.items()}
        except Exception as e:  # pragma: no cover
            logger.warning(f"Failed to read health scores for '{topic}': {e}")
            return None

    def is_healthy(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False
