"""
Factory for creating configured cache instances.

Extracted from caching.py to reduce file complexity and separate concerns.
"""

import logging
from typing import Any, Optional

from .caching import CacheConfig, DefaultHybridCache, DefaultLocalCache, DefaultRemoteCache

logger = logging.getLogger(__name__)


class CacheFactory:
    """Factory for creating configured cache instances."""

    @staticmethod
    def create_local_cache(smart_config: dict[str, Any]) -> DefaultLocalCache:
        """Create standalone local cache instance."""
        cache_config = CacheConfig(
            local_max_size=smart_config.get("cache_max_size", 1000),
            local_default_ttl_seconds=smart_config.get("cache_ttl_ms", 300000) / 1000.0,
        )
        return DefaultLocalCache(cache_config)

    @staticmethod
    def create_remote_cache(
        smart_config: dict[str, Any],
    ) -> Optional[DefaultRemoteCache]:
        """Create standalone remote cache instance if configuration is valid."""
        try:
            # Import sync redis client
            import redis

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

            # Create sync Redis client
            redis_client = redis.Redis(**redis_kwargs)

            # Test connection
            try:
                redis_client.ping()
            except Exception as e:
                logger.warning(f"Redis connection test failed: {e}")
                redis_client.close()
                return None

            # Create cache config
            cache_config = CacheConfig(
                remote_default_ttl_seconds=smart_config.get("redis_ttl_seconds", 900.0),
            )

            # Create cache instance
            remote_cache_instance = DefaultRemoteCache(redis_client, cache_config)

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
        smart_config: dict[str, Any], enable_redis: bool = True
    ) -> DefaultHybridCache:
        """Create hybrid cache - remote cache is required."""
        if not enable_redis:
            raise ValueError("Hybrid cache requires Redis to be enabled")

        cache_config = CacheConfig(
            local_max_size=smart_config["cache_max_size"],
            local_default_ttl_seconds=smart_config["cache_ttl_ms"] / 1000.0,
            remote_default_ttl_seconds=smart_config["redis_ttl_seconds"],
        )

        # Create local cache
        local_cache = CacheFactory.create_local_cache(smart_config)

        # Create remote cache - fail fast if it fails
        remote_cache = CacheFactory.create_remote_cache(smart_config)
        if remote_cache is None:
            raise RuntimeError(
                "Failed to create remote cache for hybrid mode. "
                "Check Redis configuration and connectivity."
            )

        return DefaultHybridCache(local_cache, remote_cache, cache_config)
