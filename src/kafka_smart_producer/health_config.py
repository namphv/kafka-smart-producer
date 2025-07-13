"""
Unified configuration for health managers.

This module provides a single configuration interface that works for both
sync and async health managers, with optional context-specific customization.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class HealthManagerConfig:
    """
    Unified configuration for both Partition Health Monitor class.

    This configuration works for both execution contexts and allows optional
    context-specific customization through sync_options and async_options.

    Example:
        # Simple config (works for both sync and async)

        config = HealthManagerConfig(
            consumer_group='my-group',
            health_threshold=0.8
        )

        # Advanced config with context-specific options
        config = HealthManagerConfig(
            consumer_group='my-group',
            health_threshold=0.8,
            sync_options={'thread_pool_size': 8},
            async_options={'concurrent_refresh_limit': 20}
        )
    """

    # Required configuration
    consumer_group: str

    # Common optional configuration (with sensible defaults)
    health_threshold: float = 0.5
    refresh_interval: float = 5.0
    max_lag_for_health: int = 1000
    timeout_seconds: float = 5.0
    cache_enabled: bool = True
    cache_max_size: int = 1000
    cache_ttl_seconds: int = 300

    # Context-specific configuration (optional)
    sync_options: Optional[Dict[str, Any]] = None
    async_options: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not 0.0 <= self.health_threshold <= 1.0:
            raise ValueError("health_threshold must be between 0.0 and 1.0")
        if self.refresh_interval <= 0:
            raise ValueError("refresh_interval must be positive")
        if self.max_lag_for_health <= 0:
            raise ValueError("max_lag_for_health must be positive")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")

    def get_sync_option(self, key: str, default: Any = None) -> Any:
        """
        Get sync-specific configuration option.

        Args:
            key: Option key to retrieve
            default: Default value if key not found

        Returns:
            Option value or default
        """
        if self.sync_options is None:
            return default
        return self.sync_options.get(key, default)

    def get_async_option(self, key: str, default: Any = None) -> Any:
        """
        Get async-specific configuration option.

        Args:
            key: Option key to retrieve
            default: Default value if key not found

        Returns:
            Option value or default
        """
        if self.async_options is None:
            return default
        return self.async_options.get(key, default)


# Predefined context-specific option schemas for documentation and validation
SYNC_OPTIONS_SCHEMA = {
    "thread_pool_size": int,  # Number of threads for background tasks
    "thread_priority": str,  # Thread priority ('low', 'normal', 'high')
    "cleanup_interval": float,  # Interval for cleaning up stale data (seconds)
    "max_threads": int,  # Maximum number of threads
    "thread_timeout": float,  # Timeout for thread operations (seconds)
}

ASYNC_OPTIONS_SCHEMA = {
    "concurrent_refresh_limit": int,  # Max concurrent topic refreshes
    "task_priority": str,  # Task priority ('low', 'normal', 'high')
    "event_loop_policy": str,  # Event loop policy ('asyncio', 'uvloop')
    "task_timeout": float,  # Timeout for async tasks (seconds)
    "gather_timeout": float,  # Timeout for gather operations (seconds)
}
