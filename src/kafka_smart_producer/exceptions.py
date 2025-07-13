"""
Custom exceptions for the Kafka Smart Producer library.

Exception hierarchy designed for specific error handling and
diagnostic information capture.
"""

from typing import Any, Optional


class SmartProducerError(Exception):
    """
    Base exception for all Smart Producer errors.

    Provides common functionality for error context and diagnostics.
    """

    def __init__(
        self,
        message: str,
        cause: Optional[Exception] = None,
        context: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.cause = cause
        self.context = context or {}


class LagDataUnavailableError(SmartProducerError):
    """
    Raised when lag data cannot be retrieved from the source.

    This indicates that the data collector cannot reach its source
    (Kafka cluster, Redis, monitoring system, etc.) or the data
    is malformed/unavailable.

    Usage:
        - Trigger fallback to default partitioning
        - Log diagnostic information
        - Retry with exponential backoff
    """


class HealthManagerError(SmartProducerError):
    """Raised when health management operations fail."""


class CacheError(SmartProducerError):
    """
    Raised when cache operations fail.

    This covers both local (in-memory) and distributed (Redis)
    cache failures.

    Usage:
        - Continue without caching (performance impact acceptable)
        - Retry cache operations with backoff
        - Switch to alternative cache backend if available
    """


class PartitionSelectionError(SmartProducerError):
    """
    Raised when partition selection fails.

    This indicates the smart partitioner cannot determine
    a suitable partition for message routing.

    Usage:
        - Fall back to default Kafka partitioning
        - Log selection criteria for debugging
        - Alert on persistent selection failures
    """


class ConfigurationError(SmartProducerError):
    """
    Raised when configuration is invalid.

    This covers validation errors in producer configuration,
    health manager settings, cache configuration, etc.

    Usage:
        - Fail fast during initialization
        - Provide clear configuration guidance
        - Validate all configuration at startup
    """
