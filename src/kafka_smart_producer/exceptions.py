"""Exception hierarchy for Kafka Smart Producer."""

from __future__ import annotations

from typing import Any


class SmartProducerError(Exception):
    """Base exception for all Smart Producer errors."""

    def __init__(
        self,
        message: str,
        cause: Exception | None = None,
        context: dict[str, Any] | None = None,
    ):
        super().__init__(message)
        self.cause = cause
        self.context = context or {}


class ConfigurationError(SmartProducerError):
    """Invalid configuration."""


class LagDataUnavailableError(SmartProducerError):
    """Lag data cannot be retrieved from source."""


class HealthError(SmartProducerError):
    """Health monitoring or calculation failure."""


class CacheError(SmartProducerError):
    """Cache operation failure."""


class PartitionSelectionError(SmartProducerError):
    """Partition selection failure."""
