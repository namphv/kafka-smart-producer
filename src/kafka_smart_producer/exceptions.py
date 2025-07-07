"""
Custom exceptions for the Kafka Smart Producer library.
"""


class SmartProducerError(Exception):
    """Base exception for all Smart Producer errors."""
    pass


class LagDataUnavailableError(SmartProducerError):
    """Raised when lag data cannot be retrieved from the source."""
    pass


class HealthManagerError(SmartProducerError):
    """Raised when health management operations fail."""
    pass


class CacheError(SmartProducerError):
    """Raised when cache operations fail."""
    pass


class PartitionSelectionError(SmartProducerError):
    """Raised when partition selection fails."""
    pass


class ConfigurationError(SmartProducerError):
    """Raised when configuration is invalid."""
    pass