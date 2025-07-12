"""
Health manager operation modes for intent-driven design.

This module defines the operation modes for health managers to provide
clear, type-safe configuration patterns.
"""

from enum import Enum, auto


class HealthMode(Enum):
    """
    Operation modes for health managers.

    This enum provides type-safe configuration for the two distinct
    use cases of health managers:

    - EMBEDDED: For producer integration (simple, no Redis)
    - STANDALONE: For monitoring services (full features, Redis publishing)
    """

    EMBEDDED = auto()
    """
    Embedded mode for producer integration.

    Characteristics:
    - Lightweight operation
    - No Redis publishing
    - Thread-safe data access
    - Optimized for partition selection
    """

    STANDALONE = auto()
    """
    Standalone mode for monitoring services.

    Characteristics:
    - Full feature set
    - Redis health publishing
    - Health streams for reactive patterns
    - Independent monitoring service
    """

    def __str__(self) -> str:
        """String representation for logging."""
        return self.name.lower()

    @classmethod
    def from_string(cls, mode_str: str) -> "HealthMode":
        """
        Convert string to HealthMode enum.

        Args:
            mode_str: Mode string ("embedded" or "standalone")

        Returns:
            HealthMode enum value

        Raises:
            ValueError: If mode_str is not valid
        """
        mode_map = {
            "embedded": cls.EMBEDDED,
            "standalone": cls.STANDALONE,
        }

        normalized = mode_str.lower().strip()
        if normalized not in mode_map:
            raise ValueError(
                f"Invalid health mode: '{mode_str}'. "
                f"Valid modes: {list(mode_map.keys())}"
            )

        return mode_map[normalized]
