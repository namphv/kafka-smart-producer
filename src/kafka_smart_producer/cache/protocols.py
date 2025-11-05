"""Cache protocol for key-value storage."""

from typing import Any, Optional, Protocol


class Cache(Protocol):
    """Simple key-value cache interface."""

    def get(self, key: str) -> Optional[Any]:
        """Get value by key. Returns None if not found or expired."""
        ...

    def set(self, key: str, value: Any, ttl_seconds: Optional[float] = None) -> None:
        """Set value with optional TTL."""
        ...

    def delete(self, key: str) -> None:
        """Delete key."""
        ...
