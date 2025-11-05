"""Default partition selector: random choice from healthy partitions."""

import random
from typing import Optional


class RandomHealthySelector:
    """Selects a random partition from the healthy candidates."""

    def select(
        self,
        healthy_partitions: list[int],
        key: Optional[bytes] = None,
    ) -> Optional[int]:
        if not healthy_partitions:
            return None
        return random.choice(healthy_partitions)
