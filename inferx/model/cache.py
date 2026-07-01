# inferx/model/cache.py
"""
InferX Model Cache.

Implements an LRU model instance cache that monitors GPU VRAM usage
and triggers size-based evictions.
"""

from collections import OrderedDict
from typing import List, Optional, Tuple

from inferx.model.interfaces import IModelInstance
from inferx.utils.logging import get_logger

logger = get_logger("model.cache")


class ModelCache:
    """
    LRU Cache managing active model instances in VRAM.

    Evicts least recently used models when VRAM limits are exceeded.
    """

    def __init__(self, max_vram_bytes: int = 16 * 1024 * 1024 * 1024) -> None:
        self.max_vram_bytes = max_vram_bytes
        self._cache: OrderedDict[Tuple[str, str], IModelInstance] = OrderedDict()
        self._current_vram = 0

    def get(self, key: Tuple[str, str]) -> Optional[IModelInstance]:
        """Retrieves a model instance and updates its LRU position."""
        if key not in self._cache:
            return None

        # Move to end (most recently used)
        instance = self._cache.pop(key)
        self._cache[key] = instance
        return instance

    def put(
        self, key: Tuple[str, str], instance: IModelInstance
    ) -> List[IModelInstance]:
        """
        Inserts a model instance, evicting older models if VRAM limits are crossed.

        Returns:
            A list of evicted model instances.
        """
        metadata = instance.get_metadata()
        instance_vram = metadata.estimated_vram_bytes

        if instance_vram > self.max_vram_bytes:
            raise MemoryError(
                f"Model {metadata.model_name}:{metadata.version} footprint ({instance_vram} bytes) "
                f"exceeds maximum cache size ({self.max_vram_bytes} bytes)."
            )

        evicted: List[IModelInstance] = []

        # Evict until we have enough free space
        while self._current_vram + instance_vram > self.max_vram_bytes and self._cache:
            # Pop oldest item (FIFO order from start of OrderedDict)
            old_key, old_instance = self._cache.popitem(last=False)
            old_metadata = old_instance.get_metadata()
            self._current_vram -= old_metadata.estimated_vram_bytes
            evicted.append(old_instance)
            logger.warning(
                f"VRAM congestion. Evicting model {old_metadata.model_name}:{old_metadata.version} "
                f"({old_metadata.estimated_vram_bytes} bytes) from memory cache.",
                component="model_cache",
            )

        self._cache[key] = instance
        self._current_vram += instance_vram
        return evicted

    def remove(self, key: Tuple[str, str]) -> Optional[IModelInstance]:
        """Removes a model instance from the cache, freeing VRAM footprint."""
        if key in self._cache:
            instance = self._cache.pop(key)
            self._current_vram -= instance.get_metadata().estimated_vram_bytes
            return instance
        return None

    def clear(self) -> None:
        """Clears all cached model instances."""
        self._cache.clear()
        self._current_vram = 0

    def current_vram_usage(self) -> int:
        """Returns the total VRAM bytes consumed by cached model instances."""
        return self._current_vram

    def size(self) -> int:
        """Returns the number of cached model instances."""
        return len(self._cache)
