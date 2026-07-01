# inferx/model/manager.py
"""
InferX Model Runtime Manager.

Coordinates lazy model loading, dynamic VRAM cache swapping, BPE tokenization,
and atomic model hot-reloading.
"""

import asyncio
import time
from typing import Optional

from inferx.model.interfaces import IModelInstance, ITokenizer, ModelMetadata
from inferx.model.registry import ModelRegistry
from inferx.model.loader import ModelLoader, MockTokenizer
from inferx.model.cache import ModelCache
from inferx.model.metrics import ModelMetrics
from inferx.utils.logging import get_logger

logger = get_logger("model.manager")


class ModelRuntimeManager:
    """
    Coordinator managing tokenizers, registries, caches, and execution routing.
    """

    def __init__(
        self,
        registry: ModelRegistry,
        loader: ModelLoader,
        cache: ModelCache,
        tokenizer: Optional[ITokenizer] = None,
        metrics: Optional[ModelMetrics] = None,
    ) -> None:
        self.registry = registry
        self.loader = loader
        self.cache = cache
        self.tokenizer = tokenizer or MockTokenizer()
        self.metrics = metrics or ModelMetrics()
        self._lock = asyncio.Lock()

    async def get_or_load_model(self, name: str, version: str) -> IModelInstance:
        """
        Retrieves a model instance from VRAM cache, or lazy-loads it on-demand.

        Performs fallback routing if loading fails.
        """
        async with self._lock:
            # 1. Resolve version alias
            resolved_version = self.registry.resolve_version(name, version)
            key = (name, resolved_version)

            # 2. Check if already cached in VRAM
            instance = self.cache.get(key)
            if instance is not None:
                return instance

            # 3. Cache miss: Load the model (lazy loading)
            try:
                metadata = self.registry.get_model_metadata(name, resolved_version)
                instance = await self.loader.load(metadata)
            except Exception as e:
                logger.error(
                    f"Failed to load model {name}:{resolved_version}: {e}",
                    exc_info=True,
                    component="model_manager",
                )

                # Check for registered fallback model
                fallback = self.registry.get_fallback(name, resolved_version)
                if fallback:
                    fallback_name, fallback_version = fallback
                    logger.warning(
                        f"Fallback active. Redirecting request to model {fallback_name}:{fallback_version}.",
                        component="model_manager",
                    )
                    # Bypass lock when calling recursively
                    instance = await self._get_or_load_model_unlocked(
                        fallback_name, fallback_version
                    )
                    return instance
                raise

            # 4. Save to cache (triggers LRU evictions if VRAM limits are crossed)
            self.cache.put(key, instance)
            return instance

    async def _get_or_load_model_unlocked(
        self, name: str, version: str
    ) -> IModelInstance:
        """Lockless helper for internal recursive lookups."""
        resolved_version = self.registry.resolve_version(name, version)
        key = (name, resolved_version)

        instance = self.cache.get(key)
        if instance is not None:
            return instance

        metadata = self.registry.get_model_metadata(name, resolved_version)
        instance = await self.loader.load(metadata)
        self.cache.put(key, instance)
        return instance

    async def hot_reload(
        self, name: str, version: str, new_metadata: ModelMetadata
    ) -> None:
        """
        Atomically updates a model configuration and swaps active instances.

        Loads the new instance in the background before swapping to prevent
        first-request latency spikes.
        """
        async with self._lock:
            resolved_version = self.registry.resolve_version(name, version)
            key = (name, resolved_version)

            logger.info(
                f"Initiating hot reload for model {name}:{resolved_version}...",
                component="model_manager",
            )

            # Load new model version in the background
            new_instance = await self.loader.load(new_metadata)

            # Update registry metadata definitions
            self.registry.register_model(new_metadata)

            # Atomically replace/add cache reference
            self.cache.remove(key)
            self.cache.put(key, new_instance)
            logger.info(
                f"Atomic hot reload complete for model {name}:{resolved_version}.",
                component="model_manager",
            )

    async def predict(self, name: str, version: str, text: str) -> str:
        """
        End-to-end tokenization and predict execution.

        Encodes strings, executes tensor predictions, and decodes token outputs.
        """
        start_ns = time.perf_counter_ns()

        # 1. Encode text
        tokens = self.tokenizer.encode(text)

        # 2. Retrieve instance
        model = await self.get_or_load_model(name, version)

        # 3. Predict output
        out_tokens = await model.predict(tokens)

        # 4. Decode outputs
        result_text = self.tokenizer.decode(out_tokens)

        # Record metrics
        elapsed_ns = time.perf_counter_ns() - start_ns
        self.metrics.record_inference(
            len(tokens), len(out_tokens) - len(tokens), elapsed_ns
        )

        return result_text
