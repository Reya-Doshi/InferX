# tests/test_model.py
"""
InferX Model Runtime Test Suite.

Verifies version resolution, LRU VRAM cache evictions, lazy loading, Warmup steps,
hot reloading, and fallback redirects.
"""

import unittest

from inferx.model.interfaces import ModelMetadata
from inferx.model.registry import ModelRegistry
from inferx.model.loader import ModelLoader
from inferx.model.cache import ModelCache
from inferx.model.manager import ModelRuntimeManager
from inferx.model.metrics import ModelMetrics


class TestModelRuntime(unittest.IsolatedAsyncioTestCase):
    """Unit test suite for the Model Runtime components."""

    def setUp(self) -> None:
        self.registry = ModelRegistry()
        self.loader = ModelLoader()

        # Configure cache with 10GB capacity
        self.cache = ModelCache(max_vram_bytes=10 * 1024 * 1024 * 1024)
        self.metrics = ModelMetrics()

        self.manager = ModelRuntimeManager(
            registry=self.registry,
            loader=self.loader,
            cache=self.cache,
            metrics=self.metrics,
        )

    def build_metadata(
        self,
        name: str,
        version: str,
        vram_gb: float = 2.0,
        fallback_name: str = None,
        fallback_version: str = None,
    ) -> ModelMetadata:
        return ModelMetadata(
            model_name=name,
            version=version,
            backend_type="pytorch",
            estimated_vram_bytes=int(vram_gb * 1024 * 1024 * 1024),
            fallback_model_name=fallback_name,
            fallback_version=fallback_version,
        )

    async def test_model_registry_resolution(self) -> None:
        m1 = self.build_metadata("llama", "v1.0")
        m2 = self.build_metadata("llama", "v2.0")
        self.registry.register_model(m1)
        self.registry.register_model(m2)

        # Explicit version
        self.assertEqual(self.registry.resolve_version("llama", "v1.0"), "v1.0")

        # Lexicographical 'latest' fallback
        self.assertEqual(self.registry.resolve_version("llama", "latest"), "v2.0")

        # Explicit alias map
        self.registry.register_alias("llama", "production", "v1.0")
        self.assertEqual(self.registry.resolve_version("llama", "production"), "v1.0")

    async def test_lazy_loading_and_warmup(self) -> None:
        m = self.build_metadata("llama", "v1.0")
        self.registry.register_model(m)

        # Cache is initially empty
        self.assertEqual(self.cache.size(), 0)

        # Lazy load on first predict call
        res = await self.manager.predict("llama", "v1.0", "Hello")

        # 'Hello' encoded is [72, 101, 108, 108, 111]. Mock predict appends '_output'
        # [95, 111, 117, 116, 112, 117, 116]
        self.assertEqual(res, "Hello_output")
        self.assertEqual(self.cache.size(), 1)

    async def test_lru_vram_cache_eviction(self) -> None:
        # Cache capacity is 10GB. Register three 4GB models
        m1 = self.build_metadata("model-1", "v1.0", vram_gb=4.0)
        m2 = self.build_metadata("model-2", "v1.0", vram_gb=4.0)
        m3 = self.build_metadata("model-3", "v1.0", vram_gb=4.0)

        self.registry.register_model(m1)
        self.registry.register_model(m2)
        self.registry.register_model(m3)

        # 1. Load model-1 and model-2 (total VRAM = 8GB)
        await self.manager.get_or_load_model("model-1", "v1.0")
        await self.manager.get_or_load_model("model-2", "v1.0")
        self.assertEqual(self.cache.size(), 2)
        self.assertEqual(self.cache.current_vram_usage(), 8 * 1024 * 1024 * 1024)

        # 2. Load model-3 (requires 4GB, total would be 12GB > 10GB capacity)
        # Should evict the oldest model (model-1)
        await self.manager.get_or_load_model("model-3", "v1.0")

        self.assertEqual(self.cache.size(), 2)
        self.assertEqual(self.cache.current_vram_usage(), 8 * 1024 * 1024 * 1024)
        self.assertIsNone(self.cache.get(("model-1", "v1.0")))
        self.assertIsNotNone(self.cache.get(("model-2", "v1.0")))
        self.assertIsNotNone(self.cache.get(("model-3", "v1.0")))

    async def test_hot_reload_atomic_swap(self) -> None:
        m_old = self.build_metadata("llama", "v1.0", vram_gb=2.0)
        self.registry.register_model(m_old)

        # Load initial model
        await self.manager.get_or_load_model("llama", "v1.0")
        self.assertEqual(self.cache.size(), 1)

        # Build new config (VRAM changes to 3GB)
        m_new = self.build_metadata("llama", "v1.0", vram_gb=3.0)
        await self.manager.hot_reload("llama", "v1.0", m_new)

        # Cache reference is updated atomically
        inst_new = await self.manager.get_or_load_model("llama", "v1.0")
        self.assertEqual(
            inst_new.get_metadata().estimated_vram_bytes, 3 * 1024 * 1024 * 1024
        )
        self.assertEqual(self.cache.current_vram_usage(), 3 * 1024 * 1024 * 1024)

    async def test_fallback_redirect_recovery(self) -> None:
        # Register a fallback model
        m_fallback = self.build_metadata("llama-fallback", "v1.0")
        self.registry.register_model(m_fallback)

        # Register primary model configuration routing to fallback
        m_primary = self.build_metadata(
            "llama-primary",
            "v1.0",
            fallback_name="llama-fallback",
            fallback_version="v1.0",
        )
        self.registry.register_model(m_primary)

        # Mock loader error: make loading llama-primary raise an exception
        # We can patch or dynamically subclass the loader
        class ErrorLoader(ModelLoader):
            async def load(self, metadata):
                if metadata.model_name == "llama-primary":
                    raise RuntimeError("Failed to load CUDA weights.")
                return await super().load(metadata)

        self.manager.loader = ErrorLoader()

        # Execute prediction. Should fail to load primary, fall back to llama-fallback,
        # and succeed using llama-fallback output signature!
        res = await self.manager.predict("llama-primary", "v1.0", "Hello")

        self.assertEqual(res, "Hello_output")
        # Cache should only hold the fallback instance
        self.assertIsNotNone(self.cache.get(("llama-fallback", "v1.0")))
        self.assertIsNone(self.cache.get(("llama-primary", "v1.0")))


if __name__ == "__main__":
    unittest.main()
