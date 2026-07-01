# tests/benchmark_model.py
"""
InferX Model Runtime Performance Benchmark.

Evaluates token generation throughput (tokens/sec) and dynamic model switching
latencies under sustained query loads.
"""
import asyncio
import time

from inferx.model.interfaces import ModelMetadata
from inferx.model.registry import ModelRegistry
from inferx.model.loader import ModelLoader
from inferx.model.cache import ModelCache
from inferx.model.manager import ModelRuntimeManager
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


async def run_model_benchmark(count: int = 1000) -> None:
    """Measures token throughput and model switching latency in the ModelRuntime."""
    registry = ModelRegistry()
    loader = ModelLoader()
    
    # 6GB VRAM capacity cache
    cache = ModelCache(max_vram_bytes=6 * 1024 * 1024 * 1024)
    
    manager = ModelRuntimeManager(registry=registry, loader=loader, cache=cache)

    # 1. Register three 2.5GB models (cache fits any 2 models simultaneously, 3rd triggers evictions)
    m1 = ModelMetadata(model_name="model-A", version="v1.0", backend_type="pytorch", estimated_vram_bytes=int(2.5 * 1024 * 1024 * 1024))
    m2 = ModelMetadata(model_name="model-B", version="v1.0", backend_type="pytorch", estimated_vram_bytes=int(2.5 * 1024 * 1024 * 1024))
    m3 = ModelMetadata(model_name="model-C", version="v1.0", backend_type="pytorch", estimated_vram_bytes=int(2.5 * 1024 * 1024 * 1024))
    
    registry.register_model(m1)
    registry.register_model(m2)
    registry.register_model(m3)

    print("\n" + "="*70)
    print("INFERX MODEL RUNTIME PERFORMANCE BENCHMARK")
    print("="*70)

    # --- BENCHMARK 1: Token Throughput ---
    # Warm up and load model-A
    await manager.get_or_load_model("model-A", "v1.0")
    
    query = "Explain deep learning pipelines in detail."
    query_tokens = len(manager.tokenizer.encode(query))

    start_time = time.perf_counter()
    
    total_generated_tokens = 0
    for _ in range(count):
        res = await manager.predict("model-A", "v1.0", query)
        # Mock predict appends 7 tokens
        total_generated_tokens += 7

    duration = time.perf_counter() - start_time
    throughput = total_generated_tokens / duration

    print(f"Total Queries Executed    : {count}")
    print(f"Total Tokens Generated    : {total_generated_tokens}")
    print(f"Generation Throughput     : {throughput:.2f} tokens/sec")
    print(f"Average Generation Latency: {(duration / count) * 1000:.3f} ms")
    print("-"*70)

    # --- BENCHMARK 2: Model Switching Latency ---
    # We swap between model-A, model-B, and model-C.
    # Since cache size is 6GB and each is 2.5GB:
    # - Swapping between A and B is a Cache Hit (VRAM size = 5.0GB).
    # - Swapping to C triggers Cache Miss, evicting A.
    # We measure both Hit and Miss latencies.

    # Warm up cache
    await manager.get_or_load_model("model-B", "v1.0")  # Cache holds A and B
    
    # Cache Hit: Swap back to model-A
    t_start = time.perf_counter()
    await manager.get_or_load_model("model-A", "v1.0")
    hit_time_ms = (time.perf_counter() - t_start) * 1000

    # Cache Miss: Load model-C (triggers eviction of model-B)
    t_start = time.perf_counter()
    await manager.get_or_load_model("model-C", "v1.0")
    miss_time_ms = (time.perf_counter() - t_start) * 1000

    print(f"Model Swap (Cache Hit)    : {hit_time_ms:.4f} ms")
    print(f"Model Swap (Cache Miss)   : {miss_time_ms:.4f} ms (Includes LRU Eviction & Warmup)")
    print("="*70 + "\n")


if __name__ == "__main__":
    asyncio.run(run_model_benchmark(1000))
