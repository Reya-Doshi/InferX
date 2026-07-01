# tests/benchmark_batcher.py
"""
InferX Batcher Performance Benchmark.

Evaluates batch assembly rates and measures the tensor padding efficiency gains
achieved by implementing Shape Bucketing.
"""
import asyncio
import time
import random
from typing import List

from inferx.batcher.engine import StaticBatcher
from inferx.batcher.interfaces import Batch, IBatchHandler
from inferx.batcher.padding import pad_tensors, ShapeBucketeer
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.scheduler.manager import Scheduler
from inferx.scheduler.policies import FIFOPolicy
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


class BenchmarkBatchHandler(IBatchHandler):
    """Mocks execution target tracking token metrics."""
    def __init__(self) -> None:
        self.total_actual_tokens = 0
        self.total_padded_tokens = 0
        self.batch_count = 0

    async def handle_batch(self, batch: Batch) -> None:
        self.batch_count += 1
        self.total_actual_tokens += sum(len(req.payload) for req in batch.requests)
        self.total_padded_tokens += batch.padded_shape[0] * batch.padded_shape[1]


async def run_benchmark(count: int = 5000) -> None:
    """Runs the batching throughput and padding efficiency benchmark."""
    print("\n" + "="*70)
    print(f"INFERX DYNAMIC BATCHER PERFORMANCE BENCHMARK (Requests: {count})")
    print("="*70)

    # 1. Pre-generate requests of varying sequence lengths (10 to 500 tokens)
    random.seed(42)
    requests: List[ScheduledRequest] = []
    for i in range(count):
        length = random.choice([16, 32, 64, 128, 256, 384, 512])
        tokens = [random.randint(1, 1000) for _ in range(length)]
        requests.append(ScheduledRequest(
            request_id=f"r-{i}",
            tenant_id="t1",
            priority=1,
            payload=tokens
        ))

    # --- BENCHMARK 1: Standard Static Batching (Without Shape Bucketing) ---
    scheduler = Scheduler(FIFOPolicy())
    await scheduler.start()
    handler = BenchmarkBatchHandler()
    
    # Large max size to ensure complete batching
    batcher = StaticBatcher(scheduler, handler, max_batch_size=32, max_queue_delay_ms=2)
    await batcher.start()

    start_time = time.perf_counter()
    for req in requests:
        await scheduler.enqueue(req)

    # Wait for processing and stop
    await asyncio.sleep(0.5)
    await batcher.stop()
    await scheduler.stop()
    
    duration = time.perf_counter() - start_time
    efficiency = handler.total_actual_tokens / handler.total_padded_tokens if handler.total_padded_tokens > 0 else 0.0

    print("Strategy: Standard Static Batching (No Bucketing)")
    print(f"  Flushed Batches   : {handler.batch_count}")
    print(f"  Padded Tokens     : {handler.total_padded_tokens:,}")
    print(f"  Actual Tokens     : {handler.total_actual_tokens:,}")
    print(f"  Padding Efficiency: {efficiency * 100:.2f} % (higher is better)")
    print(f"  Time Taken        : {duration:.4f} seconds")
    print(f"  Throughput        : {count / duration:.2f} req/sec")
    print("-"*70)

    # --- BENCHMARK 2: Shape Bucketing ---
    bucketeer = ShapeBucketeer(thresholds=[32, 64, 128, 256, 512])
    
    start_time = time.perf_counter()
    
    # Group all requests into buckets
    for req in requests:
        bucketeer.add_request(req)

    # Form batches from buckets
    bucketed_batches = []
    total_actual = 0
    total_padded = 0

    for threshold in bucketeer.get_active_thresholds():
        bucket_reqs = bucketeer.get_bucket(threshold)
        if not bucket_reqs:
            continue
        
        # Batch requests in chunks of 32
        for i in range(0, len(bucket_reqs), 32):
            chunk = bucket_reqs[i : i + 32]
            padded, shape = pad_tensors(chunk)
            total_actual += sum(len(req.payload) for req in chunk)
            total_padded += shape[0] * shape[1]

    duration = time.perf_counter() - start_time
    bucket_efficiency = total_actual / total_padded if total_padded > 0 else 0.0

    print("Strategy: Shape-Bucketed Batching")
    print(f"  Padded Tokens     : {total_padded:,}")
    print(f"  Actual Tokens     : {total_actual:,}")
    print(f"  Padding Efficiency: {bucket_efficiency * 100:.2f} % (higher is better)")
    print(f"  Time Taken        : {duration:.4f} seconds")
    print(f"  Token Savings     : {((handler.total_padded_tokens - total_padded) / handler.total_padded_tokens) * 100:.2f} % fewer tokens padded")
    print("="*70 + "\n")


if __name__ == "__main__":
    asyncio.run(run_benchmark(5000))
