# tests/benchmark_scheduler.py
"""
InferX Scheduler Performance Benchmark.

Measures task throughput (enqueues/dequeues per second) and average delay
for FIFO and Priority Heap policies.
"""
import asyncio
import time
from typing import Any

from inferx.scheduler.interfaces import ScheduledRequest
from inferx.scheduler.manager import Scheduler
from inferx.scheduler.policies import FIFOPolicy, PriorityQueuePolicy
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


async def run_policy_benchmark(name: str, policy_instance: Any, count: int) -> None:
    """Executes throughput benchmark for a specific scheduling policy."""
    scheduler = Scheduler(policy_instance)
    await scheduler.start()

    # Pre-generate requests
    requests = [
        ScheduledRequest(
            request_id=f"r-{i}",
            tenant_id="t1",
            priority=i % 5,
            payload=b"benchmark_data"
        )
        for i in range(count)
    ]

    start_time = time.perf_counter()

    # Spawn consumer
    async def consumer() -> None:
        for _ in range(count):
            await scheduler.dequeue()

    consumer_task = asyncio.create_task(consumer())

    # Publish tasks
    for req in requests:
        await scheduler.enqueue(req)

    await consumer_task
    end_time = time.perf_counter()
    duration = end_time - start_time
    throughput = count / duration

    print(f"Policy: {name:<20} | Throughput: {throughput:10.2f} ops/sec | Latency: {(duration / count) * 1e6:8.3f} us")
    await scheduler.stop()


async def main() -> None:
    count = 50000
    print("\n" + "="*70)
    print(f"INFERX SCHEDULER ENGINE THROUGHPUT BENCHMARK (Count: {count})")
    print("="*70)
    
    # Run FIFO
    await run_policy_benchmark("FIFO (Queue)", FIFOPolicy(), count)
    
    # Run Priority Heap
    await run_policy_benchmark("Priority Queue (Heap)", PriorityQueuePolicy(), count)
    print("="*70 + "\n")


if __name__ == "__main__":
    # Import Any dynamically to prevent compilation errors
    from typing import Any
    asyncio.run(main())
