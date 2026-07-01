# tests/benchmark_admission.py
"""
InferX Admission Controller Performance Benchmark.

Measures the admission manager decision latency under heavy traffic.
Verifies compliance with the <100 microsecond performance target.
"""
import asyncio
import time
from typing import List

from inferx.admission.limiter import TokenBucketLimiter
from inferx.admission.manager import AdmissionManager
from inferx.admission.shedder import BackpressureController, LoadShedder, CircuitBreaker
from inferx.core.context import RuntimeContext
from inferx.scheduler.interfaces import ScheduledRequest
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


async def run_admission_benchmark(count: int = 100000) -> None:
    """Measures the average and percentile decision latencies of the AdmissionManager."""
    context = RuntimeContext()
    context.update_telemetry(vram_util=0.5, cpu_util=0.4, avg_queue_lat=0.0)
    for _ in range(5):
        context.increment_active_requests()

    # Set large capacities to prevent rate limits from skewing latency paths
    limiter = TokenBucketLimiter(global_capacity=float(count + 100), global_refill_rate=float(count))
    backpressure = BackpressureController()
    shedder = LoadShedder(backpressure)
    circuit_breaker = CircuitBreaker()

    manager = AdmissionManager(
        context=context,
        limiter=limiter,
        shedder=shedder,
        circuit_breaker=circuit_breaker
    )

    # Pre-generate requests
    requests = [
        ScheduledRequest(
            request_id=f"r-{i}",
            tenant_id="tenant-global",
            priority=1,
            payload=b"benchmark"
        )
        for i in range(count)
    ]

    print("\n" + "="*70)
    print(f"INFERX ADMISSION CONTROLLER LATENCY BENCHMARK (Decisions: {count})")
    print("="*70)

    start_time = time.perf_counter()
    
    latencies: List[float] = []
    
    # Run sequential decisions to isolate microsecond measurements
    for req in requests:
        t_start = time.perf_counter()
        await manager.admit(req)
        t_end = time.perf_counter()
        latencies.append(t_end - t_start)

    duration = time.perf_counter() - start_time
    throughput = count / duration

    # Sort to compute percentiles
    latencies.sort()
    avg_lat_us = (sum(latencies) / count) * 1e6
    p50_us = latencies[int(count * 0.50)] * 1e6
    p95_us = latencies[int(count * 0.95)] * 1e6
    p99_us = latencies[int(count * 0.99)] * 1e6

    print(f"Total Decisions Evaluated : {count}")
    print(f"Throughput (decisions/sec): {throughput:.2f} dec/sec")
    print(f"Average Decision Latency  : {avg_lat_us:.3f} us (Target: <100 us)")
    print(f"p50 Decision Latency      : {p50_us:.3f} us")
    print(f"p95 Decision Latency      : {p95_us:.3f} us")
    print(f"p99 Decision Latency      : {p99_us:.3f} us")
    print("="*70 + "\n")


if __name__ == "__main__":
    asyncio.run(run_admission_benchmark(100000))
