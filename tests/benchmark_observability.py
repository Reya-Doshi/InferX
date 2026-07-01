# tests/benchmark_observability.py
"""
InferX Observability Overhead Benchmark.

Measures the performance overhead of tracer and metric instrumentations
to verify compliance with the <1% runtime overhead target.
"""

import asyncio
import time

from inferx.observability.metrics import MetricsRegistry
from inferx.observability.tracing import Tracer
from inferx.utils.logging import configure_logging, get_logger

configure_logging("INFO")
logger = get_logger("benchmark")


# Simple mock function representing a hot-path CPU action
def mock_hot_path_action() -> int:
    return sum(i * i for i in range(100))


def run_baseline(count: int) -> float:
    """Runs hot-path actions without any telemetry instrumentation."""
    start = time.perf_counter()
    for _ in range(count):
        mock_hot_path_action()
    return time.perf_counter() - start


async def run_instrumented(
    count: int, tracer: Tracer, registry: MetricsRegistry
) -> float:
    """Runs hot-path actions wrapped in trace spans and logging counters/histograms."""
    counter = registry.counter("test_counter", "Test counter metric")
    latency = registry.histogram(
        "test_latency", "Test latency", buckets=[0.1, 0.5, 1.0]
    )

    start = time.perf_counter()
    for _ in range(count):
        async with tracer.span("hot_path", attributes={"key": "val"}):
            mock_hot_path_action()
            counter.inc(1.0, labels={"action": "test"})
            latency.observe(0.02, labels={"action": "test"})

    return time.perf_counter() - start


async def main() -> None:
    count = 100000
    print("\n" + "=" * 70)
    print(f"INFERX TELEMETRY OVERHEAD BENCHMARK (Iterations: {count})")
    print("=" * 70)

    # 1. Run Baseline
    duration_baseline = run_baseline(count)

    # 2. Run Instrumented
    tracer = Tracer()
    registry = MetricsRegistry()
    duration_instrumented = await run_instrumented(count, tracer, registry)

    await tracer.exporter.stop()

    # 3. Calculate Overhead
    overhead = ((duration_instrumented - duration_baseline) / duration_baseline) * 100

    print(f"Baseline Time     : {duration_baseline:.4f} s")
    print(f"Instrumented Time : {duration_instrumented:.4f} s")
    # Note: Because python sleep yields and queue writes are extremely fast,
    # the instrumentation overhead is minimal. Under local CPU, mock bounds should verify.
    print(f"Overhead Ratio    : {overhead:.4f} % (Target: <1%)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    # Import Any dynamically to prevent compilation errors

    asyncio.run(main())
