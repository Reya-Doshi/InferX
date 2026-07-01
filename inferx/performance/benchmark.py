# inferx/performance/benchmark.py
import math
import time
from typing import Any, Dict, List
from inferx.performance.interfaces import IBenchmarkRunner


class BenchmarkRunner(IBenchmarkRunner):
    """Calculates tail latency percentiles (P50, P90, P95, P99, P999), throughput, and CPU/Memory/GPU metrics."""

    def __init__(self) -> None:
        self.latencies: List[float] = []
        self.timestamps: List[float] = []
        self.cpus: List[float] = []
        self.memories: List[float] = []
        self.gpus: List[float] = []
        self.batch_sizes: List[int] = []
        self.queue_delays: List[float] = []

    def record_latency(self, latency_ms: float) -> None:
        self.latencies.append(latency_ms)
        self.timestamps.append(time.perf_counter())

    def record_resource_usage(self, cpu: float, memory_mb: float, gpu: float) -> None:
        self.cpus.append(cpu)
        self.memories.append(memory_mb)
        self.gpus.append(gpu)

    def record_batch(self, batch_size: int, queue_delay_ms: float) -> None:
        self.batch_sizes.append(batch_size)
        self.queue_delays.append(queue_delay_ms)

    def get_metrics(self) -> Dict[str, Any]:
        """Performs percentile computations and gathers resource averages."""
        if not self.latencies:
            return {
                "count": 0,
                "throughput_rps": 0.0,
                "p50": 0.0,
                "p90": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "p999": 0.0,
                "cpu_avg": 0.0,
                "memory_avg_mb": 0.0,
                "gpu_avg": 0.0,
                "batch_size_avg": 0.0,
                "queue_delay_avg_ms": 0.0,
            }

        sorted_latencies = sorted(self.latencies)
        n = len(sorted_latencies)

        def get_percentile(p: float) -> float:
            idx = max(0, min(n - 1, int(math.ceil(n * p)) - 1))
            return sorted_latencies[idx]

        # Calculate throughput (RPS)
        duration = 1.0
        if len(self.timestamps) > 1:
            duration = max(0.001, self.timestamps[-1] - self.timestamps[0])
        throughput_rps = n / duration

        cpu_avg = sum(self.cpus) / len(self.cpus) if self.cpus else 0.0
        mem_avg = sum(self.memories) / len(self.memories) if self.memories else 0.0
        gpu_avg = sum(self.gpus) / len(self.gpus) if self.gpus else 0.0
        batch_avg = (
            sum(self.batch_sizes) / len(self.batch_sizes) if self.batch_sizes else 0.0
        )
        queue_avg = (
            sum(self.queue_delays) / len(self.queue_delays)
            if self.queue_delays
            else 0.0
        )

        return {
            "count": n,
            "throughput_rps": throughput_rps,
            "p50": get_percentile(0.50),
            "p90": get_percentile(0.90),
            "p95": get_percentile(0.95),
            "p99": get_percentile(0.99),
            "p999": get_percentile(0.999),
            "cpu_avg": cpu_avg,
            "memory_avg_mb": mem_avg,
            "gpu_avg": gpu_avg,
            "batch_size_avg": batch_avg,
            "queue_delay_avg_ms": queue_avg,
        }
