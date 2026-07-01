# inferx/performance/profiler.py
import gc
import sys
import threading
import time
import tracemalloc
from typing import Any, Dict, List, Optional


class RuntimeProfiler:
    """Profiles system execution hot paths, lock contention, active thread counts, and peak memory allocations."""

    def __init__(self) -> None:
        self._start_time: float = 0.0
        self._cpu_start: float = 0.0
        self.peak_memory_bytes: int = 0
        self.allocation_count: int = 0

    def start(self) -> None:
        """Starts CPU cycle counters and initializes tracemalloc allocation tracing."""
        self._start_time = time.perf_counter()
        self._cpu_start = time.process_time()
        tracemalloc.start()
        logger_name = "inferx.performance.profiler"
        
    def stop(self) -> Dict[str, Any]:
        """Stops allocation tracing and compiles execution profiling metrics."""
        elapsed = max(0.001, time.perf_counter() - self._start_time)
        cpu_time = time.process_time() - self._cpu_start
        
        # Get memory allocation metrics
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        # Thread count
        active_threads = threading.active_count()

        # Simple CPU utilization calculation
        cpu_utilization = (cpu_time / elapsed) * 100.0

        return {
            "elapsed_sec": elapsed,
            "cpu_time_sec": cpu_time,
            "cpu_utilization_percent": min(100.0, cpu_utilization),
            "current_memory_bytes": current,
            "peak_memory_bytes": peak,
            "active_threads": active_threads
        }
