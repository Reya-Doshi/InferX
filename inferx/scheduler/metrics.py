# inferx/scheduler/metrics.py
"""
InferX Scheduler Metrics.

Tracks queue depths, task wait durations, and starvation alarms.
"""
import threading
from typing import Any


class SchedulerMetrics:
    """
    Central collector tracking scheduler stats.
    
    Provides thread-safe atomic counters for operational monitoring.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._enqueue_total = 0
        self._dequeue_total = 0
        self._starvation_warnings = 0
        self._total_wait_time_ns = 0

    def record_enqueue(self) -> None:
        """Increments total enqueued requests count."""
        with self._lock:
            self._enqueue_total += 1

    def record_dequeue(self, wait_time_ns: int) -> None:
        """Increments dequeued requests count and logs queue wait duration."""
        with self._lock:
            self._dequeue_total += 1
            self._total_wait_time_ns += wait_time_ns

    def record_starvation_warning(self) -> None:
        """Increments starvation warn triggers."""
        with self._lock:
            self._starvation_warnings += 1

    def get_snapshot(self) -> dict[str, Any]:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            avg_wait = (self._total_wait_time_ns / self._dequeue_total) if self._dequeue_total > 0 else 0
            return {
                "enqueue_total": self._enqueue_total,
                "dequeue_total": self._dequeue_total,
                "starvation_warnings": self._starvation_warnings,
                "average_wait_time_ms": avg_wait / 1_000_000.0
            }
