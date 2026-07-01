# inferx/worker/metrics.py
"""
InferX Worker Metrics.

Tracks worker process lifetimes, telemetry checks, VRAM levels, and execution durations.
"""

from typing import Any, Dict
import threading


class WorkerMetrics:
    """
    Thread-safe metrics collector capturing worker process health and telemetry.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._restarts = 0
        self._executions_count = 0
        self._total_execution_time_ns = 0
        self._last_heartbeat_delays: Dict[str, float] = {}

    def record_restart(self) -> None:
        """Increments restart counter."""
        with self._lock:
            self._restarts += 1

    def record_execution(self, duration_ns: int) -> None:
        """Records execution latency details."""
        with self._lock:
            self._executions_count += 1
            self._total_execution_time_ns += duration_ns

    def record_heartbeat_delay(self, worker_id: str, delay_sec: float) -> None:
        """Updates the recorded heartbeat delay for a specific worker."""
        with self._lock:
            self._last_heartbeat_delays[worker_id] = delay_sec

    def get_snapshot(self) -> dict[str, Any]:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            avg_exec = (
                (self._total_execution_time_ns / self._executions_count / 1e6)
                if self._executions_count > 0
                else 0.0
            )
            return {
                "restart_total": self._restarts,
                "executions_total": self._executions_count,
                "average_execution_latency_ms": avg_exec,
                "last_heartbeat_delays_sec": dict(self._last_heartbeat_delays),
            }
