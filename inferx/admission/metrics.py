# inferx/admission/metrics.py
"""
InferX Admission Metrics.

Tracks admission telemetry including accepts, rejects, latency, and status code counts.
"""
from typing import Any, Dict
import threading


class AdmissionMetrics:
    """
    Thread-safe metrics collector capturing admission event volumes and latencies.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._accepted = 0
        self._rejected = 0
        self._counts: Dict[int, int] = {}
        self._retry_afters = 0.0
        self._total_latency_ns = 0

    def record_decision(self, admitted: bool, status_code: int, latency_ns: int, retry_after: float = 0.0) -> None:
        """Records the decision metrics from an admission check."""
        with self._lock:
            if admitted:
                self._accepted += 1
            else:
                self._rejected += 1
            
            self._counts[status_code] = self._counts.get(status_code, 0) + 1
            self._retry_afters += retry_after
            self._total_latency_ns += latency_ns

    def get_snapshot(self) -> dict[str, Any]:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            total = self._accepted + self._rejected
            avg_lat_ms = (self._total_latency_ns / total / 1e6) if total > 0 else 0.0
            return {
                "accepted_total": self._accepted,
                "rejected_total": self._rejected,
                "status_codes": dict(self._counts),
                "total_retry_after_accumulated_sec": self._retry_afters,
                "average_decision_latency_ms": avg_lat_ms
            }
