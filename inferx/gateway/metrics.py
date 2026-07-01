# inferx/gateway/metrics.py
"""
InferX Gateway Metrics.

Tracks active connections, latency distributions, and request counts.
"""
from typing import Any, Dict
import threading


class GatewayMetrics:
    """
    Thread-safe metrics collector capturing gateway connection states and latencies.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_connections = 0
        self._requests_count = 0
        self._total_latency_ns = 0

    def record_connection_start(self) -> None:
        """Increments active connections count."""
        with self._lock:
            self._active_connections += 1

    def record_connection_end(self) -> None:
        """Decrements active connections count."""
        with self._lock:
            if self._active_connections > 0:
                self._active_connections -= 1

    def record_request(self, latency_ns: int) -> None:
        """Increments request total count and updates accumulated latency."""
        with self._lock:
            self._requests_count += 1
            self._total_latency_ns += latency_ns

    def get_snapshot(self) -> dict[str, Any]:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            avg_lat_ms = (self._total_latency_ns / self._requests_count / 1e6) if self._requests_count > 0 else 0.0
            return {
                "active_connections": self._active_connections,
                "requests_total": self._requests_count,
                "average_request_latency_ms": avg_lat_ms
            }
