# inferx/event_bus/metrics.py
"""
InferX Event Bus Metrics Collector.

Instruments counters and gauges tracking message throughput, delivery rates,
and queue bottlenecks.
"""

from typing import Dict
import threading


class EventBusMetrics:
    """
    Centralized collector tracking event telemetry metrics.

    Provides thread-safe atomic counters for operational monitoring.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._published_counts: Dict[str, int] = {}
        self._delivered_counts: Dict[str, int] = {}
        self._failed_counts: Dict[str, int] = {}
        self._queue_depths: Dict[str, int] = {}

    def record_publish(self, event_type: str) -> None:
        """Increments event publish counter."""
        with self._lock:
            self._published_counts[event_type] = (
                self._published_counts.get(event_type, 0) + 1
            )

    def record_delivery(self, event_type: str) -> None:
        """Increments event delivery counter."""
        with self._lock:
            self._delivered_counts[event_type] = (
                self._delivered_counts.get(event_type, 0) + 1
            )

    def record_failure(self, event_type: str) -> None:
        """Increments event delivery failure counter."""
        with self._lock:
            self._failed_counts[event_type] = self._failed_counts.get(event_type, 0) + 1

    def update_queue_depth(self, sub_id: str, depth: int) -> None:
        """Updates queue depth gauge for a subscription."""
        with self._lock:
            self._queue_depths[sub_id] = depth

    def remove_queue_metrics(self, sub_id: str) -> None:
        """Cleans up queue depth tracking for terminated subscriptions."""
        with self._lock:
            self._queue_depths.pop(sub_id, None)

    def get_snapshot(self) -> dict[str, dict[str, int]]:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            return {
                "published": dict(self._published_counts),
                "delivered": dict(self._delivered_counts),
                "failed": dict(self._failed_counts),
                "queue_depths": dict(self._queue_depths),
            }
