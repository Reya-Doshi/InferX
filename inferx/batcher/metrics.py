# inferx/batcher/metrics.py
"""
InferX Batcher Metrics.

Tracks batch sizes, padding efficiencies, and batch splits and merges.
"""

import threading


class BatcherMetrics:
    """
    Central collector tracking batch operations.

    Provides thread-safe atomic counters for operational monitoring.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._batches_flushed = 0
        self._total_requests_batched = 0
        self._splits = 0
        self._merges = 0

        # Tracking token sizes for padding efficiency
        self._actual_tokens_total = 0
        self._padded_tokens_total = 0

    def record_batch(
        self, batch_size: int, actual_tokens: int, padded_tokens: int
    ) -> None:
        """Records details of a completed batch flush event."""
        with self._lock:
            self._batches_flushed += 1
            self._total_requests_batched += batch_size
            self._actual_tokens_total += actual_tokens
            self._padded_tokens_total += padded_tokens

    def record_split(self) -> None:
        """Increments batch split counter."""
        with self._lock:
            self._splits += 1

    def record_merge(self) -> None:
        """Increments batch merge counter."""
        with self._lock:
            self._merges += 1

    def get_snapshot(self) -> dict:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            efficiency = (
                (self._actual_tokens_total / self._padded_tokens_total)
                if self._padded_tokens_total > 0
                else 1.0
            )
            avg_batch = (
                (self._total_requests_batched / self._batches_flushed)
                if self._batches_flushed > 0
                else 0.0
            )
            return {
                "batches_flushed": self._batches_flushed,
                "average_batch_size": avg_batch,
                "padding_efficiency": efficiency,
                "batch_splits": self._splits,
                "batch_merges": self._merges,
            }
