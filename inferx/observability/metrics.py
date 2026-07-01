# inferx/observability/metrics.py
"""
InferX Prometheus Metrics Registry.

Implements thread-safe Counters, Gauges, and Histograms, exposing a native
export formatter compatible with Prometheus/Grafana scrapers.
"""
from typing import Dict, List, Optional, Tuple
import threading


def format_labels(labels: Optional[Dict[str, str]]) -> str:
    """Formats labels dictionary into a Prometheus label string bracket."""
    if not labels:
        return ""
    parts = [f'{k}="{v}"' for k, v in labels.items()]
    return "{" + ", ".join(parts) + "}"


class Counter:
    """Monotonically increasing cumulative metric representation."""
    def __init__(self, name: str, description: str) -> None:
        self.name = name
        self.description = description
        self._values: Dict[Tuple[Tuple[str, str], ...], float] = {}
        self._lock = threading.Lock()

    def inc(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Increments counter value for a given label set."""
        if value < 0:
            raise ValueError("Counter increments must be non-negative.")
        
        label_key = tuple(sorted(labels.items())) if labels else ()
        with self._lock:
            self._values[label_key] = self._values.get(label_key, 0.0) + value

    def get_lines(self) -> List[str]:
        """Returns Prometheus text lines for this counter."""
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} counter"
        ]
        with self._lock:
            for label_key, val in self._values.items():
                labels_dict = dict(label_key) if label_key else None
                lines.append(f"{self.name}{format_labels(labels_dict)} {float(val)}")
        return lines


class Gauge:
    """Metric representing a value that can arbitrarily go up and down."""
    def __init__(self, name: str, description: str) -> None:
        self.name = name
        self.description = description
        self._values: Dict[Tuple[Tuple[str, str], ...], float] = {}
        self._lock = threading.Lock()

    def set(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Sets gauge to a specific value."""
        label_key = tuple(sorted(labels.items())) if labels else ()
        with self._lock:
            self._values[label_key] = value

    def inc(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        label_key = tuple(sorted(labels.items())) if labels else ()
        with self._lock:
            self._values[label_key] = self._values.get(label_key, 0.0) + value

    def dec(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        label_key = tuple(sorted(labels.items())) if labels else ()
        with self._lock:
            self._values[label_key] = self._values.get(label_key, 0.0) - value

    def get_lines(self) -> List[str]:
        """Returns Prometheus text lines for this gauge."""
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} gauge"
        ]
        with self._lock:
            for label_key, val in self._values.items():
                labels_dict = dict(label_key) if label_key else None
                lines.append(f"{self.name}{format_labels(labels_dict)} {float(val)}")
        return lines


class Histogram:
    """Tracks value distribution occurrences inside discrete bucket bins."""
    def __init__(self, name: str, description: str, buckets: List[float]) -> None:
        self.name = name
        self.description = description
        self.buckets = sorted(buckets) + [float("inf")]
        
        # Maps labels tuple -> bucket_index -> count
        self._counts: Dict[Tuple[Tuple[str, str], ...], List[int]] = {}
        # Maps labels tuple -> total sum
        self._sums: Dict[Tuple[Tuple[str, str], ...], float] = {}
        self._lock = threading.Lock()

    def observe(self, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Records a value distribution observation."""
        label_key = tuple(sorted(labels.items())) if labels else ()
        
        with self._lock:
            if label_key not in self._counts:
                self._counts[label_key] = [0] * len(self.buckets)
                self._sums[label_key] = 0.0
            
            # Update sum
            self._sums[label_key] += value
            
            # Increment count for all matching buckets (values <= bucket boundary)
            for idx, bucket in enumerate(self.buckets):
                if value <= bucket:
                    self._counts[label_key][idx] += 1

    def get_lines(self) -> List[str]:
        """Returns Prometheus text lines for this histogram."""
        lines = [
            f"# HELP {self.name} {self.description}",
            f"# TYPE {self.name} histogram"
        ]
        
        with self._lock:
            for label_key, bucket_counts in self._counts.items():
                labels_dict = dict(label_key) if label_key else {}
                total_sum = self._sums[label_key]
                
                # Write bucket lines
                # Prometheus demands the bucket label: {le="bucket_value"}
                cumulative = 0
                for idx, bucket in enumerate(self.buckets):
                    count = bucket_counts[idx]
                    le_val = "inf" if bucket == float("inf") else str(bucket)
                    
                    # Merge labels
                    lbl = dict(labels_dict)
                    lbl["le"] = le_val
                    lines.append(f"{self.name}_bucket{format_labels(lbl)} {count}")
                
                # Write sum and count lines
                total_count = bucket_counts[-1]  # inf bucket contains total counts
                lines.append(f"{self.name}_sum{format_labels(labels_dict)} {total_sum}")
                lines.append(f"{self.name}_count{format_labels(labels_dict)} {total_count}")
                
        return lines


class MetricsRegistry:
    """
    Central catalog registering and formatting Prometheus metrics.
    """
    def __init__(self) -> None:
        self._metrics: Dict[str, Any] = {}
        self._lock = threading.Lock()

    def counter(self, name: str, description: str) -> Counter:
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = Counter(name, description)
            return self._metrics[name]

    def gauge(self, name: str, description: str) -> Gauge:
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = Gauge(name, description)
            return self._metrics[name]

    def histogram(self, name: str, description: str, buckets: List[float]) -> Histogram:
        with self._lock:
            if name not in self._metrics:
                self._metrics[name] = Histogram(name, description, buckets)
            return self._metrics[name]

    def export_prometheus(self) -> str:
        """Formats and flushes all registered metrics to Prometheus text blocks."""
        all_lines = []
        with self._lock:
            metrics_list = list(self._metrics.values())
            
        for metric in metrics_list:
            all_lines.extend(metric.get_lines())
            
        return "\n".join(all_lines) + "\n"
