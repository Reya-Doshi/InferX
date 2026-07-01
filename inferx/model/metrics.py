# inferx/model/metrics.py
"""
InferX Model Metrics.

Tracks model inference latencies, loading times, token counts, and VRAM utilization.
"""
from typing import Any
import threading


class ModelMetrics:
    """
    Thread-safe metrics collector capturing model execution statistics.
    """
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inferences_count = 0
        self._total_inference_time_ns = 0
        self._total_input_tokens = 0
        self._total_output_tokens = 0
        self._model_load_durations: dict[str, float] = {}

    def record_inference(self, input_tokens: int, output_tokens: int, duration_ns: int) -> None:
        """Records inference event latency and token sizes."""
        with self._lock:
            self._inferences_count += 1
            self._total_inference_time_ns += duration_ns
            self._total_input_tokens += input_tokens
            self._total_output_tokens += output_tokens

    def record_load_duration(self, model_key: str, duration_sec: float) -> None:
        """Records model load time stats."""
        with self._lock:
            self._model_load_durations[model_key] = duration_sec

    def get_snapshot(self) -> dict[str, Any]:
        """Returns a snapshot copy of current metrics values."""
        with self._lock:
            total_tokens = self._total_input_tokens + self._total_output_tokens
            duration_sec = self._total_inference_time_ns / 1e9
            throughput = (total_tokens / duration_sec) if duration_sec > 0 else 0.0
            avg_inference_ms = (self._total_inference_time_ns / self._inferences_count / 1e6) if self._inferences_count > 0 else 0.0
            
            return {
                "inferences_total": self._inferences_count,
                "input_tokens_total": self._total_input_tokens,
                "output_tokens_total": self._total_output_tokens,
                "average_inference_latency_ms": avg_inference_ms,
                "token_throughput_per_sec": throughput,
                "model_load_durations_sec": dict(self._model_load_durations)
            }
