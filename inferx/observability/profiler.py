# inferx/observability/profiler.py
"""
InferX Request Timeline Profiler.

Captures request milestones and logs slow requests exceeding latency budgets.
"""
import time
from typing import Dict, List, Tuple

from inferx.utils.logging import get_logger

logger = get_logger("observability.profiler")


class ExecutionTimeline:
    """
    Tracks and breaks down latency budgets for a request's lifecycle.
    """
    def __init__(self, request_id: str, slow_request_threshold_ms: float = 100.0) -> None:
        self.request_id = request_id
        self.slow_threshold_ms = slow_request_threshold_ms
        self._milestones: List[Tuple[str, int]] = []
        self.record("start")

    def record(self, milestone_name: str) -> None:
        """Appends a timestamped milestone marker."""
        self._milestones.append((milestone_name, time.perf_counter_ns()))

    def get_breakdown(self) -> Dict[str, float]:
        """
        Calculates millisecond durations between consecutive milestones.
        
        Returns:
            A dictionary mapping 'stage1_to_stage2' -> duration_ms.
        """
        if len(self._milestones) < 2:
            return {}

        breakdown = {}
        for idx in range(len(self._milestones) - 1):
            stage_start, t_start = self._milestones[idx]
            stage_end, t_end = self._milestones[idx + 1]
            
            duration_ms = (t_end - t_start) / 1_000_000.0
            breakdown[f"{stage_start}_to_{stage_end}"] = duration_ms

        return breakdown

    def get_total_duration_ms(self) -> float:
        """Calculates total elapsed request duration in milliseconds."""
        if len(self._milestones) < 2:
            return 0.0
        start_ns = self._milestones[0][1]
        end_ns = self._milestones[-1][1]
        return (end_ns - start_ns) / 1_000_000.0

    def check_slow_request(self) -> bool:
        """
        Checks if total duration violates the latency budget.
        
        Logs a warning containing the breakdown metrics if slow.
        """
        duration = self.get_total_duration_ms()
        if duration > self.slow_threshold_ms:
            breakdown = self.get_breakdown()
            logger.warning(
                f"Slow Request Detected: {self.request_id} took {duration:.2f}ms (Threshold: {self.slow_threshold_ms}ms). "
                f"Breakdown: {breakdown}",
                request_id=self.request_id,
                duration_ms=duration,
                component="profiler"
            )
            return True
        return False
